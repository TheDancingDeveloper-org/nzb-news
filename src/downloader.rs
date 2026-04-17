//! Top-level orchestrator — persistent-worker model.
//!
//! Two kinds of long-lived task coordinate dispatch:
//!
//! - **Scheduler task** (`scheduler_loop`): owns the pending queue, routes
//!   each article to the appropriate server-local queue using
//!   [`select_server`] + cascade-reset logic, and handles per-article
//!   outcomes (success → emit, transient fail → re-queue, give-up → emit
//!   failed). Exactly one per `Downloader`.
//!
//! - **Wrapper worker task** (`wrapper_worker`): one per [`NewsWrapper`].
//!   Each owns exactly one NNTP connection for its lifetime. Continuously
//!   pulls up to `pipeline` articles from its server's queue, pipelines
//!   them on the connection, reads responses, and emits per-item results
//!   back to the scheduler. The crucial property: the pipeline is kept
//!   saturated — the worker doesn't wait for a batch to finish before
//!   pulling more work, and it never goes idle if its server queue has
//!   items waiting.
//!
//! This is the model that matches classic Usenet downloaders for raw
//! throughput: the pipeline-fill effectively masks per-article RTT across
//! every connection simultaneously.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use nzb_nntp::config::ServerConfig;
use nzb_nntp::error::NntpError;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{debug, info, trace, warn};

use crate::article::{Article, NzbFile, NzbObject};
use crate::dispatch::{Selection, cascade_reset, record_article_given_up, select_server};
use crate::news_wrapper::NewsWrapper;
use crate::penalty::{PenaltyAction, penalty_for_error};
use crate::server::Server;

/// Opaque identifier paired with an article through the dispatcher so the
/// caller can match outcomes back to their scheduling context. Callers
/// typically use the article's DB primary key; the driver treats it as
/// opaque data.
pub type WorkTag = u64;

/// One unit of work submitted to the driver.
#[derive(Clone)]
pub struct WorkItem {
    pub tag: WorkTag,
    pub article: Arc<Article>,
    pub file: Arc<NzbFile>,
    pub job: Arc<NzbObject>,
}

impl std::fmt::Debug for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("tag", &self.tag)
            .field("message_id", &self.article.message_id)
            .field("file_id", &self.article.file_id)
            .field("job_id", &self.article.job_id)
            .finish()
    }
}

/// Outcome emitted by the driver on the `outcome_tx` channel for every
/// terminal resolution of a work item.
#[derive(Debug)]
pub enum FetchOutcome {
    /// Article fetched successfully. Caller assembles the bytes.
    Success {
        tag: WorkTag,
        server_id: String,
        bytes: Vec<u8>,
        article_bytes: u64,
    },
    /// Article could not be fetched from any server; given up on. Caller
    /// may decide to attempt par2 repair downstream.
    Failed {
        tag: WorkTag,
        last_error: Option<String>,
    },
    /// The driver is shutting down and this article will not be attempted
    /// further. Emitted during graceful shutdown; caller should treat as
    /// a transient failure (not permanent).
    Cancelled { tag: WorkTag },
}

/// Policy for probing backup servers before committing all missing articles
/// to them.
///
/// When an article cascades past the highest-priority server, the dispatcher
/// sends a small sample of missing articles as "probes" to the next-priority
/// server instead of immediately routing the full backlog. Only if the probe
/// hit-rate meets the threshold is the server approved for all remaining
/// cascade articles. If not, the server is rejected for this job and remaining
/// articles cascade further or fail fast.
///
/// This avoids the O(N × servers) cascade cost for fail-heavy NZBs where
/// backup servers are equally unhelpful (e.g. DMCA-removed or out-of-retention
/// content that no provider carries).
#[derive(Clone, Debug)]
pub struct ServerProbePolicy {
    /// How many articles to probe before deciding whether a backup server is
    /// useful for this job. Default: `10`.
    pub probe_count: u32,
    /// Minimum percentage (0–100) of probed articles that must succeed for
    /// the server to be approved. If the hit-rate falls below this, the server
    /// is rejected for this job and the remaining cascade articles skip it.
    /// Default: `10.0` (at least 1 out of every 10 probes must succeed).
    pub min_hit_rate_pct: f32,
}

impl Default for ServerProbePolicy {
    fn default() -> Self {
        Self {
            probe_count: 10,
            min_hit_rate_pct: 10.0,
        }
    }
}

/// Configuration for a new downloader.
#[derive(Clone)]
pub struct DownloaderConfig {
    /// Servers to dispatch against. Priority order: lowest `priority` first.
    pub servers: Vec<ServerConfig>,
    /// Cap on total concurrent fetches (wrappers × pipeline depth). A
    /// reasonable default is `sum(server.connections) * pipelining`, but
    /// the persistent-worker model self-regulates via per-server queue
    /// depth so this is mostly a safety net. Currently unused by the
    /// worker model; retained for API stability.
    pub max_concurrent_fetches: usize,
    /// Per-article timeout. Bounds how long a single `fetch_body` call
    /// may take before the batch is cancelled and its items re-queued
    /// onto other servers.
    pub article_timeout: Duration,
    /// Bounded capacity of the incoming work channel.
    pub work_channel_capacity: usize,
    /// Bounded capacity of the outgoing outcome channel. When the
    /// assembler is slower than the driver, the channel fills and
    /// `outcome_tx.send().await` blocks the scheduler.
    pub outcome_channel_capacity: usize,
    /// Optional backup-server probe policy. `None` disables probing and
    /// all missing articles cascade to every server unconditionally.
    /// Defaults to `Some(ServerProbePolicy::default())`.
    pub probe_policy: Option<ServerProbePolicy>,
}

impl DownloaderConfig {
    pub fn from_servers(servers: Vec<ServerConfig>) -> Self {
        let total_conns: usize = servers.iter().map(|s| s.connections as usize).sum();
        let max_concurrent_fetches = total_conns.max(4);
        Self {
            servers,
            max_concurrent_fetches,
            article_timeout: Duration::from_secs(60),
            work_channel_capacity: 4096,
            outcome_channel_capacity: 4096,
            probe_policy: Some(ServerProbePolicy::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// ServerQueue — per-server MPMC-ish queue (one producer: scheduler; many
// consumers: wrapper workers for that server). We roll our own rather
// than use a third-party multi-consumer channel — the semantics we need
// are specific (pop up to N items atomically, notify, close).
// ---------------------------------------------------------------------------
struct ServerQueue {
    server: Arc<Server>,
    deque: Mutex<VecDeque<WorkItem>>,
    notify: Notify,
    closed: AtomicBool,
    inflight: AtomicUsize,
}

impl ServerQueue {
    fn new(server: Arc<Server>) -> Arc<Self> {
        Arc::new(Self {
            server,
            deque: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            inflight: AtomicUsize::new(0),
        })
    }

    /// Batched push: enqueue `items` in a single mutex cycle. Much
    /// cheaper than N individual `push` calls when the scheduler has
    /// drained many items from `work_rx` in one burst, and critically
    /// produces a single `notify_waiters` pulse so all parked workers
    /// wake once and race for the new items (rather than thrashing on
    /// N back-to-back notifications).
    async fn push_many(&self, items: Vec<WorkItem>) {
        if items.is_empty() {
            return;
        }
        {
            let mut q = self.deque.lock().await;
            q.extend(items);
        }
        self.notify.notify_waiters();
    }

    /// Block until there's at least one item or the queue is closed. On
    /// return, pop up to `max` items atomically. Empty vec → closed.
    async fn pop_batch(&self, max: usize) -> Vec<WorkItem> {
        loop {
            {
                let mut q = self.deque.lock().await;
                if !q.is_empty() {
                    let take = q.len().min(max);
                    let batch: Vec<WorkItem> = q.drain(..take).collect();
                    self.inflight.fetch_add(batch.len(), Ordering::Relaxed);
                    return batch;
                }
            }
            if self.closed.load(Ordering::Acquire) {
                return Vec::new();
            }
            // Wait for push() or close.
            self.notify.notified().await;
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Drain every remaining item from the queue. Intended for the
    /// last-wrapper-retirement path so stranded items can be rerouted.
    async fn drain_all(&self) -> Vec<WorkItem> {
        let mut q = self.deque.lock().await;
        q.drain(..).collect()
    }

    /// Remove items matching `job_id` and return them to the caller. Used
    /// by `PurgeJob` so a cancelled/aborted job's queued-but-not-yet-fetched
    /// articles don't continue consuming connection slots that healthy jobs
    /// could be using.
    async fn drain_job(&self, job_id: &str) -> Vec<WorkItem> {
        let mut q = self.deque.lock().await;
        let mut removed = Vec::new();
        q.retain(|item| {
            if item.article.job_id == job_id {
                removed.push(item.clone());
                false
            } else {
                true
            }
        });
        removed
    }

    fn note_completed(&self, n: usize) {
        self.inflight.fetch_sub(n, Ordering::Relaxed);
    }

    fn depth(&self) -> usize {
        // Approximate — may race with pop_batch, but good enough for
        // scheduler-side rebalancing decisions.
        self.inflight.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Scheduler → Wrapper worker messaging: wrappers return per-item results
// to the scheduler via this channel so the scheduler can centralize
// re-queue decisions, penalty application, and outcome emission.
// ---------------------------------------------------------------------------
enum SchedulerMsg {
    FetchResult {
        item: WorkItem,
        server: Arc<Server>,
        result: Result<Vec<u8>, NntpError>,
    },
    /// Wrapper worker hit a fatal error on connect or mid-batch; items
    /// that hadn't been attempted yet get returned to the scheduler so
    /// they can be re-queued on other servers.
    ReturnUnattempted {
        items: Vec<WorkItem>,
        server: Arc<Server>,
        error: String,
    },
}

/// Out-of-band commands from the caller (distinct from worker-result
/// traffic on `SchedulerMsg`). Kept on a separate channel so the
/// scheduler can still use "all wrapper-worker senders dropped" to
/// detect pool shutdown — the control sender lives on the public
/// `DownloaderHandle`, independent of the worker fleet.
#[allow(clippy::enum_variant_names)] // the "Job" suffix reads naturally for each variant
enum ControlMsg {
    /// Pause dispatching for a job. Articles for `job_id` stay in the
    /// scheduler's pending list but are not routed to any server queue
    /// until a matching `ResumeJob` arrives. Articles already handed to
    /// a worker (in `server_queues[idx]` or mid-fetch) complete normally.
    PauseJob { job_id: String },
    /// Reverse of `PauseJob` — resume normal dispatching for this job.
    ResumeJob { job_id: String },
    /// Drop all scheduler-level state for a job: remove pending articles,
    /// clear probe state, forget paused state. Used on cancel/abort so
    /// nothing continues to route after the caller has given up.
    PurgeJob { job_id: String },
}

// ---------------------------------------------------------------------------
// Server-probe tracking — lives inside the scheduler task only.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum ProbeStatus {
    /// Probes are still in flight (sent < probe_count, or waiting for returns).
    Probing,
    /// Hit-rate met the threshold — server approved for remaining cascade work.
    Approved,
    /// Hit-rate missed the threshold — server skipped for this job.
    Rejected,
}

#[derive(Debug)]
struct ProbeState {
    probes_sent: u32,
    probes_returned: u32,
    probes_hit: u32,
    status: ProbeStatus,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            probes_sent: 0,
            probes_returned: 0,
            probes_hit: 0,
            status: ProbeStatus::Probing,
        }
    }

    fn evaluate(&mut self, policy: &ServerProbePolicy) {
        let rate = if self.probes_sent == 0 {
            0.0
        } else {
            self.probes_hit as f32 / self.probes_sent as f32 * 100.0
        };
        self.status = if rate >= policy.min_hit_rate_pct {
            ProbeStatus::Approved
        } else {
            ProbeStatus::Rejected
        };
    }
}

// ---------------------------------------------------------------------------
// DownloaderHandle
// ---------------------------------------------------------------------------

pub struct DownloaderHandle {
    work_tx: mpsc::Sender<WorkItem>,
    control_tx: mpsc::Sender<ControlMsg>,
    /// Shared view of the server pool — same `Arc<Server>` instances the
    /// scheduler uses, so `server_stats_snapshot` reads the same atomics
    /// that per-fetch handling writes.
    servers: Arc<Vec<Arc<crate::server::Server>>>,
    shutdown: Arc<Notify>,
    driver_task: Option<tokio::task::JoinHandle<()>>,
}

impl DownloaderHandle {
    pub async fn submit(&self, item: WorkItem) -> Result<(), mpsc::error::SendError<WorkItem>> {
        self.work_tx.send(item).await
    }

    pub fn try_submit(&self, item: WorkItem) -> Result<(), mpsc::error::TrySendError<WorkItem>> {
        self.work_tx.try_send(item)
    }

    pub fn sender(&self) -> mpsc::Sender<WorkItem> {
        self.work_tx.clone()
    }

    /// Pause dispatching for `job_id`. Articles stay in the scheduler's
    /// pending list. In-flight fetches complete normally; the change is
    /// observable within ~tens of ms. Idempotent — safe to call repeatedly.
    pub fn pause_job(&self, job_id: &str) {
        let _ = self.control_tx.try_send(ControlMsg::PauseJob {
            job_id: job_id.to_string(),
        });
    }

    /// Reverse of [`pause_job`]. Idempotent — calling on a non-paused job
    /// is a no-op.
    pub fn resume_job(&self, job_id: &str) {
        let _ = self.control_tx.try_send(ControlMsg::ResumeJob {
            job_id: job_id.to_string(),
        });
    }

    /// Drop all scheduler-level state for this job: remove pending articles,
    /// clear probe state, drop the pause flag. Used on cancel/abort so work
    /// already accepted into nzb-news stops flowing.
    pub fn purge_job(&self, job_id: &str) {
        let _ = self.control_tx.try_send(ControlMsg::PurgeJob {
            job_id: job_id.to_string(),
        });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Snapshot of per-server lifetime attempt counters. Returns one entry
    /// per configured server as `(server_id, stats)`. Cheap — just atomic
    /// reads — safe to call inline on hot paths like abort logging.
    pub fn server_stats_snapshot(&self) -> Vec<(String, crate::server::ServerStats)> {
        self.servers
            .iter()
            .map(|s| (s.id().to_string(), s.stats()))
            .collect()
    }

    pub async fn join(mut self) {
        if let Some(t) = self.driver_task.take() {
            let _ = t.await;
        }
    }
}

/// Spawn a downloader driver. Returns a handle + the outcome receiver.
pub fn spawn_downloader(
    config: DownloaderConfig,
) -> (DownloaderHandle, mpsc::Receiver<FetchOutcome>) {
    let (work_tx, work_rx) = mpsc::channel::<WorkItem>(config.work_channel_capacity.max(1));
    let (outcome_tx, outcome_rx) =
        mpsc::channel::<FetchOutcome>(config.outcome_channel_capacity.max(1));
    let shutdown = Arc::new(Notify::new());
    let probe_policy = config.probe_policy;

    // Build Server objects sorted by priority.
    let mut servers: Vec<Arc<Server>> = config
        .servers
        .into_iter()
        .map(|cfg| {
            let connections = cfg.connections;
            let s = Arc::new(Server::new(cfg));
            s.prime_wrapper_pool(connections);
            s
        })
        .collect();
    servers.sort_by_key(|s| s.priority());

    // One queue per server + one long-lived wrapper worker per wrapper.
    let server_queues: Vec<Arc<ServerQueue>> = servers
        .iter()
        .map(|s| ServerQueue::new(s.clone()))
        .collect();

    let (scheduler_tx, scheduler_rx) =
        mpsc::channel::<SchedulerMsg>(config.work_channel_capacity.max(1));
    // Control channel — user-facing pause/resume/purge. Small buffer: these
    // are infrequent (API-driven) and should never back up.
    let (control_tx, control_rx) = mpsc::channel::<ControlMsg>(64);

    // Spawn wrapper worker tasks. Each owns one NewsWrapper for its
    // lifetime; the Server's idle/busy sets aren't used by this path
    // (they remain the abstraction for ad-hoc callers, but persistent
    // workers hold the wrapper the whole time).
    let mut worker_handles = Vec::new();
    for (s, q) in servers.iter().zip(server_queues.iter()) {
        let conns = s.config().connections.max(1);
        for worker_id in 1..=conns {
            let wrapper = NewsWrapper::new(s.id().to_string(), worker_id as u32);
            let server = s.clone();
            let queue = q.clone();
            let scheduler_tx = scheduler_tx.clone();
            let shutdown = shutdown.clone();
            let article_timeout = config.article_timeout;
            // Register before spawn so a race where the worker exits
            // immediately still decrements from a non-zero baseline.
            server.register_wrapper();
            worker_handles.push(tokio::spawn(wrapper_worker(
                wrapper,
                server,
                queue,
                scheduler_tx,
                shutdown,
                article_timeout,
            )));
        }
    }

    // Drop the scheduler's own cloneable sender — the wrapper workers
    // own the only senders now, so the scheduler can detect "all workers
    // exited" via `scheduler_rx.recv() -> None` (once they all drop).

    // Shared server handle — same Arc<Server>s fed into the scheduler, so
    // the scheduler's fetch-result handling and `DownloaderHandle::server_stats_snapshot`
    // observe the same atomic counters.
    let servers_shared: Arc<Vec<Arc<Server>>> = Arc::new(servers.clone());

    let scheduler_shutdown = shutdown.clone();
    let task = tokio::spawn(scheduler_loop(
        servers,
        server_queues,
        work_rx,
        scheduler_rx,
        control_rx,
        outcome_tx,
        scheduler_shutdown,
        worker_handles,
        probe_policy,
    ));

    (
        DownloaderHandle {
            work_tx,
            control_tx,
            servers: servers_shared,
            shutdown,
            driver_task: Some(task),
        },
        outcome_rx,
    )
}

// ---------------------------------------------------------------------------
// Scheduler loop — routes pending articles to server queues and consumes
// wrapper-worker results.
// ---------------------------------------------------------------------------
#[allow(clippy::too_many_arguments)]
async fn scheduler_loop(
    servers: Vec<Arc<Server>>,
    server_queues: Vec<Arc<ServerQueue>>,
    mut work_rx: mpsc::Receiver<WorkItem>,
    mut scheduler_rx: mpsc::Receiver<SchedulerMsg>,
    mut control_rx: mpsc::Receiver<ControlMsg>,
    outcome_tx: mpsc::Sender<FetchOutcome>,
    shutdown: Arc<Notify>,
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
    probe_policy: Option<ServerProbePolicy>,
) {
    info!(
        servers = servers.len(),
        workers = worker_handles.len(),
        "downloader scheduler starting"
    );

    let mut pending: Vec<WorkItem> = Vec::new();
    let mut next_dispatch_retry: Option<tokio::time::Instant> = None;

    // Per-(job, server) probe state. Keyed by (job_id, server_id).
    let mut probe_tracker: HashMap<(String, String), ProbeState> = HashMap::new();
    // Tags of in-flight probe articles.
    let mut probe_tags: HashSet<WorkTag> = HashSet::new();
    // Jobs for which dispatch is paused. Articles for these job_ids stay
    // in `pending` but aren't routed to any server queue. Articles already
    // handed off (in `server_queues[idx]` or mid-fetch) complete normally.
    let mut paused_jobs: HashSet<String> = HashSet::new();

    // Rough soft cap on per-server queue depth: (connections × pipelining × 2).
    // Prevents one server's queue from absorbing all pending work when
    // another server is blocked. Revisit if this turns out to matter.
    let per_server_cap: Vec<usize> = server_queues
        .iter()
        .map(|q| {
            let cfg = q.server.config();
            (cfg.connections as usize * cfg.pipelining.max(1) as usize * 2).max(8)
        })
        .collect();

    loop {
        // Try to drain pending into per-server queues.
        dispatch_pending(
            &servers,
            &server_queues,
            &per_server_cap,
            &mut pending,
            &mut next_dispatch_retry,
            &outcome_tx,
            probe_policy.as_ref(),
            &mut probe_tracker,
            &mut probe_tags,
            &paused_jobs,
        )
        .await;

        let dispatch_retry_sleep = next_dispatch_retry.map(|t| {
            let rem = t.saturating_duration_since(tokio::time::Instant::now());
            rem.max(Duration::from_millis(1))
        });

        tokio::select! {
            biased;

            _ = shutdown.notified() => {
                info!("scheduler: shutdown requested");
                break;
            }

            maybe_item = work_rx.recv() => {
                match maybe_item {
                    Some(item) => {
                        pending.push(item);
                        // Drain any additional items already queued on
                        // the work channel so we form batches large
                        // enough to saturate per-server pipelines. Without
                        // this, each `select!` tick only promotes one
                        // item, which starves the pipeline.
                        while let Ok(extra) = work_rx.try_recv() {
                            pending.push(extra);
                        }
                    }
                    None => {
                        debug!("work channel closed");
                        if pending.is_empty() {
                            break;
                        }
                    }
                }
            }

            maybe_msg = scheduler_rx.recv() => {
                match maybe_msg {
                    Some(msg) => {
                        handle_scheduler_msg(msg, &servers, &mut pending, &outcome_tx, probe_policy.as_ref(), &mut probe_tracker, &mut probe_tags).await;
                        // Drain burst: wrappers now emit one FetchResult
                        // per article response (streaming), so a batch of
                        // N in-flight articles produces N scheduler msgs in
                        // quick succession. Without draining, dispatch_pending
                        // (O(pending × servers)) would run once per message.
                        // Consuming the whole burst here means one O(N) pass
                        // serves all of them.
                        while let Ok(extra) = scheduler_rx.try_recv() {
                            handle_scheduler_msg(extra, &servers, &mut pending, &outcome_tx, probe_policy.as_ref(), &mut probe_tracker, &mut probe_tags).await;
                        }
                    }
                    None => {
                        // All wrapper workers have exited.
                        break;
                    }
                }
            }

            maybe_ctrl = control_rx.recv() => {
                match maybe_ctrl {
                    Some(ControlMsg::PauseJob { job_id }) => {
                        if paused_jobs.insert(job_id.clone()) {
                            debug!(job_id = %job_id, "scheduler: pause");
                        }
                    }
                    Some(ControlMsg::ResumeJob { job_id }) => {
                        if paused_jobs.remove(&job_id) {
                            debug!(job_id = %job_id, "scheduler: resume");
                            // Articles for this job sat idle in `pending`;
                            // clear any retry backoff so they route on the
                            // next iteration.
                            next_dispatch_retry = None;
                        }
                    }
                    Some(ControlMsg::PurgeJob { job_id }) => {
                        // Remove pending articles for the job and emit
                        // Cancelled outcomes so upstream accounting closes
                        // them out. Also clear any probe state / pause flag
                        // tied to this job, and rip items out of each
                        // server queue so wrapper workers don't keep
                        // fetching ghost articles that would consume slots
                        // healthy jobs could use.
                        let before = pending.len();
                        let mut kept: Vec<WorkItem> = Vec::with_capacity(pending.len());
                        for item in pending.drain(..) {
                            if item.article.job_id == job_id {
                                let _ = outcome_tx
                                    .send(FetchOutcome::Cancelled { tag: item.tag })
                                    .await;
                            } else {
                                kept.push(item);
                            }
                        }
                        pending = kept;
                        let mut q_drained = 0usize;
                        for q in &server_queues {
                            let items = q.drain_job(&job_id).await;
                            q_drained += items.len();
                            for it in items {
                                let _ = outcome_tx
                                    .send(FetchOutcome::Cancelled { tag: it.tag })
                                    .await;
                            }
                        }
                        probe_tracker.retain(|(j, _), _| j != &job_id);
                        probe_tags.retain(|_tag| {
                            // Probe tags don't carry job_id directly; leave
                            // them — they'll be resolved to Cancelled/ignored
                            // on outcome by the dispatcher.
                            true
                        });
                        paused_jobs.remove(&job_id);
                        debug!(
                            job_id = %job_id,
                            purged_pending = before - pending.len(),
                            purged_queues = q_drained,
                            "scheduler: purge"
                        );
                    }
                    None => {
                        // Handle dropped — shouldn't happen while the
                        // downloader is running, but tolerate it.
                    }
                }
            }

            _ = async {
                if let Some(d) = dispatch_retry_sleep {
                    tokio::time::sleep(d).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                next_dispatch_retry = None;
                continue;
            }
        }
    }

    // Close every server queue so wrapper workers can wind down.
    for q in &server_queues {
        q.close();
    }

    // Emit Cancelled outcomes for anything still pending.
    for item in pending.drain(..) {
        let _ = outcome_tx
            .send(FetchOutcome::Cancelled { tag: item.tag })
            .await;
    }

    // Drain any remaining scheduler_rx messages so workers exiting after
    // the close signal can still emit their final results.
    while let Some(msg) = scheduler_rx.recv().await {
        handle_scheduler_msg(
            msg,
            &servers,
            &mut Vec::new(),
            &outcome_tx,
            probe_policy.as_ref(),
            &mut probe_tracker,
            &mut probe_tags,
        )
        .await;
    }

    // Wait for every wrapper worker to exit.
    for h in worker_handles {
        let _ = h.await;
    }

    info!("downloader scheduler exiting");
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_pending(
    servers: &[Arc<Server>],
    server_queues: &[Arc<ServerQueue>],
    per_server_cap: &[usize],
    pending: &mut Vec<WorkItem>,
    next_dispatch_retry: &mut Option<tokio::time::Instant>,
    outcome_tx: &mpsc::Sender<FetchOutcome>,
    probe_policy: Option<&ServerProbePolicy>,
    probe_tracker: &mut HashMap<(String, String), ProbeState>,
    probe_tags: &mut HashSet<WorkTag>,
    paused_jobs: &HashSet<String>,
) {
    // First pass: group items by target-server index. Items that select
    // another selection path (WaitRampup, CascadeReset, GiveUp) are
    // handled inline but the route-to-server path accumulates batches so
    // we can push per-server in a single mutex-locked call.
    let mut per_server: Vec<Vec<WorkItem>> = (0..servers.len()).map(|_| Vec::new()).collect();
    let mut give_ups: Vec<WorkItem> = Vec::new();

    let mut i = 0;
    while i < pending.len() {
        // Pause gate: leave paused-job articles in pending without routing.
        // `i += 1` here (not `continue` without increment) because the item
        // itself isn't changing — only a control command can unpause it,
        // and that command bumps the scheduler loop, causing a re-entry
        // that will visit the article again.
        if !paused_jobs.is_empty() && paused_jobs.contains(&pending[i].article.job_id) {
            i += 1;
            continue;
        }
        let sel = {
            let item = &pending[i];
            select_server(&item.article, &item.file, &item.job, servers)
        };
        match sel {
            Selection::Server(target) => {
                let idx = match servers.iter().position(|s| Arc::ptr_eq(s, target)) {
                    Some(x) => x,
                    None => {
                        i += 1;
                        continue;
                    }
                };

                // Probe gate: for cascade articles (already tried ≥ 1 server) sample
                // a small batch before committing the full backlog to the backup server.
                let mut is_probe = false;
                if let Some(policy) = probe_policy
                    && !pending[i].article.try_list().is_empty()
                {
                    let key = (pending[i].article.job_id.clone(), target.id().to_string());
                    let state = probe_tracker.entry(key).or_insert_with(ProbeState::new);
                    match state.status {
                        ProbeStatus::Rejected => {
                            // Pre-mark this server as tried so select_server routes
                            // to the next tier on the very next loop iteration.
                            pending[i].article.mark_server_tried(target.id());
                            // Don't increment i — re-dispatch with updated try_list.
                            continue;
                        }
                        ProbeStatus::Approved => {
                            // Server proved useful for this job; fall through.
                        }
                        ProbeStatus::Probing => {
                            if state.probes_sent < policy.probe_count {
                                state.probes_sent += 1;
                                is_probe = true;
                            } else {
                                // All probes dispatched; waiting for results before
                                // routing the remaining backlog.
                                bump_retry(next_dispatch_retry, Duration::from_millis(50));
                                i += 1;
                                continue;
                            }
                        }
                    }
                }

                let q = &server_queues[idx];
                // Cap: check depth + items we're about to push this pass.
                if q.depth() + per_server[idx].len() >= per_server_cap[idx] {
                    bump_retry(next_dispatch_retry, Duration::from_millis(25));
                    i += 1;
                    continue;
                }
                let item = pending.swap_remove(i);
                item.article.set_fetcher_priority(target.priority());
                if is_probe {
                    probe_tags.insert(item.tag);
                }
                per_server[idx].push(item);
            }
            Selection::WaitRampup(d) => {
                bump_retry(next_dispatch_retry, d);
                i += 1;
            }
            Selection::CascadeReset => {
                let item = &pending[i];
                cascade_reset(&item.article, &item.file, &item.job);
            }
            Selection::GiveUp => {
                let item = pending.swap_remove(i);
                record_article_given_up(&item.article, &item.file, &item.job);
                give_ups.push(item);
            }
        }
    }

    // Second pass: one mutex-locked enqueue per server. A single
    // notify_waiters fires for all items pushed in this batch, so parked
    // wrapper workers wake once and compete for the new batch rather
    // than being notified N times back-to-back.
    for (idx, items) in per_server.into_iter().enumerate() {
        server_queues[idx].push_many(items).await;
    }

    // Emit Failed outcomes for any articles that were given up on.
    for item in give_ups {
        let err_msg = format!("all servers exhausted for {}", item.article.message_id);
        trace!(tag = item.tag, "article given up at dispatch-time");
        let _ = outcome_tx
            .send(FetchOutcome::Failed {
                tag: item.tag,
                last_error: Some(err_msg),
            })
            .await;
    }
}

fn bump_retry(slot: &mut Option<tokio::time::Instant>, d: Duration) {
    let candidate = tokio::time::Instant::now() + d;
    *slot = Some(match *slot {
        Some(existing) if existing < candidate => existing,
        _ => candidate,
    });
}

// ---------------------------------------------------------------------------
// Probe result accounting.
// ---------------------------------------------------------------------------

/// Called for every article result (success or error). If the article was
/// a probe, update the probe state and evaluate once all probes have returned.
fn update_probe_on_result(
    tag: WorkTag,
    job_id: &str,
    server_id: &str,
    success: bool,
    probe_tags: &mut HashSet<WorkTag>,
    probe_tracker: &mut HashMap<(String, String), ProbeState>,
    policy: &ServerProbePolicy,
) {
    if !probe_tags.remove(&tag) {
        return; // not a probe article
    }
    let key = (job_id.to_string(), server_id.to_string());
    if let Some(state) = probe_tracker.get_mut(&key) {
        state.probes_returned += 1;
        if success {
            state.probes_hit += 1;
        }
        // Only evaluate once the full probe batch has returned. Evaluating
        // earlier (e.g. the moment `returned == sent` while more are still
        // to be dispatched) rejects a server off a single missed probe —
        // the bug that shipped in 0.1.4.
        //
        // Also evaluate if the server got fewer than `probe_count` probes
        // total because the pending queue ran dry (all articles for this
        // key already dispatched). Without this, the server would stay
        // in `Probing` forever and any future cascade article to it would
        // stall in the `waiting for results` branch of `dispatch_pending`.
        let batch_complete = state.probes_returned >= policy.probe_count
            || (state.probes_returned >= state.probes_sent
                && state.probes_sent >= policy.probe_count);
        if batch_complete && state.status == ProbeStatus::Probing {
            state.evaluate(policy);
            info!(
                job_id,
                server_id,
                probes_sent = state.probes_sent,
                probes_returned = state.probes_returned,
                probes_hit = state.probes_hit,
                approved = matches!(state.status, ProbeStatus::Approved),
                "server probe evaluated"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SchedulerMsg dispatch.
// ---------------------------------------------------------------------------
async fn handle_scheduler_msg(
    msg: SchedulerMsg,
    servers: &[Arc<Server>],
    pending: &mut Vec<WorkItem>,
    outcome_tx: &mpsc::Sender<FetchOutcome>,
    probe_policy: Option<&ServerProbePolicy>,
    probe_tracker: &mut HashMap<(String, String), ProbeState>,
    probe_tags: &mut HashSet<WorkTag>,
) {
    match msg {
        SchedulerMsg::FetchResult {
            item,
            server,
            result,
        } => {
            match result {
                Ok(bytes) => {
                    let article_bytes = bytes.len() as u64;
                    item.job.record_article_downloaded(article_bytes);
                    item.file.mark_article_completed();
                    server.record_attempt_success();
                    if let Some(policy) = probe_policy {
                        update_probe_on_result(
                            item.tag,
                            &item.article.job_id,
                            server.id(),
                            true,
                            probe_tags,
                            probe_tracker,
                            policy,
                        );
                    }
                    let _ = outcome_tx
                        .send(FetchOutcome::Success {
                            tag: item.tag,
                            server_id: server.id().to_string(),
                            bytes,
                            article_bytes,
                        })
                        .await;
                }
                Err(err) => {
                    item.article.mark_server_tried(server.id());
                    if matches!(err, NntpError::ArticleNotFound(_)) {
                        server.record_attempt_not_found();
                    } else {
                        item.article.mark_transient_failure();
                        server.record_attempt_transient_failed();
                    }
                    if let Some(policy) = probe_policy {
                        update_probe_on_result(
                            item.tag,
                            &item.article.job_id,
                            server.id(),
                            false,
                            probe_tags,
                            probe_tracker,
                            policy,
                        );
                    }
                    match penalty_for_error(&err) {
                        PenaltyAction::None => {}
                        PenaltyAction::Cooldown(d) => server.apply_penalty(d),
                        PenaltyAction::BadCons(d) => {
                            server.register_failure(d);
                        }
                    }
                    // Re-evaluate: either re-queue on another server,
                    // cascade, or give up.
                    let sel = select_server(&item.article, &item.file, &item.job, servers);
                    match sel {
                        Selection::GiveUp => {
                            record_article_given_up(&item.article, &item.file, &item.job);
                            let _ = outcome_tx
                                .send(FetchOutcome::Failed {
                                    tag: item.tag,
                                    last_error: Some(format!("{err}")),
                                })
                                .await;
                        }
                        _ => {
                            pending.push(item);
                        }
                    }
                }
            }
        }
        SchedulerMsg::ReturnUnattempted {
            items,
            server,
            error,
        } => {
            // Wrapper's connection died mid-batch; none of these items
            // were actually attempted (headers not yet read). Mark the
            // server as tried so priority-aware selection routes to a
            // different server, and push them back to pending so they
            // get another dispatch pass. Without the re-queue, items
            // here would be silently dropped — nobody else has a
            // reference to them.
            server.register_failure(crate::server::DEFAULT_PENALTY);
            let requeued = items.len();
            for item in items {
                item.article.mark_server_tried(server.id());
                item.article.mark_transient_failure();
                pending.push(item);
            }
            warn!(
                server = %server.id(),
                items = requeued,
                error = %error,
                "wrapper batch aborted; items re-queued for dispatch on other servers"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Wrapper worker — persistent loop, one per NewsWrapper.
// ---------------------------------------------------------------------------

/// Persistent consecutive-connect-failure threshold. A wrapper that fails
/// to connect this many times in a row with a terminal-class error
/// (auth, service unavailable, account cap) exits permanently so we
/// don't hot-loop against a provider-side limit. The scheduler's
/// per-server penalty + Server::register_failure still gates retries at
/// the server level — this is the wrapper-local stop.
const MAX_CONSECUTIVE_CONNECT_FAILURES: u32 = 3;

async fn wrapper_worker(
    mut wrapper: NewsWrapper,
    server: Arc<Server>,
    queue: Arc<ServerQueue>,
    scheduler_tx: mpsc::Sender<SchedulerMsg>,
    shutdown: Arc<Notify>,
    article_timeout: Duration,
) {
    let pipeline_depth = server.config().pipelining.max(1) as usize;
    let mut consecutive_connect_failures: u32 = 0;

    'outer: loop {
        // Wait for batch or shutdown.
        let batch = tokio::select! {
            _ = shutdown.notified() => break,
            b = queue.pop_batch(pipeline_depth) => b,
        };
        if batch.is_empty() {
            // Queue closed (shutdown).
            break;
        }
        let batch_len = batch.len();

        // Ensure connected.
        if !wrapper.is_connected() {
            match wrapper.connect(server.config()).await {
                Ok(()) => {
                    consecutive_connect_failures = 0;
                }
                Err(e) => {
                    // Connect failed: send all batch items back as
                    // transient failures.
                    queue.note_completed(batch_len);
                    let terminal_class = matches!(
                        e,
                        NntpError::Auth(_)
                            | NntpError::AuthRequired(_)
                            | NntpError::ServiceUnavailable(_)
                    );
                    let err_str = format!("{e}");
                    let _ = scheduler_tx
                        .send(SchedulerMsg::ReturnUnattempted {
                            items: batch,
                            server: server.clone(),
                            error: err_str.clone(),
                        })
                        .await;

                    consecutive_connect_failures += 1;

                    // Self-retire on persistent terminal-class failures.
                    // Typical triggers: provider-side "max connections
                    // per user" (481), deactivated account, or a long
                    // service outage. Letting the worker hot-loop in
                    // those cases wastes CPU and flaps the provider.
                    if terminal_class
                        && consecutive_connect_failures >= MAX_CONSECUTIVE_CONNECT_FAILURES
                    {
                        warn!(
                            server = %server.id(),
                            wrapper_id = wrapper.id,
                            attempts = consecutive_connect_failures,
                            error = %err_str,
                            "wrapper retiring after persistent connect failures"
                        );
                        break 'outer;
                    }

                    // Back off briefly so we don't hot-loop on a dead
                    // provider. Scaled by retry count so repeated
                    // failures back off further without spamming logs.
                    let backoff_ms = 500u64 * consecutive_connect_failures.min(6) as u64;
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue 'outer;
                }
            }
        }

        // Run pipelined fetch. Results are streamed to the scheduler per
        // response so cascade-to-next-server can start on the first 430
        // without waiting for the slower BODY responses later in the batch.
        fetch_and_stream(&mut wrapper, &server, batch, article_timeout, &scheduler_tx).await;
        queue.note_completed(batch_len);

        // If the wrapper's connection is dead after the batch, break out
        // so we reconnect on the next iteration.
        if !wrapper.is_connected() {
            continue 'outer;
        }
    }

    // Graceful retirement.
    wrapper.hard_reset().await;
    let remaining = server.unregister_wrapper();
    debug!(
        server = %server.id(),
        wrapper_id = wrapper.id,
        remaining_wrappers = remaining,
        "wrapper worker exiting"
    );
    if remaining == 0 {
        // Last wrapper for this server has retired. Drain whatever's
        // still in the server's queue back to the scheduler so those
        // items can be dispatched elsewhere, then close the queue so
        // the dispatcher stops routing here.
        let stranded = queue.drain_all().await;
        if !stranded.is_empty() {
            let _ = scheduler_tx
                .send(SchedulerMsg::ReturnUnattempted {
                    items: stranded,
                    server: server.clone(),
                    error: "all wrapper workers retired".into(),
                })
                .await;
        }
        queue.close();
        warn!(
            server = %server.id(),
            "all wrapper workers exited — server offline, items rerouted"
        );
    }
}

/// Issue every item in `batch` pipelined on the wrapper's connection and
/// collect per-item results. Wrapped with an overall timeout scaled to
/// batch size.
/// Pipelined fetch with **per-response streaming**: each article outcome
/// is forwarded to the scheduler the moment it lands, instead of the whole
/// batch being buffered and flushed at batch-end.
///
/// Why this matters: on a mostly-missing NZB, the scheduler observes 430s
/// and re-routes those items to the next server. If all 10 items in a
/// batch are buffered until the slowest BODY (sometimes 8+ seconds) comes
/// back, the cascade-latency for a missing article becomes N × batch_ms
/// instead of N × single_response_ms. That's the difference between SAB's
/// "fail in 2min" and our "fail in 6min" on the same NZB.
///
/// The cost of early-emit is paid on the scheduler side (one `send` per
/// response instead of one per batch), which is cheap — it's a bounded
/// mpsc. The payoff on failure-cascade scenarios is substantial.
async fn fetch_and_stream(
    wrapper: &mut NewsWrapper,
    server: &Arc<Server>,
    batch: Vec<WorkItem>,
    per_article_timeout: Duration,
    scheduler_tx: &mpsc::Sender<SchedulerMsg>,
) {
    use nzb_nntp::pipeline::Pipeline;
    use std::collections::HashMap;

    let batch_len = batch.len();
    let depth = server.config().pipelining.max(1);
    let batch_timeout = per_article_timeout
        .checked_mul(batch_len as u32)
        .unwrap_or(per_article_timeout);

    // Build pipeline queue + tag → WorkItem map for routing responses back.
    let mut pipeline = Pipeline::new(depth);
    let mut pending: HashMap<u64, WorkItem> = HashMap::with_capacity(batch_len);
    for item in batch {
        pipeline.submit(item.article.message_id.clone(), item.tag);
        pending.insert(item.tag, item);
    }

    let batch_start = std::time::Instant::now();
    debug!(
        server = %server.id(),
        wrapper_id = wrapper.id,
        batch_size = batch_len,
        pipeline_depth = depth,
        "batch_start"
    );

    // Streaming loop: flush as many sends as depth allows, then read one
    // response, emit it to scheduler, loop. `pending.remove(&tag)` routes
    // each response back to its originating WorkItem.
    let result = tokio::time::timeout(batch_timeout, async {
        loop {
            let conn = wrapper
                .conn_mut()
                .ok_or_else(|| NntpError::Connection("Wrapper has no connection".into()))?;
            pipeline.flush_sends(conn).await?;
            if pipeline.is_empty() {
                break;
            }
            let maybe_res = pipeline.receive_one(conn).await?;
            let Some(res) = maybe_res else { break };
            let tag = res.request.tag;
            let Some(item) = pending.remove(&tag) else {
                // Shouldn't happen — every submitted tag is in the map.
                continue;
            };
            let result = match res.result {
                Ok(resp) => {
                    let bytes = resp.data.unwrap_or_default();
                    wrapper.on_fetch_success(bytes.len() as u64);
                    Ok(bytes)
                }
                Err(e) => Err(e),
            };
            let _ = scheduler_tx
                .send(SchedulerMsg::FetchResult {
                    item,
                    server: server.clone(),
                    result,
                })
                .await;
        }
        Ok::<(), NntpError>(())
    })
    .await;

    let batch_ms = batch_start.elapsed().as_millis() as u64;
    debug!(
        server = %server.id(),
        wrapper_id = wrapper.id,
        batch_size = batch_len,
        batch_ms,
        remaining = pending.len(),
        "batch_done"
    );

    // On fatal error or timeout, emit the un-served remainder as errors.
    // Successes up to the failure point already streamed above.
    let fatal = match result {
        Ok(Ok(())) => None,
        Ok(Err(e)) => {
            warn!(
                server = %server.id(),
                wrapper_id = wrapper.id,
                remaining = pending.len(),
                error = %e,
                "batch pipeline aborted"
            );
            wrapper.hard_reset().await;
            Some(e)
        }
        Err(_elapsed) => {
            warn!(
                server = %server.id(),
                wrapper_id = wrapper.id,
                timeout_ms = batch_timeout.as_millis() as u64,
                remaining = pending.len(),
                "batch timeout — hard-reset"
            );
            wrapper.hard_reset().await;
            Some(NntpError::Timeout(format!(
                "batch timed out after {}s",
                batch_timeout.as_secs()
            )))
        }
    };
    if let Some(e) = fatal {
        for (_, item) in pending.drain() {
            let _ = scheduler_tx
                .send(SchedulerMsg::FetchResult {
                    item,
                    server: server.clone(),
                    result: Err(clone_err(&e)),
                })
                .await;
        }
    }
}

fn clone_err(err: &NntpError) -> NntpError {
    match err {
        NntpError::Connection(m) => NntpError::Connection(m.clone()),
        NntpError::Tls(m) => NntpError::Tls(m.clone()),
        NntpError::Auth(m) => NntpError::Auth(m.clone()),
        NntpError::AuthRequired(m) => NntpError::AuthRequired(m.clone()),
        NntpError::ServiceUnavailable(m) => NntpError::ServiceUnavailable(m.clone()),
        NntpError::ArticleNotFound(m) => NntpError::ArticleNotFound(m.clone()),
        NntpError::NoSuchGroup(m) => NntpError::NoSuchGroup(m.clone()),
        NntpError::NoArticleSelected(m) => NntpError::NoArticleSelected(m.clone()),
        NntpError::Protocol(m) => NntpError::Protocol(m.clone()),
        NntpError::Io(e) => NntpError::Io(std::io::Error::new(e.kind(), e.to_string())),
        NntpError::NoConnectionsAvailable(m) => NntpError::NoConnectionsAvailable(m.clone()),
        NntpError::Timeout(m) => NntpError::Timeout(m.clone()),
        NntpError::AllServersExhausted(m) => NntpError::AllServersExhausted(m.clone()),
        NntpError::Shutdown => NntpError::Shutdown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downloader_config_from_servers_caps_max_conc() {
        let mut s1 = ServerConfig::new("a", "h1");
        s1.connections = 3;
        let mut s2 = ServerConfig::new("b", "h2");
        s2.connections = 5;
        let c = DownloaderConfig::from_servers(vec![s1, s2]);
        assert_eq!(c.max_concurrent_fetches, 8);
        assert!(c.article_timeout >= Duration::from_secs(10));
    }

    #[test]
    fn downloader_config_has_min_concurrency() {
        let c = DownloaderConfig::from_servers(vec![]);
        assert!(c.max_concurrent_fetches >= 4);
    }
}
