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

use std::collections::VecDeque;
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

// ---------------------------------------------------------------------------
// DownloaderHandle
// ---------------------------------------------------------------------------

pub struct DownloaderHandle {
    work_tx: mpsc::Sender<WorkItem>,
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

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
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

    let scheduler_shutdown = shutdown.clone();
    let task = tokio::spawn(scheduler_loop(
        servers,
        server_queues,
        work_rx,
        scheduler_rx,
        outcome_tx,
        scheduler_shutdown,
        worker_handles,
    ));

    (
        DownloaderHandle {
            work_tx,
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
async fn scheduler_loop(
    servers: Vec<Arc<Server>>,
    server_queues: Vec<Arc<ServerQueue>>,
    mut work_rx: mpsc::Receiver<WorkItem>,
    mut scheduler_rx: mpsc::Receiver<SchedulerMsg>,
    outcome_tx: mpsc::Sender<FetchOutcome>,
    shutdown: Arc<Notify>,
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
) {
    info!(
        servers = servers.len(),
        workers = worker_handles.len(),
        "downloader scheduler starting"
    );

    let mut pending: Vec<WorkItem> = Vec::new();
    let mut next_dispatch_retry: Option<tokio::time::Instant> = None;

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
                    Some(msg) => handle_scheduler_msg(
                        msg,
                        &servers,
                        &mut pending,
                        &outcome_tx,
                    ).await,
                    None => {
                        // All wrapper workers have exited.
                        break;
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
        handle_scheduler_msg(msg, &servers, &mut Vec::new(), &outcome_tx).await;
    }

    // Wait for every wrapper worker to exit.
    for h in worker_handles {
        let _ = h.await;
    }

    info!("downloader scheduler exiting");
}

async fn dispatch_pending(
    servers: &[Arc<Server>],
    server_queues: &[Arc<ServerQueue>],
    per_server_cap: &[usize],
    pending: &mut Vec<WorkItem>,
    next_dispatch_retry: &mut Option<tokio::time::Instant>,
    outcome_tx: &mpsc::Sender<FetchOutcome>,
) {
    // First pass: group items by target-server index. Items that select
    // another selection path (WaitRampup, CascadeReset, GiveUp) are
    // handled inline but the route-to-server path accumulates batches so
    // we can push per-server in a single mutex-locked call.
    let mut per_server: Vec<Vec<WorkItem>> = (0..servers.len()).map(|_| Vec::new()).collect();
    let mut give_ups: Vec<WorkItem> = Vec::new();

    let mut i = 0;
    while i < pending.len() {
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
                let q = &server_queues[idx];
                // Cap: check depth + items we're about to push this pass.
                if q.depth() + per_server[idx].len() >= per_server_cap[idx] {
                    bump_retry(next_dispatch_retry, Duration::from_millis(25));
                    i += 1;
                    continue;
                }
                let item = pending.swap_remove(i);
                item.article.set_fetcher_priority(target.priority());
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
// SchedulerMsg dispatch.
// ---------------------------------------------------------------------------
async fn handle_scheduler_msg(
    msg: SchedulerMsg,
    servers: &[Arc<Server>],
    pending: &mut Vec<WorkItem>,
    outcome_tx: &mpsc::Sender<FetchOutcome>,
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
                    if !matches!(err, NntpError::ArticleNotFound(_)) {
                        item.article.mark_transient_failure();
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

        // Run pipelined fetch.
        let per_item_results =
            fetch_batch_pipelined(&mut wrapper, &server, &batch, article_timeout).await;

        // Hand results back to the scheduler.
        for (item, result) in per_item_results {
            let _ = scheduler_tx
                .send(SchedulerMsg::FetchResult {
                    item,
                    server: server.clone(),
                    result,
                })
                .await;
        }
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
async fn fetch_batch_pipelined(
    wrapper: &mut NewsWrapper,
    server: &Arc<Server>,
    batch: &[WorkItem],
    per_article_timeout: Duration,
) -> Vec<(WorkItem, Result<Vec<u8>, NntpError>)> {
    use nzb_nntp::pipeline::Pipeline;

    let batch_timeout = per_article_timeout
        .checked_mul(batch.len() as u32)
        .unwrap_or(per_article_timeout);

    let depth = server.config().pipelining.max(1);
    let mut pipeline = Pipeline::new(depth);
    for (idx, item) in batch.iter().enumerate() {
        pipeline.submit(item.article.message_id.clone(), idx as u64);
    }

    let outcome = tokio::time::timeout(batch_timeout, async {
        let conn = wrapper
            .conn_mut()
            .ok_or_else(|| NntpError::Connection("Wrapper has no connection".into()))?;
        pipeline.process_all(conn).await
    })
    .await;

    let per_item = match outcome {
        Ok(Ok(results)) => results,
        Ok(Err(e)) => {
            warn!(
                server = %server.id(),
                wrapper_id = wrapper.id,
                batch_size = batch.len(),
                error = %e,
                "batch pipeline aborted"
            );
            wrapper.hard_reset().await;
            return batch
                .iter()
                .cloned()
                .map(|it| (it, Err(clone_err(&e))))
                .collect();
        }
        Err(_elapsed) => {
            warn!(
                server = %server.id(),
                wrapper_id = wrapper.id,
                timeout_ms = batch_timeout.as_millis() as u64,
                batch_size = batch.len(),
                "batch timeout — hard-reset"
            );
            wrapper.hard_reset().await;
            return batch
                .iter()
                .cloned()
                .map(|it| {
                    (
                        it,
                        Err(NntpError::Timeout(format!(
                            "batch timed out after {}s",
                            batch_timeout.as_secs()
                        ))),
                    )
                })
                .collect();
        }
    };

    // Map pipeline results (by tag) back to items (by index).
    use std::collections::HashMap;
    let mut by_tag: HashMap<u64, Result<Vec<u8>, NntpError>> =
        HashMap::with_capacity(per_item.len());
    for r in per_item {
        let res = match r.result {
            Ok(resp) => {
                let bytes = resp.data.unwrap_or_default();
                wrapper.on_fetch_success(bytes.len() as u64);
                Ok(bytes)
            }
            Err(e) => Err(e),
        };
        by_tag.insert(r.request.tag, res);
    }

    let mut out = Vec::with_capacity(batch.len());
    for (idx, item) in batch.iter().enumerate() {
        let res = by_tag.remove(&(idx as u64)).unwrap_or_else(|| {
            Err(NntpError::Protocol(
                "pipeline dropped response for tag".into(),
            ))
        });
        out.push((item.clone(), res));
    }
    out
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
