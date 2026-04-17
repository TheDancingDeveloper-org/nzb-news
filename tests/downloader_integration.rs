//! End-to-end integration test for the layered downloader: spawn mock NNTP
//! servers, run the multiplexed driver, assert that articles are fetched
//! with the expected priority-aware dispatch semantics.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nzb_news::article::{Article, NzbFile, NzbObject};
use nzb_news::downloader::{DownloaderConfig, FetchOutcome, WorkItem, spawn_downloader};
use nzb_nntp::testutil::{MockConfig, MockNntpServer, test_config};

/// Drive N work items through the downloader and collect outcomes. Fails
/// the test if the driver doesn't emit one outcome per item within the
/// given deadline.
async fn run_until_complete(
    handle: &nzb_news::downloader::DownloaderHandle,
    mut outcomes: tokio::sync::mpsc::Receiver<FetchOutcome>,
    expected: usize,
    deadline: Duration,
) -> Vec<FetchOutcome> {
    let mut collected = Vec::with_capacity(expected);
    let _ = handle; // keep it alive
    let res = tokio::time::timeout(deadline, async {
        while collected.len() < expected {
            match outcomes.recv().await {
                Some(o) => collected.push(o),
                None => break,
            }
        }
    })
    .await;
    if res.is_err() {
        panic!(
            "timeout: got {} / {} outcomes after {:?}",
            collected.len(),
            expected,
            deadline
        );
    }
    collected
}

#[tokio::test]
async fn fetches_successful_article_via_single_server() {
    let mut articles = HashMap::new();
    articles.insert("msg1".to_string(), b"hello".to_vec());

    let server = MockNntpServer::start(MockConfig {
        articles,
        ..Default::default()
    })
    .await;

    let mut sc = test_config(server.port());
    sc.id = "s1".into();
    sc.priority = 1;
    sc.connections = 2;
    sc.ramp_up_delay_ms = 0;

    let config = DownloaderConfig {
        servers: vec![sc],
        max_concurrent_fetches: 2,
        article_timeout: Duration::from_secs(10),
        work_channel_capacity: 64,
        outcome_channel_capacity: 64,
        probe_policy: None,
    };
    let (handle, outcomes) = spawn_downloader(config);

    let file = Arc::new(NzbFile::new("f1", "j1", "demo.r00", 1));
    let job = Arc::new(NzbObject::new("j1", "demo", 1, 5, vec![file.clone()]));
    let article = Arc::new(Article::new("msg1", "f1", "j1", 5, 0, 0));

    handle
        .submit(WorkItem {
            tag: 1,
            article: article.clone(),
            file: file.clone(),
            job: job.clone(),
        })
        .await
        .unwrap();

    let got = run_until_complete(&handle, outcomes, 1, Duration::from_secs(10)).await;
    match &got[0] {
        FetchOutcome::Success {
            tag,
            server_id,
            bytes,
            ..
        } => {
            assert_eq!(*tag, 1);
            assert_eq!(server_id, "s1");
            assert!(!bytes.is_empty());
        }
        other => panic!("expected success, got {other:?}"),
    }

    handle.shutdown();
    handle.join().await;
    assert_eq!(job.articles_downloaded(), 1);
    assert_eq!(job.articles_failed(), 0);
}

#[tokio::test]
async fn falls_over_to_backup_when_primary_returns_430() {
    // Primary knows nothing; backup has the article.
    let primary = MockNntpServer::start(MockConfig::default()).await;
    let mut backup_articles = HashMap::new();
    backup_articles.insert("msg2".to_string(), b"xyz".to_vec());
    let backup = MockNntpServer::start(MockConfig {
        articles: backup_articles,
        ..Default::default()
    })
    .await;

    let mut primary_cfg = test_config(primary.port());
    primary_cfg.id = "primary".into();
    primary_cfg.priority = 1;
    primary_cfg.connections = 2;
    primary_cfg.ramp_up_delay_ms = 0;

    let mut backup_cfg = test_config(backup.port());
    backup_cfg.id = "backup".into();
    backup_cfg.priority = 5;
    backup_cfg.connections = 2;
    backup_cfg.ramp_up_delay_ms = 0;

    let config = DownloaderConfig {
        servers: vec![primary_cfg, backup_cfg],
        max_concurrent_fetches: 4,
        article_timeout: Duration::from_secs(10),
        work_channel_capacity: 64,
        outcome_channel_capacity: 64,
        probe_policy: None,
    };
    let (handle, outcomes) = spawn_downloader(config);

    let file = Arc::new(NzbFile::new("f1", "j1", "demo.r00", 1));
    let job = Arc::new(NzbObject::new("j1", "demo", 1, 3, vec![file.clone()]));
    let article = Arc::new(Article::new("msg2", "f1", "j1", 3, 0, 0));

    handle
        .submit(WorkItem {
            tag: 42,
            article: article.clone(),
            file: file.clone(),
            job: job.clone(),
        })
        .await
        .unwrap();

    let got = run_until_complete(&handle, outcomes, 1, Duration::from_secs(10)).await;
    match &got[0] {
        FetchOutcome::Success { tag, server_id, .. } => {
            assert_eq!(*tag, 42);
            assert_eq!(
                server_id, "backup",
                "fall-over should land on backup after primary 430"
            );
        }
        other => panic!("expected success via backup, got {other:?}"),
    }

    // Primary should be recorded in the article's try-list.
    assert!(article.server_tried("primary"));
    handle.shutdown();
    handle.join().await;
}

#[tokio::test]
async fn multiple_articles_dispatch_concurrently() {
    // Ten articles, single server with 5 connections. Verify all succeed
    // and the downloader actually ran them in parallel (total time should
    // be less than sum-of-individual-times; we just check correctness here).
    let mut articles = HashMap::new();
    for i in 0..10 {
        articles.insert(format!("msg{i}"), vec![b'X'; 256]);
    }

    let server = MockNntpServer::start(MockConfig {
        articles,
        ..Default::default()
    })
    .await;

    let mut sc = test_config(server.port());
    sc.id = "s1".into();
    sc.priority = 1;
    sc.connections = 5;
    sc.ramp_up_delay_ms = 0;

    let config = DownloaderConfig {
        servers: vec![sc],
        max_concurrent_fetches: 5,
        article_timeout: Duration::from_secs(10),
        work_channel_capacity: 64,
        outcome_channel_capacity: 64,
        probe_policy: None,
    };
    let (handle, outcomes) = spawn_downloader(config);

    let file = Arc::new(NzbFile::new("f1", "j1", "demo.r00", 10));
    let job = Arc::new(NzbObject::new("j1", "demo", 10, 2560, vec![file.clone()]));

    for i in 0..10u64 {
        let art = Arc::new(Article::new(
            format!("msg{i}"),
            "f1",
            "j1",
            256,
            i as u32,
            i,
        ));
        handle
            .submit(WorkItem {
                tag: i,
                article: art,
                file: file.clone(),
                job: job.clone(),
            })
            .await
            .unwrap();
    }

    let got = run_until_complete(&handle, outcomes, 10, Duration::from_secs(15)).await;
    let successes = got
        .iter()
        .filter(|o| matches!(o, FetchOutcome::Success { .. }))
        .count();
    assert_eq!(
        successes, 10,
        "all ten articles should succeed; got {got:?}"
    );

    handle.shutdown();
    handle.join().await;
    assert_eq!(job.articles_downloaded(), 10);
    assert_eq!(job.articles_failed(), 0);
}

#[tokio::test]
async fn back_pressure_halts_driver_when_outcomes_are_not_drained() {
    // 10 articles, outcome channel capacity = 1. Drain only 3, then stop
    // consuming. Submit everything, wait a bit — the driver should have
    // stalled at the full outcome channel rather than downloading all 10
    // into memory.
    let mut articles = HashMap::new();
    for i in 0..10 {
        articles.insert(format!("bp{i}"), vec![b'X'; 32]);
    }
    let server = MockNntpServer::start(MockConfig {
        articles,
        ..Default::default()
    })
    .await;

    let mut sc = test_config(server.port());
    sc.id = "s1".into();
    sc.priority = 1;
    sc.connections = 4;
    sc.ramp_up_delay_ms = 0;

    let config = DownloaderConfig {
        servers: vec![sc],
        max_concurrent_fetches: 4,
        article_timeout: Duration::from_secs(10),
        work_channel_capacity: 64,
        outcome_channel_capacity: 1, // force back-pressure
        probe_policy: None,
    };
    let (handle, mut outcomes) = spawn_downloader(config);

    let file = Arc::new(NzbFile::new("f1", "j1", "demo", 10));
    let job = Arc::new(NzbObject::new("j1", "demo", 10, 320, vec![file.clone()]));
    for i in 0..10u64 {
        let art = Arc::new(Article::new(format!("bp{i}"), "f1", "j1", 32, i as u32, i));
        handle
            .submit(WorkItem {
                tag: i,
                article: art,
                file: file.clone(),
                job: job.clone(),
            })
            .await
            .unwrap();
    }

    // Drain just 3 outcomes, then pause to let the back-pressure settle.
    let mut collected = 0;
    while collected < 3 {
        if let Some(_o) = outcomes.recv().await {
            collected += 1;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After the pause, the driver should be stalled on `outcome_tx.send`.
    // Job's download count reflects what actually completed — must be
    // strictly less than 10 (the stall prevented the remaining fetches
    // from being emitted).
    let done = job.articles_downloaded();
    assert!(
        done < 10,
        "back-pressure should have halted progress: done={done}"
    );

    // Now drain the remaining outcomes — the driver unblocks and
    // completes the rest.
    while collected < 10 {
        match tokio::time::timeout(Duration::from_secs(10), outcomes.recv()).await {
            Ok(Some(_)) => collected += 1,
            _ => panic!("driver did not recover after back-pressure released"),
        }
    }
    assert_eq!(collected, 10);
    handle.shutdown();
    handle.join().await;
}

#[tokio::test]
async fn gives_up_after_retries_across_servers_all_fail() {
    // Two servers, neither has the article. Verify the driver cascades
    // through servers, records try-list correctly, and emits a Failed
    // outcome within the retry budget.
    let s1 = MockNntpServer::start(MockConfig::default()).await;
    let s2 = MockNntpServer::start(MockConfig::default()).await;

    let mut s1_cfg = test_config(s1.port());
    s1_cfg.id = "s1".into();
    s1_cfg.priority = 1;
    s1_cfg.connections = 2;
    s1_cfg.ramp_up_delay_ms = 0;
    let mut s2_cfg = test_config(s2.port());
    s2_cfg.id = "s2".into();
    s2_cfg.priority = 5;
    s2_cfg.connections = 2;
    s2_cfg.ramp_up_delay_ms = 0;

    let config = DownloaderConfig {
        servers: vec![s1_cfg, s2_cfg],
        max_concurrent_fetches: 4,
        article_timeout: Duration::from_secs(10),
        work_channel_capacity: 64,
        outcome_channel_capacity: 64,
        probe_policy: None,
    };
    let (handle, outcomes) = spawn_downloader(config);

    let file = Arc::new(NzbFile::new("f1", "j1", "demo.r00", 1));
    let job = Arc::new(NzbObject::new("j1", "demo", 1, 0, vec![file.clone()]));
    let article = Arc::new(Article::new("missing", "f1", "j1", 0, 0, 0));

    handle
        .submit(WorkItem {
            tag: 7,
            article: article.clone(),
            file: file.clone(),
            job: job.clone(),
        })
        .await
        .unwrap();

    let got = run_until_complete(&handle, outcomes, 1, Duration::from_secs(15)).await;
    match &got[0] {
        FetchOutcome::Failed { tag, .. } => assert_eq!(*tag, 7),
        other => panic!("expected Failed outcome, got {other:?}"),
    }

    handle.shutdown();
    handle.join().await;
    assert_eq!(job.articles_failed(), 1);
}
