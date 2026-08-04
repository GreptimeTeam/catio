//! Tests for the admission-wait instrumentation added on top of the pinned
//! catio commit. These verify the *semantics* of `total_admission_wait` /
//! `admitted` under controlled backlogs, so the GreptimeDB micro-benchmark can
//! rely on them as the scheduler's own admission-queue delay.

use std::time::Duration;

use crate::{ClassStats, Scheduler, TaskClass};
use tokio::task::JoinHandle;

const CLASS_QUERY: TaskClass = TaskClass::new(1);
const CLASS_WRITE: TaskClass = TaskClass::new(2);

fn stats_after(scheduler: &Scheduler, class: TaskClass) -> ClassStats {
    scheduler
        .stats()
        .classes
        .get(&class)
        .cloned()
        .unwrap_or_default()
}

/// Under a dual-backlog with a tiny admission window, every task must wait in
/// the scheduler queue before its first poll, so `admitted` counts tasks and
/// `total_admission_wait` is strictly positive for both classes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_wait_is_positive_under_backlog() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(2)
        .weight(CLASS_QUERY, 2)
        .weight(CLASS_WRITE, 8)
        .build();

    // A task that never completes on its own; it keeps polling Pending so it
    // holds its admission slot and forces the queue to build up.
    let _never_complete = std::future::pending::<()>();

    let tasks: Vec<JoinHandle<()>> = (0..16)
        .map(|i| {
            let scheduler = scheduler.clone();
            tokio::spawn(async move {
                let class: TaskClass = if i % 2 == 0 { CLASS_QUERY } else { CLASS_WRITE };
                let handle: JoinHandle<()> = scheduler.spawn_in(class, std::future::pending::<()>());
                let _: () = handle.await.unwrap();
            })
        })
        .collect();

    // Give the scheduler time to admit a subset and queue the rest.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let query = stats_after(&scheduler, CLASS_QUERY);
    let write = stats_after(&scheduler, CLASS_WRITE);

    assert!(query.admitted > 0, "query class should have admitted tasks");
    assert!(write.admitted > 0, "write class should have admitted tasks");
    assert!(
        query.total_admission_wait > Duration::ZERO,
        "query admission wait must be positive under backlog"
    );
    assert!(
        write.total_admission_wait > Duration::ZERO,
        "write admission wait must be positive under backlog"
    );

    // 16 tasks (8 per class) with max_polls=2: every task gets admitted
    // exactly once because `pending()` returns Pending immediately and
    // releases its admission slot after the first poll. The queued ones are
    // then admitted in turn, so admitted == tasks per class.
    assert_eq!(query.admitted, 8, "query admitted count");
    assert_eq!(write.admitted, 8, "write admitted count");

    // Clean up: abort the infinite tasks.
    for task in tasks {
        task.abort();
    }
}

/// With `max_concurrent_polls` >= tasks and a single class, admission is
/// immediate: tasks are admitted without waiting, so `total_admission_wait`
/// stays near zero (only scheduling noise).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_wait_is_zero_without_backlog() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(64)
        .weight(CLASS_QUERY, 2)
        .weight(CLASS_WRITE, 8)
        .build();

    let mut handles = Vec::new();
    for _ in 0..8 {
        let scheduler = scheduler.clone();
        let handle: JoinHandle<()> = scheduler.spawn_in(CLASS_QUERY, async {});
        handles.push(tokio::spawn(async move {
            let _: () = handle.await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let query = stats_after(&scheduler, CLASS_QUERY);
    let write = stats_after(&scheduler, CLASS_WRITE);
    assert_eq!(query.admitted, 8, "query admitted count");
    assert_eq!(write.admitted, 0, "write admitted count");
    assert!(
        query.total_admission_wait < Duration::from_millis(1),
        "unexpectedly large admission wait without backlog: {:?}",
        query.total_admission_wait
    );
}

/// Weights 2:8 under a dual backlog should produce a write-class admission
/// advantage: write tasks get admitted earlier, so their mean admission wait
/// is smaller than query's. This is the property the GreptimeDB scheduler
/// benchmark uses as its fairness gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_class_has_smaller_admission_wait_under_2_8() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(2)
        .weight(CLASS_QUERY, 2)
        .weight(CLASS_WRITE, 8)
        .build();

    async fn short() {
        // Enough work that the task spans a few polls and holds admission.
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
    }

    let mut handles = Vec::new();
    for i in 0..64 {
        let scheduler = scheduler.clone();
        let class: TaskClass = if i % 2 == 0 { CLASS_QUERY } else { CLASS_WRITE };
        let handle: JoinHandle<()> = scheduler.spawn_in(class, short());
        handles.push(tokio::spawn(async move {
            let _: () = handle.await.unwrap();
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let query = stats_after(&scheduler, CLASS_QUERY);
    let write = stats_after(&scheduler, CLASS_WRITE);
    let query_mean = query.total_admission_wait.as_secs_f64() / query.admitted.max(1) as f64;
    let write_mean = write.total_admission_wait.as_secs_f64() / write.admitted.max(1) as f64;

    assert!(query.admitted > 0, "query admitted count");
    assert!(write.admitted > 0, "write admitted count");
    assert!(
        write_mean < query_mean,
        "write (weight 8) should have smaller mean admission wait than query (weight 2): \
         write={write_mean:.6}s query={query_mean:.6}s"
    );
}
