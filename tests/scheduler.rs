use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use catio::{Scheduler, TaskClass};

const A: TaskClass = TaskClass::new(1);
const B: TaskClass = TaskClass::new(2);

struct SaturatedWork {
    stop: Arc<AtomicBool>,
    polls: Arc<AtomicU64>,
}

impl Future for SaturatedWork {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stop.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        // Every poll performs the same CPU quantum. Keeping the quantum
        // cooperative makes poll admission a useful proxy for CPU share.
        for _ in 0..8_000 {
            std::hint::spin_loop();
        }
        self.polls.fetch_add(1, Ordering::Relaxed);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[test]
fn saturated_runtime_keeps_a_30_b_70() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(4)
            .weight(A, 3)
            .weight(B, 7)
            .build();
        let stop = Arc::new(AtomicBool::new(false));
        let a_polls = Arc::new(AtomicU64::new(0));
        let b_polls = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        // More runnable tasks than runtime workers keep both class queues
        // continuously backlogged.
        for _ in 0..32 {
            handles.push(scheduler.spawn_in(
                A,
                SaturatedWork {
                    stop: stop.clone(),
                    polls: a_polls.clone(),
                },
            ));
            handles.push(scheduler.spawn_in(
                B,
                SaturatedWork {
                    stop: stop.clone(),
                    polls: b_polls.clone(),
                },
            ));
        }

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let total = a_polls.load(Ordering::Relaxed) + b_polls.load(Ordering::Relaxed);
                if total >= 30_000 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("saturated workload made no progress");

        let a_at_capacity = a_polls.load(Ordering::Relaxed);
        let b_at_capacity = b_polls.load(Ordering::Relaxed);

        stop.store(true, Ordering::Release);
        for handle in handles {
            handle.await.unwrap();
        }

        let stats = scheduler.stats();
        assert_eq!(0, stats.active_polls);
        assert_eq!(
            64,
            stats.classes[&A].completed + stats.classes[&B].completed
        );

        // Accounting is real execution time: A should hold ~30% of it.
        let a_exec = stats.classes[&A].total_exec_time.as_secs_f64();
        let b_exec = stats.classes[&B].total_exec_time.as_secs_f64();
        let a_share = a_exec / (a_exec + b_exec);
        assert!(
            (0.29..=0.31).contains(&a_share),
            "expected 30% A / 70% B by exec time, got A={a_at_capacity} polls ({a_exec:.3}s), \
             B={b_at_capacity} polls ({b_exec:.3}s), share={a_share}"
        );
    });
}

struct FiniteSelfWaking {
    remaining: usize,
}

impl Future for FiniteSelfWaking {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.remaining == 0 {
            return Poll::Ready(());
        }
        self.remaining -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_classes_do_not_reserve_capacity() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(2)
        .weight(A, 3)
        .weight(B, 7)
        .build();

    let handles = (0..16)
        .map(|_| scheduler.spawn_in(A, FiniteSelfWaking { remaining: 1_000 }))
        .collect::<Vec<_>>();

    tokio::time::timeout(Duration::from_secs(5), async {
        for handle in handles {
            handle.await.unwrap();
        }
    })
    .await
    .expect("A should borrow all capacity while B is idle");

    let stats = scheduler.stats();
    // B never ran, so it must hold no exec time; A holds all of it (~100%).
    assert_eq!(0, stats.classes[&B].polls, "B must not be admitted while idle");
    assert!(
        stats.classes[&A].total_exec_time > Duration::ZERO,
        "A should have accumulated exec time"
    );
    assert_eq!(
        Duration::ZERO,
        stats.classes[&B].total_exec_time,
        "idle B must not accumulate exec time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_class_rejoins_without_old_credit_or_debt() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(4)
        .weight(A, 2)
        .weight(B, 8)
        .build();

    let first_stop = Arc::new(AtomicBool::new(false));
    let first_polls = Arc::new(AtomicU64::new(0));
    let first_handles = (0..32)
        .map(|_| {
            scheduler.spawn_in(
                A,
                SaturatedWork {
                    stop: first_stop.clone(),
                    polls: first_polls.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    while first_polls.load(Ordering::Relaxed) < 10_000 {
        tokio::task::yield_now().await;
    }
    first_stop.store(true, Ordering::Release);
    for handle in first_handles {
        handle.await.unwrap();
    }

    // Snapshot A's cumulative exec time after the solo phase so the rejoin
    // assertion below only measures the 2:8 phase. B never ran in phase 1.
    let a_exec_after_first = scheduler
        .stats()
        .classes
        .get(&A)
        .map(|c| c.total_exec_time.as_secs_f64())
        .unwrap_or(0.0);

    let stop = Arc::new(AtomicBool::new(false));
    let a_polls = Arc::new(AtomicU64::new(0));
    let b_polls = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(scheduler.spawn_in(
            A,
            SaturatedWork {
                stop: stop.clone(),
                polls: a_polls.clone(),
            },
        ));
        handles.push(scheduler.spawn_in(
            B,
            SaturatedWork {
                stop: stop.clone(),
                polls: b_polls.clone(),
            },
        ));
    }
    while a_polls.load(Ordering::Relaxed) + b_polls.load(Ordering::Relaxed) < 30_000 {
        tokio::task::yield_now().await;
    }

    let a = a_polls.load(Ordering::Relaxed);
    let b = b_polls.load(Ordering::Relaxed);

    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.await.unwrap();
    }

    let stats = scheduler.stats();
    let a_exec = stats.classes[&A].total_exec_time.as_secs_f64() - a_exec_after_first;
    let b_exec = stats.classes[&B].total_exec_time.as_secs_f64();
    let a_share = a_exec / (a_exec + b_exec);
    assert!(
        (0.19..=0.21).contains(&a_share),
        "expected A to rejoin at 20% by exec time, got A={a} polls ({a_exec:.3}s), \
         B={b} polls ({b_exec:.3}s), share={a_share}"
    );
}

struct CaptureThenReady {
    polls: Arc<AtomicU64>,
    captured: Arc<Mutex<Option<Waker>>>,
}

impl Future for CaptureThenReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = self.polls.fetch_add(1, Ordering::SeqCst);
        if poll == 0 {
            *self.captured.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

struct BlockingPoll {
    started: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}

impl Future for BlockingPoll {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.started.store(true, Ordering::Release);
        while !self.release.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        Poll::Ready(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_waker_waits_for_scheduler_admission() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let waiter_polls = Arc::new(AtomicU64::new(0));
    let captured = Arc::new(Mutex::new(None));
    let waiter = scheduler.spawn(CaptureThenReady {
        polls: waiter_polls.clone(),
        captured: captured.clone(),
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while captured.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let blocker_started = Arc::new(AtomicBool::new(false));
    let blocker_release = Arc::new(AtomicBool::new(false));
    let blocker = scheduler.spawn(BlockingPoll {
        started: blocker_started.clone(),
        release: blocker_release.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !blocker_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    captured.lock().unwrap().take().unwrap().wake();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        1,
        waiter_polls.load(Ordering::SeqCst),
        "the inner wake must queue, not directly wake the Tokio task"
    );

    blocker_release.store(true, Ordering::Release);
    blocker.await.unwrap();
    waiter.await.unwrap();
    assert_eq!(2, waiter_polls.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_queued_tasks_reclaims_scheduler_state() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let handles = (0..128)
        .map(|_| {
            scheduler.spawn(FiniteSelfWaking {
                remaining: usize::MAX,
            })
        })
        .collect::<Vec<_>>();

    tokio::task::yield_now().await;
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        assert!(handle.await.unwrap_err().is_cancelled());
    }

    tokio::time::timeout(
        Duration::from_secs(2),
        scheduler.spawn(async { "still live" }),
    )
    .await
    .expect("scheduler slot leaked")
    .unwrap();
    let stats = scheduler.stats();
    assert_eq!(0, stats.active_polls);
    assert_eq!(0, stats.classes[&TaskClass::DEFAULT].queued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_weight_change_reallocates_share() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(4)
        .weight(A, 3)
        .weight(B, 7)
        .build();

    let stop = Arc::new(AtomicBool::new(false));
    let a_polls = Arc::new(AtomicU64::new(0));
    let b_polls = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(scheduler.spawn_in(
            A,
            SaturatedWork {
                stop: stop.clone(),
                polls: a_polls.clone(),
            },
        ));
        handles.push(scheduler.spawn_in(
            B,
            SaturatedWork {
                stop: stop.clone(),
                polls: b_polls.clone(),
            },
        ));
    }

    // Let the initial 30/70 allocation run for a while.
    while a_polls.load(Ordering::Relaxed) + b_polls.load(Ordering::Relaxed) < 20_000 {
        tokio::task::yield_now().await;
    }
    let a_before_flip = a_polls.load(Ordering::Relaxed);
    let b_before_flip = b_polls.load(Ordering::Relaxed);
    // Snapshot exec time so the post-flip assertion measures only the new
    // allocation, not the initial 30/70 phase.
    let stats_before_flip = scheduler.stats();
    let a_exec_before_flip = stats_before_flip.classes[&A].total_exec_time.as_secs_f64();
    let b_exec_before_flip = stats_before_flip.classes[&B].total_exec_time.as_secs_f64();

    // Flip the weights dynamically: A 3:7 -> 7:3.
    scheduler.set_weight(A, std::num::NonZeroU32::new(7).unwrap());
    scheduler.set_weight(B, std::num::NonZeroU32::new(3).unwrap());

    // After the flip, A should dominate the polls.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let a = a_polls.load(Ordering::Relaxed);
            let b = b_polls.load(Ordering::Relaxed);
            let a_delta = a - a_before_flip;
            let b_delta = b - b_before_flip;
            let total_delta = a_delta + b_delta;
            if total_delta >= 20_000 && a_delta as f64 / total_delta as f64 >= 0.65 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("dynamic weight flip did not reallocate share");

    let a = a_polls.load(Ordering::Relaxed);
    let b = b_polls.load(Ordering::Relaxed);
    let a_delta = (a - a_before_flip) as f64;
    let total_delta = (a + b - a_before_flip - b_before_flip) as f64;
    let a_share_after = a_delta / total_delta;
    assert!(
        a_share_after >= 0.65,
        "expected A to dominate polls after flip, got A={a}, B={b}, share_after={a_share_after}"
    );

    // The exec-time share of the post-flip delta must also favor A.
    let stats = scheduler.stats();
    let a_exec = stats.classes[&A].total_exec_time.as_secs_f64() - a_exec_before_flip;
    let b_exec = stats.classes[&B].total_exec_time.as_secs_f64() - b_exec_before_flip;
    let a_exec_share = a_exec / (a_exec + b_exec);
    assert!(
        a_exec_share >= 0.65,
        "expected A to dominate by exec time after flip, got share={a_exec_share}"
    );

    // The recorded weight in stats should reflect the new value.
    assert_eq!(7, stats.classes[&A].weight);
    assert_eq!(3, stats.classes[&B].weight);

    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.await.unwrap();
    }
}

/// With time accounting disabled the scheduler must fall back to pure
/// count-based stride accounting: no clock is read on the poll path
/// (`total_exec_time` stays zero), all tasks still complete, and with
/// equal-length polls the poll share converges to the configured weights.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_accounting_disabled_uses_count_accounting() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(4)
        .weight(A, 3)
        .weight(B, 7)
        .time_accounting(false)
        .build();
    assert!(!scheduler.stats().time_accounting, "flag must be exposed");

    let stop = Arc::new(AtomicBool::new(false));
    let a_polls = Arc::new(AtomicU64::new(0));
    let b_polls = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(scheduler.spawn_in(
            A,
            SaturatedWork {
                stop: stop.clone(),
                polls: a_polls.clone(),
            },
        ));
        handles.push(scheduler.spawn_in(
            B,
            SaturatedWork {
                stop: stop.clone(),
                polls: b_polls.clone(),
            },
        ));
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let total = a_polls.load(Ordering::Relaxed) + b_polls.load(Ordering::Relaxed);
            if total >= 30_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("saturated workload made no progress");

    let a_at_capacity = a_polls.load(Ordering::Relaxed);
    let b_at_capacity = b_polls.load(Ordering::Relaxed);

    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.await.unwrap();
    }

    let stats = scheduler.stats();
    assert_eq!(0, stats.active_polls);
    assert_eq!(
        64,
        stats.classes[&A].completed + stats.classes[&B].completed
    );
    assert!(!stats.time_accounting);

    // Count accounting never touches the clock: no exec time is recorded.
    assert_eq!(
        Duration::ZERO,
        stats.classes[&A].total_exec_time,
        "time accounting disabled must not accumulate exec time"
    );
    assert_eq!(
        Duration::ZERO,
        stats.classes[&B].total_exec_time,
        "time accounting disabled must not accumulate exec time"
    );

    // Equal-length polls under count accounting converge to the weight ratio
    // (3:7 -> A ~30% of admissions).
    let total = a_at_capacity + b_at_capacity;
    let a_share = a_at_capacity as f64 / total as f64;
    assert!(
        (0.27..=0.33).contains(&a_share),
        "expected ~30% A by count accounting, got A={a_at_capacity} polls \
         ({a_share:.3}), B={b_at_capacity} polls"
    );
}
