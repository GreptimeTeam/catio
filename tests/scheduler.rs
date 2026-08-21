use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use catio::{Scheduler, SoftCpuLimit, TaskClass};

const A: TaskClass = TaskClass::new(1);
const B: TaskClass = TaskClass::new(2);
const C: TaskClass = TaskClass::new(3);

struct ReentrantSpawnWaker {
    scheduler: Scheduler,
    called: Arc<AtomicBool>,
}

impl std::task::Wake for ReentrantSpawnWaker {
    fn wake(self: Arc<Self>) {
        self.called.store(true, Ordering::Release);
        drop(self.scheduler.spawn(async {}));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabling_wakes_reentrantly_without_holding_switch_lock() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let noop = Waker::from(Arc::new(NoopWaker));
    let mut blocker = Box::pin(scheduler.schedule(std::future::pending::<()>()));
    let mut cx = Context::from_waker(&noop);
    assert!(matches!(blocker.as_mut().poll(&mut cx), Poll::Pending));

    let called = Arc::new(AtomicBool::new(false));
    let reentrant = Waker::from(Arc::new(ReentrantSpawnWaker {
        scheduler: scheduler.clone(),
        called: called.clone(),
    }));
    let mut queued = Box::pin(scheduler.schedule(std::future::pending::<()>()));
    let mut queued_cx = Context::from_waker(&reentrant);
    assert!(matches!(
        queued.as_mut().poll(&mut queued_cx),
        Poll::Pending
    ));

    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(move || scheduler.set_enabled(false)),
    )
    .await
    .expect("disable must not deadlock in a reentrant waker")
    .expect("disable task must complete");
    assert!(called.load(Ordering::Acquire));
}

struct NoopWaker;

impl std::task::Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_spawns_bypass_scheduler_and_reenable_restores_admission() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    scheduler.set_enabled(false);
    assert!(!scheduler.is_enabled());
    let spawner = scheduler.spawner(A);
    let direct = spawner.spawn(async { 7_u32 });
    assert_eq!(7, direct.await.unwrap());
    let stats = scheduler.stats();
    assert_eq!(0, stats.classes[&TaskClass::DEFAULT].tasks);
    assert!(!stats.classes.contains_key(&A));

    scheduler.set_enabled(true);
    assert!(scheduler.is_enabled());
    let managed = scheduler.spawn_in(A, async {});
    managed.await.unwrap();
    let stats = scheduler.stats();
    assert_eq!(1, stats.classes[&A].tasks);
    assert_eq!(1, stats.classes[&A].admitted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabling_drains_queued_tasks_without_hanging() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let release = Arc::new(AtomicBool::new(false));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let blocker = scheduler.spawn(BlockingPoll {
        started: blocker_started.clone(),
        release: release.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !blocker_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("blocker must hold the admission slot");
    let tasks = (0..32)
        .map(|_| scheduler.spawn(async {}))
        .collect::<Vec<_>>();
    // The blocker owns the only admission slot. Wait for the scheduler
    // counters, rather than relying on a timing-sensitive executor yield, so
    // disable is known to drain actual queued work.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stats = scheduler.stats();
            if stats.active_polls == 1 && stats.classes[&TaskClass::DEFAULT].queued > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("queued work must be admitted before disable");
    scheduler.set_enabled(false);
    release.store(true, Ordering::Release);
    blocker.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("disabling scheduler must drain queued tasks");
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_disable_wrapper_stays_bypassed_after_immediate_reenable() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let polls = Arc::new(AtomicU64::new(0));
    let captured = Arc::new(Mutex::new(None));
    let old = scheduler.spawn(CaptureThenReady {
        polls: polls.clone(),
        captured: captured.clone(),
    });
    // The captured inner waker is the precise completion signal for the first poll.
    tokio::time::timeout(Duration::from_secs(2), async {
        while captured.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("pre-disable wrapper must become pending");
    let admitted_before = scheduler.stats().classes[&TaskClass::DEFAULT].admitted;
    scheduler.set_enabled(false);
    captured.lock().unwrap().take().unwrap().wake();
    scheduler.set_enabled(true);
    old.await.unwrap();
    assert_eq!(2, polls.load(Ordering::SeqCst));
    assert_eq!(
        admitted_before,
        scheduler.stats().classes[&TaskClass::DEFAULT].admitted
    );

    let managed = scheduler.spawn(async {});
    managed.await.unwrap();
    assert_eq!(
        admitted_before + 1,
        scheduler.stats().classes[&TaskClass::DEFAULT].admitted,
        "post-enable work must be scheduler-managed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bypassed_panic_does_not_release_another_tasks_admission() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let holder_release = Arc::new(AtomicBool::new(false));
    let holder_started = Arc::new(AtomicBool::new(false));
    let holder = scheduler.spawn(BlockingPoll {
        started: holder_started.clone(),
        release: holder_release.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !holder_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("holder must own the only admission slot");

    // This wrapper is queued while enabled, then drained into sticky direct
    // mode. Re-enable before Tokio gets to poll it so the panic is observed in
    // the direct path.
    let bypassed = scheduler.spawn(PanicOnPoll);
    wait_for_default_queue(&scheduler, 1).await;
    scheduler.set_enabled(false);
    scheduler.set_enabled(true);

    let waiting = scheduler.spawn(async { 9_u8 });
    let panic_error = tokio::time::timeout(Duration::from_secs(2), bypassed)
        .await
        .expect("bypassed panic must be observed")
        .expect_err("intentional direct panic must produce JoinError");
    assert!(panic_error.is_panic());

    let stats = scheduler.stats();
    assert_eq!(1, stats.active_polls);
    assert_eq!(1, stats.classes[&TaskClass::DEFAULT].queued);

    holder_release.store(true, Ordering::Release);
    holder.await.unwrap();
    assert_eq!(9, waiting.await.unwrap());
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_queued_work_reclaims_no_longer_owned_admission() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let blocker = scheduler.spawn(BlockingPoll {
        started: started.clone(),
        release: release.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("blocker must hold the only admission slot");

    let cancelled = scheduler.spawn(std::future::pending::<()>());
    let waiting = scheduler.spawn(async { 11_u8 });
    wait_for_default_queue(&scheduler, 2).await;
    let before_cancel = scheduler.stats();
    assert_eq!(1, before_cancel.active_polls);
    assert_eq!(2, before_cancel.classes[&TaskClass::DEFAULT].queued);
    assert!(
        !waiting.is_finished(),
        "waiting work completed before cancellation"
    );

    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());

    let after_cancel = scheduler.stats();
    assert_eq!(1, after_cancel.active_polls);
    assert_eq!(1, after_cancel.classes[&TaskClass::DEFAULT].queued);
    assert_eq!(
        before_cancel.classes[&TaskClass::DEFAULT].admitted,
        after_cancel.classes[&TaskClass::DEFAULT].admitted,
        "cancelling queued work must not release the blocker's slot"
    );
    assert!(
        !waiting.is_finished(),
        "waiting work completed before blocker release"
    );

    release.store(true, Ordering::Release);
    blocker.await.unwrap();
    assert_eq!(
        11,
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("queued work must proceed after cancellation")
            .unwrap()
    );
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_panic_releases_admission_for_queued_work() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let blocker = scheduler.spawn(BlockingPoll {
        started: started.clone(),
        release: release.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("blocker must hold the only admission slot");

    let panicking = scheduler.spawn(ManagedPanicOnPoll);
    let waiting = scheduler.spawn(async { 13_u8 });
    wait_for_default_queue(&scheduler, 2).await;
    release.store(true, Ordering::Release);
    blocker.await.unwrap();

    let panic_error = tokio::time::timeout(Duration::from_secs(2), panicking)
        .await
        .expect("managed panic must be observed")
        .expect_err("intentional managed panic must produce JoinError");
    assert!(panic_error.is_panic());
    assert_eq!(
        13,
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("queued work must proceed after panic")
            .unwrap()
    );
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_work_after_disable_reclaims_slots_for_post_enable_work() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let release = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let blocker = scheduler.spawn({
        let release = release.clone();
        let started = started.clone();
        async move {
            started.notify_one();
            release.notified().await;
        }
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("blocker must start");
    let queued = (0..32)
        .map(|_| {
            scheduler.spawn(FiniteSelfWaking {
                remaining: usize::MAX,
            })
        })
        .collect::<Vec<_>>();
    // One task may already own the sole slot, so the maximum queued backlog
    // is one less than the number submitted. Waiting for the full submission
    // count would make this cancellation regression impossible to observe.
    wait_for_default_queue(&scheduler, queued.len() - 1).await;
    scheduler.set_enabled(false);
    release.notify_one();
    blocker.await.unwrap();
    for task in &queued {
        task.abort();
    }
    for task in queued {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(0, scheduler.stats().active_polls);

    scheduler.set_enabled(true);
    tokio::time::timeout(Duration::from_secs(2), scheduler.spawn(async { 9_u8 }))
        .await
        .expect("post-enable task must not be blocked by cancelled work")
        .unwrap();
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_waking_poll_racing_disable_completes_without_panic() {
    let scheduler = Scheduler::builder().max_concurrent_polls(1).build();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicU64::new(0));
    let task = scheduler.spawn(DisableWhilePolling {
        started: started.clone(),
        release: release.clone(),
        polls: polls.clone(),
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("self-waking task must enter its poll");
    scheduler.set_enabled(false);
    release.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("disabled self-waking task must complete")
        .unwrap();
    assert!(polls.load(Ordering::SeqCst) >= 2);
    assert_eq!(0, scheduler.stats().active_polls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_scheduler_spawn_apis_bypass_unknown_classes() {
    let scheduler = Scheduler::builder().build();
    let runtime = tokio::runtime::Handle::current();
    let classes = [
        TaskClass::new(101),
        TaskClass::new(102),
        TaskClass::new(103),
        TaskClass::new(104),
        TaskClass::new(105),
        TaskClass::new(106),
        TaskClass::new(107),
    ];
    scheduler.set_enabled(false);
    let spawner = scheduler.spawner(classes[4]);
    let handles = vec![
        scheduler.spawn(async {}),
        scheduler.spawn_in(classes[0], async {}),
        scheduler.spawn_on(&runtime, async {}),
        scheduler.spawn_in_on(&runtime, classes[1], async {}),
        spawner.spawn(async {}),
        spawner.spawn_on(&runtime, async {}),
    ];
    for handle in handles {
        handle.await.unwrap();
    }
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let local_default = scheduler.spawn_local(async {});
            let local_class = scheduler.spawn_local_in(classes[5], async {});
            local_default.await.unwrap();
            local_class.await.unwrap();
        })
        .await;
    let stats = scheduler.stats();
    assert_eq!(0, stats.classes[&TaskClass::DEFAULT].tasks);
    for class in classes {
        assert!(
            !stats.classes.contains_key(&class),
            "disabled API must not create scheduler state for class {class}"
        );
    }
    assert_eq!(0, stats.classes[&TaskClass::DEFAULT].admitted);
}

#[test]
fn zero_runtime_concurrency_is_rejected() {
    let result = std::panic::catch_unwind(|| Scheduler::builder().max_concurrent_polls(0));
    assert!(result.is_err());
    let scheduler = Scheduler::default();
    let result = std::panic::catch_unwind(|| scheduler.set_max_concurrent_polls(0));
    assert!(result.is_err());
}

#[test]
fn soft_cpu_limit_validates_fixed_point_millicores() {
    assert_eq!(None, SoftCpuLimit::from_millicores(0));
    assert_eq!(
        Some(500),
        SoftCpuLimit::from_millicores(500).map(SoftCpuLimit::millicores)
    );
    assert_eq!(
        Some(1_500),
        SoftCpuLimit::from_millicores(1_500).map(SoftCpuLimit::millicores)
    );
    assert_eq!(
        Some(2_000),
        SoftCpuLimit::from_cores(2).map(SoftCpuLimit::millicores)
    );
    assert_eq!(None, SoftCpuLimit::from_cores(0));
    assert_eq!(None, SoftCpuLimit::from_cores(u32::MAX / 1_000 + 1));
    let largest_cores = u32::MAX / 1_000;
    assert_eq!(
        Some(largest_cores * 1_000),
        SoftCpuLimit::from_cores(largest_cores).map(SoftCpuLimit::millicores)
    );
    assert_eq!(
        Some(u32::MAX),
        SoftCpuLimit::from_millicores(u32::MAX).map(SoftCpuLimit::millicores)
    );
}

#[test]
fn default_and_soft_class_entitlements_are_queryable_without_stats_breakage() {
    let scheduler = Scheduler::builder()
        .soft_cpu_limit(A, SoftCpuLimit::from_millicores(500).unwrap())
        .build();
    assert_eq!(1_000, scheduler.soft_cpu_limit_millicores(B));
    assert_eq!(1, scheduler.stats().classes[&TaskClass::DEFAULT].weight);
    assert_eq!(0, scheduler.stats().classes[&A].weight);
    assert_eq!(500, scheduler.soft_cpu_limit_millicores(A));
}

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

async fn wait_for_polls(polls: &[&AtomicU64], target: u64, message: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if polls
                .iter()
                .map(|polls| polls.load(Ordering::Relaxed))
                .sum::<u64>()
                >= target
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect(message);
}

#[test]
fn dynamic_api_transitions_update_weight_sentinel_and_entitlement() {
    let scheduler = Scheduler::builder().weight(A, 2).build();
    assert_eq!(2, scheduler.stats().classes[&A].weight);
    assert_eq!(2_000, scheduler.soft_cpu_limit_millicores(A));
    scheduler.set_soft_cpu_limit(A, SoftCpuLimit::from_millicores(500).unwrap());
    assert_eq!(0, scheduler.stats().classes[&A].weight);
    assert_eq!(500, scheduler.soft_cpu_limit_millicores(A));
    scheduler.set_weight(A, std::num::NonZeroU32::new(3).unwrap());
    assert_eq!(3, scheduler.stats().classes[&A].weight);
    assert_eq!(3_000, scheduler.soft_cpu_limit_millicores(A));
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

#[test]
fn saturated_soft_limits_keep_1_2_5_exec_time_shares() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(4)
            .soft_cpu_limit(A, SoftCpuLimit::from_cores(1).unwrap())
            .soft_cpu_limit(B, SoftCpuLimit::from_cores(2).unwrap())
            .soft_cpu_limit(C, SoftCpuLimit::from_cores(5).unwrap())
            .build();
        let stop = Arc::new(AtomicBool::new(false));
        let polls = [
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        ];
        let mut handles = Vec::new();
        for _ in 0..32 {
            for (class, class_polls) in [
                (A, polls[0].clone()),
                (B, polls[1].clone()),
                (C, polls[2].clone()),
            ] {
                handles.push(scheduler.spawn_in(
                    class,
                    SaturatedWork {
                        stop: stop.clone(),
                        polls: class_polls,
                    },
                ));
            }
        }
        wait_for_polls(
            &[&polls[0], &polls[1], &polls[2]],
            30_000,
            "soft-limit workload made no progress",
        )
        .await;
        stop.store(true, Ordering::Release);
        for handle in handles {
            handle.await.unwrap();
        }
        let stats = scheduler.stats();
        let exec = [
            stats.classes[&A].total_exec_time.as_secs_f64(),
            stats.classes[&B].total_exec_time.as_secs_f64(),
            stats.classes[&C].total_exec_time.as_secs_f64(),
        ];
        let total: f64 = exec.iter().sum();
        for (actual, expected) in exec.into_iter().zip([0.125, 0.25, 0.625]) {
            assert!(
                (expected - 0.02..=expected + 0.02).contains(&(actual / total)),
                "expected {expected}, got {}",
                actual / total
            );
        }
    });
}

#[test]
fn fractional_soft_limits_keep_25_75_exec_time_shares() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(4)
            .soft_cpu_limit(A, SoftCpuLimit::from_millicores(500).unwrap())
            .soft_cpu_limit(B, SoftCpuLimit::from_millicores(1_500).unwrap())
            .build();
        let stop = Arc::new(AtomicBool::new(false));
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..32 {
            for (class, polls) in [(A, a.clone()), (B, b.clone())] {
                handles.push(scheduler.spawn_in(
                    class,
                    SaturatedWork {
                        stop: stop.clone(),
                        polls,
                    },
                ));
            }
        }
        wait_for_polls(&[&a, &b], 20_000, "fractional workload made no progress").await;
        stop.store(true, Ordering::Release);
        for handle in handles {
            handle.await.unwrap();
        }
        let stats = scheduler.stats();
        let a_exec = stats.classes[&A].total_exec_time.as_secs_f64();
        let b_exec = stats.classes[&B].total_exec_time.as_secs_f64();
        assert!((0.23..=0.27).contains(&(a_exec / (a_exec + b_exec))));
    });
}

#[test]
fn legacy_weight_and_two_core_soft_limit_split_exec_time_evenly() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(4)
            .weight(A, 2)
            .soft_cpu_limit(B, SoftCpuLimit::from_cores(2).unwrap())
            .build();
        assert_eq!(2, scheduler.stats().classes[&A].weight);
        assert_eq!(0, scheduler.stats().classes[&B].weight);
        assert_eq!(2_000, scheduler.soft_cpu_limit_millicores(A));
        assert_eq!(2_000, scheduler.soft_cpu_limit_millicores(B));
        let stop = Arc::new(AtomicBool::new(false));
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..32 {
            for (class, polls) in [(A, a.clone()), (B, b.clone())] {
                handles.push(scheduler.spawn_in(
                    class,
                    SaturatedWork {
                        stop: stop.clone(),
                        polls,
                    },
                ));
            }
        }
        wait_for_polls(&[&a, &b], 20_000, "mixed workload made no progress").await;
        stop.store(true, Ordering::Release);
        for handle in handles {
            handle.await.unwrap();
        }
        let stats = scheduler.stats();
        let a_exec = stats.classes[&A].total_exec_time.as_secs_f64();
        let b_exec = stats.classes[&B].total_exec_time.as_secs_f64();
        assert!((0.48..=0.52).contains(&(a_exec / (a_exec + b_exec))));
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
    assert_eq!(
        0, stats.classes[&B].polls,
        "B must not be admitted while idle"
    );
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
        .soft_cpu_limit(A, SoftCpuLimit::from_cores(2).unwrap())
        .soft_cpu_limit(B, SoftCpuLimit::from_cores(8).unwrap())
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
    wait_for_polls(&[&first_polls], 10_000, "solo class made no progress").await;
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
    wait_for_polls(
        &[&a_polls, &b_polls],
        30_000,
        "rejoined class made no progress",
    )
    .await;

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

struct ManagedPanicOnPoll;

impl Future for ManagedPanicOnPoll {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("intentional managed panic");
    }
}

struct PanicOnPoll;

impl Future for PanicOnPoll {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("intentional direct-poll panic");
    }
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

struct DisableWhilePolling {
    started: Arc<tokio::sync::Notify>,
    release: Arc<AtomicBool>,
    polls: Arc<AtomicU64>,
}

impl Future for DisableWhilePolling {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll = self.polls.fetch_add(1, Ordering::SeqCst);
        if poll == 0 {
            self.started.notify_one();
            while !self.release.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

async fn wait_for_default_queue(scheduler: &Scheduler, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if scheduler.stats().classes[&TaskClass::DEFAULT].queued >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expected scheduler backlog was not observed");
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
    wait_for_polls(
        &[&a_polls, &b_polls],
        20_000,
        "initial dynamic-weight workload made no progress",
    )
    .await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_soft_cpu_limit_change_reallocates_share_and_updates_query() {
    let scheduler = Scheduler::builder()
        .max_concurrent_polls(4)
        .soft_cpu_limit(A, SoftCpuLimit::from_cores(3).unwrap())
        .soft_cpu_limit(B, SoftCpuLimit::from_cores(7).unwrap())
        .build();
    let stop = Arc::new(AtomicBool::new(false));
    let a = Arc::new(AtomicU64::new(0));
    let b = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        for (class, polls) in [(A, a.clone()), (B, b.clone())] {
            handles.push(scheduler.spawn_in(
                class,
                SaturatedWork {
                    stop: stop.clone(),
                    polls,
                },
            ));
        }
    }
    wait_for_polls(
        &[&a, &b],
        20_000,
        "initial dynamic soft-limit workload made no progress",
    )
    .await;
    let before = scheduler.stats();
    let a_before = before.classes[&A].total_exec_time.as_secs_f64();
    let b_before = before.classes[&B].total_exec_time.as_secs_f64();
    scheduler.set_soft_cpu_limit(A, SoftCpuLimit::from_cores(7).unwrap());
    scheduler.set_soft_cpu_limit(B, SoftCpuLimit::from_cores(3).unwrap());
    assert_eq!(0, scheduler.stats().classes[&A].weight);
    assert_eq!(7_000, scheduler.soft_cpu_limit_millicores(A));
    assert_eq!(3_000, scheduler.soft_cpu_limit_millicores(B));
    let polls_before = a.load(Ordering::Relaxed) + b.load(Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if a.load(Ordering::Relaxed) + b.load(Ordering::Relaxed) >= polls_before + 20_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("updated soft-limit workload made no progress");
    let after = scheduler.stats();
    let a_exec = after.classes[&A].total_exec_time.as_secs_f64() - a_before;
    let b_exec = after.classes[&B].total_exec_time.as_secs_f64() - b_before;
    assert!(
        a_exec / (a_exec + b_exec) >= 0.65,
        "soft update did not reallocate exec time"
    );
    stop.store(true, Ordering::Release);
    for handle in handles {
        handle.await.unwrap();
    }
}
