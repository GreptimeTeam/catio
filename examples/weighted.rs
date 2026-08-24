use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use catio::{Scheduler, TaskClass};

const A: TaskClass = TaskClass::new(1);
const B: TaskClass = TaskClass::new(2);

struct Work {
    stop: Arc<AtomicBool>,
    polls: Arc<AtomicU64>,
}

impl Future for Work {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stop.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        for _ in 0..2_000 {
            std::hint::spin_loop();
        }
        self.polls.fetch_add(1, Ordering::Relaxed);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn main() {
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
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();

        for _ in 0..32 {
            tasks.push(scheduler.spawn_in(
                A,
                Work {
                    stop: stop.clone(),
                    polls: a.clone(),
                },
            ));
            tasks.push(scheduler.spawn_in(
                B,
                Work {
                    stop: stop.clone(),
                    polls: b.clone(),
                },
            ));
        }

        loop {
            let total = a.load(Ordering::Relaxed) + b.load(Ordering::Relaxed);
            if total >= 30_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        let a_polls = a.load(Ordering::Relaxed);
        let b_polls = b.load(Ordering::Relaxed);
        println!(
            "saturated result: A={a_polls} ({:.2}%), B={b_polls} ({:.2}%)",
            a_polls as f64 * 100.0 / (a_polls + b_polls) as f64,
            b_polls as f64 * 100.0 / (a_polls + b_polls) as f64,
        );

        stop.store(true, Ordering::Release);
        for task in tasks {
            task.await.unwrap();
        }
    });
}
