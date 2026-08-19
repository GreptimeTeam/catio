use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use catio::{Scheduler, TaskClass};

const A: TaskClass = TaskClass::new(1);
const B: TaskClass = TaskClass::new(2);
const TASKS_PER_CLASS: usize = 32;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Tokio,
    Scheduled,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Tokio => "tokio",
            Self::Scheduled => "scheduled",
        }
    }
}

struct Work {
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    class_polls: Arc<AtomicU64>,
    target_polls: u64,
    spin_iterations: u64,
}

impl Future for Work {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stop.load(Ordering::Acquire) {
            return Poll::Ready(());
        }

        for _ in 0..self.spin_iterations {
            std::hint::spin_loop();
        }
        self.class_polls.fetch_add(1, Ordering::Relaxed);
        let total = self.total.fetch_add(1, Ordering::Relaxed) + 1;
        if total >= self.target_polls {
            self.stop.store(true, Ordering::Release);
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[derive(Debug)]
struct Sample {
    mode: Mode,
    elapsed_seconds: f64,
    total_polls: u64,
    a_polls: u64,
    b_polls: u64,
}

impl Sample {
    fn polls_per_second(&self) -> f64 {
        self.total_polls as f64 / self.elapsed_seconds
    }
}

fn run_sample(mode: Mode, target_polls: u64, spin_iterations: u64) -> Sample {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    runtime.block_on(async move {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(4)
            .weight(A, 3)
            .weight(B, 7)
            .build();
        let stop = Arc::new(AtomicBool::new(false));
        let total = Arc::new(AtomicU64::new(0));
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::with_capacity(TASKS_PER_CLASS * 2);
        let started = Instant::now();

        for _ in 0..TASKS_PER_CLASS {
            for (class, class_polls) in [(A, a.clone()), (B, b.clone())] {
                let work = Work {
                    stop: stop.clone(),
                    total: total.clone(),
                    class_polls,
                    target_polls,
                    spin_iterations,
                };
                tasks.push(match mode {
                    Mode::Tokio => tokio::spawn(work),
                    Mode::Scheduled => scheduler.spawn_in(class, work),
                });
            }
        }

        for task in tasks {
            task.await.unwrap();
        }
        let elapsed_seconds = started.elapsed().as_secs_f64();
        Sample {
            mode,
            elapsed_seconds,
            total_polls: total.load(Ordering::Relaxed),
            a_polls: a.load(Ordering::Relaxed),
            b_polls: b.load(Ordering::Relaxed),
        }
    })
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn parse_arg(name: &str, default: u64) -> u64 {
    let mut args = std::env::args();
    while let Some(argument) = args.next() {
        if argument == name {
            return args
                .next()
                .unwrap_or_else(|| panic!("{name} requires a value"))
                .parse()
                .unwrap_or_else(|_| panic!("{name} requires an unsigned integer"));
        }
    }
    default
}

fn main() {
    let iterations = parse_arg("--iterations", 7) as usize;
    let target_polls = parse_arg("--target-polls", 300_000);
    let spin_iterations = parse_arg("--spin-iterations", 2_000);
    assert!(iterations > 0, "--iterations must be greater than zero");
    assert!(target_polls > 0, "--target-polls must be greater than zero");

    // Discard one sample of each mode so runtime startup, code pages, and CPU
    // frequency ramp-up do not disproportionately affect the first result.
    let warmup_target = target_polls.min(30_000);
    run_sample(Mode::Tokio, warmup_target, spin_iterations);
    run_sample(Mode::Scheduled, warmup_target, spin_iterations);

    let mut samples = Vec::with_capacity(iterations * 2);
    for iteration in 0..iterations {
        let order = if iteration % 2 == 0 {
            [Mode::Tokio, Mode::Scheduled]
        } else {
            [Mode::Scheduled, Mode::Tokio]
        };
        for mode in order {
            let sample = run_sample(mode, target_polls, spin_iterations);
            println!(
                "iteration={} mode={} elapsed={:.6}s polls={} polls/s={:.0} A={:.2}% B={:.2}%",
                iteration + 1,
                sample.mode.name(),
                sample.elapsed_seconds,
                sample.total_polls,
                sample.polls_per_second(),
                sample.a_polls as f64 * 100.0 / sample.total_polls as f64,
                sample.b_polls as f64 * 100.0 / sample.total_polls as f64,
            );
            samples.push(sample);
        }
    }

    let mut tokio_throughput: Vec<_> = samples
        .iter()
        .filter(|sample| matches!(sample.mode, Mode::Tokio))
        .map(Sample::polls_per_second)
        .collect();
    let mut scheduled_throughput: Vec<_> = samples
        .iter()
        .filter(|sample| matches!(sample.mode, Mode::Scheduled))
        .map(Sample::polls_per_second)
        .collect();
    let tokio_median = median(&mut tokio_throughput);
    let scheduled_median = median(&mut scheduled_throughput);
    let throughput_regression = (1.0 - scheduled_median / tokio_median) * 100.0;
    println!(
        "median: tokio={tokio_median:.0} polls/s scheduled={scheduled_median:.0} polls/s \
         throughput_regression={throughput_regression:.2}%"
    );
}
