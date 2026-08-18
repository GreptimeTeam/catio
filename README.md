# catio

`catio` adds policy-controlled cooperative task scheduling to an existing Tokio
runtime. It does not create or replace the runtime.

The scheduler gives an inner future a proxy waker. A wake first enters catio's
weighted queue; catio wakes the real Tokio task only when a poll is admitted.
The number of admitted polls waiting in or running on Tokio is bounded, so a
backlogged class cannot fill Tokio's run queue ahead of another class.

```rust
use catio::{Scheduler, SoftCpuLimit, TaskClass};

const QUERY: TaskClass = TaskClass::new(1);
const WRITE: TaskClass = TaskClass::new(2);

let scheduler = Scheduler::builder()
    .max_concurrent_polls(8) // normally the Tokio worker count
    .soft_cpu_limit(QUERY, SoftCpuLimit::from_cores(2).unwrap())
    .soft_cpu_limit(WRITE, SoftCpuLimit::from_cores(8).unwrap())
    .build();

let query = scheduler.spawner(QUERY);
let write = scheduler.spawner(WRITE);

// Same JoinHandle and bounds as tokio::spawn.
let query_handle = query.spawn(async { /* ... */ });
let write_handle = write.spawn(async { /* ... */ });
```

`catio::spawn(future)` is a direct replacement for `tokio::spawn(future)` and
uses a process-wide default scheduler. Large applications should keep an
explicit `Scheduler` or `Spawner` so the workload class is visible at the spawn
site. Existing request tasks can be gated without creating another task via
`scheduler.schedule_in(class, request_future).await`.

Soft CPU limits are fixed-point milli-core, relative work-conserving
entitlements within this single-process scheduler: configured limits determine
elapsed poll execution-time shares while multiple classes are backlogged, but
they are **not hard CPU caps**. In particular, “2 cores” is a relative
entitlement under contention, not a guarantee of two physical cores; configured
totals need not match Tokio's runtime worker capacity. An idle class reserves no
capacity, so an active class borrows the available capacity. Legacy
`.weight(class, n)` remains supported and is equivalent to a whole-core soft
limit of `n * 1000` milli-cores; this makes legacy and soft limit calls directly
comparable in one scheduler. `ClassStats::weight` is `0` for a class configured
through the soft-limit API (zero was never a valid legacy weight); query
`scheduler.soft_cpu_limit_millicores(class)` for its normalized entitlement.
Scheduling occurs at future poll boundaries. Like Tokio itself, catio cannot
preempt a future that performs unbounded CPU work without yielding.

Run the saturated 30/70 demonstration with:

```console
cargo run --example weighted
```

Measure end-to-end scheduling overhead against direct Tokio with a fixed amount
of identical work:

```console
cargo run --release --example overhead -- \
  --iterations 7 --target-polls 300000 --spin-iterations 2000
```

The modes are interleaved, one warmup per mode is discarded, and the program
reports median useful polls/second and the throughput regression. A reference
4-worker run measured a 0.49% median throughput regression while the scheduled
mode maintained exactly 30% A / 70% B. Performance results are host- and
workload-dependent; reduce `--spin-iterations` to emphasize scheduler overhead
or increase it to model coarser cooperative work.

The unfinished Linux AIO prototype from the original repository remains
available behind the `linux-aio` feature and is independent of the task
scheduler.
