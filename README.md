# catio

## Overview

`catio` is a single-process Rust library for applying cooperative scheduling
policy to tasks on an existing Tokio runtime. It does not create or replace the
runtime. It wraps futures with a scheduling policy and lets Tokio continue to
poll admitted work.

## Features

- Weighted and soft CPU entitlement scheduling by application-defined
  `TaskClass`.
- Cooperative poll admission with bounded admitted work.
- Tokio-compatible spawning and join handles.
- Optional, separate Linux AIO prototype behind the `linux-aio` feature.

## Installation

Add `catio` and Tokio to your `Cargo.toml`:

```toml
[dependencies]
catio = { git = "https://github.com/GreptimeTeam/catio.git", branch = "feat/weighted-scheduler-soft-cpu" }
tokio = { version = "1.17.0", features = ["rt-multi-thread", "macros"] }
```

Create the Tokio runtime as usual, then use catio within it. After the scheduler
is merged or released, consumers should pin a reviewed revision or published
release appropriate for their project.

## Basic scheduler usage

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

## Scheduling model and CPU entitlements

A scheduler gives each wrapped future a proxy waker. A wake enters catio's
queue, and catio wakes the real Tokio task only when a poll is admitted. The
number of admitted polls waiting in or running on Tokio is bounded by
`max_concurrent_polls`.

Soft CPU limits are relative work-conserving entitlements, not a hard physical
CPU cap. Active classes borrow idle capacity, configured totals need not equal
the Tokio worker count, and scheduling remains cooperative at future poll
boundaries; catio cannot preempt a future that performs unbounded CPU work
without yielding.

`Scheduler::builder` constructs an explicit scheduler. Its legacy `weight`
method remains available; `soft_cpu_limit` accepts positive fixed-point
milli-core entitlements. `weight(class, n)` corresponds to `n * 1000`
milli-cores for share calculations. `Scheduler::set_weights` atomically updates
listed legacy weights; omitted classes remain unchanged. Use `spawn_in` for a
one-off class, or create a class-specific `spawner` for repeated task creation.
Use `schedule_in` to apply a class without creating another task, for example at
an existing request boundary:

```rust
let response = scheduler.schedule_in(QUERY, request_future).await;
```

For the default class, `catio::spawn(future)` is a direct replacement for
`tokio::spawn(future)` and uses a process-wide default scheduler. An explicit
scheduler or spawner keeps the workload class visible at the spawn site.

## Optional Linux AIO

The `linux-aio` feature is disabled by default. When enabled, it exposes the
separate `catio::aio` API, including `AioContext`; this AIO prototype is not
part of the scheduler. It has platform and system requirements and is
unfinished and experimental.

## Examples

Run the weighted scheduling demonstration:

```console
cargo run --example weighted
```

Measure end-to-end overhead against direct Tokio with identical work:

```console
cargo run --release --example overhead -- \
  --iterations 7 --target-polls 300000 --spin-iterations 2000
```

The overhead example interleaves the modes and discards one warmup per mode.
Its output depends on the host and workload and is not a project-wide
performance claim. Adjust `--spin-iterations` to change the amount of
cooperative work per poll.

## Tests

Run the test suite with:

```console
cargo test
```

The default suite covers soft-limit validation, scheduler configuration and
entitlement queries, legacy-weight compatibility, weighted and work-conserving
execution-time shares, fractional limits, poll admission, task lifecycle,
statistics, and wake behavior. Linux AIO tests are conditional on the
`linux-aio` feature and its platform requirements.

## Status and compatibility

- Version: `0.1.0`.
- Tokio: 1.17 or newer, as declared by Cargo.
- No minimum supported Rust version (MSRV) is declared.
- Default Cargo features are empty; Linux AIO is opt-in.
- The Linux AIO API is experimental and unfinished.

## License

Licensed under either of:

- Apache-2.0, in `LICENSE-Apache-2.0`.
- MIT, in `LICENSE-MIT`.
