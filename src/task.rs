//! Tokio-compatible task spawning with weighted, work-conserving scheduling.
//!
//! A [`Scheduled`] future never gives its inner future Tokio's waker directly.
//! The inner future receives a proxy waker. Waking that proxy places the task in
//! this scheduler's queue; only an admission selected by the policy wakes the
//! real Tokio task.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

pub use tokio::task::{yield_now, JoinError, JoinHandle};

const IDLE: u8 = 0;
const QUEUED: u8 = 1;
const ADMITTED: u8 = 2;
const POLLING: u8 = 3;
const POLLING_NOTIFIED: u8 = 4;
const FINISHED: u8 = 5;

// A fixed-point numerator for real-execution-time pass accounting: measured
// nanoseconds are scaled up so rounding is irrelevant for practical weights
// while leaving enormous headroom before a u128 counter can saturate.
const TIME_SCALE: u128 = 1_u128 << 32;

/// Identifies a scheduling class.
///
/// Classes are intentionally small, copyable values so applications can define
/// constants at their request or workload boundaries.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskClass(u32);

impl TaskClass {
    /// The class used by [`spawn`] and [`Scheduler::spawn`].
    pub const DEFAULT: Self = Self(0);

    /// Creates a class from an application-defined identifier.
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the application-defined identifier.
    pub const fn id(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TaskClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A positive, fixed-point soft CPU limit measured in milli-cores.
///
/// This is a relative, work-conserving entitlement while classes contend for
/// the scheduler. It is not a hard CPU cap: an active class borrows capacity
/// from configured classes that are idle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SoftCpuLimit(NonZeroU32);

impl SoftCpuLimit {
    /// Creates a limit from milli-cores. Zero is not a valid limit.
    pub const fn from_millicores(millicores: u32) -> Option<Self> {
        match NonZeroU32::new(millicores) {
            Some(millicores) => Some(Self(millicores)),
            None => None,
        }
    }

    /// Creates a limit from whole CPU cores. Zero and overflow are rejected.
    pub const fn from_cores(cores: u32) -> Option<Self> {
        match cores.checked_mul(1_000) {
            Some(millicores) => Self::from_millicores(millicores),
            None => None,
        }
    }

    /// Returns this limit in milli-cores.
    pub const fn millicores(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug)]
struct ClassConfig {
    // Zero is the public `ClassStats::weight` sentinel for soft-limit classes.
    legacy_weight: u32,
    entitlement_millicores: u64,
}

impl ClassConfig {
    const fn legacy(weight: NonZeroU32) -> Self {
        Self {
            legacy_weight: weight.get(),
            entitlement_millicores: weight.get() as u64 * 1_000,
        }
    }

    const fn soft(limit: SoftCpuLimit) -> Self {
        Self {
            // Zero was never a valid legacy weight, so it is a stable public
            // sentinel for classes configured through the soft-limit API.
            legacy_weight: 0,
            entitlement_millicores: limit.millicores() as u64,
        }
    }
}

/// Builds a weighted scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerBuilder {
    max_concurrent_polls: usize,
    weights: BTreeMap<TaskClass, ClassConfig>,
    sample_every_polls: usize,
}

impl Default for SchedulerBuilder {
    fn default() -> Self {
        let max_concurrent_polls = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        let mut weights = BTreeMap::new();
        weights.insert(TaskClass::DEFAULT, ClassConfig::legacy(NonZeroU32::MIN));

        Self {
            max_concurrent_polls,
            weights,
            sample_every_polls: 1,
        }
    }
}

impl SchedulerBuilder {
    /// Creates a builder with a default class of weight one.
    pub fn new() -> Self {
        Self::default()
    }

    /// Limits how many admitted polls may wait in, or execute on, Tokio.
    ///
    /// For CPU-oriented workloads this should normally equal the worker count
    /// of the Tokio runtime used by this scheduler.
    ///
    /// # Panics
    ///
    /// Panics when `limit` is zero.
    pub fn max_concurrent_polls(mut self, limit: usize) -> Self {
        assert!(limit > 0, "max_concurrent_polls must be greater than zero");
        self.max_concurrent_polls = limit;
        self
    }

    /// Sets a class's relative share.
    ///
    /// For example, weights 3 and 7 produce 30% and 70% of poll admissions
    /// while both classes remain backlogged. An idle class reserves no slots.
    ///
    /// # Panics
    ///
    /// Panics when `weight` is zero.
    pub fn weight(mut self, class: TaskClass, weight: u32) -> Self {
        let weight = NonZeroU32::new(weight).expect("class weight must be greater than zero");
        self.weights.insert(class, ClassConfig::legacy(weight));
        self
    }

    /// Sets a class's soft CPU limit in milli-cores.
    ///
    /// Limits are relative work-conserving entitlements under contention, not
    /// hard caps. Idle classes reserve no CPU capacity. Legacy
    /// [`Self::weight`] values are converted to whole-core milli-core shares,
    /// so `weight(class, 2)` and a two-core soft limit have equal shares.
    pub fn soft_cpu_limit(mut self, class: TaskClass, limit: SoftCpuLimit) -> Self {
        self.weights.insert(class, ClassConfig::soft(limit));
        self
    }

    /// Samples the clock on every N-th poll per class instead of every poll.
    ///
    /// The scheduler charges real execution time to a class by stamping
    /// [`Instant::now()`] when an admitted poll starts and measuring its
    /// elapsed time when the poll returns. With the default `1`, every poll is
    /// stamped, which is exactly how the scheduler behaved before this option
    /// existed. Setting `N > 1` stamps only every N-th poll in each class, and
    /// **unstamped polls are not charged at all**: their execution time is
    /// approximated as zero. This trades accounting/fairness precision for
    /// lower per-poll overhead; it is meant for measuring the cost of time
    /// sampling itself (every-poll vs. downsampled).
    ///
    /// The poll counter is per scheduling class, so the N-poll sampling rhythm
    /// is global within a class rather than per task. Values `0` and `1` are
    /// both treated as "sample every poll".
    pub fn sample_every_polls(mut self, every: usize) -> Self {
        self.sample_every_polls = every.max(1);
        self
    }

    /// Creates the scheduler.
    pub fn build(self) -> Scheduler {
        let classes = self
            .weights
            .into_iter()
            .map(|(class, config)| (class, ClassState::new(config, 0)))
            .collect();

        Scheduler {
            inner: Arc::new(SchedulerInner {
                state: Mutex::new(ScheduleState {
                    max_concurrent_polls: self.max_concurrent_polls,
                    active: 0,
                    virtual_time: 0,
                    classes,
                }),
                sample_every_polls: self.sample_every_polls,
                poll_counters: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

/// A weighted, work-conserving scheduler that dispatches polls to Tokio.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("stats", &self.stats())
            .finish()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        SchedulerBuilder::default().build()
    }
}

impl Scheduler {
    /// Returns a new scheduler builder.
    pub fn builder() -> SchedulerBuilder {
        SchedulerBuilder::new()
    }

    /// Wraps a future in the default scheduling class without spawning it.
    pub fn schedule<F>(&self, future: F) -> Scheduled<F>
    where
        F: Future,
    {
        self.schedule_in(TaskClass::DEFAULT, future)
    }

    /// Wraps a future in `class` without spawning it.
    ///
    /// This is useful at an existing request boundary where creating another
    /// Tokio task is undesirable.
    pub fn schedule_in<F>(&self, class: TaskClass, future: F) -> Scheduled<F>
    where
        F: Future,
    {
        Scheduled::new(self.inner.new_task(class), future)
    }

    /// Spawns a future in the default class on the current Tokio runtime.
    ///
    /// Its signature and return type match [`tokio::spawn`].
    #[track_caller]
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(self.schedule(future))
    }

    /// Spawns a future in `class` on the current Tokio runtime.
    #[track_caller]
    pub fn spawn_in<F>(&self, class: TaskClass, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(self.schedule_in(class, future))
    }

    /// Spawns a future in the default class on a specific Tokio runtime.
    pub fn spawn_on<F>(&self, handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        handle.spawn(self.schedule(future))
    }

    /// Spawns a future in `class` on a specific Tokio runtime.
    pub fn spawn_in_on<F>(
        &self,
        handle: &tokio::runtime::Handle,
        class: TaskClass,
        future: F,
    ) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        handle.spawn(self.schedule_in(class, future))
    }

    /// Spawns a non-`Send` future in the default class on the current
    /// [`tokio::task::LocalSet`].
    #[track_caller]
    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        tokio::task::spawn_local(self.schedule(future))
    }

    /// Spawns a non-`Send` future in `class` on the current
    /// [`tokio::task::LocalSet`].
    #[track_caller]
    pub fn spawn_local_in<F>(&self, class: TaskClass, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        tokio::task::spawn_local(self.schedule_in(class, future))
    }

    /// Creates a reusable Tokio-like spawner for one class.
    pub fn spawner(&self, class: TaskClass) -> Spawner {
        Spawner {
            scheduler: self.clone(),
            class,
        }
    }

    /// Returns a point-in-time scheduler snapshot.
    pub fn stats(&self) -> SchedulerStats {
        self.inner.stats()
    }

    /// Returns the class's normalized soft CPU entitlement in milli-cores.
    ///
    /// Legacy `weight(n)` configurations report `n * 1000`; classes configured
    /// through [`SchedulerBuilder::soft_cpu_limit`] or
    /// [`Self::set_soft_cpu_limit`] report their supplied milli-core limit.
    /// Unregistered classes use the default legacy weight of one (1000m).
    pub fn soft_cpu_limit_millicores(&self, class: TaskClass) -> u64 {
        self.inner.soft_cpu_limit_millicores(class)
    }

    /// Dynamically changes the weight of a class. The class's pass is realigned
    /// to the lowest pass among all classes, so historical credit/debt from the
    /// old weight does not skew the new allocation. Queued tasks are
    /// immediately re-dispatched under the new weights.
    pub fn set_weight(&self, class: TaskClass, weight: NonZeroU32) {
        self.inner.set_config(class, ClassConfig::legacy(weight));
    }

    /// Dynamically changes a class's soft CPU limit. Like [`Self::set_weight`],
    /// this realigns the class pass before immediately re-dispatching queued
    /// work. The limit is a relative work-conserving entitlement, not a hard
    /// cap; idle capacity remains borrowable.
    pub fn set_soft_cpu_limit(&self, class: TaskClass, limit: SoftCpuLimit) {
        self.inner.set_config(class, ClassConfig::soft(limit));
    }

    /// Dynamically changes the maximum number of concurrently admitted polls.
    /// Raising it admits queued tasks immediately; lowering it stops further
    /// admissions until active polls drain below the new limit.
    pub fn set_max_concurrent_polls(&self, limit: usize) {
        self.inner.set_max_concurrent_polls(limit);
    }
}

/// A Tokio-like spawn handle bound to one scheduling class.
#[derive(Clone, Debug)]
pub struct Spawner {
    scheduler: Scheduler,
    class: TaskClass,
}

impl Spawner {
    /// Returns this spawner's scheduling class.
    pub const fn class(&self) -> TaskClass {
        self.class
    }

    /// Wraps a future in this spawner's class without spawning it.
    pub fn schedule<F>(&self, future: F) -> Scheduled<F>
    where
        F: Future,
    {
        self.scheduler.schedule_in(self.class, future)
    }

    /// Spawns a future on the current Tokio runtime.
    #[track_caller]
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.scheduler.spawn_in(self.class, future)
    }

    /// Spawns a future on a specific Tokio runtime.
    pub fn spawn_on<F>(&self, handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.scheduler.spawn_in_on(handle, self.class, future)
    }

    /// Spawns a non-`Send` future on the current [`tokio::task::LocalSet`].
    #[track_caller]
    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.scheduler.spawn_local_in(self.class, future)
    }
}

/// A future whose inner wakeups are mediated by a [`Scheduler`].
pub struct Scheduled<F: Future> {
    future: Pin<Box<F>>,
    control: Arc<TaskControl>,
    returned: bool,
}

impl<F: Future> fmt::Debug for Scheduled<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduled")
            .field("class", &self.control.class)
            .field("state", &self.control.state.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl<F: Future> Scheduled<F> {
    fn new(control: Arc<TaskControl>, future: F) -> Self {
        Self {
            future: Box::pin(future),
            control,
            returned: false,
        }
    }
}

impl<F: Future> Future for Scheduled<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Moving Pin<Box<F>> does not move F, so Scheduled<F> is safe to access
        // through get_mut even when F itself is !Unpin.
        let this = self.get_mut();
        this.control.register_executor_waker(cx.waker());

        if !this.control.begin_poll() {
            return Poll::Pending;
        }

        // Mark the start of the admitted poll so real execution time can be
        // charged to this class when the poll returns. With downsampled
        // sampling (sample_every_polls > 1) only every N-th poll in the class
        // stamps a marker; unstamped polls yield exec_time=None and are not
        // charged (their execution time is approximated as zero).
        if this.control.should_sample_poll() {
            *lock(&this.control.poll_started_at) = Some(Instant::now());
        }

        let proxy_waker = Waker::from(this.control.clone());
        let mut proxy_context = Context::from_waker(&proxy_waker);
        let started = lock(&this.control.poll_started_at).take();
        match this.future.as_mut().poll(&mut proxy_context) {
            Poll::Ready(output) => {
                this.control.finish_ready(started.map(|t| t.elapsed()));
                this.returned = true;
                Poll::Ready(output)
            }
            Poll::Pending => {
                this.control.finish_pending(started.map(|t| t.elapsed()));
                Poll::Pending
            }
        }
    }
}

impl<F: Future> Drop for Scheduled<F> {
    fn drop(&mut self) {
        if !self.returned {
            self.control.cancel();
        }
    }
}

/// Per-class counters in a [`SchedulerStats`] snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClassStats {
    /// Configured legacy relative weight.
    ///
    /// A value of zero is a sentinel meaning this class was configured with
    /// the soft CPU limit API. Query [`Scheduler::soft_cpu_limit_millicores`]
    /// for its normalized milli-core entitlement.
    pub weight: u32,
    /// Live queue entries at the time of the snapshot.
    pub queued: usize,
    /// Futures wrapped for this class.
    pub tasks: u64,
    /// Proxy-waker calls received from inner futures.
    pub wakes: u64,
    /// Polls admitted to Tokio.
    pub polls: u64,
    /// Futures that returned normally.
    pub completed: u64,
    /// Futures dropped or aborted before returning.
    pub cancelled: u64,
    /// Cumulative wall time from task creation (scheduler entry) to the task's
    /// first admission (QUEUED -> ADMITTED). This is the scheduler's own
    /// admission-queue delay, excluding Tokio's local/remote queue and any
    /// poll execution time.
    pub total_admission_wait: Duration,
    /// Cumulative real execution time this class's futures spent inside
    /// admitted polls (poll start -> poll return). Aborted or cancelled polls
    /// are deliberately not charged.
    pub total_exec_time: Duration,
    /// Tasks that have been admitted at least once.
    pub admitted: u64,
}

/// A point-in-time scheduler snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchedulerStats {
    /// Maximum admitted polls waiting in or executing on Tokio.
    pub max_concurrent_polls: usize,
    /// Currently admitted polls.
    pub active_polls: usize,
    /// Clock sampling frequency: one start marker is stamped per this many
    /// polls in each class (1 = every poll, the default).
    pub sample_every_polls: usize,
    /// Counters keyed by scheduling class.
    pub classes: BTreeMap<TaskClass, ClassStats>,
}

struct TaskControl {
    scheduler: Arc<SchedulerInner>,
    class: TaskClass,
    state: AtomicU8,
    queued: AtomicBool,
    wake_counter: Arc<AtomicU64>,
    // Per-class poll counter backing downsampled clock sampling. Every task of
    // a class shares the same Arc, so each poll bumps it with a lock-free
    // `fetch_add` — no scheduler lock is touched on the hot path.
    poll_counter: Arc<AtomicU64>,
    executor_waker: Mutex<Option<Waker>>,
    created: Instant,
    queued_at: Mutex<Option<Instant>>,
    poll_started_at: Mutex<Option<Instant>>,
    // Hot-path cache of `sample_every_polls <= 1`: when the scheduler samples
    // every poll, stamping needs no lock on the scheduler state.
    sample_always: bool,
}

impl TaskControl {
    fn register_executor_waker(&self, waker: &Waker) {
        let mut stored = lock(&self.executor_waker);
        if stored
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *stored = Some(waker.clone());
        }
    }

    fn executor_waker(&self) -> Option<Waker> {
        lock(&self.executor_waker).clone()
    }

    /// Decides whether this poll should stamp `poll_started_at`.
    ///
    /// With the default every-poll sampling this is a plain field read. With
    /// downsampling it bumps the class poll counter with a single lock-free
    /// atomic `fetch_add` (the counter Arc is shared by every task of the
    /// class) and samples every N-th poll.
    fn should_sample_poll(&self) -> bool {
        if self.sample_always {
            return true;
        }
        let every = self.scheduler.sample_every_polls;
        debug_assert!(every > 1);
        self.poll_counter.fetch_add(1, Ordering::Relaxed) % every as u64 == 0
    }

    fn begin_poll(self: &Arc<Self>) -> bool {
        loop {
            match self.state.load(Ordering::Acquire) {
                IDLE => {
                    if self
                        .state
                        .compare_exchange(IDLE, QUEUED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.scheduler.enqueue(self);
                        return false;
                    }
                }
                ADMITTED => {
                    if self
                        .state
                        .compare_exchange(ADMITTED, POLLING, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return true;
                    }
                }
                QUEUED | POLLING | POLLING_NOTIFIED | FINISHED => return false,
                state => unreachable!("invalid task state {state}"),
            }
        }
    }

    fn notify(self: &Arc<Self>) {
        self.wake_counter.fetch_add(1, Ordering::Relaxed);
        loop {
            match self.state.load(Ordering::Acquire) {
                IDLE => {
                    if self
                        .state
                        .compare_exchange(IDLE, QUEUED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.scheduler.enqueue(self);
                        return;
                    }
                }
                POLLING => {
                    if self
                        .state
                        .compare_exchange(
                            POLLING,
                            POLLING_NOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                QUEUED | ADMITTED | POLLING_NOTIFIED | FINISHED => return,
                state => unreachable!("invalid task state {state}"),
            }
        }
    }

    fn finish_pending(self: &Arc<Self>, exec_time: Option<Duration>) {
        loop {
            match self.state.load(Ordering::Acquire) {
                POLLING => {
                    if self
                        .state
                        .compare_exchange(POLLING, IDLE, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.scheduler
                            .finish_poll(None, self.class, false, exec_time);
                        return;
                    }
                }
                POLLING_NOTIFIED => {
                    if self
                        .state
                        .compare_exchange(
                            POLLING_NOTIFIED,
                            QUEUED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.scheduler
                            .finish_poll(Some(self), self.class, false, exec_time);
                        return;
                    }
                }
                state => unreachable!("pending task left poll in state {state}"),
            }
        }
    }

    fn finish_ready(self: &Arc<Self>, exec_time: Option<Duration>) {
        let previous = self.state.swap(FINISHED, Ordering::AcqRel);
        debug_assert!(matches!(previous, POLLING | POLLING_NOTIFIED));
        self.scheduler
            .finish_poll(None, self.class, true, exec_time);
    }

    fn cancel(self: &Arc<Self>) {
        let previous = self.state.swap(FINISHED, Ordering::AcqRel);
        if previous == FINISHED {
            return;
        }

        // An aborted poll must not charge exec time: drop the start marker
        // without recording. This asymmetry with poll accounting is deliberate.
        let _poll_started_at = lock(&self.poll_started_at).take();

        let held_admission = matches!(previous, ADMITTED | POLLING | POLLING_NOTIFIED);
        self.scheduler.cancel(self, held_admission);
    }
}

impl Wake for TaskControl {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

struct SchedulerInner {
    state: Mutex<ScheduleState>,
    // Clock sampling frequency (N). Immutable after build, so the poll hot
    // path reads it without any lock.
    sample_every_polls: usize,
    // Per-class poll counters backing downsampled sampling. The map structure
    // changes only when a class first appears (task creation, under the map
    // lock); each task caches its class's Arc and bumps it lock-free.
    poll_counters: Mutex<BTreeMap<TaskClass, Arc<AtomicU64>>>,
}

impl SchedulerInner {
    fn new_task(self: &Arc<Self>, class: TaskClass) -> Arc<TaskControl> {
        // Task creation is infrequent, so the per-class counter is fetched
        // here under the map lock; the returned Arc is then bumped lock-free
        // on every poll (see TaskControl::should_sample_poll).
        let poll_counter = {
            let mut counters = lock(&self.poll_counters);
            counters
                .entry(class)
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone()
        };
        let (wake_counter, sample_always) = {
            let mut state = lock(&self.state);
            let virtual_time = state.virtual_time;
            let sample_always = self.sample_every_polls <= 1;
            let class_state = state.classes.entry(class).or_insert_with(|| {
                ClassState::new(ClassConfig::legacy(NonZeroU32::MIN), virtual_time)
            });
            class_state.stats.tasks += 1;
            (class_state.wakes.clone(), sample_always)
        };

        Arc::new(TaskControl {
            scheduler: self.clone(),
            class,
            state: AtomicU8::new(IDLE),
            queued: AtomicBool::new(false),
            wake_counter,
            poll_counter,
            executor_waker: Mutex::new(None),
            created: Instant::now(),
            queued_at: Mutex::new(None),
            poll_started_at: Mutex::new(None),
            sample_always,
        })
    }

    fn enqueue(&self, task: &Arc<TaskControl>) {
        let wakers = {
            let mut state = lock(&self.state);
            state.enqueue(task);
            state.dispatch()
        };
        wake_all(wakers);
    }

    fn finish_poll(
        &self,
        requeue: Option<&Arc<TaskControl>>,
        class: TaskClass,
        completed: bool,
        exec_time: Option<Duration>,
    ) {
        let wakers = {
            let mut state = lock(&self.state);
            // Read the concurrency before releasing this poll's slot: the exec
            // time it charged was spent sharing the machine with `active`
            // concurrent polls, so the pass increment is divided accordingly.
            let active_at_finish = state.active.max(1);
            state.active = state.active.saturating_sub(1);
            if completed {
                state.class_mut(class).stats.completed += 1;
            }
            if let Some(t) = exec_time {
                let cs = state.class_mut(class);
                cs.pass = cs.pass.saturating_add(pass_increment(
                    t.as_nanos(),
                    cs.entitlement_millicores,
                    active_at_finish,
                ));
                cs.stats.total_exec_time += t;
            }
            if let Some(task) = requeue {
                state.enqueue(task);
            }
            state.dispatch()
        };
        wake_all(wakers);
    }

    fn cancel(&self, task: &TaskControl, held_admission: bool) {
        let wakers = {
            let mut state = lock(&self.state);
            let class = state.class_mut(task.class);
            class.stats.cancelled += 1;
            if task.queued.swap(false, Ordering::AcqRel) {
                class.queued = class.queued.saturating_sub(1);
                if class.queued == 0 {
                    class.queue.clear();
                }
            }
            if held_admission {
                state.active = state.active.saturating_sub(1);
            }
            state.dispatch()
        };
        wake_all(wakers);
    }

    fn stats(&self) -> SchedulerStats {
        let state = lock(&self.state);
        let classes = state
            .classes
            .iter()
            .map(|(class, state)| {
                let mut stats = state.stats.clone();
                stats.queued = state.queued;
                stats.wakes = state.wakes.load(Ordering::Relaxed);
                (*class, stats)
            })
            .collect();

        SchedulerStats {
            max_concurrent_polls: state.max_concurrent_polls,
            active_polls: state.active,
            sample_every_polls: self.sample_every_polls,
            classes,
        }
    }

    fn soft_cpu_limit_millicores(&self, class: TaskClass) -> u64 {
        let state = lock(&self.state);
        state
            .classes
            .get(&class)
            .map_or(1_000, |class_state| class_state.entitlement_millicores)
    }

    fn set_config(&self, class: TaskClass, config: ClassConfig) {
        let wakers = {
            let mut state = lock(&self.state);
            // Align this class's pass with the lowest pass among all classes.
            // Pass scheduling only cares about relative pass values; a class
            // that accumulated a high pass under the old configuration would
            // keep its low priority forever if we left it alone.
            let min_pass = state
                .classes
                .values()
                .map(|c| c.pass)
                .min()
                .unwrap_or(state.virtual_time);
            let class_state = state.class_mut(class);
            class_state.pass = min_pass;
            class_state.entitlement_millicores = config.entitlement_millicores;
            class_state.stats.weight = config.legacy_weight;
            state.dispatch()
        };
        wake_all(wakers);
    }

    fn set_max_concurrent_polls(&self, limit: usize) {
        let wakers = {
            let mut state = lock(&self.state);
            state.max_concurrent_polls = limit;
            state.dispatch()
        };
        wake_all(wakers);
    }
}

struct ScheduleState {
    max_concurrent_polls: usize,
    active: usize,
    virtual_time: u128,
    classes: BTreeMap<TaskClass, ClassState>,
}

impl ScheduleState {
    fn class_mut(&mut self, class: TaskClass) -> &mut ClassState {
        let virtual_time = self.virtual_time;
        self.classes
            .entry(class)
            .or_insert_with(|| ClassState::new(ClassConfig::legacy(NonZeroU32::MIN), virtual_time))
    }

    fn enqueue(&mut self, task: &Arc<TaskControl>) {
        if task.state.load(Ordering::Acquire) != QUEUED || task.queued.swap(true, Ordering::AcqRel)
        {
            return;
        }
        *lock(&task.queued_at) = Some(Instant::now());
        let virtual_time = self.virtual_time;
        let class = self.class_mut(task.class);
        if class.queued == 0 {
            class.pass = class.pass.max(virtual_time);
        }
        class.queued += 1;
        class.queue.push_back(Arc::downgrade(task));
    }

    fn dispatch(&mut self) -> WakeBatch {
        let mut wakers = WakeBatch::default();
        while self.active < self.max_concurrent_polls {
            let Some((class, task)) = self.next_queued() else {
                break;
            };
            let Some(waker) = task.executor_waker() else {
                let _ =
                    task.state
                        .compare_exchange(QUEUED, IDLE, Ordering::AcqRel, Ordering::Acquire);
                continue;
            };
            if task
                .state
                .compare_exchange(QUEUED, ADMITTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let class_state = self
                .classes
                .get_mut(&class)
                .expect("queued class must exist");
            let selected_pass = class_state.pass;
            class_state.stats.polls += 1;
            class_state.stats.admitted += 1;
            // Admission wait is the wall time this task spent in the scheduler
            // queue (queued_at -> ADMITTED), excluding Tokio queueing and
            // poll execution. Fall back to created.elapsed() only if a task
            // reached ADMITTED without a queued_at marker.
            let wait = lock(&task.queued_at)
                .take()
                .map_or_else(|| task.created.elapsed(), |queued_at| queued_at.elapsed());
            class_state.stats.total_admission_wait += wait;
            self.virtual_time = self.virtual_time.max(selected_pass);
            self.active += 1;
            wakers.push(waker);
        }
        wakers
    }

    fn next_queued(&mut self) -> Option<(TaskClass, Arc<TaskControl>)> {
        loop {
            let class = self
                .classes
                .iter()
                .filter(|(_, state)| state.queued > 0)
                .min_by_key(|(class, state)| (state.pass, **class))
                .map(|(class, _)| *class)?;
            let queued = self.classes.get_mut(&class)?.queue.pop_front()?;
            let Some(task) = queued.upgrade() else {
                continue;
            };
            if task.queued.swap(false, Ordering::AcqRel) {
                let class_state = self
                    .classes
                    .get_mut(&class)
                    .expect("queued class must exist");
                class_state.queued = class_state.queued.saturating_sub(1);
                if class_state.queued == 0 {
                    class_state.queue.clear();
                }
                return Some((class, task));
            }
        }
    }
}

struct ClassState {
    pass: u128,
    entitlement_millicores: u64,
    queued: usize,
    queue: VecDeque<Weak<TaskControl>>,
    wakes: Arc<AtomicU64>,
    stats: ClassStats,
}

impl ClassState {
    fn new(config: ClassConfig, pass: u128) -> Self {
        Self {
            pass,
            entitlement_millicores: config.entitlement_millicores,
            queued: 0,
            queue: VecDeque::new(),
            wakes: Arc::new(AtomicU64::new(0)),
            stats: ClassStats {
                weight: config.legacy_weight,

                ..ClassStats::default()
            },
        }
    }
}

/// Computes `floor(elapsed * TIME_SCALE * 1000 / (entitlement * active))`.
///
/// The 1000 milli-core scale is first reduced against the entitlement. Thus a
/// legacy `weight(n)` entitlement of `n * 1000m` becomes exactly the former
/// `floor(elapsed * TIME_SCALE / (n * active))` calculation. Checked
/// multiplication after reduction saturates to `u128::MAX`; this is only used
/// when the mathematical numerator or denominator cannot be represented.
fn pass_increment(
    elapsed_nanos: u128,
    entitlement_millicores: u64,
    active_at_finish: usize,
) -> u128 {
    debug_assert!(entitlement_millicores > 0);
    debug_assert!(active_at_finish > 0);

    let gcd = gcd_u64(entitlement_millicores, 1_000);
    let numerator_factor = u128::from(1_000 / gcd);
    let denominator_factor = u128::from(entitlement_millicores / gcd);
    let denominator = match denominator_factor.checked_mul(active_at_finish as u128) {
        Some(denominator) => denominator,
        None => return u128::MAX,
    };
    let numerator = match elapsed_nanos
        .checked_mul(TIME_SCALE)
        .and_then(|value| value.checked_mul(numerator_factor))
    {
        Some(numerator) => numerator,
        None => return u128::MAX,
    };
    numerator / denominator
}

const fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default)]
struct WakeBatch {
    first: Option<Waker>,
    rest: Vec<Waker>,
}

impl WakeBatch {
    fn push(&mut self, waker: Waker) {
        if self.first.is_none() {
            self.first = Some(waker);
        } else {
            self.rest.push(waker);
        }
    }

    fn wake(self) {
        if let Some(waker) = self.first {
            waker.wake();
        }
        for waker in self.rest {
            waker.wake();
        }
    }
}

fn wake_all(wakers: WakeBatch) {
    wakers.wake();
}

static DEFAULT_SCHEDULER: OnceLock<Scheduler> = OnceLock::new();

/// Returns the process-wide scheduler used by [`spawn`].
pub fn default_scheduler() -> &'static Scheduler {
    DEFAULT_SCHEDULER.get_or_init(Scheduler::default)
}

/// Installs the process-wide scheduler used by [`spawn`].
///
/// This must be called before the first call to [`default_scheduler`] or
/// [`spawn`]. On failure, the supplied scheduler is returned unchanged.
pub fn set_default_scheduler(scheduler: Scheduler) -> Result<(), Scheduler> {
    DEFAULT_SCHEDULER.set(scheduler)
}

/// Spawns a future in the default class on the current Tokio runtime.
///
/// This is a drop-in replacement for [`tokio::spawn`].
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    default_scheduler().spawn(future)
}

/// Spawns a future in `class` on the current Tokio runtime.
#[track_caller]
pub fn spawn_in<F>(class: TaskClass, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    default_scheduler().spawn_in(class, future)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLASS_A: TaskClass = TaskClass::new(1);
    const CLASS_B: TaskClass = TaskClass::new(2);

    /// A future that polls `rounds` times before returning, doing a small
    /// amount of work per poll so elapsed time is measurable.
    struct CountingWork {
        polls: Arc<AtomicU64>,
        rounds: u32,
    }

    impl Future for CountingWork {
        type Output = u64;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            for _ in 0..4_000 {
                std::hint::spin_loop();
            }
            self.polls.fetch_add(1, Ordering::Relaxed);
            if self.rounds == 0 {
                return Poll::Ready(self.polls.load(Ordering::Relaxed));
            }
            self.rounds -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn pass_increment_reduces_millicores_without_overflow_or_panics() {
        for (elapsed, weight, active) in [
            (1, 1, 1),
            (1, u32::MAX, 1),
            (17, 7, 3),
            (u64::MAX as u128, 3, 4),
        ] {
            let old = elapsed
                .checked_mul(TIME_SCALE)
                .expect("representative old numerator fits")
                / (u128::from(weight) * active as u128);
            assert_eq!(
                old,
                pass_increment(elapsed, u64::from(weight) * 1_000, active)
            );
        }

        // 500m reduces to a factor of 2; 1500m reduces to 2 / 3.
        assert_eq!(2 * TIME_SCALE, pass_increment(1, 500, 1));
        assert_eq!(2 * TIME_SCALE / 3, pass_increment(1, 1_500, 1));
        assert_eq!(TIME_SCALE / 3, pass_increment(1, 1_500, 2));
        assert!(pass_increment(1, u64::from(u32::MAX) * 1_000, 1) > 0);

        // Extreme representable inputs deterministically saturate rather than
        // panicking on an intermediate multiplication.
        assert_eq!(u128::MAX, pass_increment(u128::MAX, 1, 1));
        assert_eq!(u128::MAX, pass_increment(u128::MAX, u64::MAX, usize::MAX));
    }

    #[test]
    fn sample_every_polls_defaults_to_one() {
        let scheduler = Scheduler::builder().build();
        assert_eq!(scheduler.stats().sample_every_polls, 1);

        // 0 is normalized to "every poll", same as the default.
        let scheduler = Scheduler::builder().sample_every_polls(0).build();
        assert_eq!(scheduler.stats().sample_every_polls, 1);
    }

    /// With the default every-poll sampling, executed polls are charged:
    /// `total_exec_time` grows past zero, matching the pre-sampling behavior.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_sampling_records_exec_time() {
        let scheduler = Scheduler::builder().max_concurrent_polls(2).build();
        assert_eq!(scheduler.stats().sample_every_polls, 1);

        let polls = Arc::new(AtomicU64::new(0));
        let handle = scheduler.spawn_in(
            CLASS_A,
            CountingWork {
                polls: polls.clone(),
                rounds: 10,
            },
        );
        handle.await.unwrap();

        let stats = scheduler.stats();
        let class = stats.classes.get(&CLASS_A).expect("class must exist");
        assert!(class.total_exec_time > Duration::ZERO);
        assert!(
            class.polls >= 11,
            "expected at least 11 polls, got {}",
            class.polls
        );
    }

    /// Downsampled sampling must not break scheduling: all tasks still
    /// complete, both classes keep making progress, and the configuration is
    /// exposed through `stats()`. Unsampled polls are not charged, but the
    /// sampled ones still record nonzero execution time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn downsampled_scheduler_still_completes_work() {
        let scheduler = Scheduler::builder()
            .sample_every_polls(2)
            .max_concurrent_polls(2)
            .build();
        assert_eq!(scheduler.stats().sample_every_polls, 2);

        let a_polls = Arc::new(AtomicU64::new(0));
        let b_polls = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            handles.push(scheduler.spawn_in(
                CLASS_A,
                CountingWork {
                    polls: a_polls.clone(),
                    rounds: 10,
                },
            ));
            handles.push(scheduler.spawn_in(
                CLASS_B,
                CountingWork {
                    polls: b_polls.clone(),
                    rounds: 10,
                },
            ));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        let stats = scheduler.stats();
        assert_eq!(stats.sample_every_polls, 2);
        let total_polls = a_polls.load(Ordering::Relaxed) + b_polls.load(Ordering::Relaxed);
        assert!(
            a_polls.load(Ordering::Relaxed) * 100 / total_polls >= 20,
            "class A starved: {}/{} polls",
            a_polls.load(Ordering::Relaxed),
            total_polls
        );
        assert!(
            b_polls.load(Ordering::Relaxed) * 100 / total_polls >= 20,
            "class B starved: {}/{} polls",
            b_polls.load(Ordering::Relaxed),
            total_polls
        );

        // Every-other-poll sampling still charges the sampled polls, so at
        // least one class records nonzero execution time.
        let a = stats.classes.get(&CLASS_A).expect("class A must exist");
        let b = stats.classes.get(&CLASS_B).expect("class B must exist");
        assert!(
            a.total_exec_time > Duration::ZERO || b.total_exec_time > Duration::ZERO,
            "downsampled polls must still record some execution time"
        );
    }
}
