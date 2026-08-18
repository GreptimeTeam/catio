//! Cooperative, policy-controlled task scheduling on top of Tokio.
//!
//! [`spawn`] has the same signature and return type as [`tokio::spawn`]. Use an
//! explicit [`Scheduler`] when tasks need different scheduling classes.

#[cfg(feature = "linux-aio")]
pub mod aio;
#[cfg(feature = "linux-aio")]
mod error;
#[cfg(feature = "linux-aio")]
mod fs;
pub mod task;

#[cfg(test)]
mod admission_wait_tests;

pub use task::{
    default_scheduler, set_default_scheduler, spawn, spawn_in, ClassStats, Scheduled, Scheduler,
    SchedulerBuilder, SchedulerStats, SoftCpuLimit, Spawner, TaskClass,
};
