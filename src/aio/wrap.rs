pub(super) use crate::aio::abi::io_set_eventfd;
use crate::aio::abi::{
    io_cancel as io_cancel_sys, io_destroy as io_destroy_sys, io_getevents as io_getevents_sys,
    io_setup as io_setup_sys, io_submit as io_submit_sys, IoContext, IoEvent, Iocb,
};
use crate::error::{CatioError, Result};
use libc::{c_int, c_long};
use nix::errno;

fn wrap_syscall(result: c_int) -> Result<i32> {
    if result >= 0 {
        Ok(result)
    } else {
        Err(CatioError::Syscall(errno::from_i32(result)))
    }
}

pub(super) unsafe fn io_cancel(ctx: IoContext, iocb: *mut Iocb, evt: *mut IoEvent) -> Result<i32> {
    wrap_syscall(io_cancel_sys(ctx, iocb, evt))
}

pub(super) unsafe fn io_destroy(ctx: IoContext) -> Result<i32> {
    wrap_syscall(io_destroy_sys(ctx))
}

pub(super) unsafe fn io_getevents(
    ctx: IoContext,
    min_nr: c_long,
    nr: c_long,
    events: *mut IoEvent,
    timeout: *mut libc::timespec,
) -> Result<i32> {
    wrap_syscall(io_getevents_sys(ctx, min_nr, nr, events, timeout))
}

pub(super) unsafe fn io_setup(nr_events: c_int, ctx: *mut IoContext) -> Result<i32> {
    wrap_syscall(io_setup_sys(nr_events, ctx))
}

pub(super) unsafe fn io_submit(ctx: IoContext, nr: c_long, iocb: *mut *mut Iocb) -> Result<i32> {
    wrap_syscall(io_submit_sys(ctx, nr, iocb))
}
