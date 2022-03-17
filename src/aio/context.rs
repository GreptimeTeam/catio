use std::{os::unix::prelude::RawFd, ptr};

use super::{
    abi::{IoContext, Iocb},
    wrap::io_setup,
};
use crate::{
    aio::{abi::IocbCmd, wrap::io_submit},
    error::Result,
};
use libc::c_int;
use nix::sys::eventfd;

const NR_EVENTS: c_int = 1024;

#[derive(Default)]
pub struct ContextBuilder {}

impl ContextBuilder {
    pub fn build(self) -> Result<Context> {
        let mut ctx = IoContext::default();
        unsafe {
            io_setup(NR_EVENTS, &mut ctx)?;
        }

        let eventfd = create_eventfd()?;
        Ok(Context { ctx, eventfd })
    }
}

pub struct Context {
    ctx: IoContext,
    eventfd: RawFd,
}

impl Context {
    fn submit_task(&self, op: AioOp) -> Result<()> {
        let mut task = op.into_iocb(self.eventfd);
        let mut tasks: [*mut Iocb; 1] = [&mut task as _; 1];
        unsafe { io_submit(self.ctx, 1, tasks.as_mut_ptr() as *mut *mut Iocb)? };

        Ok(())
    }
}

fn create_eventfd() -> Result<RawFd> {
    let fd = eventfd::eventfd(
        0,
        eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK,
    )?;

    Ok(fd)
}

/// Enum to describe AIO operations. `IO_CMD_POLL`, `IO_CMD_PREADV` and `IO_CMD_PWRITEV` are not included.
crate enum AioOp {
    /// Positioned read. Corresponds to `IO_CMD_PREAD`.
    ///
    /// Parameters are: fd to read, read buffer, offset, size.
    Read(RawFd, *mut u8, u64, u64),
    /// Positioned write. Corresponds to `IO_CMD_PWRITE`.
    ///
    /// Parameters are: fd to write, write buffer, offset, size.
    Write(RawFd, *mut u8, u64, u64),
    /// File sync. Corresponds to `IO_CMD_FSYNC`.
    ///
    /// Takes the fd to sync.
    Fsync(RawFd),
    /// File data sync. Corresponds to `IO_CMD_FDSYNC`.
    ///
    /// Takes the fd to sync.
    Fdatasync(RawFd),
    /// Empty operation for test usage. Corresponds to `IO_CMD_NOOP`.
    #[cfg(test)]
    Noop,
}

impl AioOp {
    fn into_iocb(self, event_fd: RawFd) -> Iocb {
        let (op, fd, buf, offset, size) = match self {
            AioOp::Read(fd, buf, offset, size) => (IocbCmd::IO_CMD_PREAD, fd, buf, offset, size),
            AioOp::Write(fd, buf, offset, size) => (IocbCmd::IO_CMD_PWRITE, fd, buf, offset, size),
            AioOp::Fsync(fd) => (IocbCmd::IO_CMD_FSYNC, fd, ptr::null_mut(), 0, 0),
            AioOp::Fdatasync(fd) => (IocbCmd::IO_CMD_FDSYNC, fd, ptr::null_mut(), 0, 0),
            #[cfg(test)]
            AioOp::Noop => (IocbCmd::IO_CMD_NOOP, 0, ptr::null_mut(), 0, 0),
        };

        Iocb {
            aio_lio_opcode: op as u16,
            aio_fildes: fd as u32,
            aio_buf: buf as u64,
            aio_nbytes: size,
            aio_offset: offset,

            // unchanged default values
            aio_data: 0,
            aio_key: 0,
            aio_reserved1: 0,
            aio_reqprio: 0,
            aio_reserved2: 0,
            aio_flags: 0,
            aio_resfd: event_fd as u32,
        }
    }
}
