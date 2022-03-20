use std::{
    future::Future, marker::PhantomData, os::unix::prelude::RawFd, pin::Pin, ptr, task::Poll,
};

use super::{
    abi::{IoContext, IoEvent, Iocb},
    wrap::{io_getevents, io_setup},
};
use crate::{
    aio::{abi::IocbCmd, wrap::io_submit},
    error::Result,
};
use libc::c_int;
use nix::sys::{epoll::EpollEvent, eventfd};

const NR_EVENTS: c_int = 4096;
/// Default event buffer size for [Context]'s `io_events` and `epoll_events`.
const EVENT_BUF_SIZE: usize = 4096;

#[derive(Default)]
pub struct ContextBuilder {}

impl ContextBuilder {
    pub fn build(self) -> Result<Context> {
        let mut ctx = IoContext::default();
        unsafe {
            io_setup(NR_EVENTS, &mut ctx)?;
        }

        let eventfd = create_eventfd()?;
        let ctx = Context {
            ctx,
            eventfd,
            io_events: [IoEvent::empty(); 4096],
            epoll_events: [EpollEvent::empty(); 4096],
        };
        ctx.start_poll();

        Ok(ctx)
    }
}

pub struct Context {
    ctx: IoContext,
    eventfd: RawFd,
    io_events: [IoEvent; EVENT_BUF_SIZE],
    epoll_events: [EpollEvent; EVENT_BUF_SIZE],
}

impl Context {
    fn start_poll(&self) {
        let ctx = self.ctx;
        let io_events_buf = self.io_events.as_ptr() as usize;
        let epoll_events_buf = self.epoll_events.as_ptr() as usize;
        std::thread::spawn(move || {
            let io_events_buf = io_events_buf as *mut IoEvent;
            let epoll_event_buf = epoll_events_buf as *mut EpollEvent;
            loop {
                // harvest io events
                let result = unsafe {
                    io_getevents(
                        ctx,
                        1,
                        EVENT_BUF_SIZE as i64,
                        io_events_buf,
                        ptr::null_mut(),
                    )
                };
                let num_events = match result {
                    Ok(num) => num,
                    Err(e) => {
                        println!("io_getevents failed with {:?}", e);
                        continue;
                    }
                };

                // process io events
                // set corresponding task's state to [TaskState::Done].
                for index in 0..num_events as usize {
                    let io_event = unsafe { io_events_buf.add(index).read() };
                    let task_ptr = io_event.data() as *mut TaskInner;
                    unsafe {
                        match (*task_ptr).state {
                            TaskState::Allocated | TaskState::Polled => {
                                (*task_ptr).state = TaskState::Done
                            }
                            TaskState::Done | TaskState::Cancelled | TaskState::Finished => {
                                unreachable!("aio task done before harvest")
                            }
                        }
                    }
                }
            }
        });
    }

    fn submit_op(&self, op: AioOp) -> Result<()> {
        let task = self.make_iocb(op);
        self.submit_iocb(task)
    }

    fn submit_iocb(&self, mut iocb: Iocb) -> Result<()> {
        let mut tasks: [*mut Iocb; 1] = [&mut iocb as _; 1];
        unsafe { io_submit(self.ctx, 1, tasks.as_mut_ptr() as *mut *mut Iocb)? };

        Ok(())
    }

    fn make_iocb(&self, op: AioOp) -> Iocb {
        let (op, fd, buf, offset, size) = match op {
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
            // aio_resfd: self.eventfd as u32,
            aio_resfd: 0,
        }
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

#[cfg(test)]
fn issue_noop(ctx: &Context) -> Result<Task<()>> {
    let op = AioOp::Noop;
    let task = Task::<()>::new();

    let mut iocb = ctx.make_iocb(op);
    iocb.aio_data = task.inner_u64();
    println!("noop iocb to submit: {:?}", iocb);
    println!("libaio context: {:?}", ctx.ctx);
    ctx.submit_iocb(iocb)?;

    Ok(task)
}

/// Wrap over the inner's raw pointer.
crate struct Task<T: Sized> {
    inner: *mut TaskInner,
    _phantom: PhantomData<T>,
}

impl<T: Sized> Task<T> {
    fn new() -> Self {
        let inner = TaskInner {
            state: TaskState::Allocated,
        };
        let inner_ptr = Box::into_raw(Box::new(inner));

        Self {
            inner: inner_ptr,
            _phantom: PhantomData {},
        }
    }

    fn inner_u64(&self) -> u64 {
        self.inner as *mut () as u64
    }

    fn state(&self) -> &TaskState {
        unsafe { &(*self.inner).state }
    }
}

impl<T: Sized> Future for Task<T> {
    // todo: type Output = T;
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.state() {
            TaskState::Allocated => Poll::Pending,
            TaskState::Polled => Poll::Pending,
            // TaskState::Done => todo!("return the result"),
            TaskState::Done => Poll::Ready(()),
            TaskState::Finished => todo!("fail this future"),
            TaskState::Cancelled => todo!("fail this future"),
        }
    }
}

crate struct TaskInner {
    // todo: this should provides two access ways for `Send` and `!Send`.
    // (atomic and normal).
    state: TaskState,
}

crate enum TaskState {
    /// The task is allocated but hasn't been polled yet.
    Allocated,
    /// The task is polled.
    Polled,
    /// Task done but the future is not finished.
    Done,
    /// The future is finished.
    Finished,
    /// The task is cancelled.
    Cancelled,
}

#[cfg(test)]
mod test {
    use std::alloc::alloc;
    use std::{alloc::Layout, os::unix::prelude::AsRawFd, path::Path};

    use crate::fs::open_file;

    use super::*;

    #[test]
    #[ignore = "submit noop will fail with InvalidArgument"]
    fn tokio_poll_noop() {
        let ctx = ContextBuilder {}.build().unwrap();
        let task = issue_noop(&ctx).unwrap();

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        tokio_rt.block_on(task);
    }

    #[test]
    fn tokio_poll_write() {
        let tempdir = Path::new("./target");
        let tempfile = tempdir.join("tempfile");
        let file = open_file(tempfile.as_path());
        let fd = file.as_raw_fd();

        let write_buf = unsafe { alloc(Layout::from_size_align(1024, 1024).unwrap()) };
        for i in 0..1024 {
            unsafe {
                *write_buf.add(i) = 0x23u8;
            }
        }

        let ctx = ContextBuilder {}.build().unwrap();

        let write_op = AioOp::Write(fd, write_buf, 0, 1024);
        let task = Task::<()>::new();
        let mut iocb = ctx.make_iocb(write_op);
        iocb.aio_data = task.inner_u64();

        ctx.submit_iocb(iocb).unwrap();

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        tokio_rt.block_on(task);
    }
}
