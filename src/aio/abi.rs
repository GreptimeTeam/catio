use libc::{c_int, c_long, c_ulong, timespec};

/// /usr/include/libaio.h
#[link(name = "aio")]
extern "C" {
    pub fn io_setup(maxevents: c_int, ctxp: *mut io_context_t) -> c_int;
    pub fn io_destroy(ctx: io_context_t) -> c_int;
    pub fn io_submit(ctx: io_context_t, nr: c_long, ios: *mut *mut iocb) -> c_int;
    pub fn io_cancel(ctx: io_context_t, iocb: *mut iocb, evt: *mut io_event) -> c_int;
    pub fn io_getevents(
        ctx_id: io_context_t,
        min_nr: c_long,
        nr: c_long,
        events: *mut io_event,
        timeout: *mut timespec,
    ) -> c_int;
}

#[allow(non_camel_case_types)]
type io_context_t = c_ulong;

/// From `linux/include/uabi/linux/aio_abi.h` and
/// https://github.com/torvalds/linux/blob/4f12b742eb2b3a850ac8be7dc4ed52976fc6cb0b/include/uapi/linux/aio_abi.h#L73-L107
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct iocb {
    /// data to be returned in event's data
    pub aio_data: u64,

    // little endian specific
    pub aio_key: u32,
    pub aio_reserved1: u32,

    // common fields
    /// see IOCB_CMD_
    pub aio_lio_opcode: u16,
    pub aio_reqprio: u16,
    pub aio_fildes: u32,

    pub aio_buf: u64,
    pub aio_nbytes: u64,
    pub aio_offset: u64,

    /// extra parameters
    pub aio_reserved2: u64,

    /// flags for the "struct iocb"
    pub aio_flags: u32,

    /// if the IOCB_FLAG_RESFD flag of "aio_flags" is set, this is an
    /// eventfd to signal AIO readiness to
    pub aio_resfd: u32,
}

/// From `linux/include/uabi/linux/aio_abi.h`
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct io_event {
    pub data: u64,
    pub obj: u64,
    pub res: i64,
    pub res2: i64,
}

#[repr(u16)]
#[allow(non_camel_case_types)]
enum io_iocb_cmd {
    IO_CMD_PREAD = 0,
    IO_CMD_PWRITE = 1,

    IO_CMD_FSYNC = 2,
    IO_CMD_FDSYNC = 3,

    IO_CMD_POLL = 5,
    IO_CMD_NOOP = 6,
    IO_CMD_PREADV = 7,
    IO_CMD_PWRITEV = 8,
}

#[cfg(test)]
mod test {
    use std::alloc::{alloc, Layout};
    use std::{os::unix::prelude::AsRawFd, path::Path, ptr};

    use crate::fs::open_file;

    use super::*;

    #[test]
    fn try_write() {
        let tempdir = Path::new("./target");
        let tempfile = tempdir.join("tempfile");

        let file = open_file(tempfile.as_path());
        let fd = file.as_raw_fd();

        let mut io_context = io_context_t::default();

        unsafe {
            io_setup(1024, &mut io_context);
        }

        let buf = unsafe { alloc(Layout::from_size_align(1024, 1024).unwrap()) };
        for i in 0..1024 {
            unsafe {
                *buf.add(i) = 0x23u8;
            }
        }

        let mut write_req = iocb {
            aio_lio_opcode: io_iocb_cmd::IO_CMD_PWRITE as u16,
            aio_fildes: fd as u32,
            aio_buf: buf as u64,
            aio_nbytes: 1024,
            aio_offset: 0,

            // unchanged default values
            aio_data: 0,
            aio_key: 0,
            aio_reserved1: 0,
            aio_reqprio: 0,
            aio_reserved2: 0,
            aio_flags: 0,
            aio_resfd: 0,
        };

        let mut ios: [*mut iocb; 1] = [&mut write_req as _; 1];
        unsafe {
            io_submit(io_context, 1, ios.as_mut_ptr() as *mut *mut iocb);
        }

        let mut events: [io_event; 1] = [io_event {
            data: 0,
            obj: 0,
            res: 0,
            res2: 0,
        }];
        unsafe {
            io_getevents(io_context, 1, 1, events.as_mut_ptr(), ptr::null_mut());
        }
    }
}
