use super::{abi::IoContext, wrap::io_setup};
use crate::error::Result;
use libc::c_int;

const NR_EVENTS: c_int = 1024;

#[derive(Default)]
pub struct ContextBuilder {}

impl ContextBuilder {
    pub fn build(self) -> Result<Context> {
        let mut ctx = IoContext::default();
        unsafe {
            io_setup(NR_EVENTS, &mut ctx)?;
        }
        Ok(Context { ctx })
    }
}

pub struct Context {
    ctx: IoContext,
}

impl Context {}
