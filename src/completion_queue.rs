use crate::{context::Context, event_channel::EventChannel};
use libc::c_void;
use rdma_sys::{ibv_create_cq, ibv_destroy_cq};
use std::{io, marker::PhantomData, ptr};

pub struct CompletionQueue<'ec> {
    _phantom: PhantomData<&'ec ()>,
    pub(super) inner_cq: *mut rdma_sys::ibv_cq,
}

impl CompletionQueue<'_> {
    pub fn create(ctx: &Context, cq_size: u32, ec: Option<&EventChannel>) -> io::Result<Self> {
        let ec = match ec {
            Some(ec) => ec.inner_ec,
            _ => ptr::null::<c_void>() as *mut _,
        };
        let inner_cq =
            unsafe { ibv_create_cq(ctx.inner_ctx, cq_size as i32, std::ptr::null_mut(), ec, 0) };
        if inner_cq.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(CompletionQueue {
            _phantom: PhantomData,
            inner_cq,
        })
    }
}

impl Drop for CompletionQueue<'_> {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_cq(self.inner_cq) };
        assert_eq!(errno, 0);
    }
}
