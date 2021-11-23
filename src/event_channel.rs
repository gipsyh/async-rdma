use crate::Context;
use rdma_sys::ibv_comp_channel;
use std::{io, ptr::NonNull, sync::Arc};

pub struct EventChannel {
    pub ctx: Arc<Context>,
    pub inner_ec: NonNull<ibv_comp_channel>,
}

impl EventChannel {
    pub(crate) fn as_ptr(&self) -> *const ibv_comp_channel {
        self.inner_ec.as_ptr()
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut ibv_comp_channel {
        self.inner_ec.as_ptr() as _
    }

    pub fn new(ctx: Arc<Context>) -> io::Result<Self> {
        let inner_ec = NonNull::new(unsafe { rdma_sys::ibv_create_comp_channel(ctx.inner_ctx) })
            .ok_or(io::ErrorKind::Other)?;
        Ok(Self { ctx, inner_ec })
    }
}

impl Drop for EventChannel {
    fn drop(&mut self) {
        let errno = unsafe { rdma_sys::ibv_destroy_comp_channel(self.as_mut_ptr()) };
        assert_eq!(errno, 0);
    }
}
