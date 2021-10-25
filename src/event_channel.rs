use crate::*;
use std::sync::Arc;

pub struct EventChannel {
    pub ctx: Arc<Context>,
    pub inner_ec: *mut rdma_sys::ibv_comp_channel,
}

impl Drop for EventChannel {
    fn drop(&mut self) {
        let errno = unsafe { rdma_sys::ibv_destroy_comp_channel(self.inner_ec) };
        assert_eq!(errno, 0);
    }
}
