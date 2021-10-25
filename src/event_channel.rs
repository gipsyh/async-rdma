use crate::*;

pub struct EventChannel<'ctx> {
    pub ctx: &'ctx Context,
    pub inner_ec: *mut rdma_sys::ibv_comp_channel,
}

impl Drop for EventChannel<'_> {
    fn drop(&mut self) {
        let errno = unsafe { rdma_sys::ibv_destroy_comp_channel(self.inner_ec) };
        assert_eq!(errno, 0);
    }
}
