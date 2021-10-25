use crate::*;
use std::{alloc::Layout, io};

pub struct ProtectionDomain<'ctx> {
    pub(super) ctx: &'ctx Context,
    pub(super) inner_pd: *mut rdma_sys::ibv_pd,
}

impl<'ctx> ProtectionDomain<'ctx> {
    pub fn create(ctx: &'ctx Context) -> io::Result<Self> {
        let inner_pd = unsafe { rdma_sys::ibv_alloc_pd(ctx.inner_ctx) };
        if inner_pd.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(ProtectionDomain { ctx, inner_pd })
    }

    pub fn create_queue_pair_builder(&self) -> QueuePairBuilder {
        QueuePairBuilder::new(self)
    }
    pub fn alloc_memory_region(
        &self,
        layout: Layout,
        access: rdma_sys::ibv_access_flags,
    ) -> io::Result<MemoryRegion> {
        MemoryRegion::create(self, layout, access)
    }
}

impl Drop for ProtectionDomain<'_> {
    fn drop(&mut self) {
        let errno = unsafe { rdma_sys::ibv_dealloc_pd(self.inner_pd) };
        assert_eq!(errno, 0);
    }
}
