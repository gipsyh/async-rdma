use crate::*;
use std::{alloc::Layout, io, sync::Arc};

pub struct ProtectionDomain {
    pub(super) ctx: Arc<Context>,
    pub(super) inner_pd: *mut rdma_sys::ibv_pd,
}

impl ProtectionDomain {
    pub fn create(ctx: &Arc<Context>) -> io::Result<Self> {
        let inner_pd = unsafe { rdma_sys::ibv_alloc_pd(ctx.inner_ctx) };
        if inner_pd.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(ProtectionDomain {
            ctx: ctx.clone(),
            inner_pd,
        })
    }

    pub fn create_queue_pair_builder(self: &Arc<Self>) -> QueuePairBuilder {
        QueuePairBuilder::new(self)
    }

    pub fn alloc_memory_region(
        self: &Arc<Self>,
        layout: Layout,
        access: rdma_sys::ibv_access_flags,
    ) -> io::Result<MemoryRegion> {
        MemoryRegion::new_from_pd(self, layout, access)
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let errno = unsafe { rdma_sys::ibv_dealloc_pd(self.inner_pd) };
        assert_eq!(errno, 0);
    }
}

unsafe impl Send for ProtectionDomain {}

unsafe impl Sync for ProtectionDomain {}
