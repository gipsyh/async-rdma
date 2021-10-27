use crate::*;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{alloc::Layout, io, sync::Arc};
#[allow(unused)]
pub struct MemoryRegion {
    _pd: Arc<ProtectionDomain>,
    data: Vec<u8>,
    pub(super) inner_mr: *mut rdma_sys::ibv_mr,
}

impl MemoryRegion {
    pub fn new(pd: &Arc<ProtectionDomain>, layout: Layout) -> io::Result<Self> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        MemoryRegion::new_from_pd(pd, layout, access)
    }

    pub fn remote_mr(&self) -> RemoteMemoryRegion {
        RemoteMemoryRegion {
            addr: self.addr() as _,
            len: self.length(),
            rkey: self.rkey(),
        }
    }

    pub fn rkey(&self) -> u32 {
        unsafe { *self.inner_mr }.rkey
    }
}

impl RdmaMemory for MemoryRegion {
    fn addr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn length(&self) -> usize {
        self.data.len()
    }
}

impl RdmaLocalMemory for MemoryRegion {
    fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self>
    where
        Self: Sized,
    {
        let data = vec![0_u8; layout.size()];
        let inner_mr = unsafe {
            rdma_sys::ibv_reg_mr(
                pd.inner_pd,
                data.as_ptr() as *mut _,
                data.len(),
                access.0 as i32,
            )
        };
        if inner_mr.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(MemoryRegion {
            _pd: pd.clone(),
            data,
            inner_mr,
        })
    }

    fn lkey(&self) -> u32 {
        unsafe { *self.inner_mr }.lkey
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        let rc = unsafe { rdma_sys::ibv_dereg_mr(self.inner_mr) };
        assert_eq!(rc, 0);
    }
}

#[derive(Serialize, Deserialize)]
pub struct RemoteMemoryRegion {
    pub addr: usize,
    pub len: usize,
    pub rkey: u32,
}

impl RdmaMemory for RemoteMemoryRegion {
    fn addr(&self) -> *const u8 {
        self.addr as *const u8
    }

    fn length(&self) -> usize {
        self.len
    }
}

impl RdmaRemoteMemory for RemoteMemoryRegion {
    fn rkey(&self) -> u32 {
        self.rkey
    }
}
