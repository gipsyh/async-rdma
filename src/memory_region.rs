use crate::*;
use rdma_sys::ibv_access_flags;
use std::{
    alloc::{Allocator, Global, Layout},
    io,
};

pub struct MemoryRegion<'pd> {
    _pd: &'pd ProtectionDomain<'pd>,
    pub(super) inner_mr: *mut rdma_sys::ibv_mr,
}

impl<'pd> MemoryRegion<'pd> {
    pub fn create(
        pd: &'pd ProtectionDomain,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self> {
        let ptr = Global.allocate(layout).unwrap();
        let inner_mr = unsafe {
            rdma_sys::ibv_reg_mr(
                pd.inner_pd,
                ptr.as_ptr() as *mut _,
                ptr.len(),
                access.0 as i32,
            )
        };
        if inner_mr.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(MemoryRegion { _pd: pd, inner_mr })
    }

    pub fn len(&self) -> usize {
        unsafe { *self.inner_mr }.length
    }

    pub fn lkey(&self) -> u32 {
        unsafe { *self.inner_mr }.lkey
    }

    pub fn rkey(&self) -> u32 {
        unsafe { *self.inner_mr }.rkey
    }
}

impl Drop for MemoryRegion<'_> {
    fn drop(&mut self) {
        let rc = unsafe { rdma_sys::ibv_dereg_mr(self.inner_mr) };
        assert_eq!(rc, 0);
    }
}
