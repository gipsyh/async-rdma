use crate::*;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{alloc::Layout, marker::PhantomData, ops::Deref, ptr::NonNull, sync::Arc};

pub struct RdmaLocalBox<T> {
    mr: MemoryRegion,
    data: NonNull<T>,
}

impl<T> RdmaLocalBox<T> {
    pub fn new(pd: &Arc<ProtectionDomain>, x: T) -> Self {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let mr = pd.alloc_memory_region(Layout::new::<T>(), access).unwrap();
        let ptr = unsafe { *mr.inner_mr }.addr as *mut T;
        unsafe { ptr.write(x) };
        let data = NonNull::new(ptr).unwrap();
        RdmaLocalBox { mr, data }
    }

    pub fn new_with_zerod(pd: &Arc<ProtectionDomain>) -> Self {
        let x = unsafe { std::mem::zeroed::<T>() };
        Self::new(pd, x)
    }

    pub fn ptr(&self) -> *mut T {
        self.data.as_ptr()
    }

    pub fn len(&self) -> usize {
        self.mr.len()
    }

    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    pub fn remote_box(&self) -> RdmaRemoteBox {
        RdmaRemoteBox {
            _phantom: PhantomData,
            ptr: self.ptr() as _,
            len: self.len(),
            rkey: self.mr.rkey(),
        }
    }
}

impl<T> Deref for RdmaLocalBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.data.as_ref() }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RdmaRemoteBox<'b> {
    _phantom: PhantomData<&'b ()>,
    pub ptr: usize,
    pub len: usize,
    pub rkey: u32,
}

// enum RdmaBoxType<'a, T> {
//     Local(RdmaLocalBox<'a, T>),
//     Remote(RdmaRemoteBox<'a>),
// }

// struct RdmaBox<'a, T> {
//     default_qp: Option<&'a QueuePair<'a>>,
//     box_type: RdmaBoxType<'a, T>,

// }
