use crate::*;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    io,
    ops::Range,
    sync::{Arc, Mutex},
};

struct Root {
    inner_mr: *mut rdma_sys::ibv_mr,
    _pd: Arc<ProtectionDomain>,
    data: Vec<u8>,
}

struct Node {
    fa: Arc<MemoryRegion>,
}

enum MemoryRegionType {
    Root(Root),
    Node(Node),
}

pub struct MemoryRegion {
    addr: usize,
    length: usize,
    mr_type: MemoryRegionType,
    sub: Mutex<Vec<Range<usize>>>,
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

    pub(super) fn inner_mr(&self) -> *mut rdma_sys::ibv_mr {
        if let MemoryRegionType::Root(root) = &self.mr_type {
            root.inner_mr
        } else {
            panic!()
        }
    }

    pub fn slice(self: &mut Arc<Self>, range: Range<usize>) -> Result<MemoryRegion, ()> {
        if range.start >= range.end || range.end > self.length {
            return Err(());
        }
        if !self
            .sub
            .lock()
            .unwrap()
            .iter()
            .all(|sub_range| range.end <= sub_range.start || range.start >= sub_range.end)
        {
            return Err(());
        }
        self.sub.lock().unwrap().push(range.clone());
        self.sub
            .lock()
            .unwrap()
            .sort_by(|a, b| a.start.cmp(&b.start));
        Ok(MemoryRegion {
            addr: self.addr + range.start,
            length: range.len(),
            mr_type: MemoryRegionType::Node(Node { fa: self.clone() }),
            sub: Mutex::new(Vec::new()),
        })
    }

    pub fn alloc(self: &mut Arc<Self>, layout: Layout) -> Result<MemoryRegion, ()> {
        let range = {
            let mut last = 0;
            let mut ans = Err(());
            for range in self.sub.lock().unwrap().iter() {
                if last + layout.size() <= range.start {
                    ans = Ok(last..last + layout.size());
                    break;
                }
                last = range.end
            }
            if last + layout.size() <= self.length {
                ans = Ok(last..last + layout.size());
            }
            ans?
        };
        self.slice(range)
    }

    pub fn rkey(&self) -> u32 {
        unsafe { *self.inner_mr() }.rkey
    }
}

impl RdmaMemory for MemoryRegion {
    fn addr(&self) -> *const u8 {
        self.addr as *const u8
    }

    fn length(&self) -> usize {
        self.length
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
            addr: data.as_ptr() as _,
            length: data.len(),
            mr_type: MemoryRegionType::Root(Root {
                inner_mr,
                _pd: pd.clone(),
                data,
            }),
            sub: Mutex::new(Vec::new()),
        })
    }

    fn lkey(&self) -> u32 {
        unsafe { *self.inner_mr() }.lkey
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        assert_eq!(self.sub.lock().unwrap().len(), 0);
        match &self.mr_type {
            MemoryRegionType::Root(root) => {
                let errno = unsafe { rdma_sys::ibv_dereg_mr(self.inner_mr()) };
                assert_eq!(errno, 0);
            }
            MemoryRegionType::Node(node) => {
                let index = node
                    .fa
                    .sub
                    .lock()
                    .unwrap()
                    .iter()
                    .position(|x| {
                        (self.addr - node.fa.addr..self.length + self.addr - node.fa.addr) == *x
                    })
                    .unwrap();
                node.fa.sub.lock().unwrap().remove(index);
            }
        }
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

#[cfg(test)]
mod tests {
    use crate::*;
    #[test]
    fn mr_slice() -> io::Result<()> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let ctx = Arc::new(Context::open(None)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut mr =
            Arc::new(pd.alloc_memory_region(Layout::from_size_align(128, 8).unwrap(), access)?);
        let sub = mr.slice(0..64).unwrap();
        assert_eq!(mr.addr(), sub.addr());
        assert_eq!(sub.length(), 64);
        let sub = mr.slice(0..64).err().unwrap();
        let sub = mr.slice(64..128).unwrap();
        Ok(())
    }

    #[test]
    fn mr_alloc() -> io::Result<()> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let ctx = Arc::new(Context::open(None)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut mr =
            Arc::new(pd.alloc_memory_region(Layout::from_size_align(128, 8).unwrap(), access)?);
        let sub = mr.alloc(Layout::from_size_align(128, 8).unwrap()).unwrap();
        drop(sub);
        let sub = mr.alloc(Layout::from_size_align(128, 8).unwrap()).unwrap();
        let sub = mr
            .alloc(Layout::from_size_align(128, 8).unwrap())
            .err()
            .unwrap();
        Ok(())
    }
}
