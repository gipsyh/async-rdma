use crate::*;
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    io,
    ops::Range,
    ptr::NonNull,
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;

struct Node {
    fa: Arc<MemoryRegion>,
    root: Arc<MemoryRegion>,
}

struct LocalRoot {
    inner_mr: NonNull<ibv_mr>,
    _pd: Arc<ProtectionDomain>,
    data: Vec<u8>,
}

unsafe impl Send for LocalRoot {}

unsafe impl Sync for LocalRoot {}

struct RemoteRoot {
    token: MemoryRegionRemoteToken,
    agent: Arc<Agent>,
}

enum Kind {
    LocalRoot(LocalRoot),
    LocalNode(Node),
    RemoteRoot(RemoteRoot),
    RemoteNode(Node),
}

pub struct MemoryRegion {
    addr: usize,
    length: usize,
    key: u32,
    kind: Kind,
    sub: Mutex<Vec<Range<usize>>>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MemoryRegionRemoteToken {
    pub addr: usize,
    pub length: usize,
    pub rkey: u32,
}

impl MemoryRegion {
    pub fn is_node(&self) -> bool {
        matches!(self.kind, Kind::LocalNode(_) | Kind::RemoteNode(_))
    }

    pub fn is_local(&self) -> bool {
        matches!(self.kind, Kind::LocalRoot(_) | Kind::LocalNode(_))
    }

    fn is_leaf(&self) -> bool {
        self.sub.lock().unwrap().len() == 0
    }

    fn root(self: &Arc<Self>) -> Arc<MemoryRegion> {
        match &self.kind {
            Kind::LocalNode(node) | Kind::RemoteNode(node) => node.root.clone(),
            _ => self.clone(),
        }
    }

    pub fn new(pd: &Arc<ProtectionDomain>, layout: Layout) -> io::Result<Self> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        MemoryRegion::new_from_pd(pd, layout, access)
    }

    pub fn remote_token(&self) -> MemoryRegionRemoteToken {
        MemoryRegionRemoteToken {
            addr: self.addr() as _,
            length: self.length(),
            rkey: unsafe { *self.inner_mr() }.rkey,
        }
    }

    pub fn from_remote_token(token: MemoryRegionRemoteToken, agent: Arc<Agent>) -> Self {
        Self {
            addr: token.addr,
            length: token.length,
            key: token.rkey,
            kind: Kind::RemoteRoot(RemoteRoot { token, agent }),
            sub: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn inner_mr(&self) -> *mut ibv_mr {
        if let Kind::LocalRoot(lroot) = &self.kind {
            lroot.inner_mr.as_ptr()
        } else {
            panic!()
        }
    }

    pub fn slice(self: &mut Arc<Self>, range: Range<usize>) -> Result<MemoryRegion, String> {
        if range.start >= range.end || range.end > self.length {
            return Err("Invalid Range".to_string());
        }
        if !self
            .sub
            .lock()
            .unwrap()
            .iter()
            .all(|sub_range| range.end <= sub_range.start || range.start >= sub_range.end)
        {
            return Err("No Enough Memory".to_string());
        }
        self.sub.lock().unwrap().push(range.clone());
        self.sub
            .lock()
            .unwrap()
            .sort_by(|a, b| a.start.cmp(&b.start));
        let new_node = Node {
            fa: self.clone(),
            root: self.root(),
        };
        let kind = if self.is_local() {
            Kind::LocalNode(new_node)
        } else {
            Kind::RemoteNode(new_node)
        };
        Ok(MemoryRegion {
            addr: self.addr + range.start,
            length: range.len(),
            key: self.key,
            kind,
            sub: Mutex::new(Vec::new()),
        })
    }

    pub fn alloc(self: &mut Arc<Self>, layout: Layout) -> Result<MemoryRegion, String> {
        let range = {
            let mut last = 0;
            let mut ans = Err("No Enough Memory");
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
}

impl RdmaMemory for MemoryRegion {
    fn addr(&self) -> *const u8 {
        assert!(self.is_leaf());
        self.addr as *const u8
    }

    fn length(&self) -> usize {
        assert!(self.is_leaf());
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
        let inner_mr = NonNull::new(unsafe {
            ibv_reg_mr(pd.as_ptr(), data.as_ptr() as _, data.len(), access.0 as _)
        })
        .ok_or_else(io::Error::last_os_error)?;
        Ok(MemoryRegion {
            addr: data.as_ptr() as _,
            length: data.len(),
            key: unsafe { *inner_mr.as_ptr() }.lkey,
            kind: Kind::LocalRoot(LocalRoot {
                inner_mr,
                _pd: pd.clone(),
                data,
            }),
            sub: Mutex::new(Vec::new()),
        })
    }

    fn lkey(&self) -> u32 {
        assert!(self.is_leaf());
        assert!(self.is_local());
        self.key
    }
}

impl RdmaRemoteMemory for MemoryRegion {
    fn rkey(&self) -> u32 {
        assert!(self.is_leaf());
        // assert!(!self.is_local());
        self.key
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        assert_eq!(self.sub.lock().unwrap().len(), 0);
        match &self.kind {
            Kind::LocalRoot(_root) => {
                let errno = unsafe { ibv_dereg_mr(self.inner_mr()) };
                assert_eq!(errno, 0);
            }
            Kind::LocalNode(node) | Kind::RemoteNode(node) => {
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
            Kind::RemoteRoot(root) => {
                Runtime::new()
                    .unwrap()
                    .block_on(root.agent.release_mr(root.token))
                    .unwrap();
            }
        }
    }
}

unsafe impl Sync for MemoryRegion {}

unsafe impl Send for MemoryRegion {}

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
        let _sub = mr.slice(0..64).err().unwrap();
        let _sub = mr.slice(64..128).unwrap();
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
        let _sub = mr.alloc(Layout::from_size_align(128, 8).unwrap()).unwrap();
        let _sub = mr
            .alloc(Layout::from_size_align(128, 8).unwrap())
            .err()
            .unwrap();
        Ok(())
    }

    // #[test]
    // fn server() -> io::Result<()> {
    //     let rdma = RdmaBuilder::default().build()?;

    //     let (stream, _) = std::net::TcpListener::bind("127.0.0.1:8000")?.accept()?;
    //     let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    //     bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();

    //     rdma.handshake(remote)?;
    //     let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    //     let token: MemoryRegionRemoteToken = bincode::deserialize_from(&stream).unwrap();
    //     let remote_mr = MemoryRegion::from_remote_token(token);
    //     rdma.qp.write(&local_box, &remote_mr)?;
    //     Ok(())
    // }

    // #[test]
    // fn client() -> io::Result<()> {
    //     let rdma = RdmaBuilder::default().build()?;

    //     let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    //     bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    //     let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();

    //     rdma.handshake(remote)?;
    //     let local_mr = Arc::new(MemoryRegion::new(&rdma.pd, Layout::new::<[i32; 4]>()).unwrap());
    //     bincode::serialize_into(&stream, &local_mr.remote_token()).unwrap();
    //     std::thread::sleep(std::time::Duration::from_secs(1));
    //     dbg!(unsafe { *(local_mr.addr() as *mut i32) });
    //     Ok(())
    // }
}
