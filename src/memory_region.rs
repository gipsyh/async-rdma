use crate::{Agent, ProtectionDomain};
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    io,
    ops::Range,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MemoryRegionToken {
    pub addr: usize,
    pub len: usize,
    pub rkey: u32,
}

pub trait LocalRemoteMR {
    fn rkey(&self) -> u32;
}

struct Node<T: LocalRemoteMR> {
    fa: Arc<MemoryRegion<T>>,
    root: Arc<MemoryRegion<T>>,
}

enum MemoryRegionKind<T: LocalRemoteMR> {
    Root(T),
    Node(Node<T>),
}

impl<T: LocalRemoteMR> MemoryRegionKind<T> {
    fn rkey(&self) -> u32 {
        match self {
            MemoryRegionKind::Root(root) => root.rkey(),
            MemoryRegionKind::Node(node) => node.root.rkey(),
        }
    }
}

pub struct MemoryRegion<T: LocalRemoteMR> {
    addr: usize,
    len: usize,
    kind: MemoryRegionKind<T>,
    sub: Mutex<Vec<Range<usize>>>,
}

impl<T: LocalRemoteMR> MemoryRegion<T> {
    fn new(addr: usize, len: usize, t: T) -> Self {
        Self {
            addr,
            len,
            kind: MemoryRegionKind::Root(t),
            sub: Mutex::new(Vec::new()),
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.addr as _
    }

    pub fn length(&self) -> usize {
        self.len
    }

    pub fn rkey(&self) -> u32 {
        self.kind.rkey()
    }

    pub fn token(&self) -> MemoryRegionToken {
        MemoryRegionToken {
            addr: self.addr,
            len: self.len,
            rkey: self.rkey(),
        }
    }

    fn root(self: &Arc<Self>) -> Arc<Self> {
        match &self.kind {
            MemoryRegionKind::Root(_) => self.clone(),
            MemoryRegionKind::Node(node) => node.root.clone(),
        }
    }

    pub fn slice(self: &Arc<Self>, range: Range<usize>) -> io::Result<Self> {
        if range.start >= range.end || range.end > self.len {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid Range"));
        }
        if !self
            .sub
            .lock()
            .unwrap()
            .iter()
            .all(|sub_range| range.end <= sub_range.start || range.start >= sub_range.end)
        {
            return Err(io::Error::new(io::ErrorKind::Other, "No Enough Memory"));
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
        let kind = MemoryRegionKind::Node(new_node);
        Ok(Self {
            addr: self.addr + range.start,
            len: range.len(),
            kind,
            sub: Mutex::new(Vec::new()),
        })
    }

    pub fn alloc(self: &Arc<Self>, layout: Layout) -> io::Result<Self> {
        let mut last = 0;
        let mut ans = Err(io::Error::new(
            io::ErrorKind::Other,
            "No Enough Memory".to_string(),
        ));
        for range in self.sub.lock().unwrap().iter() {
            if last + layout.size() <= range.start {
                ans = Ok(last..last + layout.size());
                break;
            }
            last = range.end
        }
        if last + layout.size() <= self.len {
            ans = Ok(last..last + layout.size());
        }
        ans.map(|range| self.slice(range).unwrap())
    }
}

impl<T: LocalRemoteMR> Drop for MemoryRegion<T> {
    fn drop(&mut self) {
        if let MemoryRegionKind::Node(node) = &self.kind {
            let index = node
                .fa
                .sub
                .lock()
                .unwrap()
                .iter()
                .position(|x| (self.addr - node.fa.addr..self.len + self.addr - node.fa.addr) == *x)
                .unwrap();
            node.fa.sub.lock().unwrap().remove(index);
        }
    }
}

pub struct Local {
    inner_mr: NonNull<ibv_mr>,
    _pd: Arc<ProtectionDomain>,
    _data: Vec<u8>,
}

impl Drop for Local {
    fn drop(&mut self) {
        let errno = unsafe { ibv_dereg_mr(self.inner_mr.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

impl Local {
    fn lkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.lkey
    }
}

impl LocalRemoteMR for Local {
    fn rkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.rkey
    }
}

unsafe impl Sync for Local {}

unsafe impl Send for Local {}

pub type LocalMemoryRegion = MemoryRegion<Local>;

impl LocalMemoryRegion {
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_ptr() as _
    }

    pub fn lkey(&self) -> u32 {
        match &self.kind {
            MemoryRegionKind::Root(root) => root.lkey(),
            MemoryRegionKind::Node(node) => node.fa.lkey(),
        }
    }

    pub fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self> {
        let data = vec![0_u8; layout.size()];
        let inner_mr = NonNull::new(unsafe {
            ibv_reg_mr(pd.as_ptr(), data.as_ptr() as _, data.len(), access.0 as _)
        })
        .ok_or_else(io::Error::last_os_error)?;
        let addr = data.as_ptr() as _;
        let len = data.len();
        let local = Local {
            inner_mr,
            _pd: pd.clone(),
            _data: data,
        };
        Ok(MemoryRegion::new(addr, len, local))
    }
}

pub struct Remote {
    token: MemoryRegionToken,
    agent: Arc<Agent>,
}

impl LocalRemoteMR for Remote {
    fn rkey(&self) -> u32 {
        self.token.rkey
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        let agent = self.agent.clone();
        let token = self.token;
        tokio::spawn(async move { Agent::release_mr(&agent, token).await });
    }
}

pub type RemoteMemoryRegion = MemoryRegion<Remote>;

impl RemoteMemoryRegion {
    pub fn new_from_token(token: MemoryRegionToken, agent: Arc<Agent>) -> Self {
        let addr = token.addr;
        let len = token.len;
        let remote = Remote { token, agent };
        Self {
            addr,
            len,
            kind: MemoryRegionKind::Root(remote),
            sub: Mutex::new(Vec::new()),
        }
    }
}
