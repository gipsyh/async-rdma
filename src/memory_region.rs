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

pub trait MemoryRegionTrait {
    fn as_ptr(&self) -> *const u8;

    fn length(&self) -> usize;

    fn rkey(&self) -> u32;

    fn token(&self) -> MemoryRegionToken;
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

    fn slice(self: &Arc<Self>, _range: Range<usize>) -> io::Result<()> {
        todo!()
    }

    fn alloc(self: &Arc<Self>, _layout: Layout) -> io::Result<Arc<Self>> {
        todo!()
    }
}

impl<T: LocalRemoteMR> MemoryRegionTrait for MemoryRegion<T> {
    fn as_ptr(&self) -> *const u8 {
        self.addr as _
    }

    fn length(&self) -> usize {
        self.len
    }

    fn rkey(&self) -> u32 {
        self.kind.rkey()
    }

    fn token(&self) -> MemoryRegionToken {
        MemoryRegionToken {
            addr: self.addr,
            len: self.len,
            rkey: self.rkey(),
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
