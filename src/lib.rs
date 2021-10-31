#![feature(ptr_internals, slice_ptr_get, slice_ptr_len, bool_to_option)]
#![allow(unused)]

mod completion_queue;
mod context;
mod event_channel;
mod gid;
mod memory_region;
mod memory_window;
mod protection_domain;
mod queue_pair;
mod rdma_box;

pub use completion_queue::*;
pub use context::*;
pub use event_channel::*;
pub use gid::*;
pub use memory_region::*;
pub use protection_domain::*;
pub use queue_pair::*;
pub use rdma_box::*;

use rdma_sys::ibv_access_flags;
use std::{alloc::Layout, io, sync::Arc};

pub struct RdmaBuilder {
    dev_name: Option<String>,
    access: ibv_access_flags,
    cq_size: u32,
}

impl RdmaBuilder {
    pub fn build(&self) -> io::Result<Rdma> {
        Rdma::new(self.dev_name.as_deref(), self.access, self.cq_size)
    }

    pub fn set_dev(&mut self, dev: &str) {
        self.dev_name = Some(dev.to_string());
    }

    pub fn set_cq_size(&mut self, cq_size: u32) {
        self.cq_size = cq_size
    }
}

impl Default for RdmaBuilder {
    fn default() -> Self {
        Self {
            dev_name: None,
            access: ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
            cq_size: 16,
        }
    }
}

#[allow(dead_code)]
pub struct Rdma {
    ctx: Arc<Context>,
    ec: Arc<EventChannel>,
    cq: Arc<CompletionQueue>,
    pub pd: Arc<ProtectionDomain>,
    pub qp: Arc<QueuePair>,
}

impl Rdma {
    pub fn new(dev_name: Option<&str>, access: ibv_access_flags, cq_size: u32) -> io::Result<Self> {
        let ctx = Arc::new(Context::open(dev_name)?);
        let ec = Arc::new(ctx.create_event_channel()?);
        let cq = Arc::new(ctx.create_completion_queue(cq_size, Some(&ec))?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut qpb = pd.create_queue_pair_builder();
        let qp = Arc::new(qpb.set_cq(&cq).build()?);
        qp.modify_to_init(access)?;
        Ok(Self {
            ctx,
            ec,
            cq,
            pd,
            qp,
        })
    }

    pub fn endpoint(&self) -> QueuePairEndpoint {
        self.qp.endpoint()
    }

    pub fn handshake(&self, remote: QueuePairEndpoint) -> io::Result<()> {
        self.qp.modify_to_rtr(remote, 0, 1, 0x12)?;
        self.qp.modify_to_rts(0x12, 6, 0, 0, 1)?;
        Ok(())
    }

    pub fn post_send<LM: RdmaLocalMemory>(&self, data: &LM) -> io::Result<()> {
        self.qp.post_send(data)
    }

    pub fn post_receive<LM: RdmaLocalMemory + SizedLayout>(&self) -> io::Result<LM> {
        self.qp.post_receive()
    }

    pub fn write<LM, RM>(&self, local: &LM, remote: &RM) -> io::Result<()>
    where
        LM: RdmaLocalMemory,
        RM: RdmaRemoteMemory,
    {
        self.qp.write(local, remote)
    }

    pub fn read<LM, RM>(&self, local: &mut LM, remote: &RM) -> io::Result<()>
    where
        LM: RdmaLocalMemory,
        RM: RdmaRemoteMemory,
    {
        self.qp.read(local, remote)
    }
}

pub trait RdmaMemory {
    fn addr(&self) -> *const u8;

    fn length(&self) -> usize;
}

pub trait RdmaLocalMemory: RdmaMemory {
    fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self>
    where
        Self: Sized;

    fn lkey(&self) -> u32;
}

pub trait RdmaRemoteMemory: RdmaMemory {
    fn rkey(&self) -> u32;
}

pub trait SizedLayout {
    fn layout() -> Layout;
}
