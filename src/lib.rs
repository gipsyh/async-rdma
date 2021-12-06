mod agent;
mod completion_queue;
mod context;
mod event_channel;
mod event_listener;
mod gid;
mod memory_region;
mod memory_window;
mod protection_domain;
mod queue_pair;
mod rdma_box;
mod rdma_stream;

pub use agent::*;
pub use completion_queue::*;
pub use context::*;
pub use event_channel::*;
use event_listener::EventListener;
pub use gid::*;
pub use memory_region::*;
pub use protection_domain::*;
pub use queue_pair::*;
pub use rdma_box::*;
use rdma_stream::RdmaStream;
use rdma_sys::ibv_access_flags;
use std::{alloc::Layout, io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

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
    pub pd: Arc<ProtectionDomain>,
    pub qp: Arc<QueuePair>,
    agent: Option<Arc<Agent>>,
    event_listener: EventListener,
}

impl Rdma {
    pub fn new(dev_name: Option<&str>, access: ibv_access_flags, cq_size: u32) -> io::Result<Self> {
        let ctx = Arc::new(Context::open(dev_name)?);
        let ec = ctx.create_event_channel()?;
        let cq = Arc::new(ctx.create_completion_queue(cq_size, Some(ec))?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut qpb = pd.create_queue_pair_builder();
        let qp = Arc::new(qpb.set_cq(&cq).build()?);
        qp.modify_to_init(access)?;
        let event_listener = EventListener::new(cq);
        Ok(Self {
            ctx,
            pd,
            qp,
            event_listener,
            agent: None,
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

    pub async fn post_send<LM: RdmaLocalMemory>(&self, data: &LM) -> io::Result<()> {
        self.qp.post_send(data)
    }

    pub async fn post_receive<LM: RdmaLocalMemory + SizedLayout>(&self) -> io::Result<LM> {
        self.qp.post_receive()
    }

    pub async fn write<LM, RM>(&self, local: &LM, remote: &RM) -> io::Result<()>
    where
        LM: RdmaLocalMemory,
        RM: RdmaRemoteMemory,
    {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let res = self.qp.write(local, remote, wr_id);
        resp_rx.recv().await.unwrap().unwrap();
        res
    }

    pub async fn read<LM, RM>(&self, local: &mut LM, remote: &RM) -> io::Result<()>
    where
        LM: RdmaLocalMemory,
        RM: RdmaRemoteMemory,
    {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let res = self.qp.read(local, remote, wr_id);
        resp_rx.recv().await.unwrap().unwrap();
        res
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let mut rdma = RdmaBuilder::default().build()?;
        let mut stream = TcpStream::connect(addr).await?;
        let mut endpoint = bincode::serialize(&rdma.endpoint()).unwrap();
        stream.write_all(&endpoint).await?;
        stream.read_exact(endpoint.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&endpoint).unwrap();
        rdma.handshake(remote)?;
        let stream = RdmaStream::new(stream);
        let agent = Agent::new(stream, rdma.pd.clone());
        rdma.agent = Some(agent);
        Ok(rdma)
    }

    pub fn alloc_local_mr(&self, layout: Layout) -> io::Result<MemoryRegion> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        self.pd.alloc_memory_region(layout, access)
    }

    pub async fn alloc_remote_mr(&self, layout: Layout) -> io::Result<MemoryRegion> {
        if let Some(agent) = &self.agent {
            agent.alloc_mr(layout).await
        } else {
            panic!();
        }
    }

    pub async fn send_mr(&self, mr: Arc<MemoryRegion>) -> io::Result<()> {
        if let Some(agent) = &self.agent {
            agent.send_mr(mr).await
        } else {
            panic!();
        }
    }

    pub async fn receive_mr(&self) -> io::Result<Arc<MemoryRegion>> {
        if let Some(agent) = &self.agent {
            agent.receive_mr().await
        } else {
            panic!();
        }
    }
}

pub struct RdmaListener {
    tcp_listener: TcpListener,
}

impl RdmaListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self { tcp_listener })
    }

    pub async fn accept(&self) -> io::Result<Rdma> {
        let (mut stream, _) = self.tcp_listener.accept().await?;
        let mut rdma = RdmaBuilder::default().build()?;
        let mut remote = vec![0_u8; 22];
        stream.read_exact(remote.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&remote).unwrap();
        let local = bincode::serialize(&rdma.endpoint()).unwrap();
        stream.write_all(&local).await?;
        rdma.handshake(remote)?;
        let stream = RdmaStream::new(stream);
        let agent = Agent::new(stream, rdma.pd.clone());
        rdma.agent = Some(agent);
        Ok(rdma)
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
