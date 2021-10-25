use rdma_sys::ibv_access_flags;
use rust_libibverbs::{
    CompletionQueue, Context, EventChannel, ProtectionDomain, QueuePair, QueuePairEndpoint,
    RdmaLocalBox, RdmaRemoteBox,
};
use std::{io, net::TcpStream, sync::Arc};
#[allow(dead_code)]
struct Rdma {
    ctx: Arc<Context>,
    ec: Arc<EventChannel>,
    cq: Arc<CompletionQueue>,
    pd: Arc<ProtectionDomain>,
    qp: Arc<QueuePair>,
    stream: TcpStream,
}

impl Rdma {
    fn server_init(addr: &str) -> io::Result<Self> {
        let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let ctx = Arc::new(Context::open(None)?);
        let ec = Arc::new(ctx.create_event_channel()?);
        let cq = Arc::new(ctx.create_completion_queue(10, Some(&ec))?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut qpb = pd.create_queue_pair_builder();
        let qp = Arc::new(qpb.set_cq(&cq).build()?);
        qp.modify_to_init(ibv_access)?;
        let local_ep = qp.endpoint();
        let listener = std::net::TcpListener::bind(addr)?;
        let (stream, _) = listener.accept()?;
        let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
        bincode::serialize_into(&stream, &local_ep).unwrap();
        qp.modify_to_rtr(remote_ep, 0, 1, 0x12)?;
        qp.modify_to_rts(0x12, 6, 0, 0, 1)?;
        Ok(Rdma {
            ctx,
            ec,
            cq,
            pd,
            qp,
            stream,
        })
    }

    fn client_init(addr: &str) -> io::Result<Self> {
        let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let ctx = Arc::new(Context::open(None)?);
        let ec = Arc::new(ctx.create_event_channel()?);
        let cq = Arc::new(ctx.create_completion_queue(10, Some(&ec))?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mut qpb = pd.create_queue_pair_builder();
        let qp = Arc::new(qpb.set_cq(&cq).build()?);
        qp.modify_to_init(ibv_access)?;
        let local_ep = qp.endpoint();
        let stream = std::net::TcpStream::connect(addr)?;
        bincode::serialize_into(&stream, &local_ep).unwrap();
        let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
        qp.modify_to_rtr(remote_ep, 0, 1, 0x12)?;
        qp.modify_to_rts(0x12, 6, 0, 0, 1)?;
        Ok(Rdma {
            ctx,
            ec,
            cq,
            pd,
            qp,
            stream,
        })
    }
}

#[test]
fn test_server1() -> io::Result<()> {
    let rdma = Rdma::server_init("127.0.0.1:8000")?;
    let data = rdma.qp.post_receive::<[u32; 4]>();
    std::thread::sleep(std::time::Duration::from_micros(1000));
    dbg!(*data);
    Ok(())
}

#[test]
fn test_client1() -> io::Result<()> {
    let rdma = Rdma::client_init("127.0.0.1:8000")?;
    std::thread::sleep(std::time::Duration::from_micros(10));
    let data = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    rdma.qp.post_send(data);
    Ok(())
}

#[test]
pub fn test_server2() -> io::Result<()> {
    let rdma = Rdma::server_init("127.0.0.1:8000")?;
    let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    let remote_box: RdmaRemoteBox = bincode::deserialize_from(&rdma.stream).unwrap();
    assert!(remote_box.len == local_box.len());
    rdma.qp.remote_write(local_box, remote_box);
    Ok(())
}

#[test]
pub fn test_client2() -> io::Result<()> {
    let rdma = Rdma::client_init("127.0.0.1:8000")?;
    let local_box = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
    bincode::serialize_into(&rdma.stream, &local_box.remote_box()).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    dbg!(*local_box);
    Ok(())
}
