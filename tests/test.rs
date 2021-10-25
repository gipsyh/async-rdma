use rdma_sys::ibv_access_flags;
use rust_libibverbs::{Context, QueuePairEndpoint, RdmaLocalBox, RdmaRemoteBox};
use std::io;

#[test]
pub fn test_server1() -> io::Result<()> {
    let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let ctx = Context::open(None)?;
    let ec = ctx.create_event_channel()?;
    let cq = ctx.create_completion_queue(10, Some(&ec))?;
    let pd = ctx.create_protection_domain()?;
    let mut qpb = pd.create_queue_pair_builder();
    let qp = qpb.set_cq(&cq).build()?;
    qp.modify_to_init(ibv_access)?;
    let local_ep = qp.endpoint();
    let listener = std::net::TcpListener::bind("127.0.0.1:8000")?;
    let (stream, _) = listener.accept()?;
    let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &local_ep).unwrap();
    qp.modify_to_rtr(remote_ep, 0, 1, 0x12)?;
    qp.modify_to_rts(0x12, 6, 0, 0, 1)?;
    let data = qp.post_receive::<[u32; 4]>();
    std::thread::sleep(std::time::Duration::from_micros(1000));
    dbg!(*data);
    Ok(())
}

#[test]
pub fn test_client1() -> io::Result<()> {
    let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let ctx = Context::open(None)?;
    let ec = ctx.create_event_channel()?;
    let cq = ctx.create_completion_queue(10, Some(&ec))?;
    let pd = ctx.create_protection_domain()?;
    let mut qpb = pd.create_queue_pair_builder();
    let qp = qpb.set_cq(&cq).build()?;
    qp.modify_to_init(ibv_access)?;
    let local_ep = qp.endpoint();
    let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    bincode::serialize_into(&stream, &local_ep).unwrap();
    let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    qp.modify_to_rtr(remote_ep, 0, 1, 0x12)?;
    qp.modify_to_rts(0x12, 6, 0, 0, 1)?;
    std::thread::sleep(std::time::Duration::from_micros(10));
    let data = RdmaLocalBox::new(&pd, [1, 2, 3, 4]);
    qp.post_send(data);
    Ok(())
}

#[test]
pub fn test_server2() -> io::Result<()> {
    let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let ctx = Context::open(None)?;
    let ec = ctx.create_event_channel()?;
    let cq = ctx.create_completion_queue(10, Some(&ec))?;
    let pd = ctx.create_protection_domain()?;
    let mut qpb = pd.create_queue_pair_builder();
    let qp = qpb.set_cq(&cq).build()?;
    qp.modify_to_init(ibv_access)?;
    let local_ep = qp.endpoint();
    let listener = std::net::TcpListener::bind("127.0.0.1:8000")?;
    let (stream, _) = listener.accept()?;
    let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &local_ep).unwrap();
    qp.modify_to_rtr(remote_ep, 0, 1, 0x12)?;
    qp.modify_to_rts(0x12, 6, 0, 0, 1).unwrap();
    let local_box = RdmaLocalBox::new(&pd, [1, 2, 3, 4]);
    let remote_box: RdmaRemoteBox = bincode::deserialize_from(&stream).unwrap();
    assert!(remote_box.len == local_box.len());
    qp.remote_write(local_box, remote_box);
    Ok(())
}

#[test]
pub fn test_client2() -> io::Result<()> {
    let ibv_access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let ctx = Context::open(None)?;
    let ec = ctx.create_event_channel()?;
    let cq = ctx.create_completion_queue(10, Some(&ec))?;
    let pd = ctx.create_protection_domain()?;
    let mut qpb = pd.create_queue_pair_builder();
    let qp = qpb.set_cq(&cq).build()?;
    qp.modify_to_init(ibv_access).unwrap();
    let local_ep = qp.endpoint();
    let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    bincode::serialize_into(&stream, &local_ep).unwrap();
    let remote_ep: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    qp.modify_to_rtr(remote_ep, 0, 1, 0x12).unwrap();
    qp.modify_to_rts(0x12, 6, 0, 0, 1).unwrap();
    let local_box = RdmaLocalBox::new(&pd, [0, 0, 0, 0]);
    bincode::serialize_into(&stream, &local_box.remote_box()).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    dbg!(*local_box);
    Ok(())
}
