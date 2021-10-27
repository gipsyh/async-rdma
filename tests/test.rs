use rust_libibverbs::{QueuePairEndpoint, RdmaBuilder, RdmaLocalBox, RdmaRemoteBox};
use std::io;

#[test]
fn test_server1() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let (stream, _) = std::net::TcpListener::bind("127.0.0.1:8000")?.accept()?;
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    rdma.handshake(remote)?;
    let data = rdma.post_receive::<[u32; 4]>();
    std::thread::sleep(std::time::Duration::from_micros(1000));
    dbg!(*data);
    Ok(())
}

#[test]
fn test_client1() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    rdma.handshake(remote)?;
    std::thread::sleep(std::time::Duration::from_micros(10));
    let data = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    rdma.post_send(&data);
    Ok(())
}

#[test]
fn test_server2() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let (stream, _) = std::net::TcpListener::bind("127.0.0.1:8000")?.accept()?;
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    rdma.handshake(remote)?;
    let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    let remote_box: RdmaRemoteBox = bincode::deserialize_from(&stream).unwrap();
    assert!(remote_box.len == local_box.len());
    rdma.qp.write(&local_box, &remote_box);
    Ok(())
}

#[test]
fn test_client2() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    rdma.handshake(remote)?;
    let local_box = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
    bincode::serialize_into(&stream, &local_box.remote_box()).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    dbg!(*local_box);
    Ok(())
}

#[test]
fn test_server3() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let (stream, _) = std::net::TcpListener::bind("127.0.0.1:8000")?.accept()?;
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    rdma.handshake(remote)?;
    let mut local_box = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
    let remote_box: RdmaRemoteBox = bincode::deserialize_from(&stream).unwrap();
    assert!(remote_box.len == local_box.len());
    rdma.read(&mut local_box, &remote_box);
    dbg!(*local_box);
    Ok(())
}

#[test]
fn test_client3() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let stream = std::net::TcpStream::connect("127.0.0.1:8000")?;
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    rdma.handshake(remote)?;
    let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    bincode::serialize_into(&stream, &local_box.remote_box()).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    Ok(())
}

#[test]
fn test_loopback() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    rdma.handshake(rdma.endpoint())?;
    let box1 = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
    let box2 = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    rdma.write(&box2, &box1.remote_box());
    std::thread::sleep(std::time::Duration::from_secs(1));
    dbg!(*box1);
    Ok(())
}
