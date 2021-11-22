use async_rdma::{QueuePairEndpoint, Rdma, RdmaBuilder, RdmaLocalBox, RdmaRemoteBox};
use std::{
    io,
    net::{TcpStream, ToSocketAddrs},
    thread::spawn,
};

fn server<A: ToSocketAddrs>(addr: A, f: fn(Rdma, TcpStream) -> io::Result<()>) -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let (stream, _) = std::net::TcpListener::bind(addr)?.accept()?;
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    rdma.handshake(remote)?;
    std::thread::sleep(std::time::Duration::from_millis(100));
    f(rdma, stream)
}

fn client<A: ToSocketAddrs>(addr: A, f: fn(Rdma, TcpStream) -> io::Result<()>) -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    let stream = std::net::TcpStream::connect(addr).unwrap();
    bincode::serialize_into(&stream, &rdma.endpoint()).unwrap();
    let remote: QueuePairEndpoint = bincode::deserialize_from(&stream).unwrap();
    rdma.handshake(remote)?;
    std::thread::sleep(std::time::Duration::from_millis(100));
    f(rdma, stream)
}

fn test_server_client<A: 'static + ToSocketAddrs + Send + Copy>(
    addr: A,
    s: fn(Rdma, TcpStream) -> io::Result<()>,
    c: fn(Rdma, TcpStream) -> io::Result<()>,
) -> io::Result<()> {
    let server = spawn(move || server(addr, s));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let client = spawn(move || client(addr, c));
    client.join().unwrap()?;
    server.join().unwrap()
}

mod test1 {
    use crate::*;

    fn server(rdma: Rdma, _stream: TcpStream) -> io::Result<()> {
        let data: RdmaLocalBox<[u32; 4]> = rdma.post_receive()?;
        std::thread::sleep(std::time::Duration::from_millis(1000));
        assert_eq!(*data, [4, 3, 2, 1]);
        Ok(())
    }

    fn client(rdma: Rdma, _stream: TcpStream) -> io::Result<()> {
        let data = RdmaLocalBox::new(&rdma.pd, [4, 3, 2, 1]);
        rdma.post_send(&data)?;
        std::thread::sleep(std::time::Duration::from_millis(1000));
        Ok(())
    }

    #[test]
    fn test() -> io::Result<()> {
        test_server_client("127.0.0.1:8000", server, client)
    }
}

mod test2 {
    use crate::*;

    fn server(rdma: Rdma, stream: TcpStream) -> io::Result<()> {
        let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
        let remote_box: RdmaRemoteBox = bincode::deserialize_from(&stream).unwrap();
        rdma.qp.write(&local_box, &remote_box)
    }

    fn client(rdma: Rdma, stream: TcpStream) -> io::Result<()> {
        let local_box = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
        bincode::serialize_into(&stream, &local_box.remote_box()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(*local_box, [1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn test() -> io::Result<()> {
        test_server_client("127.0.0.1:8001", server, client)
    }
}

mod test3 {
    use crate::*;

    fn server(rdma: Rdma, stream: TcpStream) -> io::Result<()> {
        let mut local_box = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
        let remote_box: RdmaRemoteBox = bincode::deserialize_from(&stream).unwrap();
        rdma.read(&mut local_box, &remote_box)?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(*local_box, [1, 2, 3, 4]);
        Ok(())
    }

    fn client(rdma: Rdma, stream: TcpStream) -> io::Result<()> {
        let local_box = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
        bincode::serialize_into(&stream, &local_box.remote_box()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
        Ok(())
    }

    #[test]
    fn test() -> io::Result<()> {
        test_server_client("127.0.0.1:8002", server, client)
    }
}

#[test]
fn test_loopback() -> io::Result<()> {
    let rdma = RdmaBuilder::default().build()?;
    rdma.handshake(rdma.endpoint())?;
    let box1 = RdmaLocalBox::new(&rdma.pd, [0, 0, 0, 0]);
    let box2 = RdmaLocalBox::new(&rdma.pd, [1, 2, 3, 4]);
    rdma.write(&box2, &box1.remote_box())?;
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert_eq!(*box1, [1, 2, 3, 4]);
    Ok(())
}
