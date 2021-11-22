use crate::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::Mutex,
    thread::{spawn, JoinHandle},
};
use stream_channel::StreamChannel;

#[derive(Serialize, Deserialize)]
struct MessageHeader {
    endpoint: usize,
    length: usize,
}

fn message_line_read_listener_main(
    mut stream: TcpStream,
    mut table: HashMap<usize, StreamChannel>,
) {
    loop {
        let header: MessageHeader = bincode::deserialize_from(&stream).unwrap();
        let mut vec = vec![0_u8; header.length];
        stream.read_exact(vec.as_mut()).unwrap();
        let _channel_stream = table
            .get_mut(&header.endpoint)
            .unwrap()
            .write_data_in_vec(vec);
    }
}

pub struct MessageStream {
    stream_channel: StreamChannel,
    tcp_stream: Arc<Mutex<TcpStream>>,
    target_endpoint: usize,
}

impl MessageStream {}

impl Read for MessageStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream_channel.read(buf)
    }
}

impl Write for MessageStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let header = MessageHeader {
            endpoint: self.target_endpoint,
            length: buf.len(),
        };
        let mut tcp_stream = self.tcp_stream.lock().unwrap();
        bincode::serialize_into(&*tcp_stream, &header).unwrap();
        tcp_stream.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        todo!()
    }
}

pub struct MessageLine {
    joinhandle: JoinHandle<()>,
}

impl MessageLine {
    pub fn new(stream: TcpStream) -> (Self, MessageStream, MessageStream, MessageStream) {
        let w_stream = Arc::new(Mutex::new(stream.try_clone().unwrap()));
        let mut table = HashMap::new();
        let (agent_client_l, agent_client_r) = StreamChannel::new();
        let (agent_server_l, agent_server_r) = StreamChannel::new();
        let (normal_l, normal_r) = StreamChannel::new();
        table.insert(1, agent_client_r);
        table.insert(2, agent_server_r);
        table.insert(3, normal_r);
        let agent_client = MessageStream {
            stream_channel: agent_client_l,
            tcp_stream: w_stream.clone(),
            target_endpoint: 2,
        };
        let agent_server = MessageStream {
            stream_channel: agent_server_l,
            tcp_stream: w_stream.clone(),
            target_endpoint: 1,
        };
        let normal = MessageStream {
            stream_channel: normal_l,
            tcp_stream: w_stream,
            target_endpoint: 3,
        };
        let joinhandle = spawn(|| message_line_read_listener_main(stream, table));
        (Self { joinhandle }, agent_client, agent_server, normal)
    }
}
