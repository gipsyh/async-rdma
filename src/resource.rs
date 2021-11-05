use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    io,
    net::{TcpStream, ToSocketAddrs},
};

#[derive(Serialize, Deserialize)]

struct TransResource<A: ToSocketAddrs + Serialize, T: Serialize> {
    addr: A,
    token: T,
}

impl<A, T> TransResource<A, T>
where
    A: ToSocketAddrs + Serialize + DeserializeOwned,
    T: Serialize + DeserializeOwned,
{
    fn new(addr: A, token: T) -> TransResource<A, T> {
        Self { addr, token }
    }

    fn send<W: io::Write>(self, write: W) {
        bincode::serialize_into(write, &self).unwrap();
    }

    fn recieve<R: io::Read>(read: R) -> Self {
        bincode::deserialize_from(read).unwrap()
    }

    fn release(self) {
        let stream = TcpStream::connect(self.addr).unwrap();
        bincode::serialize_into(stream, &self.token).unwrap();
    }
}

trait RdmaTransResource {
    fn send<W: io::Write>(self, write: W);

    fn recieve<R: io::Read>(read: R) -> Self;

    fn release(self);
}

// impl Resource {

//     // fn release<W: io::Write>(write:)
// }
