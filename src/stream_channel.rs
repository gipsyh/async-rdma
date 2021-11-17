use crate::*;
use std::{
    borrow::BorrowMut,
    io::{Cursor, Read, Write},
    sync::mpsc::{channel, Receiver, Sender},
};

pub struct StreamChannel {
    sender: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    reader: Option<Cursor<Vec<u8>>>,
    writer: Vec<u8>,
}

impl StreamChannel {
    pub fn new() -> (Self, Self) {
        let (ls, rr) = channel();
        let (rs, lr) = channel();
        let l = Self {
            sender: ls,
            receiver: lr,
            reader: None,
            writer: vec![],
        };
        let r = Self {
            sender: rs,
            receiver: rr,
            reader: None,
            writer: vec![],
        };
        (l, r)
    }

    pub fn write_data_in_vec(&mut self, vec: Vec<u8>) {
        self.flush();
        self.sender.send(vec);
    }
}

impl Read for StreamChannel {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.reader.is_none() || self.reader.as_ref().unwrap().is_empty() {
            self.reader = Some(Cursor::new(self.receiver.recv().unwrap()));
        }
        self.reader.as_mut().unwrap().read(buf)
    }
}

impl Write for StreamChannel {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.writer.is_empty() {
            return Ok(());
        }
        let send = self.writer.to_owned();
        self.writer = vec![];
        self.sender
            .send(send)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_channel::StreamChannel;
    use std::io::{Read, Write};

    #[test]
    fn test() {
        let (mut l, mut r) = StreamChannel::new();
        let send = vec![1, 2, 3, 4];
        l.write(&send);
        l.flush();
        let send = vec![5, 6, 7, 8];
        l.write(&send);
        l.flush();
        let mut recv = vec![0; 2];
        r.read_exact(recv.as_mut());
        assert_eq!(recv, vec![1, 2]);
        r.read_exact(recv.as_mut());
        assert_eq!(recv, vec![3, 4]);
        r.read_exact(recv.as_mut());
        assert_eq!(recv, vec![5, 6]);
        r.read_exact(recv.as_mut());
        assert_eq!(recv, vec![7, 8]);
        r.read_exact(recv.as_mut());
    }
}
