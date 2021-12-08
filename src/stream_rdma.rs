use futures::ready;
use libc::sleep;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{channel, Receiver, Sender},
};

use crate::{mr_allocator::MRAllocator, queue_pair::QueuePair, LocalMemoryRegion};
use std::{
    alloc::Layout, fmt::Debug, io, mem::forget, pin::Pin, slice, sync::Arc, task::Poll,
    time::Duration,
};

pub struct StreamRdma {
    qp: Arc<QueuePair>,
    receiver: Receiver<LocalMemoryRegion>,
    writer: Arc<LocalMemoryRegion>,
    remain: Option<(LocalMemoryRegion, usize)>,
    poll_recv: Option<Receiver<io::Result<usize>>>,
}

impl StreamRdma {
    pub fn new(allocator: Arc<MRAllocator>, qp: Arc<QueuePair>) -> Self {
        let (sender, receiver) = channel(128);
        let writer = Arc::new(
            allocator
                .alloc(Layout::from_size_align(4096, 4096).unwrap())
                .unwrap(),
        );
        let _handle = tokio::spawn(receive_worker(
            allocator.clone(),
            qp.clone(),
            sender.clone(),
        ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        // let _handle = tokio::spawn(receive_worker(
        //     allocator.clone(),
        //     qp.clone(),
        //     sender.clone(),
        // ));
        Self {
            receiver,
            qp,
            remain: None,
            writer,
            poll_recv: None,
        }
    }
}

impl AsyncRead for StreamRdma {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // println!("read poll");
        let s = self.get_mut();
        match &mut s.remain {
            Some((mr, pos)) => {
                let sz = buf.initialize_unfilled().len().min(mr.length() - *pos);
                let slice = unsafe { slice::from_raw_parts(mr.as_ptr().add(*pos), sz) };
                buf.initialize_unfilled()[..sz].copy_from_slice(slice);
                *pos += sz;
                assert!(*pos <= mr.length());
                if *pos == mr.length() {
                    s.remain = None;
                }
                forget(slice);
                // dbg!(sz);
                buf.advance(sz);
                Poll::Ready(Ok(()))
            }
            None => {
                println!("poll read ------");
                let mr = ready!(s.receiver.poll_recv(cx)).unwrap();
                println!("poll read ------ end");
                s.remain = Some((mr, 0));
                Pin::new(s).poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for StreamRdma {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        // println!("poll write");
        let s = self.get_mut();
        match &mut s.poll_recv {
            Some(poll_recv) => {
                // Poll::Ready(ready!(
                let res = ready!(poll_recv.poll_recv(cx)).unwrap();
                s.poll_recv = None;
                match res {
                    Ok(sz) => {
                        assert_eq!(sz, buf.len());
                        Poll::Ready(Ok(sz))
                    }
                    Err(err) => match err.kind() {
                        io::ErrorKind::Other => {
                            println!("{}", err);
                            Pin::new(s).poll_write(cx, buf)
                        }
                        _ => Poll::Ready(Err(err)),
                    },
                }
            }
            None => {
                assert!(buf.len() <= s.writer.length());
                let slice = unsafe {
                    slice::from_raw_parts_mut(
                        Arc::get_mut(&mut s.writer).unwrap().as_mut_ptr(),
                        buf.len(),
                    )
                };
                slice.copy_from_slice(buf);
                let (wr_id, resp_rx) = s.qp.event_listener.register();
                s.poll_recv = Some(resp_rx);
                std::thread::sleep(Duration::from_secs(1));
                s.qp.post_send(&s.writer.slice(0..buf.len()).unwrap(), wr_id)
                    .unwrap();
                Pin::new(s).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo!()
    }
}

impl Debug for StreamRdma {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRdma").finish()
    }
}

async fn receive_worker(
    allocator: Arc<MRAllocator>,
    qp: Arc<QueuePair>,
    sender: Sender<LocalMemoryRegion>,
) {
    loop {
        let lm = Arc::new(
            allocator
            .alloc(Layout::new::<[u8; 128]>())
            .unwrap(),
        );
        dbg!("worker start");
        let sz = qp.receive(&lm).await.unwrap();
        assert!(sz > 0);
        dbg!(sz);
        let data = lm.slice(0..sz).unwrap();
        assert!(sender.send(data).await.is_ok());
    }
}
