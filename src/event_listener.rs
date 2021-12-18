use crate::{CompletionQueue, WCError, WorkRequestId};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::io;
use std::os::unix::prelude::AsRawFd;
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = mpsc::Sender<Result<T, WCError>>;
type ReqMap<T> = Arc<LockFreeCuckooHash<WorkRequestId, Responder<T>>>;
pub struct EventListener {
    pub cq: Arc<CompletionQueue>,
    req_map: ReqMap<usize>,
    _poller_handle: tokio::task::JoinHandle<()>,
}

impl EventListener {
    pub fn new(cq: Arc<CompletionQueue>) -> EventListener {
        let req_map = Arc::new(LockFreeCuckooHash::new());
        let req_map_move = req_map.clone();
        Self {
            req_map,
            _poller_handle: Self::start(cq.clone(), req_map_move),
            cq,
        }
    }

    pub fn start(cq: Arc<CompletionQueue>, req_map: ReqMap<usize>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let async_fd = AsyncFd::new(cq.event_channel().as_raw_fd()).unwrap();
            loop {
                let mut guard = async_fd.readable().await.unwrap();
                let res = match guard.try_io(|_| Self::get_res(&cq)) {
                    Ok(result) => result,
                    Err(_would_block) => {
                        continue;
                    }
                };
                match res {
                    Ok((wr_id, res)) => {
                        let map_guard = pin();
                        let _ = match req_map.remove_with_guard(&wr_id, &map_guard) {
                            Some(val) => {
                                let _ = val
                                    .clone()
                                    .try_send(res)
                                    .map_err(|err| panic!("TODO:process try_send err : {:?}", err));
                            }
                            None => {
                                println!(
                                    "Unknown wr_id :{:?}. Maybe get event triggered by other API",
                                    wr_id
                                );
                                continue;
                            }
                        };
                    }
                    _ => {
                        println!("res : {:?}", res);
                        continue;
                    }
                }
            }
        })
    }

    pub fn register(&self) -> (WorkRequestId, mpsc::Receiver<Result<usize, WCError>>) {
        let (tx, rx) = mpsc::channel(2);
        let mut wr_id = WorkRequestId::new();
        loop {
            if self.req_map.insert_if_not_exists(wr_id, tx.clone()) {
                break;
            }
            wr_id = WorkRequestId::new();
        }
        (wr_id, rx)
    }

    fn get_res(cq: &CompletionQueue) -> io::Result<(WorkRequestId, Result<usize, WCError>)> {
        let wc = cq.poll_single()?;
        Ok((wc.wr_id(), wc.err()))
    }
}
