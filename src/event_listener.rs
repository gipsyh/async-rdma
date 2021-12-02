use crate::CompletionQueue;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use rand::Rng;
use rdma_sys::{ibv_poll_cq, ibv_wc, ibv_wc_status, ibv_wc_status_str};
use std::cmp::Ordering;
use std::ffi::CStr;
use std::io;
use std::os::unix::prelude::AsRawFd;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = mpsc::Sender<io::Result<T>>;
type ReqMap<T> = Arc<LockFreeCuckooHash<u64, Responder<T>>>;
pub struct EventListener {
    req_map: ReqMap<u64>,
    poller_handle: tokio::task::JoinHandle<()>,
}

impl EventListener {
    pub fn new(cq: Arc<CompletionQueue>) -> EventListener {
        let req_map = Arc::new(LockFreeCuckooHash::new());
        let req_map_move = req_map.clone();
        Self {
            // event_channel,
            // cq_addr,
            req_map,
            poller_handle: Self::start(cq, req_map_move),
        }
    }

    pub fn start(cq: Arc<CompletionQueue>, req_map: ReqMap<u64>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            println!("EventListener is going to poll");
            let async_fd = AsyncFd::new(cq.event_channel().as_raw_fd()).unwrap();
            loop {
                let mut guard = async_fd.readable().await.unwrap();
                println!("poller wake up");
                let res = match guard.try_io(|_| Self::get_res(&cq)) {
                    Ok(result) => result,
                    Err(_would_block) => {
                        println!("poller is going to block and wait for the next event");
                        continue;
                    }
                };
                match res {
                    Ok(wr_id) => {
                        let map_guard = pin();
                        let _ = match req_map.remove_with_guard(&wr_id, &map_guard) {
                            Some(val) => {
                                println!("get wr_id {}, and now wake up the related task.", wr_id);
                                let _ = val
                                    .clone()
                                    .try_send(res)
                                    .map_err(|err| panic!("TODO:process try_send err : {:?}", err));
                            }
                            None => {
                                println!(
                                    "Unknown wr_id :{}. Maybe get event triggered by other API",
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

    pub fn register(&self) -> (u64, mpsc::Receiver<io::Result<u64>>) {
        let (tx, rx) = mpsc::channel(2);
        let mut wr_id = Self::rand_wrid();
        loop {
            if self.req_map.insert_if_not_exists(wr_id, tx.clone()) {
                break;
            }
            wr_id = Self::rand_wrid();
        }
        (wr_id, rx)
    }

    fn rand_wrid() -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let time = since_the_epoch.subsec_micros();
        let rand = rand::thread_rng().gen::<u32>();
        let left: u64 = time.into();
        let right: u64 = rand.into();
        let res = (left << 32) | right;
        println!("Get rand wr_id : {:?}", res);
        res
    }

    pub fn get_res(cq_addr: &CompletionQueue) -> io::Result<u64> {
        let cq = cq_addr.as_ptr();
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        let poll_result = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
        match poll_result.cmp(&0) {
            Ordering::Less => {
                // poll CQ failed
                // rc = 1;
                // panic!("poll CQ failed");
                Err(io::Error::new(io::ErrorKind::Other, "poll CQ failed"))
            }
            Ordering::Equal => {
                // the CQ is empty
                // rc = 1;
                // panic!("completion wasn't found in the CQ after timeout");
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "completion wasn't found in the CQ after timeout",
                ))
            }
            Ordering::Greater => {
                // CQE found
                println!("completion was found in CQ with wr_id={}", wc.wr_id);
                // rc = 0;
                // check the completion status (here we don't care about the completion opcode
                debug_assert_eq!(
                    wc.status,
                    ibv_wc_status::IBV_WC_SUCCESS,
                    "got bad completion with status={}, vendor syndrome={}, the error is: {:?}",
                    wc.status,
                    wc.vendor_err,
                    unsafe { CStr::from_ptr(ibv_wc_status_str(wc.status)) },
                );
                Ok(wc.wr_id)
            }
        }
    }
}
