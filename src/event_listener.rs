use crate::CompletionQueue;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use rand::Rng;
use rdma_sys::{ibv_poll_cq, ibv_wc, ibv_wc_status};
use std::cmp::Ordering;
use std::io;
use std::os::unix::prelude::AsRawFd;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

#[derive(Error, Debug, FromPrimitive)]
pub enum WCError {
    #[error("Local Length Error: this happens if a Work Request that was posted in a local Send Queue contains a message that is greater than the maximum message size that is supported by the RDMA device port that should send the message or an Atomic operation which its size is different than 8 bytes was sent. This also may happen if a Work Request that was posted in a local Receive Queue isn't big enough for holding the incoming message or if the incoming message size if greater the maximum message size supported by the RDMA device port that received the message.")]
    LocLenErr = 1,
    #[error("Local QP Operation Error: an internal QP consistency error was detected while processing this Work Request: this happens if a Work Request that was posted in a local Send Queue of a UD QP contains an Address Handle that is associated with a Protection Domain to a QP which is associated with a different Protection Domain or an opcode which isn't supported by the transport type of the QP isn't supported (for example: RDMA Write over a UD QP).")]
    LocQpOpErr = 2,
    #[error("Local EE Context Operation Error: an internal EE Context consistency error was detected while processing this Work Request (unused, since its relevant only to RD QPs or EE Context, which aren’t supported).")]
    LocEecOpErr = 3,
    #[error("Local Protection Error: the locally posted Work Request’s buffers in the scatter/gather list does not reference a Memory Region that is valid for the requested operation.")]
    LocProtErr = 4,
    #[error("Work Request Flushed Error: A Work Request was in process or outstanding when the QP transitioned into the Error State.")]
    WrFlushErr = 5,
    #[error("Memory Window Binding Error: A failure happened when tried to bind a MW to a MR.")]
    MwBindErr = 6,
    #[error("Bad Response Error: an unexpected transport layer opcode was returned by the responder. Relevant for RC QPs.")]
    BadRespErr = 7,
    #[error("Local Access Error: a protection error occurred on a local data buffer during the processing of a RDMA Write with Immediate operation sent from the remote node. Relevant for RC QPs.")]
    LocAccessErr = 8,
    #[error("Remote Invalid Request Error: The responder detected an invalid message on the channel. Possible causes include the operation is not supported by this receive queue (qp_access_flags in remote QP wasn't configured to support this operation), insufficient buffering to receive a new RDMA or Atomic Operation request, or the length specified in a RDMA request is greater than 2^31 bytes. Relevant for RC QPs.")]
    RemInvReqErr = 9,
    #[error("Remote Access Error: a protection error occurred on a remote data buffer to be read by an RDMA Read, written by an RDMA Write or accessed by an atomic operation. This error is reported only on RDMA operations or atomic operations. Relevant for RC QPs.")]
    RemAccessErr = 10,
    #[error("Remote Operation Error: the operation could not be completed successfully by the responder. Possible causes include a responder QP related error that prevented the responder from completing the request or a malformed WQE on the Receive Queue. Relevant for RC QPs.")]
    RemOpErr = 11,
    #[error("Transport Retry Counter Exceeded: The local transport timeout retry counter was exceeded while trying to send this message. This means that the remote side didn't send any Ack or Nack. If this happens when sending the first message, usually this mean that the connection attributes are wrong or the remote side isn't in a state that it can respond to messages. If this happens after sending the first message, usually it means that the remote QP isn't available anymore. Relevant for RC QPs.")]
    RetryExc = 12,
    #[error("RNR Retry Counter Exceeded: The RNR NAK retry count was exceeded. This usually means that the remote side didn't post any WR to its Receive Queue. Relevant for RC QPs.")]
    RnrRetryExc = 13,
    #[error("Local RDD Violation Error: The RDD associated with the QP does not match the RDD associated with the EE Context (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    LocRddViolErr = 14,
    #[error("Remote Invalid RD Request: The responder detected an invalid incoming RD message. Causes include a Q_Key or RDD violation (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    RemInvRdReq = 15,
    #[error("Remote Aborted Error: For UD or UC QPs associated with a SRQ, the responder aborted the operation.")]
    RemAbortErr = 16,
    #[error("Invalid EE Context Number: An invalid EE Context number was detected (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    InvEecn = 17,
    #[error("Invalid EE Context State Error: Operation is not legal for the specified EE Context state (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    InvEecState = 18,
    #[error("Fatal Error.")]
    Fatal = 19,
    #[error("Response Timeout Error.")]
    RespTimeout = 20,
    #[error("General Error: other error which isn't one of the above errors.")]
    GeneralErr = 21,
}

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = mpsc::Sender<io::Result<T>>;
type ReqMap<T> = Arc<LockFreeCuckooHash<u64, Responder<T>>>;
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

    pub fn register(&self) -> (u64, mpsc::Receiver<io::Result<usize>>) {
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
        (left << 32) | right
    }

    pub fn get_res(cq_addr: &CompletionQueue) -> io::Result<(u64, io::Result<usize>)> {
        let cq = cq_addr.as_ptr();
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        let poll_result = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
        match poll_result.cmp(&0) {
            Ordering::Less => {
                // poll CQ failed
                //TODO:process cq failed.
                Err(io::Error::new(io::ErrorKind::Other, ""))
            }
            Ordering::Equal => {
                // the CQ is empty
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "completion wasn't found in the CQ after timeout",
                ))
            }
            Ordering::Greater => {
                // CQE found
                // check the completion status (here we don't care about the completion opcode
                let res = if wc.status == ibv_wc_status::IBV_WC_SUCCESS {
                    Ok(wc.byte_len as usize)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        WCError::from_u32(wc.status).unwrap(),
                    ))
                };
                Ok((wc.wr_id, res))
                // debug_assert_eq!(
                //     wc.status,
                //     ibv_wc_status::IBV_WC_SUCCESS,
                //     "got bad completion with status={}, vendor syndrome={}, the error is: {:?}",
                //     wc.status,
                //     wc.vendor_err,
                //     unsafe { CStr::from_ptr(ibv_wc_status_str(wc.status)) },
                // );
            }
        }
    }
}
