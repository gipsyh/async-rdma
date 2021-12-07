use crate::CompletionQueue;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use rand::Rng;
use rdma_sys::{ibv_poll_cq, ibv_wc, ibv_wc_status};
use std::cmp::Ordering;
use std::io;
use std::os::unix::prelude::AsRawFd;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use thiserror::Error;

const WC_ERROR_NUM: usize = 22;
const WC_ERROR_MSG: [&'static str; WC_ERROR_NUM] = [
    "IBV_WC_SUCCESS (0) - Operation completed successfully: this means that the corresponding Work Request (and all of the unsignaled Work Requests that were posted previous to it) ended and the memory buffers that this Work Request refers to are ready to be (re)used.",
    "IBV_WC_LOC_LEN_ERR (1) - Local Length Error: this happens if a Work Request that was posted in a local Send Queue contains a message that is greater than the maximum message size that is supported by the RDMA device port that should send the message or an Atomic operation which its size is different than 8 bytes was sent. This also may happen if a Work Request that was posted in a local Receive Queue isn't big enough for holding the incoming message or if the incoming message size if greater the maximum message size supported by the RDMA device port that received the message.",
    "IBV_WC_LOC_QP_OP_ERR (2) - Local QP Operation Error: an internal QP consistency error was detected while processing this Work Request: this happens if a Work Request that was posted in a local Send Queue of a UD QP contains an Address Handle that is associated with a Protection Domain to a QP which is associated with a different Protection Domain or an opcode which isn't supported by the transport type of the QP isn't supported (for example: RDMA Write over a UD QP).",
    "IBV_WC_LOC_EEC_OP_ERR (3) - Local EE Context Operation Error: an internal EE Context consistency error was detected while processing this Work Request (unused, since its relevant only to RD QPs or EE Context, which aren’t supported).",
    "IBV_WC_LOC_PROT_ERR (4) - Local Protection Error: the locally posted Work Request’s buffers in the scatter/gather list does not reference a Memory Region that is valid for the requested operation.",
    "IBV_WC_WR_FLUSH_ERR (5) - Work Request Flushed Error: A Work Request was in process or outstanding when the QP transitioned into the Error State.",
    "IBV_WC_MW_BIND_ERR (6) - Memory Window Binding Error: A failure happened when tried to bind a MW to a MR.",
    "IBV_WC_BAD_RESP_ERR (7) - Bad Response Error: an unexpected transport layer opcode was returned by the responder. Relevant for RC QPs.",
    "IBV_WC_LOC_ACCESS_ERR (8) - Local Access Error: a protection error occurred on a local data buffer during the processing of a RDMA Write with Immediate operation sent from the remote node. Relevant for RC QPs.",
    "IBV_WC_REM_INV_REQ_ERR (9) - Remote Invalid Request Error: The responder detected an invalid message on the channel. Possible causes include the operation is not supported by this receive queue (qp_access_flags in remote QP wasn't configured to support this operation), insufficient buffering to receive a new RDMA or Atomic Operation request, or the length specified in a RDMA request is greater than 2^{31} bytes. Relevant for RC QPs.",
    "IBV_WC_REM_ACCESS_ERR (10) - Remote Access Error: a protection error occurred on a remote data buffer to be read by an RDMA Read, written by an RDMA Write or accessed by an atomic operation. This error is reported only on RDMA operations or atomic operations. Relevant for RC QPs.",
    "IBV_WC_REM_OP_ERR (11) - Remote Operation Error: the operation could not be completed successfully by the responder. Possible causes include a responder QP related error that prevented the responder from completing the request or a malformed WQE on the Receive Queue. Relevant for RC QPs.",
    "IBV_WC_RETRY_EXC_ERR (12) - Transport Retry Counter Exceeded: The local transport timeout retry counter was exceeded while trying to send this message. This means that the remote side didn't send any Ack or Nack. If this happens when sending the first message, usually this mean that the connection attributes are wrong or the remote side isn't in a state that it can respond to messages. If this happens after sending the first message, usually it means that the remote QP isn't available anymore. Relevant for RC QPs.",
    "IBV_WC_RNR_RETRY_EXC_ERR (13) - RNR Retry Counter Exceeded: The RNR NAK retry count was exceeded. This usually means that the remote side didn't post any WR to its Receive Queue. Relevant for RC QPs.",
    "IBV_WC_LOC_RDD_VIOL_ERR (14) - Local RDD Violation Error: The RDD associated with the QP does not match the RDD associated with the EE Context (unused, since its relevant only to RD QPs or EE Context, which aren't supported).",
    "IBV_WC_REM_INV_RD_REQ_ERR (15) - Remote Invalid RD Request: The responder detected an invalid incoming RD message. Causes include a Q_Key or RDD violation (unused, since its relevant only to RD QPs or EE Context, which aren't supported)",
    "IBV_WC_REM_ABORT_ERR (16) - Remote Aborted Error: For UD or UC QPs associated with a SRQ, the responder aborted the operation.",
    "IBV_WC_INV_EECN_ERR (17) - Invalid EE Context Number: An invalid EE Context number was detected (unused, since its relevant only to RD QPs or EE Context, which aren't supported).",
    "IBV_WC_INV_EEC_STATE_ERR (18) - Invalid EE Context State Error: Operation is not legal for the specified EE Context state (unused, since its relevant only to RD QPs or EE Context, which aren't supported).",
    "IBV_WC_FATAL_ERR (19) - Fatal Error.",
    "IBV_WC_RESP_TIMEOUT_ERR (20) - Response Timeout Error.",
    "IBV_WC_GENERAL_ERR (21) - General Error: other error which isn't one of the above errors.",
];

#[derive(Error, Debug)]
pub enum WCError {
    #[error("{err_code:?} {err_info:?}")]
    ERR{
        err_code: ibv_wc_status::Type,
        err_info: &'static str,
    },
    #[error("unknown error")]
    UNKNOWN,
}


/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = mpsc::Sender<io::Result<T>>;
type ReqMap<T> = Arc<LockFreeCuckooHash<u64, Responder<T>>>;
pub struct EventListener {
    pub cq: Arc<CompletionQueue>,
    req_map: ReqMap<u64>,
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

    pub fn start(cq: Arc<CompletionQueue>, req_map: ReqMap<u64>) -> tokio::task::JoinHandle<()> {
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
                    Ok(wr_id) => {
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
        (left << 32) | right
    }

    pub fn get_res(cq_addr: &CompletionQueue) -> io::Result<u64> {
        let cq = cq_addr.as_ptr();
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        let poll_result = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
        match poll_result.cmp(&0) {
            Ordering::Less => {
                // poll CQ failed
                //TODO:process cq failed.
                Err(io::Error::new(io::ErrorKind::Other, WCError::UNKNOWN))
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
                if wc.status == ibv_wc_status::IBV_WC_SUCCESS {
                    return Ok(wc.wr_id)
                }else {
                    let err_code  = wc.status;
                    if (err_code as usize) < WC_ERROR_NUM {
                        let err_info = WC_ERROR_MSG[err_code as usize];
                        return Err(io::Error::new(
                        io::ErrorKind::Other,
                        WCError::ERR{err_code, err_info}))
                    }else {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            WCError::UNKNOWN))
                    };
                }
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