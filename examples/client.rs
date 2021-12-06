use async_rdma::{Rdma, RdmaMemory};
use std::{alloc::Layout, sync::Arc};

#[tokio::main]
async fn main() {
    let rdma = Rdma::connect("127.0.0.1:5555").await.unwrap();
    let rmr = Arc::new(rdma.alloc_remote_mr(Layout::new::<i32>()).await.unwrap());
    let lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.addr() as *mut i32) = 1 };
    rdma.write(&lmr, rmr.as_ref()).await.unwrap();
    rdma.send_mr(rmr.clone()).await.unwrap();
    println!("client done");
    loop {}
}
