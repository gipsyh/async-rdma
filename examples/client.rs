use async_rdma::{Rdma, RdmaMemory};
use std::{alloc::Layout, sync::Arc};

async fn example1(rdma: &Rdma) {
    let rmr = Arc::new(rdma.alloc_remote_mr(Layout::new::<i32>()).await.unwrap());
    let lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.addr() as *mut i32) = 55 };
    rdma.write(&lmr, rmr.as_ref()).await.unwrap();
    rdma.send_mr(rmr.clone()).await.unwrap();
}

async fn example2(rdma: &Rdma) {
    let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(lmr.addr() as *mut i32) = 55 };
    rdma.send_mr(lmr.clone()).await.unwrap();
}

#[tokio::main]
async fn main() {
    let rdma = Rdma::connect("127.0.0.1:5555").await.unwrap();
    example1(&rdma).await;
    example2(&rdma).await;
    println!("client done");
    loop {}
}
