use async_rdma::{Rdma, RdmaMemory};
use std::{alloc::Layout, sync::Arc, time::Duration};

async fn example1(rdma: &Rdma) {
    let rmr = Arc::new(rdma.alloc_remote_mr(Layout::new::<i32>()).await.unwrap());
    let lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.addr() as *mut i32) = 5 };
    rdma.write(&lmr, rmr.as_ref()).await.unwrap();
    rdma.send_mr(rmr.clone()).await.unwrap();
}

async fn example2(rdma: &Rdma) {
    let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(lmr.addr() as *mut i32) = 55 };
    rdma.send_mr(lmr.clone()).await.unwrap();
}

async fn example3(rdma: &Rdma) {
    let lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(lmr.addr() as *mut i32) = 555 };
    std::thread::sleep(Duration::from_secs(1));
    rdma.post_send(lmr.as_ref()).await.unwrap();
}

#[tokio::main]
async fn main() {
    let rdma = Rdma::connect("127.0.0.1:5555").await.unwrap();
    example1(&rdma).await;
    example2(&rdma).await;
    example3(&rdma).await;
    println!("client done");
    loop {}
}
