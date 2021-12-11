use async_rdma::{Rdma, RdmaListener};
use log::debug;
use std::{alloc::Layout, time::Duration};

async fn example1(rdma: &Rdma) {
    let mr = rdma.receive_local_mr().await.unwrap();
    dbg!(unsafe { *(mr.as_ptr() as *mut i32) });
}

async fn example2(rdma: &Rdma) {
    let rmr = rdma.receive_remote_mr().await.unwrap();
    debug!("e2 receive");
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    rdma.read(&mut lmr, rmr.as_ref()).await.unwrap();
    debug!("e2 read");
    dbg!(unsafe { *(lmr.as_ptr() as *mut i32) });
}

async fn example3(rdma: &Rdma) {
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    let sz = rdma.receive(&lmr).await.unwrap();
    debug!("e3 lmr : {:?}", unsafe { *(lmr.as_ptr() as *mut i32) });
    dbg!(sz);
    dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

#[tokio::main]
async fn main() {
    env_logger::init();
    debug!("server start");
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    debug!("accepted");
    example1(&rdma).await;
    example2(&rdma).await;
    example3(&rdma).await;
    println!("server done");
    loop {
        tokio::time::sleep(Duration::new(100, 0)).await;
    }
}
