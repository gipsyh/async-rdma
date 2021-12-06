use async_rdma::{Rdma, RdmaListener, RdmaMemory};
use std::alloc::Layout;

async fn example1(rdma: &Rdma) {
    let mr = rdma.receive_mr().await.unwrap();
    dbg!(unsafe { *(mr.addr() as *mut i32) });
}

async fn example2(rdma: &Rdma) {
    let rmr = rdma.receive_mr().await.unwrap();
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    rdma.read(&mut lmr, rmr.as_ref()).await.unwrap();
    dbg!(unsafe { *(lmr.addr() as *mut i32) });
}

#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    example1(&rdma).await;
    example2(&rdma).await;
    println!("server done");
    loop {}
}
