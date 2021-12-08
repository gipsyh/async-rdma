use async_rdma::{Rdma, RdmaListener};
use std::alloc::Layout;
async fn example1(rdma: &Rdma) {
    let mr = rdma.receive_local_mr().await.unwrap();
    dbg!(unsafe { *(mr.as_ptr() as *mut i32) });
}

async fn example2(rdma: &Rdma) {
    let rmr = rdma.receive_remote_mr().await.unwrap();
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    rdma.read(&mut lmr, rmr.as_ref()).await.unwrap();
    dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

async fn example3(rdma: &Rdma) {
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    let sz = rdma.receive(&lmr).await.unwrap();
    dbg!(sz);
    dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    example1(&rdma).await;
    example2(&rdma).await;
    example3(&rdma).await;
    println!("server done");
    loop {}
}
