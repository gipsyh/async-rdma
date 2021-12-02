use async_bincode::AsyncBincodeStream;
use async_rdma::{Rdma, RdmaMemory};
use futures::SinkExt;
use std::alloc::Layout;

#[tokio::main]
async fn main() {
    let rdma = Rdma::connect("127.0.0.1:5555").await.unwrap();
    let rmr = rdma.alloc_remote_memory_region(Layout::new::<i32>()).await;
    let lmr = rdma.alloc_memory_region(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.addr() as *mut i32) = 1 };
    rdma.write(&lmr, &rmr).await.unwrap();
    let mut stream =
    AsyncBincodeStream::<_, usize, usize, _>::from(rdma.normal.unwrap()).for_async();
    stream.send(rmr.addr() as usize).await.unwrap();
    println!("client done");
    loop {}
}
