use async_rdma::Rdma;
use std::alloc::Layout;

#[tokio::main]
async fn main() {
    let rdma = Rdma::connect("127.0.0.1:5555").await.unwrap();
    let rmr = rdma.alloc_remote_memory_region(Layout::new::<i32>()).await;
    let lmr = rdma.alloc_memory_region(Layout::new::<i32>()).unwrap();
    // unsafe { *(lmr.addr() as *mut i32) = 1 };
    // rdma.write(&lmr, &rmr).unwrap();
    // bincode::serialize_into(rdma.normal.unwrap(), &(rmr.addr() as usize)).unwrap();
    loop {}
}
