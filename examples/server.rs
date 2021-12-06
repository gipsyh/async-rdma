use async_rdma::{RdmaListener, RdmaMemory};
#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    let mr = rdma.receive_mr().await.unwrap();
    dbg!(unsafe { *(mr.addr() as *mut i32) });
    println!("server done");
    loop {}
}
