use async_rdma::{Rdma, RdmaListener, RdmaMemory};

async fn example1(rdma: &Rdma) {
    let mr = rdma.receive_mr().await.unwrap();
    dbg!(unsafe { *(mr.addr() as *mut i32) });
}

#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    example1(&rdma).await;
    println!("server done");
    loop {}
}
