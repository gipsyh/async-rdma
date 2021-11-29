use async_rdma::RdmaListener;
#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    // let ptr: usize = bincode::deserialize_from(rdma.normal.unwrap()).unwrap();
    // dbg!(unsafe { *(ptr as *mut i32) });
    loop {}
}
