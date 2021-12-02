use async_bincode::AsyncBincodeStream;
use async_rdma::RdmaListener;
use futures::StreamExt;
#[tokio::main]
async fn main() {
    let rdmalistener = RdmaListener::bind("127.0.0.1:5555").await.unwrap();
    let rdma = rdmalistener.accept().await.unwrap();
    let mut stream =
    AsyncBincodeStream::<_, usize, usize, _>::from(rdma.normal.unwrap()).for_async();
    let ptr = stream.next().await.unwrap().unwrap();
    dbg!(unsafe { *(ptr as *mut i32) });
    println!("server done");
    loop {}
}
