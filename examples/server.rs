use async_rdma::{Rdma, RdmaListener, DoubleRdmaListener, DoubleRdma};
use futures::FutureExt;
use std::{alloc::Layout, pin::Pin, time::Duration};
use tokio::{io::{AsyncWriteExt, AsyncReadExt}, time::sleep};

async fn example1(rdma: &mut Rdma) {
    let mr = rdma.receive_local_mr().await.unwrap();
    dbg!(unsafe { *(mr.as_ptr() as *mut i32) });
}

async fn example2(rdma: &mut Rdma) {
    // let rmr = rdma.receive_remote_mr().await.unwrap();
    // let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    // rdma.read(&mut lmr, rmr.as_ref()).await.unwrap();
    // dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

async fn example3(rdma: &mut Rdma) {
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    let sz = rdma.receive(&lmr).await.unwrap();
    dbg!(sz);
    dbg!(unsafe { *(lmr.as_mut_ptr() as *mut i32) });
}

async fn example4(rdma: &mut DoubleRdma) {
    for _ in 0..5 {
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0];
        println!("begin read");
        rdma.read_exact(data.as_mut()).await.unwrap();
        println!("read done");
        dbg!(data);
    }
    for _ in 0..5 {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        rdma.write_all(data.as_slice()).await.unwrap();
        println!("write done");
    }
}

#[tokio::main]
async fn main() {
    let rdmalistener = DoubleRdmaListener::bind("127.0.0.1:5555", "127.0.0.1:6666").await.unwrap();
    let mut rdma = rdmalistener.accept().await.unwrap();
    // example1(&mut rdma).await;
    // example2(&mut rdma).await;
    // example3(&mut rdma).await;
    example4(&mut rdma).await;
    // println!("server done");
    loop {}
}
