use async_rdma::{DoubleRdma, Rdma};
use std::{alloc::Layout, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn example1(rdma: &Rdma) {
    let rmr = Arc::new(rdma.alloc_remote_mr(Layout::new::<i32>()).await.unwrap());
    let mut lmr = rdma.alloc_local_mr(Layout::new::<i32>()).unwrap();
    unsafe { *(lmr.as_mut_ptr() as *mut i32) = 5 };
    rdma.write(&lmr, rmr.as_ref()).await.unwrap();
    rdma.send_mr(rmr.clone()).await.unwrap();
}

async fn example2(rdma: &Rdma) {
    let mut lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(Arc::get_mut(&mut lmr).unwrap().as_mut_ptr() as *mut i32) = 55 };
    rdma.send_mr(lmr.clone()).await.unwrap();
}

async fn example3(rdma: &Rdma) {
    let mut lmr = Arc::new(rdma.alloc_local_mr(Layout::new::<i32>()).unwrap());
    unsafe { *(Arc::get_mut(&mut lmr).unwrap().as_mut_ptr() as *mut i32) = 555 };
    std::thread::sleep(Duration::from_micros(10));
    rdma.send(lmr.as_ref()).await.unwrap();
}

async fn example4(rdma: &mut DoubleRdma) {
    for _ in 0..10 {
        let data = vec![1, 2, 3, 4];
        println!("begin write");
        rdma.write_all(data.as_slice()).await.unwrap();
        println!("write done");
    }
    for _ in 0..10 {
        let mut data = vec![0, 0, 0, 0];
        rdma.read_exact(data.as_mut()).await.unwrap();
        println!("read done");
        dbg!(data);
    }
}

#[tokio::main]
async fn main() {
    let mut rdma = DoubleRdma::connect("127.0.0.1:5555", "127.0.0.1:6666")
        .await
        .unwrap();
    // example1(&mut rdma).await;
    // example2(&mut rdma).await;
    // example3(&mut rdma).await;
    example4(&mut rdma).await;
    // println!("client done");
    loop {}
}
