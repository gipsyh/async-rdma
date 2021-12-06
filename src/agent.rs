use crate::{rdma_stream::RdmaStream, MemoryRegion, MemoryRegionRemoteToken};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{alloc::Layout, collections::HashMap, io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Mutex},
    task::JoinHandle,
};

pub struct Agent {
    stream_write: Arc<Mutex<StreamWrite<RdmaStream>>>,
    response_waits: Arc<Mutex<ResponseWaitsMap>>,
    listen_main_handle: JoinHandle<io::Result<()>>,
}

impl Agent {
    pub fn new(stream: RdmaStream) -> Self {
        let stream = AsyncBincodeStream::<_, Message, Message, _>::from(stream).for_async();
        let (stream_write, stream_read) = stream.split();
        let stream_write = Arc::new(Mutex::new(stream_write));
        let response_waits = Arc::new(Mutex::new(HashMap::new()));
        let listen_main_handle = tokio::spawn(listen_main(
            stream_read,
            stream_write.clone(),
            response_waits.clone(),
        ));
        Self {
            listen_main_handle,
            stream_write,
            response_waits,
        }
    }

    pub async fn alloc_mr(&self, layout: Layout) -> io::Result<MemoryRegion> {
        todo!()
    }

    pub async fn release_mr(&self, token: MemoryRegionRemoteToken) -> io::Result<()> {
        todo!()
    }

    pub async fn send_mr(&self, mr: &MemoryRegion) -> io::Result<()> {
        todo!()
    }

    pub async fn receive_mr(&self) -> io::Result<MemoryRegion> {
        todo!()
    }

    pub async fn post_send(&self) -> io::Result<()> {
        todo!()
    }

    pub async fn post_receive(&self) -> io::Result<()> {
        todo!()
    }
}

type Stream<S> = AsyncBincodeStream<S, Message, Message, AsyncDestination>;

type StreamRead<SR> = SplitStream<Stream<SR>>;

type StreamWrite<SW> = SplitSink<Stream<SW>, Message>;

type ResponseWaitsMap = HashMap<RequestId, oneshot::Sender<io::Result<()>>>;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RequestId(usize);

#[derive(Serialize, Deserialize)]
struct AllocMRRequest {
    size: usize,
    align: usize,
}

#[derive(Serialize, Deserialize)]
struct AllocMRResponse {
    mr_token: MemoryRegionRemoteToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRRequest {
    mr_token: MemoryRegionRemoteToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRResponse {
    status: usize,
}

#[derive(Serialize, Deserialize)]
enum RequestKind {
    AllocMR(AllocMRRequest),
    ReceiveMR,
    ReleaseMR(ReleaseMRRequest),
}

#[derive(Serialize, Deserialize)]
struct Request {
    request_id: RequestId,
    kind: RequestKind,
}

#[derive(Serialize, Deserialize)]
enum ResponseKind {
    AllocMR(AllocMRResponse),
    ReceiveMR,
    SendMR,
    ReleaseMR(ReleaseMRResponse),
    SendData,
    ReceiveData,
}

#[derive(Serialize, Deserialize)]
struct Response {
    request_id: RequestId,
    kind: ResponseKind,
}

#[derive(Serialize, Deserialize)]
enum Message {
    Request(Request),
    Response(Response),
}

async fn handle_request<SW: AsyncWriteExt + Unpin + Send>(
    request: Request,
    stream_write: Arc<Mutex<StreamWrite<SW>>>,
) {
    todo!()
}

async fn handle_response(response: Response, response_waits: Arc<Mutex<ResponseWaitsMap>>) {
    let a = response_waits
        .lock()
        .await
        .remove(&response.request_id)
        .unwrap();
    todo!()
}

async fn listen_main<SR: AsyncReadExt + Unpin, SW: AsyncWriteExt + Unpin + Send + 'static>(
    mut stream_read: StreamRead<SR>,
    stream_write: Arc<Mutex<StreamWrite<SW>>>,
    response_waits: Arc<Mutex<ResponseWaitsMap>>,
) -> io::Result<()> {
    loop {
        let message = stream_read.next().await.unwrap().unwrap();
        match message {
            Message::Request(request) => {
                tokio::spawn(handle_request(request, stream_write.clone()));
            }
            Message::Response(response) => {
                tokio::spawn(handle_response(response, response_waits.clone()));
            }
        }
    }
}
