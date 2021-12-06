use crate::{rdma_stream::RdmaStream, MemoryRegion, MemoryRegionRemoteToken, ProtectionDomain};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures::{
    stream::{SplitSink, SplitStream, StreamExt},
    SinkExt,
};
use rand::Rng;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{alloc::Layout, collections::HashMap, io, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
};

pub struct Agent {
    stream_write: Arc<Mutex<StreamWrite<RdmaStream>>>,
    response_waits: Arc<Mutex<ResponseWaitsMap>>,
    mr_own: Arc<Mutex<HashMap<MemoryRegionRemoteToken, Arc<MemoryRegion>>>>,
    mr_recv: Mutex<Receiver<io::Result<Arc<MemoryRegion>>>>,
    handle: Option<JoinHandle<io::Result<()>>>,
}

impl Agent {
    pub fn new(stream: RdmaStream, pd: Arc<ProtectionDomain>) -> Arc<Self> {
        let stream = AsyncBincodeStream::<_, Message, Message, _>::from(stream).for_async();
        let (stream_write, stream_read) = stream.split();
        let stream_write = Arc::new(Mutex::new(stream_write));
        let response_waits = Arc::new(Mutex::new(HashMap::new()));
        let mr_own = Arc::new(Mutex::new(HashMap::new()));
        let (mr_send, mr_recv) = channel(1024);
        let mr_recv = Mutex::new(mr_recv);
        let ans = Arc::new(Self {
            handle: None,
            stream_write,
            mr_own,
            response_waits,
            mr_recv,
        });
        let _handle = tokio::spawn(listen_main(ans.clone(), stream_read, pd, mr_send));
        // ans.handle = Some(handle);
        ans
    }

    pub async fn alloc_mr(self: &Arc<Self>, layout: Layout) -> io::Result<MemoryRegion> {
        let request = AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::AllocMR(request),
        };
        let response = self.send_request(request).await.unwrap();
        if let ResponseKind::AllocMR(response) = response {
            Ok(MemoryRegion::from_remote_token(
                response.token,
                self.clone(),
            ))
        } else {
            panic!()
        }
    }

    pub async fn release_mr(&self, token: MemoryRegionRemoteToken) -> io::Result<()> {
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::ReleaseMR(ReleaseMRRequest { token }),
        };
        let _response = self.send_request(request).await.unwrap();
        Ok(())
    }

    pub async fn send_mr(&self, mr: Arc<MemoryRegion>) -> io::Result<()> {
        let request = if mr.is_local() {
            let ans = SendMRKind::Local(mr.token());
            self.mr_own.lock().await.insert(mr.token(), mr);
            ans
        } else {
            SendMRKind::Remote(mr.token())
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::SendMR(SendMRRequest { kind: request }),
        };
        let _response = self.send_request(request).await.unwrap();
        Ok(())
    }

    pub async fn receive_mr(&self) -> io::Result<Arc<MemoryRegion>> {
        self.mr_recv.lock().await.recv().await.unwrap()
    }

    pub async fn post_send(&self) -> io::Result<()> {
        todo!()
    }

    pub async fn post_receive(&self) -> io::Result<()> {
        todo!()
    }

    async fn send_request(&self, request: Request) -> io::Result<ResponseKind> {
        let (send, recv) = oneshot::channel();
        self.response_waits
            .lock()
            .await
            .insert(request.request_id, send);
        self.stream_write
            .lock()
            .await
            .send(Message::Request(request))
            .await
            .unwrap();
        recv.await.unwrap()
    }
}

type Stream<S> = AsyncBincodeStream<S, Message, Message, AsyncDestination>;

type StreamRead<SR> = SplitStream<Stream<SR>>;

type StreamWrite<SW> = SplitSink<Stream<SW>, Message>;

type ResponseWaitsMap = HashMap<RequestId, oneshot::Sender<io::Result<ResponseKind>>>;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RequestId(usize);

impl RequestId {
    fn new() -> Self {
        Self(rand::thread_rng().gen())
    }
}

#[derive(Serialize, Deserialize)]
struct AllocMRRequest {
    size: usize,
    align: usize,
}

#[derive(Serialize, Deserialize)]
struct AllocMRResponse {
    token: MemoryRegionRemoteToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRRequest {
    token: MemoryRegionRemoteToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRResponse {
    status: usize,
}

#[derive(Serialize, Deserialize)]
enum SendMRKind {
    Local(MemoryRegionRemoteToken),
    Remote(MemoryRegionRemoteToken),
}

#[derive(Serialize, Deserialize)]
struct SendMRRequest {
    kind: SendMRKind,
}

#[derive(Serialize, Deserialize)]
struct SendMRResponse {}

#[derive(Serialize, Deserialize)]
struct ReceiveMRRequest {}

#[derive(Serialize, Deserialize)]
struct ReceiveMRResponse {}

#[derive(Serialize, Deserialize)]
enum RequestKind {
    AllocMR(AllocMRRequest),
    ReleaseMR(ReleaseMRRequest),
    SendMR(SendMRRequest),
    ReceiveMR,
    SendData,
    ReceiveData,
}

#[derive(Serialize, Deserialize)]
struct Request {
    request_id: RequestId,
    kind: RequestKind,
}

#[derive(Serialize, Deserialize)]
enum ResponseKind {
    AllocMR(AllocMRResponse),
    ReleaseMR(ReleaseMRResponse),
    SendMR(SendMRResponse),
    ReceiveMR,
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

async fn handle_request(
    agent: Arc<Agent>,
    request: Request,
    pd: Arc<ProtectionDomain>,
    mr_send: Sender<io::Result<Arc<MemoryRegion>>>,
) {
    let response = match request.kind {
        RequestKind::AllocMR(param) => {
            let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
            let mr = Arc::new(
                pd.alloc_memory_region(
                    Layout::from_size_align(param.size, param.align).unwrap(),
                    access,
                )
                .unwrap(),
            );
            let token = mr.token();
            let response = AllocMRResponse { token };
            agent.mr_own.lock().await.insert(token, mr);
            ResponseKind::AllocMR(response)
        }
        RequestKind::ReleaseMR(param) => {
            assert!(agent.mr_own.lock().await.remove(&param.token).is_some());
            ResponseKind::ReleaseMR(ReleaseMRResponse { status: 0 })
        }
        RequestKind::SendMR(param) => {
            match param.kind {
                SendMRKind::Local(token) => {
                    assert!(mr_send
                        .send(Ok(Arc::new(MemoryRegion::from_remote_token(
                            token,
                            agent.clone()
                        ))))
                        .await
                        .is_ok());
                }
                SendMRKind::Remote(token) => {
                    let mr = agent.mr_own.lock().await.get(&token).unwrap().clone();
                    assert!(mr_send.send(Ok(mr)).await.is_ok());
                }
            }
            ResponseKind::SendMR(SendMRResponse {})
        }
        RequestKind::ReceiveMR => todo!(),
        RequestKind::SendData => todo!(),
        RequestKind::ReceiveData => todo!(),
    };
    let response = Response {
        request_id: request.request_id,
        kind: response,
    };
    agent
        .stream_write
        .lock()
        .await
        .send(Message::Response(response))
        .await
        .unwrap();
}

async fn handle_response(response: Response, response_waits: Arc<Mutex<ResponseWaitsMap>>) {
    let sender = response_waits
        .lock()
        .await
        .remove(&response.request_id)
        .unwrap();
    match sender.send(Ok(response.kind)) {
        Ok(_) => (),
        Err(_) => todo!(),
    }
}

async fn listen_main<SR: AsyncReadExt + Unpin>(
    agent: Arc<Agent>,
    mut stream_read: StreamRead<SR>,
    pd: Arc<ProtectionDomain>,
    mr_send: Sender<io::Result<Arc<MemoryRegion>>>,
) -> io::Result<()> {
    loop {
        let message = stream_read.next().await.unwrap().unwrap();
        match message {
            Message::Request(request) => {
                tokio::spawn(handle_request(
                    agent.clone(),
                    request,
                    pd.clone(),
                    mr_send.clone(),
                ));
            }
            Message::Response(response) => {
                tokio::spawn(handle_response(response, agent.response_waits.clone()));
            }
        }
    }
}
