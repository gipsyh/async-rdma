use crate::*;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{spawn, JoinHandle},
};
use tokio::time::{sleep, Duration};

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
enum Request {
    AllocMR(AllocMRRequest),
    ReceiveMR,
    ReleaseMR(ReleaseMRRequest),
}

#[derive(Serialize, Deserialize)]
enum Response {
    AllocMR(AllocMRResponse),
    ReceiveMR,
    ReleaseMR(ReleaseMRResponse),
}

fn alloc_memory_region(
    pd: &Arc<ProtectionDomain>,
    request: AllocMRRequest,
    own: Arc<Mutex<HashMap<MemoryRegionRemoteToken, Arc<MemoryRegion>>>>,
) -> Response {
    let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let mr = Arc::new(
        pd.alloc_memory_region(
            Layout::from_size_align(request.size, request.align).unwrap(),
            access,
        )
        .unwrap(),
    );
    let token = mr.remote_token();
    let response = AllocMRResponse { mr_token: token };
    own.lock().unwrap().insert(token, mr);
    tokio::spawn(resource_guard(token, Duration::from_secs(1), own));
    Response::AllocMR(response)
}

fn release_memory_region(
    request: ReleaseMRRequest,
    own: Arc<Mutex<HashMap<MemoryRegionRemoteToken, Arc<MemoryRegion>>>>,
) -> Response {
    own.lock().unwrap().remove(&request.mr_token);
    Response::ReleaseMR(ReleaseMRResponse { status: 0 })
}

async fn memory_region_timeout(
    token: MemoryRegionRemoteToken,
    duration: Duration,
    own: Arc<Mutex<HashMap<MemoryRegionRemoteToken, Arc<MemoryRegion>>>>,
) {
    sleep(duration).await;
    own.lock().unwrap().remove(&token);
}

#[tokio::main]
async fn agent_main(endp: MStreamEndPoint, pd: Arc<ProtectionDomain>) {
    let mut endp = AsyncBincodeStream::<_, Request, Response, _>::from(endp).for_async();
    let own = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let request = endp.next().await.unwrap().unwrap();
        let response = match request {
            Request::AllocMR(request) => alloc_memory_region(&pd, request, own.clone()),
            Request::ReceiveMR => todo!(),
            Request::ReleaseMR(request) => release_memory_region(request, own.clone()),
        };
        endp.send(response).await.unwrap();
    }
}

async fn resource_guard(
    token: MemoryRegionRemoteToken,
    duration: Duration,
    own: Arc<Mutex<HashMap<MemoryRegionRemoteToken, Arc<MemoryRegion>>>>,
) {
    tokio::time::sleep(duration).await;
    own.lock().unwrap().remove(&token);
}

pub struct AgentServer {
    handle: JoinHandle<()>,
}

impl AgentServer {
    pub fn new(endp: MStreamEndPoint, pd: Arc<ProtectionDomain>) -> Self {
        let handle = spawn(move || agent_main(endp, pd));
        Self { handle }
    }

    pub fn transfer_mr(&mut self, _mr: Arc<MemoryRegion>) {
        todo!()
    }
}

pub struct AgentClient {
    endp: Mutex<AsyncBincodeStream<MStreamEndPoint, Response, Request, AsyncDestination>>,
}

impl AgentClient {
    pub fn new(endp: MStreamEndPoint) -> Self {
        Self {
            endp: Mutex::new(AsyncBincodeStream::<_, Response, Request, _>::from(endp).for_async()),
        }
    }

    pub async fn alloc_mr(self: &Arc<Self>, layout: Layout) -> MemoryRegion {
        let request = Request::AllocMR(AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
        });
        self.endp.lock().unwrap().send(request).await.unwrap();
        let response = self.endp.lock().unwrap().next().await.unwrap().unwrap();
        if let Response::AllocMR(response) = response {
            MemoryRegion::from_remote_token(response.mr_token, self.clone())
        } else {
            panic!();
        }
    }

    pub async fn receive_mr() -> MemoryRegion {
        todo!()
    }

    pub async fn release_mr(self: &Arc<Self>, mr_token: MemoryRegionRemoteToken) {
        let _request = ReleaseMRRequest { mr_token };
    }
}
