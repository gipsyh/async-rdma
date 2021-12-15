use crate::{
    event_listener::{EventListener, WCError},
    Gid, LocalMemoryRegion, ProtectionDomain, RemoteMemoryRegion,
};
use log::debug;
use rdma_sys::{
    ibv_access_flags, ibv_cq, ibv_destroy_qp, ibv_modify_qp, ibv_post_recv, ibv_post_send, ibv_qp,
    ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr, ibv_qp_state, ibv_recv_wr, ibv_send_flags,
    ibv_send_wr, ibv_sge, ibv_wr_opcode,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io,
    ptr::{self, NonNull},
    sync::Arc,
};

struct QueuePairInitAttr {
    qp_init_attr_inner: rdma_sys::ibv_qp_init_attr,
}

impl Default for QueuePairInitAttr {
    fn default() -> Self {
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_context = ptr::null::<libc::c_void>() as _;
        qp_init_attr.send_cq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.recv_cq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.srq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.cap.max_send_wr = 10;
        qp_init_attr.cap.max_recv_wr = 10;
        qp_init_attr.cap.max_send_sge = 10;
        qp_init_attr.cap.max_recv_sge = 10;
        qp_init_attr.cap.max_inline_data = 0;
        qp_init_attr.qp_type = rdma_sys::ibv_qp_type::IBV_QPT_RC;
        qp_init_attr.sq_sig_all = 0;
        Self {
            qp_init_attr_inner: qp_init_attr,
        }
    }
}

impl Debug for QueuePairInitAttr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePairInitAttr")
            .field("qp_context", &self.qp_init_attr_inner.qp_context)
            .field("send_cq", &self.qp_init_attr_inner.send_cq)
            .field("recv_cq", &self.qp_init_attr_inner.recv_cq)
            .field("srq", &self.qp_init_attr_inner.srq)
            .field("max_send_wr", &self.qp_init_attr_inner.cap.max_send_wr)
            .field("max_recv_wr", &self.qp_init_attr_inner.cap.max_recv_wr)
            .field("max_send_sge", &self.qp_init_attr_inner.cap.max_send_sge)
            .field("max_recv_sge", &self.qp_init_attr_inner.cap.max_recv_sge)
            .field(
                "max_inline_data",
                &self.qp_init_attr_inner.cap.max_inline_data,
            )
            .field("qp_type", &self.qp_init_attr_inner.qp_type)
            .field("sq_sig_all", &self.qp_init_attr_inner.sq_sig_all)
            .finish()
    }
}

pub struct QueuePairBuilder {
    pub pd: Arc<ProtectionDomain>,
    event_listener: Option<EventListener>,
    qp_init_attr: QueuePairInitAttr,
}

impl QueuePairBuilder {
    pub fn new(pd: &Arc<ProtectionDomain>) -> Self {
        Self {
            pd: pd.clone(),
            qp_init_attr: QueuePairInitAttr::default(),
            event_listener: None,
        }
    }

    pub fn build(mut self) -> io::Result<QueuePair> {
        let inner_qp = NonNull::new(unsafe {
            rdma_sys::ibv_create_qp(
                self.pd.as_ptr(),
                &mut self.qp_init_attr.qp_init_attr_inner as *mut _,
            )
        })
        .ok_or(io::ErrorKind::Other)?;
        Ok(QueuePair {
            pd: self.pd.clone(),
            inner_qp,
            event_listener: self.event_listener.unwrap(),
        })
    }

    pub fn set_event_listener(mut self, el: EventListener) -> Self {
        self.qp_init_attr.qp_init_attr_inner.send_cq = el.cq.as_ptr();
        self.qp_init_attr.qp_init_attr_inner.recv_cq = el.cq.as_ptr();
        self.event_listener = Some(el);
        self
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct QueuePairEndpoint {
    qp_num: u32,
    lid: u16,
    gid: Gid,
}

pub struct QueuePair {
    pd: Arc<ProtectionDomain>,
    event_listener: EventListener,
    inner_qp: NonNull<ibv_qp>,
}

impl QueuePair {
    pub(crate) fn as_ptr(&self) -> *mut ibv_qp {
        self.inner_qp.as_ptr()
    }

    pub fn endpoint(&self) -> QueuePairEndpoint {
        QueuePairEndpoint {
            qp_num: unsafe { (*self.as_ptr()).qp_num },
            lid: self.pd.ctx.get_lid(),
            gid: self.pd.ctx.gid,
        }
    }

    pub fn modify_to_init(&self, flag: ibv_access_flags) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.pkey_index = 0;
        attr.port_num = 1;
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = flag.0;
        let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn modify_to_rtr(
        &self,
        remote: QueuePairEndpoint,
        start_psn: u32,
        max_dest_rd_atomic: u8,
        min_rnr_timer: u8,
    ) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = self.pd.ctx.get_active_mtu();
        attr.dest_qp_num = remote.qp_num;
        attr.rq_psn = start_psn;
        attr.max_dest_rd_atomic = max_dest_rd_atomic;
        attr.min_rnr_timer = min_rnr_timer;
        attr.ah_attr.dlid = remote.lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.ah_attr.grh.dgid = remote.gid.into();
        attr.ah_attr.grh.hop_limit = 0xff;
        //TODO: Configurable sgid_index
        attr.ah_attr.grh.sgid_index = 1;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        debug!(
            "modify qp to rtr, err info : {:?}",
            io::Error::from_raw_os_error(errno)
        );
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn modify_to_rts(
        &self,
        timeout: u8,
        retry_cnt: u8,
        rnr_retry: u8,
        start_psn: u32,
        max_rd_atomic: u8,
    ) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = timeout;
        attr.retry_cnt = retry_cnt;
        attr.rnr_retry = rnr_retry;
        attr.sq_psn = start_psn;
        attr.max_rd_atomic = max_rd_atomic;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn post_send(&self, lm: &LocalMemoryRegion, wr_id: u64) -> io::Result<()> {
        let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        sge.addr = lm.as_ptr() as u64;
        sge.length = lm.length() as u32;
        sge.lkey = lm.lkey();
        sr.next = std::ptr::null_mut();
        sr.wr_id = wr_id;
        sr.sg_list = &mut sge;
        sr.num_sge = 1;
        sr.opcode = ibv_wr_opcode::IBV_WR_SEND;
        sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_send(self.as_ptr(), &mut sr, &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn post_receive(&self, lm: &LocalMemoryRegion, wr_id: u64) -> io::Result<()> {
        let mut rr = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
        let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
        sge.addr = lm.as_ptr() as u64;
        sge.length = lm.length() as u32;
        sge.lkey = lm.lkey();
        rr.next = std::ptr::null_mut();
        rr.wr_id = wr_id;
        rr.sg_list = &mut sge;
        rr.num_sge = 1;
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_recv(self.as_ptr(), &mut rr, &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn read_write(
        &self,
        lm: &LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
        opcode: u32,
        wr_id: u64,
    ) -> io::Result<()> {
        let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        sge.addr = lm.as_ptr() as u64;
        sge.length = lm.length() as u32;
        sge.lkey = lm.lkey();
        sr.next = std::ptr::null_mut();
        sr.wr_id = wr_id;
        sr.sg_list = &mut sge;
        sr.num_sge = 1;
        sr.opcode = opcode;
        sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.wr.rdma.remote_addr = rm.as_ptr() as u64;
        sr.wr.rdma.rkey = rm.rkey();
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_send(self.as_ptr(), &mut sr, &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub async fn send(&self, lm: &LocalMemoryRegion) -> Result<(), WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        self.post_send(lm, wr_id).unwrap();
        resp_rx
            .recv()
            .await
            .unwrap()
            .map(|sz| assert_eq!(sz, lm.length()))
    }

    pub async fn receive(&self, lm: &LocalMemoryRegion) -> Result<usize, WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        self.post_receive(lm, wr_id).unwrap();
        resp_rx.recv().await.unwrap()
    }

    pub async fn read(
        &self,
        lm: &mut LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        self.read_write(lm, rm, ibv_wr_opcode::IBV_WR_RDMA_READ, wr_id)
            .unwrap();
        resp_rx
            .recv()
            .await
            .unwrap()
            .map(|sz| assert_eq!(sz, lm.length()))
    }

    pub async fn write(
        &self,
        lm: &LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        self.read_write(lm, rm, ibv_wr_opcode::IBV_WR_RDMA_WRITE, wr_id)
            .unwrap();
        resp_rx
            .recv()
            .await
            .unwrap()
            .map(|sz| assert_eq!(sz, lm.length()))
    }
}

unsafe impl Sync for QueuePair {}

unsafe impl Send for QueuePair {}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_qp(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}
