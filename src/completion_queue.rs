use crate::{context::Context, event_channel::EventChannel};
use libc::c_void;
use rdma_sys::{ibv_create_cq, ibv_destroy_cq, ibv_req_notify_cq};
use std::{io, ptr, sync::Arc};

pub struct CompletionQueue {
    ec: Option<Arc<EventChannel>>,
    pub(super) inner_cq: *mut rdma_sys::ibv_cq,
}

impl CompletionQueue {
    pub fn create(ctx: &Context, cq_size: u32, ec: Option<&Arc<EventChannel>>) -> io::Result<Self> {
        let ec_inner = match ec {
            Some(ec) => ec.inner_ec,
            _ => ptr::null::<c_void>() as *mut _,
        };
        let inner_cq = unsafe {
            ibv_create_cq(
                ctx.inner_ctx,
                cq_size as i32,
                std::ptr::null_mut(),
                ec_inner,
                0,
            )
        };
        if inner_cq.is_null() {
            return Err(io::Error::last_os_error());
        }
        let ec = ec.cloned();
        Ok(CompletionQueue { ec, inner_cq })
    }
    pub fn req_notify(&self, solicited_only: bool) -> io::Result<()> {
        if self.ec.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "no event channel".to_string(),
            ));
        }
        let errno = unsafe { ibv_req_notify_cq(self.inner_cq, solicited_only as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_cq(self.inner_cq) };
        assert_eq!(errno, 0);
    }
}
