use crate::{context::Context, event_channel::EventChannel};
use libc::c_void;
use rdma_sys::{ibv_cq, ibv_create_cq, ibv_destroy_cq, ibv_poll_cq, ibv_req_notify_cq, ibv_wc};
use std::{
    cmp::Ordering,
    io, mem,
    ptr::{self, NonNull},
    sync::Arc,
};

pub struct CompletionQueue {
    ec: Option<Arc<EventChannel>>,
    inner_cq: NonNull<ibv_cq>,
}

impl CompletionQueue {
    pub(crate) fn as_ptr(&self) -> *mut ibv_cq {
        self.inner_cq.as_ptr()
    }

    pub fn create(ctx: &Context, cq_size: u32, ec: Option<&Arc<EventChannel>>) -> io::Result<Self> {
        let ec_inner = match ec {
            Some(ec) => ec.as_ptr(),
            _ => ptr::null::<c_void>() as _,
        };
        let inner_cq = NonNull::new(unsafe {
            ibv_create_cq(
                ctx.as_ptr(),
                cq_size as _,
                std::ptr::null_mut(),
                ec_inner,
                0,
            )
        })
        .ok_or(io::ErrorKind::Other)?;
        Ok(CompletionQueue {
            ec: ec.cloned(),
            inner_cq,
        })
    }

    pub fn req_notify(&self, solicited_only: bool) -> io::Result<()> {
        if self.ec.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "no event channel".to_string(),
            ));
        }
        let errno = unsafe { ibv_req_notify_cq(self.inner_cq.as_ptr(), solicited_only as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn poll(&self, num_entries: u32) -> io::Result<Vec<WorkCompletion>> {
        let mut ans = vec![WorkCompletion::default(); num_entries as _];
        let poll_res =
            unsafe { ibv_poll_cq(self.as_ptr(), num_entries as _, ans.as_mut_ptr() as _) };
        match poll_res.cmp(&0) {
            Ordering::Greater | Ordering::Equal => {
                let poll_res = poll_res as _;
                for _ in poll_res..num_entries as _ {
                    ans.remove(poll_res);
                }
                assert_eq!(ans.len(), poll_res);
                Ok(ans)
            }
            Ordering::Less => Err(io::Error::new(io::ErrorKind::Other, "")),
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_cq(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

#[repr(C)]
pub struct WorkCompletion {
    inner_wc: ibv_wc,
}

impl WorkCompletion {
    pub(crate) fn as_ptr(&self) -> *mut ibv_wc {
        &self.inner_wc as *const _ as _
    }
}

impl Default for WorkCompletion {
    fn default() -> Self {
        Self {
            inner_wc: unsafe { mem::zeroed() },
        }
    }
}

impl Clone for WorkCompletion {
    fn clone(&self) -> Self {
        let ans = Self::default();
        unsafe { ptr::copy(self.as_ptr(), ans.as_ptr(), 1) };
        ans
    }
}
