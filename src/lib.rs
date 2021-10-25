#![feature(
    ptr_internals,
    allocator_api,
    slice_ptr_get,
    slice_ptr_len,
    bool_to_option
)]

mod completion_queue;
mod context;
mod event_channel;
mod gid;
mod memory_region;
mod protection_domain;
mod queue_pair;
mod rdma_box;

pub use completion_queue::*;
pub use context::*;
pub use event_channel::*;
pub use gid::*;
pub use memory_region::*;
pub use protection_domain::*;
pub use queue_pair::*;
pub use rdma_box::*;
