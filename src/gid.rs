#[derive(
    serde::Serialize, serde::Deserialize, Default, Copy, Clone, Debug, Eq, PartialEq, Hash,
)]
#[repr(transparent)]
pub struct Gid {
    raw: [u8; 16],
}

#[allow(dead_code)]
impl Gid {
    /// Expose the subnet_prefix component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.subnet_prefix` component of the
    /// `ffi::ibv_gid` union.
    fn subnet_prefix(&self) -> u64 {
        u64::from_be_bytes(self.raw[..8].try_into().unwrap())
    }

    /// Expose the interface_id component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.interface_id` component of the
    /// `ffi::ibv_gid` union.
    fn interface_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..].try_into().unwrap())
    }
}

impl From<rdma_sys::ibv_gid> for Gid {
    fn from(gid: rdma_sys::ibv_gid) -> Self {
        Self {
            raw: unsafe { gid.raw },
        }
    }
}

impl From<Gid> for rdma_sys::ibv_gid {
    fn from(mut gid: Gid) -> Self {
        *gid.as_mut()
    }
}

impl AsRef<rdma_sys::ibv_gid> for Gid {
    fn as_ref(&self) -> &rdma_sys::ibv_gid {
        unsafe { &*self.raw.as_ptr().cast::<rdma_sys::ibv_gid>() }
    }
}

impl AsMut<rdma_sys::ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut rdma_sys::ibv_gid {
        unsafe { &mut *self.raw.as_mut_ptr().cast::<rdma_sys::ibv_gid>() }
    }
}
