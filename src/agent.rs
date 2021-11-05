use crate::*;
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    sync::Arc,
    time::SystemTime,
};

// trait Resource {

// }

// trait ResourceKey {

// }
// trait RemoteOwnResource {
//     fn key(&self) -> u128;
// }

// struct Resource {
//     data: Arc<MemoryRegion>,
//     // release_time: SystemTime,
// }

// pub struct ResourceFakeOwner {
//     own: BTreeMap<MemoryRegionRemoteToken, Resource>,
// }

// impl ResourceFakeOwner {
//     fn release(&mut self, token: MemoryRegionRemoteToken) {}
//     pub fn own(&mut self, obj: Arc<MemoryRegion>) {
//         todo!()
//         // self.own.insert(obj.remote_token(), Resource { data: obj });
//     }
// }

// struct A {}
// struct B {}
// impl RemoteOwnResource for A {}
// impl RemoteOwnResource for B {}

// #[test]
// fn test() {
//     let mut b = ResourceFakeOwner {
//         own: BTreeMap::new(),
//     };
//     b.own(Arc::new(A {}));
//     b.own(Arc::new(B {}));
// }
