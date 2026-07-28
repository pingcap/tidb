//! A bounded, foreground PD control-plane client.
//!
//! This crate implements the source call chain needed to discover a plaintext
//! PD member set, locate one region, and resolve its stores. A dedicated worker
//! owns the Tokio runtime so the public synchronous API never nests a runtime
//! owned by its caller. Membership refresh and direct endpoint failover happen
//! only in the foreground and are bounded by the current member set.

mod client;
mod error;
mod etcd;
mod model;
mod tso;

pub use client::{
    PdClient, BATCH_SCAN_REGIONS_PATH, GET_MEMBERS_PATH, GET_PREV_REGION_PATH,
    GET_REGION_BY_ID_PATH, GET_REGION_PATH, GET_STORE_PATH, SCAN_REGIONS_PATH, TSO_PATH,
};
pub use error::{PdClientError, PdClientShutdownError, PdOperation};
pub use etcd::{
    EtcdClient, EtcdError, EtcdWatchEvent, EtcdWatchStats, EtcdWatcher,
    DDL_GLOBAL_SCHEMA_VERSION_KEY, ETCD_PUT_PATH, ETCD_RANGE_PATH, ETCD_WATCH_PATH,
    PRIVILEGE_UPDATE_KEY,
};
pub use model::{
    PdBucketStats, PdBuckets, PdKeyRange, PdMemberSet, PdNodeState, PdPeer, PdRegion,
    PdRegionEpoch, PdStore, PdStoreState,
};
