//! PD control-plane adapter with shared native discovery and channel ownership.
//!
//! A dedicated worker owns the Tokio runtime behind the synchronous API.
//! Membership, health and timestamp discovery run under the client lifetime;
//! configured cluster security applies to foreground and background requests.

mod client;
mod error;
mod etcd;
mod metrics;
mod model;
mod security;
mod tso;

pub use client::{is_unimplemented, PdClient, PdTimestampFuture};
pub use error::{PdClientError, PdClientShutdownError, PdOperation};
pub use etcd::{
    EtcdClient, EtcdCreateOrGet, EtcdError, EtcdKeyValue, EtcdLeaseSession, EtcdWatchEvent,
    EtcdWatchResponse, EtcdWatchStats, EtcdWatcher, DDL_GLOBAL_SCHEMA_VERSION_KEY, ETCD_PUT_PATH,
    ETCD_RANGE_PATH, ETCD_WATCH_PATH, KEY_OP_DEFAULT_RETRY_CNT, KEY_OP_DEFAULT_TIMEOUT,
    KEY_OP_RETRY_INTERVAL, PRIVILEGE_UPDATE_KEY, SYSVAR_UPDATE_KEY,
};
pub use model::{
    PdBucketStats, PdBuckets, PdGcState, PdKeyRange, PdMemberSet, PdNodeState, PdPeer, PdRegion,
    PdRegionEpoch, PdStore, PdStoreState,
};
pub use security::{secure_endpoint, ClusterSecurity, TlsConfigError};
