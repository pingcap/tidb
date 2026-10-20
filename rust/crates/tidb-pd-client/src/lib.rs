//! TiDB's etcd client over PD's embedded etcd (Go `domain.etcdClient` and
//! `pkg/util/etcd`), the cluster TLS settings the node's cluster clients
//! share, and the store records PD's HTTP API answers. The node's PD client
//! is client-rust's, reached through its store (Go `store.GetPDClient()`).

mod etcd;
mod model;
mod security;

pub use etcd::{
    EtcdClient, EtcdCreateOrGet, EtcdError, EtcdKeyValue, EtcdLeaseSession, EtcdWatchEvent,
    EtcdWatchResponse, EtcdWatchStats, EtcdWatcher, DDL_GLOBAL_SCHEMA_VERSION_KEY, ETCD_PUT_PATH,
    ETCD_RANGE_PATH, ETCD_WATCH_PATH, KEY_OP_DEFAULT_RETRY_CNT, KEY_OP_DEFAULT_TIMEOUT,
    KEY_OP_RETRY_INTERVAL, PRIVILEGE_UPDATE_KEY, SYSVAR_UPDATE_KEY,
};
pub use model::{PdNodeState, PdStore, PdStoreState};
pub use security::{secure_endpoint, ClusterSecurity, TlsConfigError};
