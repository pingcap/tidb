//! Complete in-process counterpart of client-go `internal/mockstore/mocktikv`.
//!
//! The reusable state engine lives in the standalone `unistore` crate. This
//! module owns the package's kvproto, cluster, PD, session, and RPC adaptation.

use std::sync::Arc;

mod cluster;
mod pd;
mod rpc;
mod session;

pub use cluster::{
    bootstrap_with_multi_regions, bootstrap_with_multi_stores, bootstrap_with_multi_zones,
    bootstrap_with_single_store, region_contains, Cluster, RegionLookup, RegionState,
};
pub use pd::{GcBarrier, GcState, MockPdClient};
pub use rpc::{must_prewrite, put_mutations, CoprocessorHandler, RpcClient, REQUEST_MAX_SIZE};
pub use session::Session;
pub use unistore::{
    Action, Assertion, AssertionLevel, IsolationLevel, LockInfo, LockRecord, MockEngine, MockError,
    MvccInfo, Op, Pair, PessimisticAction, PessimisticLockKeyResult, PessimisticLockKeyResultType,
    PessimisticLockRequest, PessimisticWakeUpMode, PrewriteRequest, TxnMutation, WriteRecord,
    WriteType,
};

/// TiKV's memcomparable storage-key representation.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MvccKey(Vec<u8>);

impl MvccKey {
    pub fn new(raw: &[u8]) -> Self {
        let mut encoded = Vec::new();
        if !raw.is_empty() {
            crate::kv::codec::encode_bytes(&mut encoded, raw);
        }
        Self(encoded)
    }

    pub fn raw(&self) -> Vec<u8> {
        if self.0.is_empty() {
            return Vec::new();
        }
        let mut raw = Vec::new();
        crate::kv::codec::decode_bytes(&self.0, &mut raw).expect("invalid encoded MVCC key");
        raw
    }

    pub fn encoded(&self) -> &[u8] {
        &self.0
    }
}

pub fn new_mvcc_key(raw: &[u8]) -> MvccKey {
    MvccKey::new(raw)
}

pub fn new_mvcc_level_db(path: &str) -> Result<MockEngine, MockError> {
    if path.is_empty() {
        Ok(MockEngine::new())
    } else {
        MockEngine::open(path)
    }
}

pub fn must_new_mvcc_store() -> MockEngine {
    new_mvcc_level_db("").expect("in-memory mock MVCC store must open")
}

/// Builds a source-compatible mock TiKV client, cluster, and PD client.
pub fn new_tikv_and_pd_client(
    path: &str,
    coprocessor: Option<Arc<dyn CoprocessorHandler>>,
) -> Result<(RpcClient, Cluster, MockPdClient), MockError> {
    let engine = new_mvcc_level_db(path)?;
    let cluster = Cluster::new(engine.clone());
    let mut client = RpcClient::new(cluster.clone(), engine);
    if let Some(handler) = coprocessor {
        client = client.with_coprocessor_handler(handler);
    }
    let pd = MockPdClient::new(cluster.clone());
    Ok((client, cluster, pd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_mvcc_key_round_trips_raw_and_empty_keys() {
        assert_eq!(new_mvcc_key(b"").encoded(), b"");
        assert_eq!(new_mvcc_key(b"").raw(), b"");
        let key = new_mvcc_key(b"a\0z");
        assert_ne!(key.encoded(), b"a\0z");
        assert_eq!(key.raw(), b"a\0z");
    }
}
