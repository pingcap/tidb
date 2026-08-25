// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Public test-support facade over the in-process mock TiKV implementation.
//!
//! This is the Rust counterpart of client-go's `testutils` package. It stays
//! behind the `internal-tests` feature so production users do not compile the
//! mock transport unless they explicitly request test support.

use std::sync::Arc;

pub use crate::mock::cluster::Cluster;
pub use crate::mock::mocktikv::{
    bootstrap_with_multi_regions, bootstrap_with_multi_stores, bootstrap_with_single_store,
    Cluster as MockCluster, CoprocessorHandler as CoprRpcHandler, Pair as MvccPair,
    RpcClient as MockClient, Session as RpcSession,
};
pub use unistore::MockEngine as MvccStore;

use crate::mock::mocktikv::{MockError, MockPdClient};

/// Rust consolidates the source mock error structs into one typed enum; the
/// source `ErrLocked` contract is its `Locked` variant.
pub type ErrLocked = MockError;

/// Creates a mock TiKV RPC client, concrete cluster, and PD client.
pub fn new_mock_tikv(
    path: &str,
    coprocessor: Option<Arc<dyn CoprRpcHandler>>,
) -> Result<(MockClient, MockCluster, MockPdClient), MockError> {
    crate::mock::mocktikv::new_tikv_and_pd_client(path, coprocessor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::mocktikv::{MockError, Op};

    fn assert_cluster_contract<T: Cluster>() {}

    #[test]
    fn source_facade_aliases_factory_and_bootstrap_helpers() {
        assert_cluster_contract::<MockCluster>();

        let (client, cluster, _pd) = new_mock_tikv("", None).unwrap();
        let _: MockClient = client.clone();
        let engine: MvccStore = client.engine();
        let _: RpcSession = RpcSession::new(cluster.clone(), engine.clone(), 0);
        let _: MvccPair = MvccPair {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            commit_ts: 2,
            error: None,
        };
        let locked: ErrLocked = MockError::Locked {
            key: b"key".to_vec(),
            primary: b"key".to_vec(),
            start_ts: 1,
            for_update_ts: 0,
            ttl: 100,
            txn_size: 1,
            lock_type: Op::Put,
            min_commit_ts: 0,
        };
        assert!(matches!(locked, ErrLocked::Locked { start_ts: 1, .. }));
        assert_eq!(bootstrap_with_single_store(&cluster), (1, 2, 3));

        let (_, cluster, _) = new_mock_tikv("", None).unwrap();
        let (stores, peers, region, leader) = bootstrap_with_multi_stores(&cluster, 3);
        assert_eq!(stores.len(), 3);
        assert_eq!(peers.len(), 3);
        assert_eq!(region, 7);
        assert_eq!(leader, peers[0]);

        let (_, cluster, _) = new_mock_tikv("", None).unwrap();
        let (store, regions, peers) =
            bootstrap_with_multi_regions(&cluster, &[b"b".to_vec(), b"d".to_vec()]);
        assert_eq!(store, 1);
        assert_eq!(regions.len(), 3);
        assert_eq!(peers.len(), 3);
    }
}
