// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A scripted cluster snapshot over the cluster storage seam, for the tests
//! of the layers built on it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tidb_txnkv::Key;

use crate::cluster_storage::*;
use tidb_kv::cluster_storage::{ClusterSnapshot, SnapshotPairs};
use tidb_kv::storage::StorageError;

/// A snapshot that answers from a fixed map and counts what the storage
/// asked it for, so a test can prove a read never reached the cluster.
#[derive(Debug, Default)]
pub struct MockSnapshot {
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    pub gets: Vec<Vec<u8>>,
    pub scans: Vec<(Vec<u8>, Vec<u8>)>,
    /// Every pair this snapshot handed back, summed over all scans.
    pub rows_read: usize,
    pub fail_with: Option<StorageError>,
    pub index_gets: usize,
    pub fail_index_get: Option<(usize, StorageError)>,
}

impl ClusterSnapshot for MockSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        if tidb_tablecodec::table_key::is_index_key(key.as_bytes()) {
            self.index_gets += 1;
            if self
                .fail_index_get
                .as_ref()
                .is_some_and(|(at, _)| *at == self.index_gets)
            {
                return Err(self.fail_index_get.take().unwrap().1);
            }
        }
        if let Some(error) = self.fail_with.clone() {
            return Err(error);
        }
        self.gets.push(key.as_bytes().to_vec());
        Ok(self.data.get(key.as_bytes()).cloned())
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        if let Some(error) = self.fail_with.clone() {
            return Err(error);
        }
        self.scans
            .push((start.as_bytes().to_vec(), end.as_bytes().to_vec()));
        let pairs: SnapshotPairs = self
            .data
            .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
            .take(limit.unwrap_or(usize::MAX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        // What the cluster actually served, which is the cost a scan pays
        // whether or not the caller goes on to consume it.
        self.rows_read += pairs.len();
        Ok(pairs)
    }
}

pub fn key(bytes: &[u8]) -> Key {
    Key::from_bytes(bytes.to_vec())
}

pub fn storage(
    pairs: &[(&[u8], &[u8])],
) -> (
    ClusterTableStorage,
    Arc<Mutex<MockSnapshot>>,
    MutationBuffer,
) {
    let snapshot = Arc::new(Mutex::new(MockSnapshot {
        data: pairs
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect(),
        ..MockSnapshot::default()
    }));
    let buffer = MutationBuffer::new();
    let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
    (
        ClusterTableStorage::new(buffer.clone(), handle),
        snapshot,
        buffer,
    )
}

/// One in-memory store that every clone shares, standing in for Go's mock
/// store under autocommit: a statement's writes are visible to the next
/// statement and to the test. (A `MemTableStorage` clone copies its rows, so
/// a statement context handing out clones would drop the writes.)
#[derive(Clone, Debug, Default)]
pub struct SharedMemStorage {
    inner: Arc<Mutex<tidb_kv::storage::MemTableStorage>>,
    /// A coprocessor a test answers this store's requests with in place of
    /// the store's own [`UnistoreCoprocessor`].
    client: Option<Arc<dyn tidb_txnkv::Client>>,
}

impl SharedMemStorage {
    fn lock(&self) -> std::sync::MutexGuard<'_, tidb_kv::storage::MemTableStorage> {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl tidb_kv::storage::TableStorage for SharedMemStorage {
    fn staging(&mut self) -> tidb_kv::storage::StagingHandle {
        self.lock().staging()
    }

    fn cleanup(&mut self, handle: tidb_kv::storage::StagingHandle) {
        self.lock().cleanup(handle);
    }

    fn release(&mut self, handle: tidb_kv::storage::StagingHandle) {
        self.lock().release(handle);
    }

    fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.lock().get(key)
    }

    fn size_bytes(&self) -> u64 {
        self.lock().size_bytes()
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
        self.lock().set(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<(), StorageError> {
        self.lock().delete(key)
    }

    fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.lock().get_local(key)
    }

    fn attach_txn_staging(&mut self, staging: Option<tidb_kv::staged_writes::TxnStaging>) {
        self.lock().attach_txn_staging(staging);
    }

    fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn tidb_kv::storage::StorageIterator>, StorageError> {
        self.lock().iter(start, upper_bound)
    }

    fn iter_reverse(
        &mut self,
        upper_bound: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Box<dyn tidb_kv::storage::StorageIterator>, StorageError> {
        self.lock().iter_reverse(upper_bound, lower_bound)
    }

    fn key_count(&self) -> usize {
        self.lock().key_count()
    }

    fn clear(&mut self) {
        self.lock().clear();
    }

    fn clone_box(&self) -> Box<dyn tidb_kv::storage::TableStorage> {
        Box::new(self.clone())
    }

    /// Go's mock store answers every reader's DAG with unistore's
    /// coprocessor over its rows, and so does this store.
    fn client(&self) -> Option<Arc<dyn tidb_txnkv::Client>> {
        Some(self.client.clone().unwrap_or_else(|| {
            let rows = Arc::downgrade(&self.inner);
            Arc::new(UnistoreCoprocessor::new(move |_| {
                rows.upgrade().map_or_else(Vec::new, |inner| {
                    SharedMemStorage { inner, client: None }.pairs()
                })
            }))
        }))
    }
}

/// Go's mock store coprocessor (`mockstore` over unistore): each request's
/// DAG answered by unistore's cop handler over the rows its source holds at
/// the request's start ts, committed in one transaction (versions 1, 2) the
/// request reads after, with the region layer's scan detail and keys-read
/// charge.
pub struct UnistoreCoprocessor {
    rows: Box<dyn Fn(u64) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync>,
}

impl UnistoreCoprocessor {
    /// A coprocessor over the pairs `rows` reads at a start ts.
    pub fn new(rows: impl Fn(u64) -> Vec<(Vec<u8>, Vec<u8>)> + Send + Sync + 'static) -> Self {
        Self { rows: Box::new(rows) }
    }
}

impl std::fmt::Debug for UnistoreCoprocessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnistoreCoprocessor")
    }
}

/// The mock store's one region, which holds every key: the store answers a
/// pushed-down index lookup's table rows from itself (Go unistore's
/// `ExtraDbReaderProvider` over a single local region).
struct OneRegionStore<'s>(&'s tidb_unistore_tikv::mvcc_store::MvccStore);

impl tidb_unistore_dbreader::DbReader for OneRegionStore<'_> {
    fn scan(&self, req: &tidb_unistore_dbreader::ScanReq) -> Vec<tidb_unistore_dbreader::KvPair> {
        tidb_unistore_dbreader::DbReader::scan(self.0, req)
    }

    fn extra_db_reader_provider(&self) -> Option<&dyn tidb_unistore_dbreader::ExtraDbReaderProvider> {
        Some(self)
    }
}

impl tidb_unistore_dbreader::ExtraDbReaderProvider for OneRegionStore<'_> {
    fn locate_extra_region(&self, _key: &[u8]) -> Result<tidb_unistore_dbreader::LocateExtraRegionResult, String> {
        Ok(tidb_unistore_dbreader::LocateExtraRegionResult {
            found: true,
            region: Some(tidb_proto::metapb::Region { id: 1, ..tidb_proto::metapb::Region::default() }),
            peer: Some(tidb_proto::metapb::Peer::default()),
            is_leader: true,
        })
    }

    fn get_extra_db_reader_by_region<'r>(
        &'r self,
        _ctx: &tidb_unistore_dbreader::GetExtraDbReaderContext,
    ) -> Result<Box<dyn tidb_unistore_dbreader::DbReader + 'r>, tidb_proto::errorpb::Error> {
        Ok(Box::new(self.0))
    }
}

/// One coprocessor response, the whole answer in one subset.
struct UnistoreCopResponse(Option<tidb_txnkv::QueryResultSubset>);

impl tidb_txnkv::Response for UnistoreCopResponse {
    fn next(&mut self) -> Result<Option<tidb_txnkv::QueryResultSubset>, tidb_txnkv::QueryResponseError> {
        Ok(self.0.take())
    }

    fn close(&mut self) -> Result<(), tidb_txnkv::QueryResponseError> {
        self.0 = None;
        Ok(())
    }
}

impl tidb_txnkv::Client for UnistoreCoprocessor {
    fn send(&self, request: &tidb_txnkv::TransportRequest) -> Box<dyn tidb_txnkv::Response + Send> {
        let metadata = request.metadata();
        let ranges = metadata
            .key_ranges
            .as_ref()
            .map(|ranges| {
                ranges
                    .partitions()
                    .iter()
                    .flatten()
                    .map(|range| tidb_proto::coprocessor::KeyRange {
                        start: range.start_key.as_bytes().to_vec(),
                        end: range.end_key.as_bytes().to_vec(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut store = tidb_unistore_tikv::mvcc_store::MvccStore::new();
        let mutations: Vec<tidb_proto::KvrpcMutation> = (self.rows)(metadata.start_ts)
            .into_iter()
            .map(|(key, value)| tidb_proto::KvrpcMutation {
                op: tidb_proto::KvrpcOp::Put as i32,
                key,
                value,
                ..tidb_proto::KvrpcMutation::default()
            })
            .collect();
        if let Some(primary) = mutations.first().map(|mutation| mutation.key.clone()) {
            let keys: Vec<Vec<u8>> = mutations.iter().map(|mutation| mutation.key.clone()).collect();
            store
                .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                    mutations,
                    primary_lock: primary,
                    start_version: 1,
                    ..tidb_unistore_tikv::mvcc_store::PrewriteReq::default()
                })
                .expect("the snapshot's rows prewrite");
            store.commit(&keys, 1, 2).expect("the snapshot's rows commit");
        }
        let response = tidb_unistore_cophandler::handle_cop_request(
            &OneRegionStore(&store),
            &tidb_proto::coprocessor::Request {
                tp: tidb_unistore_cophandler::REQ_TYPE_DAG,
                data: metadata.data.clone().unwrap_or_default(),
                ranges,
                start_ts: 3,
                ..tidb_proto::coprocessor::Request::default()
            },
        );
        if !response.other_error.is_empty() {
            return tidb_txnkv::CopErrorResponse::boxed(tidb_txnkv::QueryResponseError::Source(format!(
                "coprocessor other error: {}",
                response.other_error
            )));
        }
        // The region layer's part (Go `copIteratorWorker.collectCopRuntimeStats`
        // and the statement-wide keys-read counter it charges).
        let mut scan = tikv_client::util::ScanDetail::default();
        if let Some(details) = &response.exec_details_v2 {
            scan.merge_from_pb(details.scan_detail_v2.as_ref());
        }
        if let (Some(counter), Ok(keys)) = (&metadata.max_keys_read_counter, u64::try_from(scan.processed_keys)) {
            counter.fetch_add(keys, std::sync::atomic::Ordering::Relaxed);
        }
        Box::new(UnistoreCopResponse(Some(tidb_txnkv::QueryResultSubset {
            data: response.data.into(),
            runtime: Some(tidb_txnkv::ResponseRuntimeStats {
                scan_detail: Some(scan),
                ..tidb_txnkv::ResponseRuntimeStats::default()
            }),
        })))
    }

    fn is_request_type_supported(&self, _request_type: i64, _sub_type: i64) -> bool {
        true
    }
}

impl SharedMemStorage {
    /// This store answering coprocessor requests through `client`.
    #[must_use]
    pub fn with_client(mut self, client: Arc<dyn tidb_txnkv::Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Every committed pair the store holds, in key order: what a
    /// coprocessor over it reads.
    #[must_use]
    pub fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut store = self.lock();
        let mut iter = tidb_kv::storage::TableStorage::iter(&mut *store, None, None)
            .expect("an in-memory store iterates");
        let mut pairs = Vec::new();
        while iter.valid() {
            pairs.push((iter.key().as_bytes().to_vec(), iter.value().to_vec()));
            iter.next().expect("an in-memory iterator advances");
        }
        pairs
    }

    /// A copy of the rows that no longer shares them, for a test that
    /// corrupts one copy and keeps the original.
    #[must_use]
    pub fn fork(&self) -> Self {
        Self {
            inner: Arc::new(Mutex::new(self.lock().clone())),
            client: self.client.clone(),
        }
    }
}
