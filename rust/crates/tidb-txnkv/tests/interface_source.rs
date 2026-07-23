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

//! Compile-time and runtime translation of `pkg/kv/mock_test.go::TestInterface`
//! and its `interface_mock_test.go` support implementation.

#![allow(non_snake_case)]

use std::collections::HashMap;
use std::time::Duration;

use tidb_txnkv::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct MockError;

impl std::fmt::Display for MockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("mock error")
    }
}

impl std::error::Error for MockError {}

impl BatchGetError for MockError {
    fn is_not_found(&self) -> bool {
        false
    }
}

#[derive(Default)]
struct MockIterator {
    key: Key,
}

impl KvIterator for MockIterator {
    type Error = MockError;

    fn valid(&self) -> bool {
        false
    }

    fn key(&self) -> &Key {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        Err(MockError)
    }

    fn close(&mut self) {}
}

#[derive(Default)]
struct MockSnapshot {
    options: HashMap<OptionKey, i32>,
}

impl Getter for MockSnapshot {
    type Error = MockError;

    fn get(&mut self, _: &Key, _: GetOptions) -> Result<ValueEntry, Self::Error> {
        Ok(ValueEntry::default())
    }
}

impl BatchGetter for MockSnapshot {
    type Error = MockError;

    fn batch_get(
        &mut self,
        _: &[Key],
        _: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(HashMap::new())
    }
}

impl Retriever for MockSnapshot {
    type Iterator = MockIterator;

    fn iter(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(MockIterator::default())
    }

    fn iter_reverse(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(MockIterator::default())
    }
}

impl Snapshot for MockSnapshot {
    type OptionValue = i32;

    fn set_option(&mut self, option: OptionKey, value: Option<Self::OptionValue>) {
        if let Some(value) = value {
            self.options.insert(option, value);
        } else {
            self.options.remove(&option);
        }
    }
}

#[derive(Default)]
struct MockBuffer {
    snapshot: MockSnapshot,
}

impl Getter for MockBuffer {
    type Error = MockError;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.snapshot.get(key, options)
    }
}

impl BatchGetter for MockBuffer {
    type Error = MockError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        self.snapshot.batch_get(keys, options)
    }
}

impl Retriever for MockBuffer {
    type Iterator = MockIterator;

    fn iter(
        &mut self,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.snapshot.iter(key, upper_bound)
    }

    fn iter_reverse(
        &mut self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.snapshot.iter_reverse(key, lower_bound)
    }
}

impl Mutator for MockBuffer {
    type Error = MockError;

    fn set(&mut self, _: Key, _: Vec<u8>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn delete(&mut self, _: Key) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl MemBuffer for MockBuffer {
    type SnapshotGetter = MockSnapshot;
    type SnapshotIterator = MockIterator;

    fn read_lock(&self) {}
    fn read_unlock(&self) {}
    fn flags(&self, _: &Key) -> Result<KeyFlags, <Self as Getter>::Error> {
        Ok(KeyFlags::default())
    }
    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        _: &[FlagsOp],
    ) -> Result<(), <Self as Getter>::Error> {
        self.set(key, value)
    }
    fn update_flags(&mut self, _: &Key, _: &[FlagsOp]) {}
    fn update_assertion_flags(&mut self, _: &Key, _: AssertionOp) {}
    fn delete_with_flags(
        &mut self,
        key: Key,
        _: &[FlagsOp],
    ) -> Result<(), <Self as Getter>::Error> {
        self.delete(key)
    }
    fn staging(&mut self) -> StagingHandle {
        StagingHandle::new(1)
    }
    fn release(&mut self, _: StagingHandle) {}
    fn cleanup(&mut self, _: StagingHandle) {}
    fn inspect_stage(
        &self,
        _: StagingHandle,
        _: &mut dyn FnMut(&Key, KeyFlags, &[u8]),
    ) {
    }
    fn snapshot_getter(&self) -> Self::SnapshotGetter {
        MockSnapshot::default()
    }
    fn snapshot_iter(&self, _: Option<&Key>, _: Option<&Key>) -> Self::SnapshotIterator {
        MockIterator::default()
    }
    fn snapshot_iter_reverse(
        &self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Self::SnapshotIterator {
        MockIterator::default()
    }
    fn len(&self) -> usize {
        0
    }
    fn size(&self) -> usize {
        0
    }
    fn remove_from_buffer(&mut self, _: &Key) {}
    fn get_local(&self, _: &[u8]) -> Result<Vec<u8>, <Self as Getter>::Error> {
        Ok(Vec::new())
    }
}

struct MockTransaction {
    valid: bool,
    options: HashMap<OptionKey, i32>,
    buffer: MockBuffer,
    snapshot: MockSnapshot,
}

impl Default for MockTransaction {
    fn default() -> Self {
        Self {
            valid: true,
            options: HashMap::new(),
            buffer: MockBuffer::default(),
            snapshot: MockSnapshot::default(),
        }
    }
}

impl MockTransaction {
    fn reset(&mut self) {
        self.valid = true;
    }
}

impl Getter for MockTransaction {
    type Error = MockError;
    fn get(&mut self, _: &Key, _: GetOptions) -> Result<ValueEntry, Self::Error> {
        Ok(ValueEntry::default())
    }
}

impl BatchGetter for MockTransaction {
    type Error = MockError;
    fn batch_get(
        &mut self,
        _: &[Key],
        _: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(HashMap::new())
    }
}

impl Retriever for MockTransaction {
    type Iterator = MockIterator;
    fn iter(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(MockIterator::default())
    }
    fn iter_reverse(
        &mut self,
        _: Option<&Key>,
        _: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        Ok(MockIterator::default())
    }
}

impl Mutator for MockTransaction {
    type Error = MockError;
    fn set(&mut self, _: Key, _: Vec<u8>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn delete(&mut self, _: Key) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl FairLockingController for MockTransaction {
    type Context = ();
    type Error = MockError;
    fn start_fair_locking(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn retry_fair_locking(&mut self, _: &Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }
    fn cancel_fair_locking(&mut self, _: &Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }
    fn done_fair_locking(&mut self, _: &Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }
    fn is_in_fair_locking_mode(&self) -> bool {
        false
    }
}

impl Transaction for MockTransaction {
    type LockContext = ();
    type OptionValue = i32;
    type Variables = ();
    type TableInfo = ();
    type DiskFullOption = ();
    type Checkpoint = ();
    type Buffer = MockBuffer;
    type Snapshot = MockSnapshot;

    fn size(&self) -> usize {
        0
    }
    fn memory_usage(&self) -> u64 {
        0
    }
    fn set_memory_footprint_hook(&mut self, _: Box<dyn FnMut(u64) + Send>) {}
    fn memory_hook_is_set(&self) -> bool {
        false
    }
    fn len(&self) -> usize {
        0
    }
    fn commit(&mut self, _: &()) -> Result<(), <Self as Getter>::Error> {
        Err(MockError)
    }
    fn rollback(&mut self) -> Result<(), <Self as Getter>::Error> {
        self.valid = false;
        Ok(())
    }
    fn diagnostic_string(&self) -> String {
        String::new()
    }
    fn lock_keys(
        &mut self,
        _: &(),
        _: &mut Self::LockContext,
        _: &[Key],
    ) -> Result<(), <Self as Getter>::Error> {
        Ok(())
    }
    fn lock_keys_with(
        &mut self,
        _: &(),
        _: &mut Self::LockContext,
        before_unlock: &mut dyn FnMut(),
        _: &[Key],
    ) -> Result<(), <Self as Getter>::Error> {
        before_unlock();
        Ok(())
    }
    fn set_option(&mut self, option: OptionKey, value: Option<Self::OptionValue>) {
        if let Some(value) = value {
            self.options.insert(option, value);
        }
    }
    fn option(&self, option: OptionKey) -> Option<&Self::OptionValue> {
        self.options.get(&option)
    }
    fn is_read_only(&self) -> bool {
        true
    }
    fn start_ts(&self) -> u64 {
        0
    }
    fn commit_ts(&self) -> u64 {
        0
    }
    fn valid(&self) -> bool {
        self.valid
    }
    fn mem_buffer(&mut self) -> &mut Self::Buffer {
        &mut self.buffer
    }
    fn snapshot(&mut self) -> &mut Self::Snapshot {
        &mut self.snapshot
    }
    fn set_variables(&mut self, _: Self::Variables) {}
    fn variables(&self) -> &Self::Variables {
        &()
    }
    fn is_pessimistic(&self) -> bool {
        false
    }
    fn cache_table_info(&mut self, _: i64, _: Self::TableInfo) {}
    fn table_info(&self, _: i64) -> Option<&Self::TableInfo> {
        None
    }
    fn set_disk_full_option(&mut self, _: Self::DiskFullOption) {}
    fn clear_disk_full_option(&mut self) {}
    fn mem_db_checkpoint(&self) -> Self::Checkpoint {}
    fn rollback_mem_db_to_checkpoint(&mut self, _: &Self::Checkpoint) {}
    fn is_pipelined(&self) -> bool {
        false
    }
    fn may_flush(&mut self) -> Result<(), <Self as Getter>::Error> {
        Ok(())
    }
}

#[derive(Default)]
struct MockResponse;

impl Response for MockResponse {
    type Context = ();
    type ResultSubset = MockResult;
    type Error = MockError;
    fn next(&mut self, _: &()) -> Result<Option<Self::ResultSubset>, Self::Error> {
        Ok(None)
    }
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct MockResult {
    key: Key,
}

impl ResultSubset for MockResult {
    fn data(&self) -> &[u8] {
        &[]
    }
    fn start_key(&self) -> &Key {
        &self.key
    }
    fn memory_size(&self) -> i64 {
        0
    }
    fn response_time(&self) -> Duration {
        Duration::ZERO
    }
}

#[derive(Default)]
struct MockClient;

impl Client for MockClient {
    type Context = ();
    type Variables = ();
    type Response = MockResponse;
    type Warning = MockError;
    fn send(
        &mut self,
        _: &(),
        _: &Request,
        _: &mut (),
        _: &mut ClientSendOption<Self::Warning>,
    ) -> Self::Response {
        MockResponse
    }
    fn is_request_type_supported(&self, request_type: i64, sub_type: i64) -> bool {
        RequestTypeSupportedChecker.is_request_type_supported(request_type, sub_type)
    }
}

#[derive(Default)]
struct MockMppClient;

impl MppClient for MockMppClient {
    type Context = ();
    type Error = MockError;
    type Backoffer = ();
    type DispatchPolicy = ();
    type ReplicaRead = ();
    type DispatchResponse = ();
    type StreamResponse = ();
    fn construct_mpp_tasks(
        &mut self,
        _: &(),
        _: &MppBuildTasksRequest,
        _: Duration,
        _: (),
        _: (),
        _: &mut dyn FnMut(&Self::Error),
    ) -> Result<Vec<MppTaskMeta>, Self::Error> {
        Ok(Vec::new())
    }
    fn dispatch_mpp_task(
        &mut self,
        _: DispatchMppTaskParam<(), ()>,
    ) -> Result<(Self::DispatchResponse, bool), Self::Error> {
        Ok(((), false))
    }
    fn establish_mpp_connections(
        &mut self,
        _: EstablishMppConnsParam<(), ()>,
    ) -> Result<(Self::StreamResponse, bool), Self::Error> {
        Ok(((), false))
    }
    fn cancel_mpp_tasks(&mut self, _: CancelMppTasksParam) {}
    fn check_visibility(&self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
    fn mpp_store_count(&self) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

struct MockStorage {
    cache: CacheDb,
    client: MockClient,
    mpp: MockMppClient,
}

impl Default for MockStorage {
    fn default() -> Self {
        Self {
            cache: CacheDb::new(),
            client: MockClient,
            mpp: MockMppClient,
        }
    }
}

impl Storage for MockStorage {
    type Error = MockError;
    type TransactionOption = ();
    type Transaction = MockTransaction;
    type Snapshot = MockSnapshot;
    type Client = MockClient;
    type MppClient = MockMppClient;
    type Oracle = ();
    type Status = ();
    type LockWait = ();
    type Codec = ();
    type OptionKey = ();
    type OptionValue = ();
    type Context = ();

    fn begin(&mut self, _: &[()]) -> Result<Self::Transaction, Self::Error> {
        Ok(MockTransaction::default())
    }
    fn snapshot(&self, _: Version) -> Self::Snapshot {
        MockSnapshot::default()
    }
    fn client(&self) -> &Self::Client {
        &self.client
    }
    fn mpp_client(&self) -> &Self::MppClient {
        &self.mpp
    }
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn uuid(&self) -> &str {
        ""
    }
    fn current_version(&self, _: &str) -> Result<Version, Self::Error> {
        Ok(Version::new(1))
    }
    fn oracle(&self) -> &Self::Oracle {
        &()
    }
    fn supports_delete_range(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "KVMockStorage"
    }
    fn describe(&self) -> &str {
        "KVMockStorage is a mock Store implementation, only for unittests in KV package"
    }
    fn show_status(&self, _: &(), _: &str) -> Result<Self::Status, Self::Error> {
        Ok(())
    }
    fn memory_cache(&self) -> &CacheDb {
        &self.cache
    }
    fn min_safe_ts(&self, _: &str) -> u64 {
        0
    }
    fn lock_waits(&self) -> Result<Vec<Self::LockWait>, Self::Error> {
        Ok(Vec::new())
    }
    fn codec(&self) -> &Self::Codec {
        &()
    }
    fn set_storage_option(&mut self, _: (), _: ()) {}
    fn storage_option(&self, _: &()) -> Option<&()> {
        None
    }
    fn cluster_id(&self) -> u64 {
        1
    }
    fn keyspace(&self) -> &str {
        ""
    }
}

#[test]
fn TestInterface() {
    fn assert_storage<T: Storage>() {}
    fn assert_transaction<T: Transaction>() {}
    fn assert_snapshot<T: Snapshot>() {}
    fn assert_client<T: Client>() {}
    fn assert_mpp_client<T: MppClient>() {}
    assert_storage::<MockStorage>();
    assert_transaction::<MockTransaction>();
    assert_snapshot::<MockSnapshot>();
    assert_client::<MockClient>();
    assert_mpp_client::<MockMppClient>();

    let mut storage = MockStorage::default();
    let _ = storage.client();
    assert_eq!(storage.uuid(), "");
    assert_eq!(storage.current_version(GLOBAL_REPLICA_SCOPE), Ok(Version::new(1)));
    let mut snapshot = storage.snapshot(Version::new(1));
    assert_eq!(
        snapshot.batch_get(
            &[
                Key::from_bytes(b"abc".as_slice()),
                Key::from_bytes(b"def".as_slice()),
            ],
            BatchGetOptions::default()
        ),
        Ok(HashMap::new())
    );
    snapshot.set_option(OptionKey::Priority, Some(0));
    assert_eq!(snapshot.options.get(&OptionKey::Priority), Some(&0));

    let mut transaction = storage.begin(&[]).expect("begin");
    transaction
        .lock_keys(&(), &mut (), &[Key::from_bytes(b"lock".as_slice())])
        .expect("lock");
    transaction.set_option(OptionKey::MatchStoreLabels, Some(23));
    assert_eq!(transaction.option(OptionKey::MatchStoreLabels), Some(&23));
    assert_eq!(transaction.start_ts(), 0);
    assert!(transaction.is_read_only());
    let lock = Key::from_bytes(b"lock".as_slice());
    assert_eq!(
        transaction.get(&lock, GetOptions::default()),
        Ok(ValueEntry::default())
    );
    transaction
        .set(lock.clone(), Vec::new())
        .expect("set empty value");
    transaction
        .iter(Some(&lock), None)
        .expect("forward iterator");
    transaction
        .iter_reverse(Some(&lock), None)
        .expect("reverse iterator");
    assert_eq!(transaction.diagnostic_string(), "");
    assert!(transaction.valid());
    assert_eq!(transaction.len(), 0);
    assert_eq!(transaction.size(), 0);
    assert_eq!(transaction.mem_buffer().len(), 0);
    assert_eq!(transaction.commit(&()), Err(MockError));

    let mut transaction = storage.begin(&[]).expect("begin");
    transaction.reset();
    transaction.rollback().expect("rollback");
    assert!(!transaction.valid());
    assert!(!transaction.is_pessimistic());
    transaction
        .delete(Key::default())
        .expect("delete empty key is delegated");
    let _ = storage.oracle();
    assert_eq!(storage.name(), "KVMockStorage");
    assert_eq!(
        storage.describe(),
        "KVMockStorage is a mock Store implementation, only for unittests in KV package"
    );
    assert!(!storage.supports_delete_range());
    assert_eq!(storage.show_status(&(), ""), Ok(()));
    storage.close().expect("close");

    let config = InjectionConfig::new();
    config.set_get_error(Some(MockError));
    config.set_commit_error(Some(MockError));
    let mut injected = new_injected_storage(MockStorage::default(), &config);
    let mut transaction = injected.begin(&[]).expect("injected begin");
    assert_eq!(
        transaction.get(
            &Key::from_bytes(b"injected".as_slice()),
            GetOptions::default()
        ),
        Err(MockError)
    );
    assert_eq!(Transaction::commit(&mut transaction, &()), Err(MockError));
    let mut snapshot = injected.snapshot(Version::new(1));
    assert_eq!(
        snapshot.get(
            &Key::from_bytes(b"injected".as_slice()),
            GetOptions::default()
        ),
        Err(MockError)
    );
}
