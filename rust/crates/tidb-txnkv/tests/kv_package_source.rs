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

//! Source-shaped translation of every non-benchmark test in `pkg/kv`.

#![allow(non_snake_case)]

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use tidb_codec::encode_key;
use tidb_datatype::{BinaryLiteral, Datum, Decimal};
use tidb_proto::{CoprocessorKeyRange, ResourceGroupTag};
use tidb_txnkv::*;

#[test]
fn TestVersion() {
    assert!(Version::new(42) < Version::new(43));
    assert!(Version::new(42) > Version::new(41));
    assert_eq!(Version::new(42), Version::new(42));
    assert!(MIN_VERSION < MAX_VERSION);
}

#[test]
fn TestMppVersion() {
    assert_eq!(MppVersion::NEWEST.as_i64(), 3);
    for (name, expected) in [
        ("unspecified", MppVersion::Unspecified),
        ("-1", MppVersion::Unspecified),
        ("0", MppVersion::V0),
        ("1", MppVersion::V1),
        ("2", MppVersion::V2),
        ("3", MppVersion::V3),
    ] {
        assert_eq!(MppVersion::parse(name), Some(expected), "{name}");
    }
    assert_eq!(MppVersion::from_raw(99).as_i64(), 99);
}

#[test]
fn package_root_data_and_option_contracts_are_distinct_and_complete() {
    let entry = Entry::new(Key::from_bytes(b"k".as_slice()), b"v".as_slice());
    assert_eq!(entry.key, b"k");
    assert_eq!(entry.value, b"v");

    let empty = ValueEntry::new(Vec::new(), 7);
    assert!(empty.is_value_empty());
    assert_eq!(empty.size(), 32);
    let value = ValueEntry::new(b"value".as_slice(), 9);
    assert!(!value.is_value_empty());
    assert_eq!(value.size(), 37);

    let shared = with_return_commit_ts();
    let batch_options = [shared];
    let get_options = batch_get_to_get_options(&batch_options);
    assert_eq!(get_options, vec![shared]);

    let mut applied_get = GetOptions::default();
    applied_get.apply(&get_options);
    assert!(applied_get.return_commit_ts);
    let mut applied_batch = BatchGetOptions::default();
    applied_batch.apply(&batch_options);
    assert!(applied_batch.return_commit_ts);

    assert_eq!(StagingHandle::INVALID.raw(), 0);
    assert_eq!(StagingHandle::INVALID.index(), None);
    assert_eq!(StagingHandle::LAST_ACTIVE.raw(), -1);
    assert_eq!(StagingHandle::LAST_ACTIVE.index(), None);

    fn assert_mem_manager<T: MemManager>() {}
    assert_mem_manager::<CacheDb>();
    let _cache = new_cache_db();

    assert_eq!(RequestType::Select.raw(), REQ_TYPE_SELECT);
    assert_eq!(RequestType::Index.raw(), REQ_TYPE_INDEX);
    assert_eq!(RequestType::from_raw(9_001).raw(), 9_001);
    assert_eq!(StoreType::from_raw(17).name(), "unspecified");
    assert_eq!(StoreType::from_raw(17).raw(), 17);
    assert_eq!(IsolationLevel::from_raw(-7).raw(), -7);
    assert_eq!(Priority::from_raw(99).raw(), 99);
    assert!(ReplicaReadType::from_raw(255).is_follower_read());
    assert_eq!(ReplicaReadType::from_raw(255).raw(), 255);

    let request = Request {
        concurrency: -1,
        store_batch_size: -2,
        store_busy_threshold_ns: -3,
        ..Request::default()
    };
    assert_eq!(request.concurrency, -1);
    assert_eq!(request.store_batch_size, -2);
    assert_eq!(request.store_busy_threshold_ns, -3);
}

#[test]
fn TestPartialNext() {
    let key_a = tidb_codec::encode_value(&[Datum::new_string("abc"), Datum::new_string("def")])
        .expect("encode key A");
    let key_b = tidb_codec::encode_value(&[Datum::new_string("abca"), Datum::new_string("def")])
        .expect("encode key B");
    let seek = tidb_codec::encode_value(&[Datum::new_string("abc")]).expect("encode seek");
    let seek = Key::from_bytes(seek);
    assert!(seek.next().as_bytes() < key_a.as_slice());
    assert!(seek.prefix_next().as_bytes() > key_a.as_slice());
    assert!(seek.prefix_next().as_bytes() < key_b.as_slice());
}

#[test]
fn TestIsPoint() {
    for (start, end, expected) in [
        (b"rowkey1".to_vec(), b"rowkey2".to_vec(), true),
        (b"rowkey1".to_vec(), b"rowkey3".to_vec(), false),
        (Vec::new(), vec![0], true),
        (vec![123, 123, 255, 255], vec![123, 124, 0, 0], true),
        (vec![123, 123, 255, 255], vec![123, 124, 0, 1], false),
        (vec![123, 123], vec![123, 123, 0], true),
        (vec![255], vec![0], false),
    ] {
        assert_eq!(
            KeyRange::new(Key::from_bytes(start), Key::from_bytes(end)).is_point(),
            expected
        );
    }
}

#[test]
fn TestBasicFunc() {
    assert!(!is_txn_retryable_error(None));
    assert!(is_txn_retryable_error(Some(&ERR_TXN_RETRYABLE)));
    assert!(!is_txn_retryable_error(Some(&ERR_NOT_EXIST)));
}

#[test]
fn TestHandle() {
    let int = Handle::from(IntHandle::new(100));
    assert!(int.is_int());
    assert_eq!(int.int_value(), Some(100));
    assert_eq!(int.next().int_value(), Some(101));
    assert!(!int.equal(&int.next()));
    assert_eq!(int.compare(&int.next()), Ok(std::cmp::Ordering::Less));
    assert_eq!(int.to_string(), "100");

    let common = CommonHandle::new(
        encode_key(&[Datum::new_int(100), Datum::new_string("abc")]).expect("common key"),
    )
    .expect("common handle");
    let next = common.next();
    assert!(!common.equal(&Handle::from(next.clone())));
    assert_eq!(common.compare(&next), std::cmp::Ordering::Less);
    assert_eq!(next.encoded().len(), common.encoded().len());
    assert_eq!(common.num_columns(), 2);
    let decoded = common.data().expect("decode");
    assert_eq!(decoded[0], Datum::Int(100));
    assert_eq!(decoded[1].as_raw_bytes(), Some(b"abc".as_slice()));
    assert_eq!(common.to_string(), "{100, abc}");

    let partition_int = Handle::from(PartitionHandle::new(2, int.clone()));
    assert!(partition_int.equal(&int));
    assert!(int.equal(&partition_int));

    let next = Handle::from(next);
    let partition_common = Handle::from(PartitionHandle::new(1, next.clone()));
    assert!(partition_common.equal(&next));
    assert!(next.equal(&partition_common));
}

#[test]
fn TestPaddingHandle() {
    let encoded = encode_key(&[Datum::new_decimal(Decimal::from_int(1))]).expect("decimal key");
    assert!(encoded.len() < 9);
    let handle = CommonHandle::new(encoded.clone()).expect("common handle");
    assert_eq!(handle.encoded().len(), 9);
    assert_eq!(handle.encoded_column(0), Some(encoded.as_slice()));
    let reparsed = CommonHandle::new(handle.encoded().to_vec()).expect("reparse");
    assert_eq!(reparsed.encoded_column(0), handle.encoded_column(0));
}

#[test]
fn TestHandleMap() {
    let mut map = HandleMap::new();
    let int = Handle::from(IntHandle::new(1));
    assert_eq!(map.mem_usage(), 32);
    map.set(int.clone(), 1_i32);
    assert_eq!(map.get(&int), Some(&1));
    assert_eq!(map.mem_usage(), 32 + 8 + 16);
    assert_eq!(map.delete(&int), Some(1));
    assert_eq!(map.mem_usage(), 32);

    let common = Handle::from(
        CommonHandle::new(
            encode_key(&[Datum::new_int(100), Datum::new_string("abc")]).expect("key"),
        )
        .expect("handle"),
    );
    map.set(common.clone(), 1000);
    assert_eq!(map.get(&common), Some(&1000));
    assert_eq!(map.mem_usage(), 32 + 16 + common.encoded().len() as i64 + 32);
    assert_eq!(map.delete(&common), Some(1000));
    assert_eq!(map.get(&common), None);

    let mut expected = Vec::new();
    for (value, mapped) in [(100, 1), (101, 2), (99, 3)] {
        let handle = Handle::from(
            CommonHandle::new(
                encode_key(&[
                    Datum::new_int(value),
                    Datum::new_string(if value == 99 { "def" } else { "abc" }),
                ])
                .expect("key"),
            )
            .expect("handle"),
        );
        expected.push((handle.clone(), mapped));
        map.set(handle, mapped);
    }
    assert_eq!(map.len(), 3);
    let mut visited = 0;
    map.range(|handle, value| {
        assert_eq!(
            expected
                .iter()
                .find(|(expected_handle, _)| expected_handle.equal(handle))
                .map(|(_, expected_value)| expected_value),
            Some(value)
        );
        visited += 1;
        visited != 2
    });
    assert_eq!(visited, 2);
}

#[test]
fn TestCommonHandlesFitIntHandleRange() {
    let minimum = IntHandle::new(i64::MIN).encoded();
    let maximum = IntHandle::new(i64::MAX).encoded();
    let cases = [
        vec![Datum::new_int(101), Datum::new_string("abc")],
        vec![Datum::new_string("abc"), Datum::new_int(101)],
        vec![Datum::new_int(-101), Datum::new_string("abc")],
        vec![Datum::new_int(i64::MIN), Datum::new_int(i64::MAX)],
        vec![Datum::new_bytes(vec![0xff, 0xff])],
        vec![Datum::new_bytes(vec![0, 0])],
        vec![Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff, 0xff]))],
    ];
    for data in cases {
        let handle = CommonHandle::new(encode_key(&data).expect("key")).expect("handle");
        assert!(minimum.as_slice() < handle.encoded());
        assert!(maximum.as_slice() > handle.encoded());
    }
}

fn partial_handles() -> Vec<(Handle, i32)> {
    let decimal = CommonHandle::new(
        encode_key(&[Datum::new_decimal(Decimal::from_int(1))]).expect("decimal"),
    )
    .expect("common");
    vec![
        (Handle::from(PartitionHandle::new(1, IntHandle::new(1))), 1),
        (Handle::from(PartitionHandle::new(2, IntHandle::new(1))), 2),
        (Handle::from(PartitionHandle::new(1, IntHandle::new(3))), 5),
        (Handle::from(IntHandle::new(1)), 3),
        (Handle::from(decimal), 4),
    ]
}

#[test]
fn TestHandleMapWithPartialHandle() {
    let mut map = HandleMap::new();
    let handles = partial_handles();
    for (handle, value) in &handles {
        map.set(handle.clone(), *value);
    }
    for (handle, value) in &handles {
        assert_eq!(map.get(handle), Some(value));
    }
    assert_eq!(map.len(), 5);
    map.delete(&handles[0].0);
    assert_eq!(map.get(&handles[0].0), None);
    map.delete(&Handle::from(PartitionHandle::new(3, IntHandle::new(1))));
    assert_eq!(map.len(), 4);
}

#[test]
fn TestMemAwareHandleMapWithPartialHandle() {
    let mut map = MemAwareHandleMap::new();
    let handles = partial_handles();
    for (handle, value) in &handles {
        map.set(handle.clone(), *value);
    }
    for (handle, value) in &handles {
        assert_eq!(map.get(handle), Some(value));
    }
}

#[test]
fn TestKeyRangeDefinition() {
    assert_eq!(
        std::mem::size_of::<KeyRange>(),
        std::mem::size_of::<CoprocessorKeyRange>()
    );
    let range = KeyRange::default();
    let protobuf = CoprocessorKeyRange::default();
    assert!(range.start_key.is_empty());
    assert!(range.end_key.is_empty());
    assert!(protobuf.start.is_empty());
    assert!(protobuf.end.is_empty());
    let ranges = vec![
        KeyRange::new(Key::from_bytes(b"s1".as_slice()), Key::from_bytes(b"e1".as_slice())),
        KeyRange::new(Key::from_bytes(b"s2".as_slice()), Key::from_bytes(b"e2".as_slice())),
    ];
    assert_eq!(key_range_slice_mem_usage(&ranges, ranges.capacity()), 104);
}

#[test]
fn TestResourceGroupTagEncoding() {
    let mut builder = ResourceGroupTagBuilder::new(None);
    builder.set_sql_digest(&[]);
    let encoded = builder.encode_tag_with_key(&[]);
    assert_eq!(encoded.len(), 2);
    let decoded = ResourceGroupTag::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded.sql_digest, None);
    assert_eq!(decoded.keyspace_name, None);

    builder.set_sql_digest(b"aa");
    let encoded = builder.encode_tag_with_key(&[]);
    assert_eq!(encoded.len(), 6);
    assert_eq!(
        ResourceGroupTag::decode(encoded.as_slice())
            .expect("decode")
            .sql_digest
            .as_deref(),
        Some(b"aa".as_slice())
    );

    let mut keyspace_builder = ResourceGroupTagBuilder::new(Some(b"123"));
    let digest = vec![b'a'; 64];
    keyspace_builder.set_sql_digest(&digest);
    let decoded =
        ResourceGroupTag::decode(keyspace_builder.encode_tag_with_key(&[]).as_slice())
            .expect("decode");
    assert_eq!(decoded.sql_digest.as_deref(), Some(digest.as_slice()));
    assert_eq!(decoded.keyspace_name.as_deref(), Some(b"123".as_slice()));

    let long_digest = vec![b'f'; 510];
    for keyspace_name in [None, Some(b"system".as_slice())] {
        let mut builder = ResourceGroupTagBuilder::new(keyspace_name);
        builder.set_sql_digest(&long_digest);
        let decoded = ResourceGroupTag::decode(builder.encode_tag_with_key(&[]).as_slice())
            .expect("decode long digest");
        assert_eq!(
            decoded.sql_digest.as_deref(),
            Some(long_digest.as_slice())
        );
        assert_eq!(decoded.keyspace_name.as_deref(), keyspace_name);
    }
}

#[test]
fn TestMain() {
    assert_eq!(MAX_RETRY_COUNT, 100);
    assert!(!GLOBAL_INNER_TXN_START_TS.contains(0));
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MockError {
    Retryable,
    Ordinary,
}

impl fmt::Display for MockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for MockError {}

impl BatchGetError for MockError {
    fn is_not_found(&self) -> bool {
        false
    }
}

impl NewTxnError for MockError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable)
    }
}

#[derive(Default)]
struct MockNewTransaction {
    start_ts: u64,
    commit_error: Option<MockError>,
    options: Vec<(OptionKey, TxnOptionValue)>,
    rolled_back: bool,
}

impl NewTxnTransaction for MockNewTransaction {
    type Error = MockError;

    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn set_option(&mut self, option: OptionKey, value: TxnOptionValue) {
        self.options.push((option, value));
    }

    fn rollback(&mut self) -> Result<(), Self::Error> {
        self.rolled_back = true;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Self::Error> {
        self.commit_error.take().map_or(Ok(()), Err)
    }
}

struct MockNewStorage {
    starts: u64,
    commit_error: Option<MockError>,
}

impl NewTxnStorage for MockNewStorage {
    type Transaction = MockNewTransaction;
    type Error = MockError;

    fn begin(&mut self) -> Result<Self::Transaction, Self::Error> {
        self.starts += 1;
        Ok(MockNewTransaction {
            start_ts: self.starts,
            commit_error: self.commit_error.clone(),
            ..MockNewTransaction::default()
        })
    }
}

#[test]
fn TestBackOff() {
    assert_eq!(retry_backoff_upper_bound_ms(1), 2);
    assert_eq!(retry_backoff_upper_bound_ms(2), 4);
    assert_eq!(retry_backoff_upper_bound_ms(3), 8);
    assert_eq!(retry_backoff_upper_bound_ms(100_000), 100);
    for attempt in [1, 2, 3, 100_000] {
        assert!(retry_backoff_delay(attempt) < Duration::from_millis(
            retry_backoff_upper_bound_ms(attempt)
        ));
    }
}

#[test]
fn set_txn_resource_group_uses_the_package_option() {
    let mut transaction = MockNewTransaction::default();
    set_txn_resource_group(&mut transaction, "analytics");
    assert_eq!(
        transaction.options,
        vec![(
            OptionKey::ResourceGroupName,
            TxnOptionValue::String("analytics".to_owned())
        )]
    );
}

#[test]
fn TestRetryExceedCountError() {
    let context = RunInNewTxnContext {
        request_source: Some(RequestSource {
            internal: true,
            source_type: INTERNAL_TXN_OTHERS.to_owned(),
            explicit_source_type: String::new(),
        }),
    };
    let registry = InnerTxnStartTsBox::new();

    let mut storage = MockNewStorage {
        starts: 0,
        commit_error: Some(MockError::Retryable),
    };
    let result = run_in_new_txn_with(
        &context,
        &mut storage,
        true,
        5,
        &registry,
        |_| Ok(()),
        |_| {},
    );
    assert_eq!(result, Err(MockError::Retryable));
    assert_eq!(storage.starts, 5);
    assert!(!registry.contains(1));

    let mut storage = MockNewStorage {
        starts: 0,
        commit_error: None,
    };
    let result = run_in_new_txn_with(
        &context,
        &mut storage,
        true,
        5,
        &registry,
        |_| Err(MockError::Retryable),
        |_| {},
    );
    assert_eq!(result, Err(MockError::Retryable));
    assert_eq!(storage.starts, 5);

    let mut storage = MockNewStorage {
        starts: 0,
        commit_error: None,
    };
    let result = run_in_new_txn_with(
        &context,
        &mut storage,
        true,
        5,
        &registry,
        |_| Err(MockError::Ordinary),
        |_| {},
    );
    assert_eq!(result, Err(MockError::Ordinary));
    assert_eq!(storage.starts, 1);

    let mut storage = MockNewStorage {
        starts: 0,
        commit_error: Some(MockError::Ordinary),
    };
    let result = run_in_new_txn_with(
        &context,
        &mut storage,
        true,
        5,
        &registry,
        |_| Ok(()),
        |_| {},
    );
    assert_eq!(result, Err(MockError::Ordinary));
    assert_eq!(storage.starts, 1);
}

fn tso_at_millis(milliseconds: u64) -> u64 {
    milliseconds << 18
}

#[test]
fn TestInnerTxnStartTsBox() {
    let registry = InnerTxnStartTsBox::new();
    registry.store_inner_txn_ts(5);
    assert!(registry.contains(5));
    registry.delete_inner_txn_ts(5);
    assert!(!registry.contains(5));

    let day_ms = 24 * 60 * 60 * 1_000;
    let now_ms = 1_646_931_300_000_u64;
    let ts0 = tso_at_millis(now_ms - 2 * day_ms);
    let ts1 = tso_at_millis(now_ms - day_ms + 1);
    let ts2 = tso_at_millis(now_ms - 60_000);
    let ts3 = tso_at_millis(now_ms - 55_000);
    for timestamp in [ts0, ts1, ts2, ts3] {
        registry.store_inner_txn_ts(timestamp);
    }
    assert_eq!(
        get_min_inner_txn_start_ts(
            &registry,
            UNIX_EPOCH + Duration::from_millis(now_ms),
            tso_at_millis(now_ms - day_ms),
            tso_at_millis(now_ms)
        ),
        ts1
    );
    for timestamp in [ts0, ts1, ts2, ts3] {
        registry.delete_inner_txn_ts(timestamp);
        assert!(!registry.contains(timestamp));
    }
}

#[derive(Debug, Clone, Copy)]
struct MapError;

impl fmt::Display for MapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("map error")
    }
}

impl Error for MapError {}

#[derive(Default)]
struct CounterMap(BTreeMap<Key, Vec<u8>>);

impl CounterStorage for CounterMap {
    type Error = MapError;

    fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.0.get(key).cloned())
    }

    fn set(&mut self, key: &Key, value: &[u8]) -> Result<(), Self::Error> {
        self.0.insert(key.clone(), value.to_vec());
        Ok(())
    }
}

#[test]
fn TestIncInt64() {
    let mut map = CounterMap::default();
    let key = Key::from_bytes(b"key".as_slice());
    assert_eq!(inc_int64(&mut map, &key, 1).expect("increment"), 1);
    assert_eq!(inc_int64(&mut map, &key, 10).expect("increment"), 11);
    map.set(&key, b"not int").expect("store");
    assert!(inc_int64(&mut map, &key, 1).is_err());
    map.set(&key, u32::MAX.to_string().as_bytes()).expect("store");
    assert_eq!(
        inc_int64(&mut map, &key, 1).expect("increment"),
        i64::from(u32::MAX) + 1
    );
}

#[test]
fn TestGetInt64() {
    let mut map = CounterMap::default();
    let key = Key::from_bytes(b"key".as_slice());
    assert_eq!(get_int64(&map, &key).expect("missing"), 0);
    inc_int64(&mut map, &key, 15).expect("increment");
    assert_eq!(get_int64(&map, &key).expect("present"), 15);
}

#[test]
fn TestIsUserKS() {
    assert!(!is_user_keyspace(KernelType::Classic, ""));
    assert!(is_user_keyspace(KernelType::NextGen, "user"));
    assert!(!is_user_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
}

#[test]
fn TestIsSystemKS() {
    assert!(!is_system_keyspace(KernelType::Classic, ""));
    assert!(!is_system_keyspace(KernelType::NextGen, "user"));
    assert!(is_system_keyspace(KernelType::NextGen, SYSTEM_KEYSPACE));
}

#[derive(Default)]
struct FaultRead {
    values: HashMap<Key, ValueEntry>,
    commit_error: Option<MockError>,
}

impl Getter for FaultRead {
    type Error = MockError;

    fn get(&mut self, key: &Key, _: GetOptions) -> Result<ValueEntry, Self::Error> {
        self.values.get(key).cloned().ok_or(MockError::Ordinary)
    }
}

impl BatchGetter for FaultRead {
    type Error = MockError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        _: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        Ok(keys
            .iter()
            .filter_map(|key| self.values.get(key).cloned().map(|value| (key.clone(), value)))
            .collect())
    }
}

impl KvTransaction for FaultRead {
    fn commit(&mut self) -> Result<(), <Self as Getter>::Error> {
        self.commit_error.take().map_or(Ok(()), Err)
    }
}

impl KvSnapshot for FaultRead {}

#[derive(Default)]
struct FaultStore;

impl KvStorage for FaultStore {
    type Error = MockError;
    type Transaction = FaultRead;
    type Snapshot = FaultRead;

    fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        Ok(FaultRead {
            commit_error: Some(MockError::Retryable),
            ..FaultRead::default()
        })
    }

    fn get_snapshot(&self, _: Version) -> Self::Snapshot {
        FaultRead::default()
    }
}

#[test]
fn TestFaultInjectionBasic() {
    let config = InjectionConfig::new();
    config.set_get_error(Some(MockError::Ordinary));
    config.set_commit_error(Some(MockError::Ordinary));
    let store = new_injected_store(FaultStore, &config);
    let mut transaction = store.begin().expect("begin");
    let mut snapshot = store.get_snapshot(Version::new(1));
    let key = Key::from_bytes(b"a".as_slice());
    assert_eq!(
        transaction.get(&key, GetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(
        snapshot.get(&key, GetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(
        snapshot.batch_get(&[], BatchGetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(
        transaction.batch_get(&[], BatchGetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(transaction.commit(), Err(MockError::Ordinary));

    config.set_get_error(None);
    config.set_commit_error(None);
    let store = new_injected_store(FaultStore, &config);
    let mut transaction = store.begin().expect("begin");
    let mut snapshot = store.get_snapshot(Version::new(1));
    assert_eq!(
        transaction.get(&key, GetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(
        transaction.batch_get(&[], BatchGetOptions::default()),
        Ok(HashMap::new())
    );
    assert_eq!(
        snapshot.get(&key, GetOptions::default()),
        Err(MockError::Ordinary)
    );
    assert_eq!(
        snapshot.batch_get(std::slice::from_ref(&key), BatchGetOptions::default()),
        Ok(HashMap::new())
    );
    assert_eq!(transaction.commit(), Err(MockError::Retryable));
}

#[test]
fn TestSetCDCWriteSource() {
    for (value, expected) in [(1, Some(1)), (0, Some(0)), (16, None)] {
        let mut source = 0;
        let result = set_cdc_write_source(&mut source, value);
        match expected {
            Some(expected) => {
                result.expect("valid source");
                assert_eq!(is_cdc_write_source_set(source), expected != 0);
                assert_eq!(get_cdc_write_source(source), expected);
            }
            None => assert!(
                result
                    .expect_err("invalid source")
                    .to_string()
                    .contains("out of TiCDC write source range")
            ),
        }
    }
}

#[test]
fn TestSetLossyDDLReorgSource() {
    for (current, value, expected) in [
        (0, 1, Some(1)),
        (12, 1, Some(1)),
        (12, 0, Some(0)),
        (12, 256, None),
    ] {
        let mut source = current;
        let result = set_lossy_ddl_reorg_source(&mut source, value);
        match expected {
            Some(expected) => {
                result.expect("valid source");
                assert_eq!(is_lossy_ddl_reorg_source_set(source), expected != 0);
                assert_eq!(get_lossy_ddl_reorg_source(source), expected);
            }
            None => assert!(
                result
                    .expect_err("invalid source")
                    .to_string()
                    .contains("out of lossy DDL reorg source range")
            ),
        }
    }
}

#[test]
fn TestError() {
    for error in [
        &ERR_NOT_EXIST,
        &ERR_TXN_RETRYABLE,
        &ERR_CANNOT_SET_NIL_VALUE,
        &ERR_INVALID_TXN,
        &ERR_TXN_TOO_LARGE,
        &ERR_ENTRY_TOO_LARGE,
        &ERR_NOT_IMPLEMENTED,
        &ERR_WRITE_CONFLICT,
        &ERR_WRITE_CONFLICT_IN_TIDB,
    ] {
        assert_ne!(error.mysql_code().as_u16(), 1105);
        assert_eq!(
            error.mysql_code().as_u16().to_string(),
            error.rfc_code().split(':').nth(1).expect("code")
        );
    }
}

#[test]
fn TestIsRequestTypeSupported() {
    let checker = RequestTypeSupportedChecker;
    assert!(checker.is_request_type_supported(REQ_TYPE_SELECT, REQ_SUB_TYPE_GROUP_BY));
    assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_SIGNATURE));
    assert!(checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_DESC));
    assert!(checker.is_request_type_supported(REQ_TYPE_SELECT, 3001));
    assert!(!checker.is_request_type_supported(REQ_TYPE_DAG, REQ_SUB_TYPE_ANALYZE_IDX));
    assert!(checker.is_request_type_supported(REQ_TYPE_ANALYZE, 0));
    assert!(!checker.is_request_type_supported(REQ_TYPE_CHECKSUM, 0));
}

#[test]
fn cache_db_preserves_empty_values_and_table_deletion() {
    struct Snapshot {
        reads: usize,
    }

    impl Getter for Snapshot {
        type Error = MockError;

        fn get(&mut self, _: &Key, _: GetOptions) -> Result<ValueEntry, Self::Error> {
            self.reads += 1;
            Ok(ValueEntry::new(Vec::new(), 0))
        }
    }

    let cache = CacheDb::new();
    let key = Key::from_bytes(b"k".as_slice());
    let mut snapshot = Snapshot { reads: 0 };
    assert_eq!(
        cache.union_get(1, &mut snapshot, &key).expect("first"),
        Vec::<u8>::new()
    );
    assert_eq!(
        cache.union_get(1, &mut snapshot, &key).expect("cached"),
        Vec::<u8>::new()
    );
    assert_eq!(snapshot.reads, 1);
    cache.delete(1);
    cache.union_get(1, &mut snapshot, &key).expect("after delete");
    assert_eq!(snapshot.reads, 2);
}

#[test]
fn variables_and_unistore_preserve_source_defaults() {
    let killed = Arc::new(std::sync::atomic::AtomicU32::new(7));
    let variables = KvVariables::new(killed);
    assert_eq!(variables.backoff_lock_fast, 10);
    assert_eq!(variables.backoff_weight, 2);
    assert_eq!(variables.kill_reason(), 7);

    set_standalone_tidb(false);
    assert!(!standalone_tidb());
    set_standalone_tidb(true);
    assert!(standalone_tidb());
    set_standalone_tidb(false);
}

#[test]
fn long_running_inner_transaction_uses_tso_physical_time() {
    let now = UNIX_EPOCH + Duration::from_secs(1_000);
    let start = tso_at_millis(600_000);
    assert_eq!(
        long_running_inner_txn(now, start, true)
            .expect("older than five minutes")
            .start_ts,
        start
    );
    assert_eq!(long_running_inner_txn(SystemTime::now(), 0, true), None);
}
