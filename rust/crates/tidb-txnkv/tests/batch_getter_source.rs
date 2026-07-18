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

//! Direct transaction buffer batch-get obligations from TiDB's Go tests.

use std::collections::HashMap;

use tidb_txnkv::{
    BatchBufferGetter, BatchGetError, BatchGetOptions, BatchGetter, BufferBatchGetter, Getter, Key,
    ValueEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestError {
    NotFound,
}

impl BatchGetError for TestError {
    fn is_not_found(&self) -> bool {
        *self == Self::NotFound
    }
}

struct MockStore {
    values: HashMap<Key, Vec<u8>>,
    commit_ts_base: u64,
}

impl MockStore {
    fn new(commit_ts_base: u64) -> Self {
        Self {
            values: HashMap::new(),
            commit_ts_base,
        }
    }

    fn set(&mut self, key: &str, value: &str) {
        self.values
            .insert(Key::from_bytes(key.as_bytes()), value.as_bytes().to_vec());
    }

    fn delete(&mut self, key: &str) {
        self.values
            .insert(Key::from_bytes(key.as_bytes()), Vec::new());
    }
}

impl Getter for MockStore {
    type Error = TestError;

    fn get(&mut self, key: &Key, options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        let value = self.values.get(key).ok_or(TestError::NotFound)?;
        let commit_ts = if options.return_commit_ts {
            self.commit_ts_base + u64::from(key.as_bytes()[0])
        } else {
            0
        };
        Ok(ValueEntry::new(value.clone(), commit_ts))
    }
}

impl BatchGetter for MockStore {
    type Error = TestError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        let mut values = HashMap::new();
        for key in keys {
            match Getter::get(self, key, options) {
                Ok(value) => {
                    values.insert(key.clone(), value);
                }
                Err(TestError::NotFound) => {}
            }
        }
        Ok(values)
    }
}

impl BatchBufferGetter for MockStore {
    type Error = TestError;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        BatchGetter::batch_get(self, keys, options)
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

fn key(value: &str) -> Key {
    Key::from_bytes(value.as_bytes())
}

#[test]
fn test_buffer_batch_getter_source_table_and_commit_ts() {
    let mut snapshot = MockStore::new(1000);
    snapshot.set("a", "a");
    snapshot.set("b", "b");
    snapshot.set("c", "c");
    snapshot.set("d", "d");

    let mut middle = MockStore::new(2000);
    middle.set("a", "a1");
    middle.set("c", "c1");

    let mut buffer = MockStore::new(3000);
    buffer.set("a", "a2");
    buffer.delete("b");

    let keys = [key("a"), key("b"), key("c"), key("d")];
    let mut getter = BufferBatchGetter::new(buffer, Some(middle), snapshot);
    assert_eq!(getter.len(), 2);
    let result = getter.batch_get(&keys, BatchGetOptions::default()).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[&key("a")], ValueEntry::new(b"a2".as_slice(), 0));
    assert_eq!(result[&key("c")], ValueEntry::new(b"c1".as_slice(), 0));
    assert_eq!(result[&key("d")], ValueEntry::new(b"d".as_slice(), 0));
    assert!(!result.contains_key(&key("b")));

    let keys = [key("a"), key("b"), key("c"), key("d"), key("xx")];
    let result = getter
        .batch_get(&keys, BatchGetOptions::with_return_commit_ts())
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(
        result[&key("a")],
        ValueEntry::new(b"a2".as_slice(), 3000 + u64::from(b'a'))
    );
    assert_eq!(
        result[&key("c")],
        ValueEntry::new(b"c1".as_slice(), 2000 + u64::from(b'c'))
    );
    assert_eq!(
        result[&key("d")],
        ValueEntry::new(b"d".as_slice(), 1000 + u64::from(b'd'))
    );
}

#[test]
fn middle_cache_tombstone_suppresses_snapshot() {
    let mut snapshot = MockStore::new(1000);
    snapshot.set("c", "snapshot");
    let mut middle = MockStore::new(2000);
    middle.delete("c");
    let buffer = MockStore::new(3000);

    let mut getter = BufferBatchGetter::new(buffer, Some(middle), snapshot);
    let result = getter
        .batch_get(&[key("c")], BatchGetOptions::default())
        .unwrap();
    assert!(result.is_empty());
}
