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

//! Direct translations of `pkg/kv/utils_test.go` counter assertions.

use std::collections::BTreeMap;
use std::fmt;

use tidb_txnkv::{get_int64, inc_int64, CounterError, CounterStorage, Key};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct StorageError;

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("storage failure")
    }
}

impl std::error::Error for StorageError {}

#[derive(Default)]
struct MockMap {
    values: BTreeMap<Key, Vec<u8>>,
}

impl CounterStorage for MockMap {
    type Error = StorageError;

    fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.values.get(key).cloned())
    }

    fn set(&mut self, key: &Key, value: &[u8]) -> Result<(), Self::Error> {
        self.values.insert(key.clone(), value.to_vec());
        Ok(())
    }
}

/// Complete translation of `pkg/kv/utils_test.go:25-50 TestIncInt64`.
#[test]
fn test_inc_int64() {
    let mut storage = MockMap::default();
    let key = Key::from_bytes(b"key".as_slice());

    let value = inc_int64(&mut storage, &key, 1).expect("missing key initializes");
    assert_eq!(value, 1);
    let value = inc_int64(&mut storage, &key, 10).expect("existing key increments");
    assert_eq!(value, 11);

    storage.set(&key, b"not int").expect("store invalid value");
    let error = inc_int64(&mut storage, &key, 1).expect_err("invalid value fails");
    assert!(matches!(error, CounterError::InvalidInteger { .. }));

    let max_uint32 = i64::from(u32::MAX);
    storage
        .set(&key, max_uint32.to_string().as_bytes())
        .expect("store maxUint32");
    let value = inc_int64(&mut storage, &key, 1).expect("maxUint32 increments");
    assert_eq!(value, max_uint32 + 1);
}

/// Complete translation of `pkg/kv/utils_test.go:52-65 TestGetInt64`.
#[test]
fn test_get_int64() {
    let mut storage = MockMap::default();
    let key = Key::from_bytes(b"key".as_slice());

    assert_eq!(get_int64(&storage, &key).expect("missing key is zero"), 0);
    inc_int64(&mut storage, &key, 15).expect("counter initializes");
    assert_eq!(get_int64(&storage, &key).expect("created value reads"), 15);
}
