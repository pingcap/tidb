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

//! Direct source tests for `pkg/util/prefix_helper.go::RowKeyPrefixFilter`.

use tidb_txnkv::{next_until, row_key_prefix_filter, Key, KvIterator};

#[test]
fn test_prefix_filter() {
    let mut row_key = b"test@#$%l(le[0]..prefix) 2uio".to_vec();
    row_key[8] = 0;
    row_key[9] = 0;

    let filter = row_key_prefix_filter(Key::from_bytes(row_key.clone()));
    let mut child = row_key;
    child.extend_from_slice(b"akjdf3*(34");
    assert!(!filter(&Key::from_bytes(child)));
    assert!(filter(&Key::from_bytes(b"sjfkdlsaf")));
}

#[test]
fn prefix_filter_composes_with_next_until() {
    let prefix = Key::from_bytes(b"row:");
    let mut iterator = KeyIterator::new([b"row:1".as_slice(), b"row:2", b"tail"]);

    next_until(&mut iterator, row_key_prefix_filter(prefix)).expect("iterator advances");

    assert!(iterator.valid());
    assert_eq!(iterator.key().as_bytes(), b"tail");
    assert_eq!(iterator.next_calls, 2);
}

struct KeyIterator {
    keys: Vec<Key>,
    index: usize,
    next_calls: usize,
}

impl KeyIterator {
    fn new<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Self {
        Self {
            keys: keys.into_iter().map(Key::from_bytes).collect(),
            index: 0,
            next_calls: 0,
        }
    }
}

impl KvIterator for KeyIterator {
    type Error = std::convert::Infallible;

    fn valid(&self) -> bool {
        self.index < self.keys.len()
    }

    fn key(&self) -> &Key {
        &self.keys[self.index]
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.index += 1;
        self.next_calls += 1;
        Ok(())
    }

    fn close(&mut self) {}
}
