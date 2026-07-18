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

//! Decimal `int64` counters translated from `pkg/kv/utils.go`.
//!
//! The Go implementation is deliberately small, but it sits at a wide storage
//! interface (`RetrieverMutator`).  The Rust rewrite keeps the same behavior at
//! a narrow leaf: a counter store only has to get an optional byte value and
//! set a byte value.  `None` is the source `ErrNotExist` case.  This module does
//! not pretend to provide a transaction client, MVCC, iteration, or a
//! mem-buffer abstraction.

use std::error::Error;
use std::fmt;

use crate::Key;

/// The storage operations needed by [`inc_int64`] and [`get_int64`].
///
/// `None` from [`CounterStorage::get`] is the source `kv.ErrNotExist` result.
/// Implementations own their storage and error type; the counter layer does
/// not invent a client or require any transaction protocol.
pub trait CounterStorage {
    /// The implementation's storage failure type.
    type Error: Error + Send + Sync + 'static;

    /// Gets the raw value for a key, or `None` when the key is absent.
    fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Stores the raw value for a key.
    fn set(&mut self, key: &Key, value: &[u8]) -> Result<(), Self::Error>;
}

/// Failures returned by the source counter operations.
#[derive(Debug)]
pub enum CounterError<E> {
    /// The backing store failed a get or set operation.
    Storage(E),
    /// A present value was not a base-ten signed 64-bit integer.
    ///
    /// The original bytes are retained for diagnostics and to make the
    /// no-mutation-on-parse-failure contract observable in tests.
    InvalidInteger {
        /// The exact bytes that failed decimal parsing.
        value: Vec<u8>,
    },
}

impl<E: fmt::Display> fmt::Display for CounterError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "counter storage error: {error}"),
            Self::InvalidInteger { value } => {
                write!(formatter, "invalid int64 counter value {:?}", value)
            }
        }
    }
}

impl<E: Error + 'static> Error for CounterError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::InvalidInteger { .. } => None,
        }
    }
}

fn parse_int64<E>(value: Vec<u8>) -> Result<i64, CounterError<E>> {
    let text = std::str::from_utf8(&value).map_err(|_| CounterError::InvalidInteger {
        value: value.clone(),
    })?;
    text.parse::<i64>()
        .map_err(|_| CounterError::InvalidInteger { value })
}

/// Increments the decimal `int64` value stored at `key` by `step`.
///
/// Missing keys are initialized to `step`.  Signed overflow wraps exactly as
/// Go's non-constant `int64` addition does, and the value is only written after
/// the existing bytes have parsed successfully.
pub fn inc_int64<S: CounterStorage>(
    storage: &mut S,
    key: &Key,
    step: i64,
) -> Result<i64, CounterError<S::Error>> {
    let current = match storage.get(key).map_err(CounterError::Storage)? {
        None => step,
        Some(value) => parse_int64(value)?.wrapping_add(step),
    };
    storage
        .set(key, current.to_string().as_bytes())
        .map_err(CounterError::Storage)?;
    Ok(current)
}

/// Reads a decimal `int64` value created by [`inc_int64`].
///
/// Missing keys return zero without mutating the store, matching Go's
/// `GetInt64` contract.
pub fn get_int64<S: CounterStorage>(storage: &S, key: &Key) -> Result<i64, CounterError<S::Error>> {
    match storage.get(key).map_err(CounterError::Storage)? {
        None => Ok(0),
        Some(value) => parse_int64(value),
    }
}

#[cfg(test)]
mod tests {
    use super::{get_int64, inc_int64, CounterError, CounterStorage};
    use crate::Key;
    use std::collections::BTreeMap;
    use std::fmt;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestStorageError;

    impl fmt::Display for TestStorageError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("test storage failure")
        }
    }

    impl std::error::Error for TestStorageError {}

    #[derive(Default)]
    struct MockMap {
        values: BTreeMap<Key, Vec<u8>>,
        fail_get: bool,
        fail_set: bool,
    }

    impl CounterStorage for MockMap {
        type Error = TestStorageError;

        fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, Self::Error> {
            if self.fail_get {
                return Err(TestStorageError);
            }
            Ok(self.values.get(key).cloned())
        }

        fn set(&mut self, key: &Key, value: &[u8]) -> Result<(), Self::Error> {
            if self.fail_set {
                return Err(TestStorageError);
            }
            self.values.insert(key.clone(), value.to_vec());
            Ok(())
        }
    }

    #[test]
    fn inc_int64_matches_every_source_assertion() {
        let mut storage = MockMap::default();
        let key = Key::from_bytes(b"key".as_slice());

        let value = inc_int64(&mut storage, &key, 1).expect("missing key initializes");
        assert_eq!(value, 1);
        let value = inc_int64(&mut storage, &key, 10).expect("existing key increments");
        assert_eq!(value, 11);

        storage
            .set(&key, b"not int")
            .expect("test value can be stored");
        let error = inc_int64(&mut storage, &key, 1).expect_err("non-integer must fail");
        assert!(matches!(error, CounterError::InvalidInteger { .. }));
        assert_eq!(
            storage.get(&key).expect("read test value"),
            Some(b"not int".to_vec())
        );

        // Go's test names this maxUint32, then checks the int64 increment.
        let max_uint32 = i64::from(u32::MAX);
        storage
            .set(&key, max_uint32.to_string().as_bytes())
            .expect("test value can be stored");
        let value = inc_int64(&mut storage, &key, 1).expect("maxUint32 increments");
        assert_eq!(value, max_uint32 + 1);
    }

    #[test]
    fn get_int64_matches_missing_and_created_source_cases() {
        let mut storage = MockMap::default();
        let key = Key::from_bytes(b"key".as_slice());

        assert_eq!(get_int64(&storage, &key).expect("missing is zero"), 0);
        inc_int64(&mut storage, &key, 15).expect("counter initializes");
        assert_eq!(get_int64(&storage, &key).expect("created value reads"), 15);
    }

    #[test]
    fn invalid_and_storage_errors_are_typed_and_do_not_mutate() {
        let mut storage = MockMap::default();
        let key = Key::from_bytes(b"key".as_slice());
        storage
            .values
            .insert(key.clone(), b"9223372036854775808".to_vec());

        let error = get_int64(&storage, &key).expect_err("overflow is invalid");
        assert!(matches!(error, CounterError::InvalidInteger { .. }));
        let error = inc_int64(&mut storage, &key, 1).expect_err("overflow is invalid");
        assert!(matches!(error, CounterError::InvalidInteger { .. }));
        assert_eq!(
            storage.get(&key).expect("read unchanged value"),
            Some(b"9223372036854775808".to_vec())
        );

        storage.fail_get = true;
        let error = get_int64(&storage, &key).expect_err("get failure propagates");
        assert!(matches!(error, CounterError::Storage(TestStorageError)));
        storage.fail_get = false;
        storage
            .set(&key, b"1")
            .expect("valid value enables set failure test");
        storage.fail_set = true;
        let error = inc_int64(&mut storage, &key, 1).expect_err("set failure propagates");
        assert!(matches!(error, CounterError::Storage(TestStorageError)));
    }

    #[test]
    fn signed_overflow_wraps_like_go() {
        let mut storage = MockMap::default();
        let key = Key::from_bytes(b"key".as_slice());
        storage
            .set(&key, i64::MAX.to_string().as_bytes())
            .expect("test value can be stored");

        let value = inc_int64(&mut storage, &key, 1).expect("addition wraps");
        assert_eq!(value, i64::MIN);
        assert_eq!(
            storage.get(&key).expect("read wrapped value"),
            Some(i64::MIN.to_string().into_bytes())
        );
    }
}
