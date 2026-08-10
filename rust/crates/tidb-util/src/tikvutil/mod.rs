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

//! Native implementation of `pkg/util/tikvutil/tikvutil.go`.
//!
//! The Go package exports a `go.uber.org/atomic.Int32`, so that wrapper's
//! reachable `Load`, arithmetic, compare-and-swap, `Store`, `Swap`, JSON, and
//! string contracts are included here. Rust exposes a non-null,
//! non-rebindable static instead of Go's rebindable pointer variable.

use std::fmt;
use std::sync::atomic::{AtomicI32, Ordering};

const ATOMIC_ORDERING: Ordering = Ordering::SeqCst;

/// Sequentially consistent equivalent of `go.uber.org/atomic.Int32`.
#[derive(Debug, Default)]
pub struct AtomicInt32 {
    value: AtomicI32,
}

impl AtomicInt32 {
    /// Creates an atomic with the supplied initial value.
    #[must_use]
    pub const fn new(value: i32) -> Self {
        Self {
            value: AtomicI32::new(value),
        }
    }

    /// Atomically loads the wrapped value.
    pub fn load(&self) -> i32 {
        self.value.load(ATOMIC_ORDERING)
    }

    /// Atomically adds `delta` and returns the new wrapped value.
    pub fn add(&self, delta: i32) -> i32 {
        self.value
            .fetch_add(delta, ATOMIC_ORDERING)
            .wrapping_add(delta)
    }

    /// Atomically subtracts `delta` and returns the new wrapped value.
    pub fn sub(&self, delta: i32) -> i32 {
        self.value
            .fetch_sub(delta, ATOMIC_ORDERING)
            .wrapping_sub(delta)
    }

    /// Atomically increments and returns the new value.
    pub fn inc(&self) -> i32 {
        self.add(1)
    }

    /// Atomically decrements and returns the new value.
    pub fn dec(&self) -> i32 {
        self.sub(1)
    }

    /// Deprecated source spelling for [`Self::compare_and_swap`].
    #[deprecated(note = "use compare_and_swap")]
    pub fn cas(&self, old: i32, new: i32) -> bool {
        self.compare_and_swap(old, new)
    }

    /// Atomically replaces `old` with `new` if the current value is `old`.
    pub fn compare_and_swap(&self, old: i32, new: i32) -> bool {
        self.value
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING)
            .is_ok()
    }

    /// Atomically stores a value.
    pub fn store(&self, value: i32) {
        self.value.store(value, ATOMIC_ORDERING);
    }

    /// Atomically replaces the value and returns the previous value.
    pub fn swap(&self, value: i32) -> i32 {
        self.value.swap(value, ATOMIC_ORDERING)
    }

    /// Serializes the current value as its JSON number.
    pub fn marshal_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.load())
    }

    /// Parses an `i32` JSON number and stores it only after successful parsing.
    pub fn unmarshal_json(&self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        let value = serde_json::from_slice(bytes)?;
        self.store(value);
        Ok(())
    }

    /// Encodes the wrapped value as a decimal string.
    pub fn string(&self) -> String {
        self.load().to_string()
    }
}

impl fmt::Display for AtomicInt32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.load().fmt(formatter)
    }
}

/// Current value of the `tidb_committer_concurrency` system variable.
pub static COMMITTER_CONCURRENCY: AtomicInt32 = AtomicInt32::new(128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_atomic_int32_contract_is_complete() {
        assert_eq!(ATOMIC_ORDERING, Ordering::SeqCst);
        assert_eq!(AtomicInt32::default().load(), 0);
        let value = AtomicInt32::new(128);
        assert_eq!(value.load(), 128);
        assert_eq!(value.add(2), 130);
        assert_eq!(value.sub(3), 127);
        assert_eq!(value.inc(), 128);
        assert_eq!(value.dec(), 127);
        assert!(!value.compare_and_swap(128, 1));
        assert!(value.compare_and_swap(127, 1));
        #[allow(deprecated)]
        {
            assert!(value.cas(1, 2));
        }
        assert_eq!(value.swap(-5), 2);
        assert_eq!(value.load(), -5);
        value.store(i32::MAX);
        assert_eq!(value.inc(), i32::MIN);
        assert_eq!(value.string(), i32::MIN.to_string());
        assert_eq!(value.to_string(), i32::MIN.to_string());
        assert_eq!(
            value.marshal_json().expect("serialize an i32"),
            i32::MIN.to_string().as_bytes()
        );
        value.store(i32::MIN);
        assert_eq!(value.dec(), i32::MAX);

        value.unmarshal_json(b"321").expect("parse an i32");
        assert_eq!(value.load(), 321);
        assert!(value.unmarshal_json(b"2147483648").is_err());
        assert_eq!(value.load(), 321, "failed JSON must not mutate the atomic");
    }

    #[test]
    fn global_default_matches_the_source_sysvar_default() {
        assert_eq!(COMMITTER_CONCURRENCY.load(), 128);
    }
}
