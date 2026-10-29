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

//! Go `pkg/kv`'s read surface (`kv.go`): `Getter`, `BatchGetter`, the
//! get options and `ValueEntry`. The buffer-first batch getter over them is
//! the transaction driver's (`tidb_store_driver_txn::batch_getter`, Go
//! `pkg/store/driver/txn/batch_getter.go`).

use std::collections::HashMap;

use crate::{is_err_not_found, Key, KvError};

/// The value and optional commit timestamp returned by a KV read.
///
/// This is the bounded `kv.ValueEntry` surface consumed by the source batch
/// getter. A later complete `pkg/kv/kv.go` owner can move this shared authority
/// without changing the merge contract.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValueEntry {
    /// Raw stored value. An empty buffer value is a transaction tombstone.
    pub value: Vec<u8>,
    /// Commit timestamp requested by `WithReturnCommitTS`, or zero otherwise.
    pub commit_ts: u64,
}

impl ValueEntry {
    /// Creates a value entry.
    pub fn new(value: impl Into<Vec<u8>>, commit_ts: u64) -> Self {
        Self {
            value: value.into(),
            commit_ts,
        }
    }

    /// Returns whether the stored value is empty.
    #[must_use]
    pub fn is_value_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Returns the Go source memory accounting for this entry.
    #[must_use]
    pub fn size(&self) -> usize {
        // A Go `ValueEntry` is one 24-byte slice header plus one `uint64`.
        32_usize.saturating_add(self.value.len())
    }
}

/// One option accepted by both point and batch reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetOrBatchGetOption {
    /// Return the commit timestamp with the value.
    ReturnCommitTs,
}

/// Option accepted by a point read.
pub type GetOption = GetOrBatchGetOption;

/// Option accepted by a batch read.
pub type BatchGetOption = GetOrBatchGetOption;

/// Returns the source option that requests commit timestamps.
#[must_use]
pub const fn with_return_commit_ts() -> GetOrBatchGetOption {
    GetOrBatchGetOption::ReturnCommitTs
}

/// Converts batch-read options that are also valid for point reads.
///
/// The current client-go source has one shared option. Keeping this explicit
/// conversion preserves the package boundary when either option domain grows.
#[must_use]
pub fn batch_get_to_get_options(options: &[BatchGetOption]) -> Vec<GetOption> {
    options.to_vec()
}

/// Applied options for one point read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GetOptions {
    /// Requests the commit timestamp from the input layer.
    pub return_commit_ts: bool,
}

impl GetOptions {
    /// Applies point-read options in source order.
    pub fn apply(&mut self, options: &[GetOption]) {
        self.return_commit_ts |= options
            .iter()
            .any(|option| matches!(option, GetOrBatchGetOption::ReturnCommitTs));
    }

    /// Constructs the commonly used commit-timestamp option set.
    #[must_use]
    pub fn with_return_commit_ts() -> Self {
        let mut options = Self::default();
        options.apply(&[with_return_commit_ts()]);
        options
    }
}

impl From<BatchGetOptions> for GetOptions {
    fn from(options: BatchGetOptions) -> Self {
        Self {
            return_commit_ts: options.return_commit_ts,
        }
    }
}

/// Applied options for one batch read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchGetOptions {
    /// Requests commit timestamps from every input layer.
    pub return_commit_ts: bool,
}

impl BatchGetOptions {
    /// Applies batch-read options in source order.
    pub fn apply(&mut self, options: &[BatchGetOption]) {
        self.return_commit_ts |= options
            .iter()
            .any(|option| matches!(option, GetOrBatchGetOption::ReturnCommitTs));
    }

    /// Constructs the commonly used commit-timestamp option set.
    #[must_use]
    pub fn with_return_commit_ts() -> Self {
        let mut options = Self::default();
        options.apply(&[with_return_commit_ts()]);
        options
    }
}

/// Error operations needed by the client-go batch-buffer adapter.
pub trait BatchGetError: Clone {
    /// Returns whether this error has the KV not-found identity.
    fn is_not_found(&self) -> bool;
}

impl BatchGetError for KvError {
    fn is_not_found(&self) -> bool {
        is_err_not_found(Some(self))
    }
}

/// Single-key read surface used by the transaction buffer and middle cache.
pub trait Getter {
    /// Read error identity.
    type Error: BatchGetError;

    /// Gets one key with the source option set.
    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error>;
}

/// Batch read surface used by the snapshot.
pub trait BatchGetter {
    /// Read error identity.
    type Error: BatchGetError;

    /// Gets all present values, keyed by raw KV key identity.
    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error>;
}
