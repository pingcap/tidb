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

//! Transaction-buffer batch reads translated from
//! `pkg/store/driver/txn/batch_getter.go`.

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
}

/// The only get option observed by `batch_getter.go` and its source test.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchGetOptions {
    /// Requests commit timestamps from every input layer.
    pub return_commit_ts: bool,
}

impl BatchGetOptions {
    /// Equivalent to Go `kv.WithReturnCommitTS()` for this bounded consumer.
    pub const fn with_return_commit_ts() -> Self {
        Self {
            return_commit_ts: true,
        }
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
    fn get(&mut self, key: &Key, options: BatchGetOptions) -> Result<ValueEntry, Self::Error>;
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

/// Batch-capable transaction buffer surface.
///
/// Go embeds `Getter` into `BatchBufferGetter`; spelling the three operations
/// together here keeps one unambiguous error type while leaving snapshot-only
/// [`BatchGetter`] implementations free of an invented single-key method.
pub trait BatchBufferGetter {
    /// Read error identity.
    type Error: BatchGetError;

    /// Gets all present buffered values.
    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error>;

    /// Returns the current buffer length.
    fn len(&self) -> usize;

    /// Returns whether the transaction buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Merges reads from a transaction buffer, optional middle cache, and snapshot.
pub struct BufferBatchGetter<B, M, S> {
    buffer: B,
    middle_cache: Option<M>,
    snapshot: S,
}

#[allow(clippy::len_without_is_empty)]
impl<B, M, S> BufferBatchGetter<B, M, S>
where
    B: BatchBufferGetter,
    M: Getter<Error = B::Error>,
    S: BatchGetter<Error = B::Error>,
{
    /// Creates the source three-layer batch getter.
    pub fn new(buffer: B, middle_cache: Option<M>, snapshot: S) -> Self {
        Self {
            buffer,
            middle_cache,
            snapshot,
        }
    }

    /// Returns the transaction buffer length through the source adapter.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Merges a batch with buffer > middle-cache > snapshot precedence.
    ///
    /// Empty values found in the transaction buffer are deletion tombstones:
    /// they suppress lower layers and are omitted from the returned map.
    pub fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, B::Error> {
        batch_get_from_layers(
            &mut self.buffer,
            self.middle_cache.as_mut(),
            &mut self.snapshot,
            keys,
            options,
        )
    }
}

/// Executes the canonical buffer > middle cache > snapshot merge over borrowed
/// layers.
///
/// [`BufferBatchGetter`] owns its layers for the standalone source contract.
/// The transaction driver already owns those same layers, so this borrowed
/// entry point lets that real consumer execute one implementation instead of
/// copying the merge algorithm or wrapping it in a second authority.
pub(crate) fn batch_get_from_layers<B, M, S>(
    buffer: &mut B,
    mut middle_cache: Option<&mut M>,
    snapshot: &mut S,
    keys: &[Key],
    options: BatchGetOptions,
) -> Result<HashMap<Key, ValueEntry>, B::Error>
where
    B: BatchBufferGetter,
    M: Getter<Error = B::Error>,
    S: BatchGetter<Error = B::Error>,
{
    let mut buffer_values = buffer.batch_get(keys, options)?;
    if let Some(middle_cache) = middle_cache.as_mut() {
        for key in keys {
            if buffer_values.contains_key(key) {
                continue;
            }
            match middle_cache.get(key, options) {
                Ok(value) => {
                    buffer_values.insert(key.clone(), value);
                }
                Err(error) if error.is_not_found() => {}
                Err(error) => return Err(error),
            }
        }
    }

    // This is client-go BufferBatchGetter's second merge stage. Keep it
    // separate from middle-cache population so empty middle-cache values have
    // exactly the same tombstone behavior as empty buffer values.
    if buffer_values.is_empty() {
        return snapshot.batch_get(keys, options);
    }
    let mut snapshot_keys = Vec::with_capacity(keys.len().saturating_sub(buffer_values.len()));
    for key in keys {
        match buffer_values.get(key).map(|value| value.value.is_empty()) {
            None => snapshot_keys.push(key.clone()),
            Some(true) => {
                buffer_values.remove(key);
            }
            Some(false) => {}
        }
    }

    let snapshot_values = snapshot.batch_get(&snapshot_keys, options)?;
    for (key, value) in snapshot_values {
        buffer_values.insert(key, value);
    }
    Ok(buffer_values)
}
