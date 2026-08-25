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

//! [`MemBufferBackend`] over the `tikv-client` transaction memory buffer.
//!
//! This is the concrete TiKV-client backend anticipated by
//! [`super::mem_buffer`]: the one explicit place where TiDB's typed
//! transaction-buffer contract is converted to client-go's bit-packed key
//! flags and `MemDB` surface, exactly as Go's
//! `pkg/store/driver/txn/unionstore_driver.go` wraps
//! `tikv/client-go/v2/internal/unionstore.MemDB`. The wrapped
//! [`MemDb`](tikv_client::transaction::unionstore::MemDb) is client-go's
//! source-default ART buffer, so staging, tombstone, flag, and snapshot
//! semantics come from the transcreated client-go implementation rather than
//! being re-derived here.
//!
//! Flag-bit positions are never hard-coded: the masks that split client-go's
//! combined `HasPresumeKeyNotExists` view back into TiDB's separate
//! presume/previous-presume flags are derived at runtime from
//! `tikv_client::kv::apply_flags_ops` itself, so an upstream re-layout of the
//! bit assignments cannot silently corrupt the conversion.

use std::collections::HashMap;
use std::sync::OnceLock;

use tikv_client::error::StaticError as TikvStaticError;
use tikv_client::kv::{
    apply_flags_ops as tikv_apply_flags_ops, FlagsOp as TikvFlagsOp, KeyFlags as TikvKeyFlags,
};
use tikv_client::transaction::unionstore::{
    KvIterator as TikvKvIterator, MemDb, MemDbSnapshot, SnapshotError,
};

use crate::batch_getter::{BatchGetError, BatchGetOptions, GetOptions, Getter, ValueEntry};
use crate::driver::mem_buffer::{MemBufferBackend, StagingHandle};
use crate::driver::read::TransactionReadError;
use crate::iteration::KvIterator;
use crate::key_flags::AssertionState;
use crate::{AssertionOp, FlagsOp, Key, KeyFlags};

/// Error surface of the TiKV-client memory-buffer backend.
///
/// The buffer itself fails in exactly three ways: a missing key, client-go's
/// nil-value rejection, and buffer-level invalidation (an iterator or snapshot
/// outliving a write, or a configured size limit). The driver contract only
/// distinguishes not-found; the other cases stay observable through their
/// message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TikvMemBufferError {
    /// The canonical TiDB not-found identity.
    NotFound,
    /// Client-go rejects storing an empty value through `Set`.
    CannotSetNilValue,
    /// Any other buffer-level failure, preserved as its source message.
    Backend(String),
}

impl std::fmt::Display for TikvMemBufferError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => formatter.write_str("key not exist"),
            Self::CannotSetNilValue => formatter.write_str("can not set nil value"),
            Self::Backend(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for TikvMemBufferError {}

impl BatchGetError for TikvMemBufferError {
    fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}

impl TransactionReadError for TikvMemBufferError {
    fn not_found() -> Self {
        Self::NotFound
    }
}

fn map_read_error(error: TikvStaticError) -> TikvMemBufferError {
    match error {
        TikvStaticError::NotExist => TikvMemBufferError::NotFound,
        other => TikvMemBufferError::Backend(other.to_string()),
    }
}

fn map_write_error(error: Box<dyn std::error::Error + Send + Sync>) -> TikvMemBufferError {
    match error.downcast_ref::<TikvStaticError>() {
        Some(TikvStaticError::CannotSetNilValue) => TikvMemBufferError::CannotSetNilValue,
        _ => TikvMemBufferError::Backend(error.to_string()),
    }
}

fn map_snapshot_error(error: SnapshotError) -> TikvMemBufferError {
    match error {
        SnapshotError::NotExist => TikvMemBufferError::NotFound,
        SnapshotError::Invalidated => TikvMemBufferError::Backend(
            "the buffer snapshot was invalidated by a later write".to_owned(),
        ),
    }
}

/// Client-go flag bits derived from upstream's own operation application, so a
/// bit-layout change upstream re-derives these instead of corrupting reads.
fn tikv_flag_mask(operation: TikvFlagsOp) -> u16 {
    tikv_apply_flags_ops(TikvKeyFlags::default(), &[operation]).bits()
}

fn previous_presume_mask() -> u16 {
    static MASK: OnceLock<u16> = OnceLock::new();
    *MASK.get_or_init(|| tikv_flag_mask(TikvFlagsOp::SetPreviousPresumeKeyNotExists))
}

fn presume_mask() -> u16 {
    // `SetPresumeKeyNotExists` also sets client-go's internal need-check bit;
    // both are cleared together and unreachable independently through this
    // driver, so testing the combined mask is exact for driver-visible states.
    static MASK: OnceLock<u16> = OnceLock::new();
    *MASK.get_or_init(|| {
        tikv_flag_mask(TikvFlagsOp::SetPresumeKeyNotExists) & !previous_presume_mask()
    })
}

const fn to_tikv_flags_op(operation: FlagsOp) -> TikvFlagsOp {
    match operation {
        FlagsOp::SetPresumeKeyNotExists => TikvFlagsOp::SetPresumeKeyNotExists,
        FlagsOp::SetNeedLocked => TikvFlagsOp::SetNeedLocked,
        FlagsOp::SetNeedConstraintCheckInPrewrite => TikvFlagsOp::SetNeedConstraintCheckInPrewrite,
        FlagsOp::SetPreviousPresumeKeyNotExists => TikvFlagsOp::SetPreviousPresumeKeyNotExists,
    }
}

const fn to_tikv_assertion_op(operation: AssertionOp) -> TikvFlagsOp {
    match operation {
        AssertionOp::AssertExist => TikvFlagsOp::SetAssertExist,
        AssertionOp::AssertNotExist => TikvFlagsOp::SetAssertNotExist,
        AssertionOp::AssertUnknown => TikvFlagsOp::SetAssertUnknown,
        AssertionOp::AssertNone => TikvFlagsOp::SetAssertNone,
    }
}

fn to_tikv_flags_ops(operations: &[FlagsOp]) -> Vec<TikvFlagsOp> {
    operations.iter().copied().map(to_tikv_flags_op).collect()
}

fn to_driver_flags(flags: TikvKeyFlags) -> KeyFlags {
    let bits = flags.bits();
    let mut operations = Vec::with_capacity(4);
    if bits & presume_mask() != 0 {
        operations.push(FlagsOp::SetPresumeKeyNotExists);
    }
    if bits & previous_presume_mask() != 0 {
        operations.push(FlagsOp::SetPreviousPresumeKeyNotExists);
    }
    if flags.has_need_locked() {
        operations.push(FlagsOp::SetNeedLocked);
    }
    if flags.has_need_constraint_check_in_prewrite() {
        operations.push(FlagsOp::SetNeedConstraintCheckInPrewrite);
    }
    let assertion = if flags.has_assert_unknown() {
        AssertionState::Unknown
    } else if flags.has_assert_exist() {
        AssertionState::Exists
    } else if flags.has_assert_not_exist() {
        AssertionState::NotExists
    } else {
        AssertionState::Unset
    };
    KeyFlags::new()
        .apply_flags_ops(operations)
        .with_assertion_state(assertion)
}

/// Ordered buffer iterator adapting client-go's borrowed-bytes positions to
/// the driver's typed [`Key`] surface.
pub struct TikvMemBufferIterator {
    inner: Box<dyn TikvKvIterator>,
    current_key: Key,
}

impl TikvMemBufferIterator {
    fn new(inner: Box<dyn TikvKvIterator>) -> Self {
        let mut iterator = Self {
            inner,
            current_key: Key::from(Vec::new()),
        };
        iterator.refresh_key();
        iterator
    }

    fn refresh_key(&mut self) {
        if self.inner.valid() {
            self.current_key = Key::from(self.inner.key().to_vec());
        }
    }
}

impl KvIterator for TikvMemBufferIterator {
    type Error = TikvMemBufferError;

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> &Key {
        &self.current_key
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.inner
            .next()
            .map_err(|message| TikvMemBufferError::Backend(message.to_owned()))?;
        self.refresh_key();
        Ok(())
    }

    fn close(&mut self) {
        self.inner.close();
    }
}

/// Stable getter over a buffer snapshot that excludes active staged writes,
/// like Go `MemBuffer.SnapshotGetter`.
pub struct TikvMemBufferSnapshotGetter {
    snapshot: MemDbSnapshot,
}

impl Getter for TikvMemBufferSnapshotGetter {
    type Error = TikvMemBufferError;

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        // The local buffer never supplies a commit timestamp, regardless of
        // read options, exactly like client-go's `MemBuffer` getter.
        self.snapshot
            .get(key.as_bytes())
            .map(|value| ValueEntry::new(value, 0))
            .map_err(map_snapshot_error)
    }
}

/// TiKV-client concrete backend for [`super::mem_buffer::MemBufferDriver`].
#[derive(Default)]
pub struct TikvMemBufferBackend {
    memdb: MemDb,
}

impl TikvMemBufferBackend {
    /// Creates an empty transaction buffer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            memdb: MemDb::new(),
        }
    }

    /// Borrows the wrapped client-go buffer for client-specific operations.
    #[must_use]
    pub const fn memdb(&self) -> &MemDb {
        &self.memdb
    }

    /// Mutably borrows the wrapped client-go buffer.
    pub fn memdb_mut(&mut self) -> &mut MemDb {
        &mut self.memdb
    }

    /// Returns the wrapped client-go buffer.
    #[must_use]
    pub fn into_memdb(self) -> MemDb {
        self.memdb
    }
}

impl MemBufferBackend for TikvMemBufferBackend {
    type Error = TikvMemBufferError;
    type Iter = TikvMemBufferIterator;
    type SnapshotGetter = TikvMemBufferSnapshotGetter;

    fn len(&self) -> usize {
        self.memdb.len()
    }

    fn size(&self) -> usize {
        self.memdb.size()
    }

    fn get(&mut self, key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
        // Tombstones surface as empty values; commit timestamps never come
        // from the local buffer.
        self.memdb
            .get_entry(key.as_bytes(), &[])
            .map(|entry| ValueEntry::new(entry.value, 0))
            .map_err(map_read_error)
    }

    fn batch_get(
        &mut self,
        keys: &[Key],
        _options: BatchGetOptions,
    ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        Ok(self
            .memdb
            .batch_get_entries(&raw_keys, &[])
            .into_iter()
            .map(|(key, entry)| (Key::from(key), ValueEntry::new(entry.value, 0)))
            .collect())
    }

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error> {
        self.memdb
            .set(key.as_bytes(), &value)
            .map_err(map_write_error)
    }

    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), Self::Error> {
        self.memdb
            .set_with_flags(key.as_bytes(), &value, &to_tikv_flags_ops(operations))
            .map_err(map_write_error)
    }

    fn delete(&mut self, key: Key) -> Result<(), Self::Error> {
        self.memdb.delete(key.as_bytes()).map_err(map_write_error)
    }

    fn delete_with_flags(&mut self, key: Key, operations: &[FlagsOp]) -> Result<(), Self::Error> {
        self.memdb
            .delete_with_flags(key.as_bytes(), &to_tikv_flags_ops(operations))
            .map_err(map_write_error)
    }

    fn remove_from_buffer(&mut self, key: &Key) {
        self.memdb.remove_from_buffer(key.as_bytes());
    }

    fn update_flags(&mut self, key: &Key, operations: &[FlagsOp]) {
        self.memdb
            .update_flags(key.as_bytes(), &to_tikv_flags_ops(operations));
    }

    fn update_assertion_flags(&mut self, key: &Key, operation: AssertionOp) {
        self.memdb
            .update_flags(key.as_bytes(), &[to_tikv_assertion_op(operation)]);
    }

    fn get_flags(&self, key: &Key) -> Result<KeyFlags, Self::Error> {
        // Missing keys report not-found like Go `MemBuffer.GetFlags`, not the
        // zero flags.
        self.memdb
            .get_flags_readonly(key.as_bytes())
            .map(to_driver_flags)
            .map_err(map_read_error)
    }

    fn staging(&mut self) -> StagingHandle {
        // Client-go staging handles are 1-based, matching the driver's
        // positive-index representation exactly.
        StagingHandle::new(self.memdb.staging())
    }

    fn cleanup(&mut self, handle: StagingHandle) {
        if let Some(index) = handle.index() {
            self.memdb.cleanup(index);
        }
    }

    fn release(&mut self, handle: StagingHandle) {
        if let Some(index) = handle.index() {
            self.memdb.release(index);
        }
    }

    fn inspect_stage(&self, handle: StagingHandle, inspect: &mut dyn FnMut(&Key, KeyFlags, &[u8])) {
        let Some(index) = handle.index() else {
            return;
        };
        self.memdb.inspect_stage(index, |key, flags, value| {
            inspect(&Key::from(key.to_vec()), to_driver_flags(flags), value);
        });
    }

    fn iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Result<Self::Iter, Self::Error> {
        Ok(TikvMemBufferIterator::new(self.memdb.iter(
            Some(start.as_bytes()),
            upper_bound.map(Key::as_bytes),
        )))
    }

    fn iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iter, Self::Error> {
        Ok(TikvMemBufferIterator::new(self.memdb.iter_reverse(
            start.map(Key::as_bytes),
            lower_bound.map(Key::as_bytes),
        )))
    }

    fn snapshot_iter(&mut self, start: &Key, upper_bound: Option<&Key>) -> Self::Iter {
        TikvMemBufferIterator::new(
            self.memdb
                .snapshot_iter(Some(start.as_bytes()), upper_bound.map(Key::as_bytes)),
        )
    }

    fn snapshot_iter_reverse(
        &mut self,
        start: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Self::Iter {
        TikvMemBufferIterator::new(
            self.memdb
                .snapshot_iter_reverse(start.map(Key::as_bytes), lower_bound.map(Key::as_bytes)),
        )
    }

    fn snapshot_getter(&mut self) -> Self::SnapshotGetter {
        TikvMemBufferSnapshotGetter {
            snapshot: self.memdb.snapshot_getter(),
        }
    }

    fn get_local(&mut self, key: &Key) -> Result<Vec<u8>, Self::Error> {
        self.memdb.get(key.as_bytes()).map_err(map_read_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the derived masks against upstream's own operation application, so
    /// an upstream flag-bit re-layout fails here instead of corrupting reads.
    #[test]
    fn derived_masks_track_upstream_flag_layout() {
        assert_ne!(presume_mask(), 0);
        assert_ne!(previous_presume_mask(), 0);
        assert_eq!(presume_mask() & previous_presume_mask(), 0);

        let presume = tikv_apply_flags_ops(
            TikvKeyFlags::default(),
            &[TikvFlagsOp::SetPresumeKeyNotExists],
        );
        assert!(presume.bits() & presume_mask() != 0);
        assert!(presume.bits() & previous_presume_mask() == 0);

        let previous = tikv_apply_flags_ops(
            TikvKeyFlags::default(),
            &[TikvFlagsOp::SetPreviousPresumeKeyNotExists],
        );
        assert!(previous.bits() & previous_presume_mask() != 0);
        assert!(previous.bits() & presume_mask() == 0);
    }

    /// Every driver-reachable flag state converts to the exact typed flags.
    #[test]
    fn flag_conversion_round_trips_every_driver_reachable_state() {
        const DRIVER_OPS: [FlagsOp; 4] = [
            FlagsOp::SetPresumeKeyNotExists,
            FlagsOp::SetNeedLocked,
            FlagsOp::SetNeedConstraintCheckInPrewrite,
            FlagsOp::SetPreviousPresumeKeyNotExists,
        ];
        const ASSERTIONS: [AssertionOp; 4] = [
            AssertionOp::AssertExist,
            AssertionOp::AssertNotExist,
            AssertionOp::AssertUnknown,
            AssertionOp::AssertNone,
        ];

        for op_bits in 0_u8..16 {
            for assertion in ASSERTIONS {
                let selected: Vec<FlagsOp> = DRIVER_OPS
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| op_bits & (1 << index) != 0)
                    .map(|(_, op)| *op)
                    .collect();

                let mut tikv_ops = to_tikv_flags_ops(&selected);
                tikv_ops.push(to_tikv_assertion_op(assertion));
                let tikv_flags = tikv_apply_flags_ops(TikvKeyFlags::default(), &tikv_ops);

                let expected = KeyFlags::new()
                    .apply_flags_ops(selected.iter().copied())
                    .apply_assertion_op(assertion);
                assert_eq!(
                    to_driver_flags(tikv_flags),
                    expected,
                    "op_bits={op_bits:#06b}, assertion={assertion:?}"
                );
            }
        }
    }
}
