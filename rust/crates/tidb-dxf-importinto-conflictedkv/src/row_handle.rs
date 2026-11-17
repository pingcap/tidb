// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictedkv/row_handle.go`: the data row keys already handled.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use tidb_log::{Field, Value};

/// Go `initMapSizeForConflictedRows`: the initial capacity hint of a set.
/// A conflicted row may have several conflicted KVs, and its data KV may not
/// be in the cluster, so the hint is small.
const INIT_MAP_SIZE_FOR_CONFLICTED_ROWS: usize = 128;
/// Go `rowKeyMapEntryShallowSize`: `unsafe.Sizeof("") + unsafe.Sizeof(true)`
/// on 64-bit Go, what each entry adds to the shared size besides its key.
const ROW_KEY_MAP_ENTRY_SHALLOW_SIZE: i64 = 16 + 1;

/// Go `KeyFilter`: the row keys handled in earlier index KV groups (global)
/// and by this handler in the current one (local).
pub struct KeyFilter {
    global_set: Arc<RwLock<BoundedKeySet>>,
    local_set: BoundedKeySet,
}

impl KeyFilter {
    /// Go `NewKeyFilter`.
    #[must_use]
    pub fn new(global_set: Arc<RwLock<BoundedKeySet>>, local_set: BoundedKeySet) -> Self {
        Self { global_set, local_set }
    }

    /// Go `isHandledGlobally`: the row key was handled with another index KV
    /// group.
    pub(crate) fn is_handled_globally(&self, row_key: &[u8]) -> bool {
        self.global_set.read().unwrap_or_else(std::sync::PoisonError::into_inner).contains(row_key)
    }

    /// Go `isHandledLocally`.
    pub(crate) fn is_handled_locally(&self, row_key: &[u8]) -> bool {
        self.local_set.contains(row_key)
    }

    /// Go `addLocal`.
    pub(crate) fn add_local(&mut self, row_key: &[u8]) {
        self.local_set.add(row_key);
    }

    /// The local set, for the caller to merge into the global one.
    #[must_use]
    pub fn into_local_set(self) -> BoundedKeySet {
        self.local_set
    }
}

/// Go `BoundedKeySet`: data row keys with a size limit shared with the
/// other sets of the subtask. A row key, not a handle, so the partition
/// is part of it.
pub struct BoundedKeySet {
    shared_size: Arc<AtomicI64>,
    size_limit: i64,
    row_keys: HashSet<Vec<u8>>,
}

impl BoundedKeySet {
    /// Go `NewBoundedKeySet`.
    #[must_use]
    pub fn new(shared_size: Arc<AtomicI64>, limit: i64) -> Self {
        let size = if shared_size.load(Ordering::SeqCst) >= limit { 0 } else { INIT_MAP_SIZE_FOR_CONFLICTED_ROWS };
        Self { shared_size, size_limit: limit, row_keys: HashSet::with_capacity(size) }
    }

    /// Go `Add` / `addStr`: nothing once the bound is exceeded. The key that
    /// crosses the bound is still kept, so a merged set skips as many rows
    /// of later KV groups as it can.
    pub fn add(&mut self, row_key: &[u8]) {
        if self.bound_exceeded() {
            return;
        }
        let delta = row_key.len() as i64 + ROW_KEY_MAP_ENTRY_SHALLOW_SIZE;
        let new_size = self.shared_size.fetch_add(delta, Ordering::SeqCst) + delta;
        self.row_keys.insert(row_key.to_vec());
        if new_size >= self.size_limit && new_size - delta < self.size_limit {
            tidb_logutil::bg_logger().info(
                "too many conflict rows from index, skip checking",
                &[
                    Field::new(
                        "totalRowKeySize",
                        Value::Str(tidb_config_configtypes::bytes_size(self.shared_size.load(Ordering::SeqCst) as f64)),
                    ),
                    Field::new("sizeLimit", Value::Str(tidb_config_configtypes::bytes_size(self.size_limit as f64))),
                    Field::new("localRowKeyCount", Value::I64(self.row_keys.len() as i64)),
                ],
            );
        }
    }

    /// Go `Contains`.
    #[must_use]
    pub fn contains(&self, row_key: &[u8]) -> bool {
        !self.row_keys.is_empty() && self.row_keys.contains(row_key)
    }

    /// Go `Merge`.
    pub fn merge(&mut self, other: BoundedKeySet) {
        self.row_keys.extend(other.row_keys);
    }

    /// Go `BoundExceeded`.
    #[must_use]
    pub fn bound_exceeded(&self) -> bool {
        let mut limit = self.size_limit;
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv/mockKeySetSizeLimit",
            &mut limit,
        );
        self.shared_size.load(Ordering::SeqCst) >= limit
    }
}
