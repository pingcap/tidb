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

//! Go `pkg/table/temptable/interceptor.go`: the snapshot interceptor that
//! answers reads of a temporary table's keys from the session instead of
//! the store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_kv::cluster_storage::{ClusterSnapshot, SnapshotPairs};
use tidb_kv::storage::StorageError;
use tidb_model::TempTableType;
use tidb_tablecodec::table_key::{decode_table_id, encode_table_prefix, TABLE_PREFIX};
use tidb_txnkv::Key;
use tidb_variable::temporary_table_data::TemporaryTableData;

/// Go `tableWithIDPrefixLen`: `t` plus the eight-byte table id.
const TABLE_WITH_ID_PREFIX_LEN: usize = 9;

/// The temporary tables of a session's infoschema by id, all Go's
/// `temporaryTableInfoByID` reads of it; the session refreshes it as its
/// infoschema changes.
pub type TemporaryTableKinds = Arc<std::sync::RwLock<HashMap<i64, TempTableType>>>;

/// Go `TemporaryTableSnapshotInterceptor`, wrapping the snapshot it
/// intercepts (Go passes it to every `On*` call).
#[derive(Debug)]
pub struct TemporaryTableSnapshotInterceptor {
    snap: Box<dyn ClusterSnapshot>,
    is: TemporaryTableKinds,
    session_data: Option<Arc<Mutex<TemporaryTableData>>>,
}

/// Go `SessionSnapshotInterceptor`: `None` when the infoschema has no
/// temporary table (Go's `!is.HasTemporaryTable()`), so the snapshot is used
/// as it is.
#[must_use]
pub fn session_snapshot_interceptor(
    snap: Box<dyn ClusterSnapshot>,
    is: TemporaryTableKinds,
    session_data: Option<Arc<Mutex<TemporaryTableData>>>,
) -> Result<TemporaryTableSnapshotInterceptor, Box<dyn ClusterSnapshot>> {
    if is
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty()
    {
        return Err(snap);
    }
    Ok(TemporaryTableSnapshotInterceptor::new(snap, is, session_data))
}

impl TemporaryTableSnapshotInterceptor {
    /// Go `NewTemporaryTableSnapshotInterceptor`.
    #[must_use]
    pub fn new(
        snap: Box<dyn ClusterSnapshot>,
        is: TemporaryTableKinds,
        session_data: Option<Arc<Mutex<TemporaryTableData>>>,
    ) -> Self {
        Self {
            snap,
            is,
            session_data,
        }
    }

    fn temporary_table_info_by_id(&self, tbl_id: i64) -> Option<TempTableType> {
        self.is
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&tbl_id)
            .copied()
            .filter(|kind| *kind != TempTableType::NONE)
    }

    /// Go `OnGet`, over either the snapshot's ordinary or committed read.
    fn on_get(
        &mut self,
        k: &Key,
        get: impl FnOnce(&mut dyn ClusterSnapshot, &Key) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(tbl_id) = get_key_accessed_table_id(k.as_bytes()) {
            if let Some(kind) = self.temporary_table_info_by_id(tbl_id) {
                return get_session_key(kind, self.session_data.as_ref(), k);
            }
        }
        get(self.snap.as_mut(), k)
    }

    /// Go `OnBatchGet`.
    fn on_batch_get(
        &mut self,
        keys: &[Key],
        batch_get: impl FnOnce(&mut dyn ClusterSnapshot, &[Key]) -> Result<SnapshotPairs, StorageError>,
    ) -> Result<SnapshotPairs, StorageError> {
        let (keys, mut result) = self.batch_get_temporary_table_keys(keys)?;
        if !keys.is_empty() {
            let snap_result = batch_get(self.snap.as_mut(), &keys)?;
            result.extend(snap_result);
        }
        Ok(result)
    }

    fn batch_get_temporary_table_keys(
        &self,
        keys: &[Key],
    ) -> Result<(Vec<Key>, SnapshotPairs), StorageError> {
        let mut snap_keys = Vec::new();
        let mut result = Vec::new();
        for k in keys {
            let Some(kind) = get_key_accessed_table_id(k.as_bytes())
                .and_then(|tbl_id| self.temporary_table_info_by_id(tbl_id))
            else {
                snap_keys.push(k.clone());
                continue;
            };
            if let Some(val) = get_session_key(kind, self.session_data.as_ref(), k)? {
                result.push((k.as_bytes().to_vec(), val));
            }
        }
        Ok((snap_keys, result))
    }

    /// Go `OnIter`. The snapshot seam reads ranges by batch, so the
    /// iterator is `[k, upper_bound)` read at most `limit` pairs at a time.
    fn on_iter(
        &mut self,
        k: &Key,
        upper_bound: &Key,
        limit: Option<usize>,
        committed: bool,
    ) -> Result<SnapshotPairs, StorageError> {
        if not_table_range(k.as_bytes(), upper_bound.as_bytes()) {
            return scan(self.snap.as_mut(), k, upper_bound, limit, committed);
        }
        if let Some(tbl_id) = get_range_accessed_table_id(k.as_bytes(), upper_bound.as_bytes()) {
            return self.iter_table(tbl_id, k, upper_bound, limit, committed);
        }
        create_union_iter(
            self.session_data.as_ref(),
            Some(self.snap.as_mut()),
            k,
            upper_bound,
            limit,
            committed,
        )
    }

    fn iter_table(
        &mut self,
        tbl_id: i64,
        k: &Key,
        upper_bound: &Key,
        limit: Option<usize>,
        committed: bool,
    ) -> Result<SnapshotPairs, StorageError> {
        let Some(kind) = self.temporary_table_info_by_id(tbl_id) else {
            return scan(self.snap.as_mut(), k, upper_bound, limit, committed);
        };
        if kind == TempTableType::GLOBAL || self.session_data.is_none() {
            return Ok(Vec::new());
        }
        // Still a union, to filter out the empty values of the session data.
        create_union_iter(self.session_data.as_ref(), None, k, upper_bound, limit, committed)
    }
}

fn scan(
    snap: &mut dyn ClusterSnapshot,
    k: &Key,
    upper_bound: &Key,
    limit: Option<usize>,
    committed: bool,
) -> Result<SnapshotPairs, StorageError> {
    if committed {
        snap.scan_committed(k, upper_bound, limit)
    } else {
        snap.scan(k, upper_bound, limit)
    }
}

/// Go `getSessionKey`. `None` is Go's `kv.ErrNotExist`.
fn get_session_key(
    kind: TempTableType,
    session_data: Option<&Arc<Mutex<TemporaryTableData>>>,
    k: &Key,
) -> Result<Option<Vec<u8>>, StorageError> {
    if kind == TempTableType::NONE {
        return Err(StorageError::Backend(
            "Cannot get normal table key from session".to_owned(),
        ));
    }
    let Some(session_data) = session_data.filter(|_| kind != TempTableType::GLOBAL) else {
        return Ok(None);
    };
    let value = session_data
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(k);
    match value {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(StorageError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Go `createUnionIter` with `txn.NewUnionIter`: the session data's pairs
/// over the snapshot's, the session's winning a shared key and its empty
/// values (deletions) hiding the key.
fn create_union_iter(
    session_data: Option<&Arc<Mutex<TemporaryTableData>>>,
    mut snap: Option<&mut dyn ClusterSnapshot>,
    k: &Key,
    upper_bound: &Key,
    limit: Option<usize>,
    committed: bool,
) -> Result<SnapshotPairs, StorageError> {
    let Some(session_data) = session_data else {
        return match snap {
            Some(snap) => scan(snap, k, upper_bound, limit, committed),
            None => Ok(Vec::new()),
        };
    };
    let mut dirty: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let mut session_data = session_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An empty bound is Go's nil: unbounded.
        let upper = (!upper_bound.as_bytes().is_empty()).then_some(upper_bound);
        let mut iter = session_data.iter(Some(k), upper)?;
        while iter.valid() {
            dirty.push((iter.key().as_bytes().to_vec(), iter.value().to_vec()));
            iter.next()?;
        }
        iter.close();
    }
    let mut result = SnapshotPairs::new();
    let mut dirty = dirty.into_iter().peekable();
    let mut start = k.clone();
    loop {
        // One snapshot batch; a full batch may have more behind it, so the
        // dirty pairs are merged only up to its last key.
        let (batch, drained) = match snap.as_deref_mut() {
            Some(snap) => {
                let batch = scan(snap, &start, upper_bound, limit, committed)?;
                let drained = limit.is_none_or(|limit| batch.len() < limit);
                (batch, drained)
            }
            None => (Vec::new(), true),
        };
        let last = batch.last().map(|(key, _)| key.clone());
        let mut batch = batch.into_iter().peekable();
        loop {
            if limit.is_some_and(|limit| result.len() >= limit) {
                return Ok(result);
            }
            let take_dirty = match (dirty.peek(), batch.peek()) {
                (Some((d, _)), Some((s, _))) => d <= s,
                (Some((d, _)), None) => drained || last.as_ref().is_some_and(|last| d <= last),
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if take_dirty {
                let (key, value) = dirty.next().expect("peeked");
                if batch.peek().is_some_and(|(s, _)| *s == key) {
                    batch.next();
                }
                if !value.is_empty() {
                    result.push((key, value));
                }
            } else if let Some(pair) = batch.next() {
                result.push(pair);
            } else {
                break;
            }
        }
        match (drained, last) {
            (false, Some(mut last)) => {
                last.push(0);
                start = Key::from_bytes(last);
            }
            _ => return Ok(result),
        }
    }
}

/// Go `getRangeAccessedTableID`.
fn get_range_accessed_table_id(start_key: &[u8], end_key: &[u8]) -> Option<i64> {
    let tbl_id = get_key_accessed_table_id(start_key)?;
    let tbl_start = encode_table_prefix(tbl_id);
    let tbl_end = encode_table_prefix(tbl_id + 1);
    (end_key.starts_with(&tbl_start) || end_key == tbl_end.as_slice()).then_some(tbl_id)
}

/// Go `getKeyAccessedTableID`.
fn get_key_accessed_table_id(k: &[u8]) -> Option<i64> {
    if k.starts_with(TABLE_PREFIX) && k.len() >= TABLE_WITH_ID_PREFIX_LEN {
        let tb_id = decode_table_id(k);
        if tb_id > 0 && tb_id != i64::MAX {
            return Some(tb_id);
        }
    }
    None
}

/// Go `notTableRange`.
fn not_table_range(k: &[u8], upper_bound: &[u8]) -> bool {
    (k > TABLE_PREFIX && !k.starts_with(TABLE_PREFIX))
        || (!upper_bound.is_empty() && upper_bound < TABLE_PREFIX)
}

/// A snapshot that holds nothing: what a temporary table's reader reads
/// from the store, since its rows are never there (Go builds a dummy
/// `TableReader` for it). Under the interceptor, every temporary key still
/// resolves from the session.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptySnapshot;

impl ClusterSnapshot for EmptySnapshot {
    fn get(&mut self, _key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }

    fn scan(
        &mut self,
        _start: &Key,
        _end: &Key,
        _limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(Vec::new())
    }
}

impl ClusterSnapshot for TemporaryTableSnapshotInterceptor {
    fn set_snapshot_read_options(&mut self, options: tidb_txnkv::SnapshotReadOptions) {
        self.snap.set_snapshot_read_options(options);
    }

    fn set_statement_kv_context(&mut self, context: &tidb_txnkv::StatementKvContextCell) {
        self.snap.set_statement_kv_context(context);
    }

    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.snap.point_rpc_counts()
    }

    fn lock_keys_for_read(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
        only_if_exists: bool,
    ) -> Result<(), StorageError> {
        self.snap.lock_keys_for_read(keys, wait, only_if_exists)
    }

    fn do_lock_keys(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<(), StorageError> {
        self.snap.do_lock_keys(keys, wait)
    }

    fn prepare(&mut self) -> Result<(), StorageError> {
        self.snap.prepare()
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.on_get(key, |snap, key| snap.get(key))
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.on_batch_get(keys, |snap, keys| snap.batch_get(keys))
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.on_iter(start, end, limit, false)
    }

    fn scan_committed(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.on_iter(start, end, limit, true)
    }

    fn get_committed(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.on_get(key, |snap, key| snap.get_committed(key))
    }

    fn batch_get_committed(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.on_batch_get(keys, |snap, keys| snap.batch_get_committed(keys))
    }

    fn start_ts(&self) -> u64 {
        self.snap.start_ts()
    }

    fn declare_autocommit_point_get(&mut self) -> bool {
        self.snap.declare_autocommit_point_get()
    }
}

#[cfg(test)]
mod tests;
