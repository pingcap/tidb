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

use super::{TableCommon, KvTableError};
use tidb_txnkv::Key;

const CACHE_TABLE_SIZE_LIMIT: usize = 64 * (1 << 20);

impl TableCommon {
    /// Whether Go's persisted table-cache state is enabled.
    #[must_use]
    pub fn is_cached(&self) -> bool {
        self.cache_status() == tidb_model::TableCacheStatusType::ENABLE
    }

    /// Go's `table.CachedTable` view of the table: its cache runtime.
    #[must_use]
    pub fn cached_table(&self) -> Option<&std::sync::Arc<CachedTable>> {
        self.cached.as_ref()
    }

    /// Go `newCachedTable`: makes the table a cached table.
    pub fn set_cached_table(&mut self, cached: std::sync::Arc<CachedTable>) {
        self.cached = Some(cached);
    }

    /// Go `cachedTable.AddRecord` / `UpdateRecord` (which first refuse a table
    /// already too large) and `RemoveRecord`: the transaction records the
    /// cached table it writes (`txnCtxAddCachedTable`), whose write lock its
    /// commit takes.
    pub(crate) fn txn_ctx_add_cached_table(
        &self,
        ctx: &dyn tidb_tblctx::MutateContext,
        check_size: bool,
    ) -> Result<(), KvTableError> {
        let Some(cached) = &self.cached else {
            return Ok(());
        };
        if check_size {
            cached.check_write_size()?;
        }
        ctx.add_cached_table_handle_to_txn(
            self.table_id,
            std::sync::Arc::clone(cached) as std::sync::Arc<dyn std::any::Any + Send + Sync>,
        );
        Ok(())
    }

    /// Drops the cache runtime of a table no lock handle can be given to.
    pub fn clear_cached_table(&mut self) {
        self.cached = None;
    }

    /// Whether the table is in any non-disabled cache state. Go constructs
    /// the cached-table wrapper and blocks ordinary DDL for both ENABLE and
    /// the transient SWITCHING state.
    #[must_use]
    pub fn is_cache_table(&self) -> bool {
        self.cache_status() != tidb_model::TableCacheStatusType::DISABLE
    }

    /// Go `TableInfo.Lock`.
    #[must_use]
    pub fn lock(&self) -> Option<tidb_model::TableLockInfo> {
        self.meta.read().lock.as_ref().map(|lock| lock.read().clone())
    }

    /// Go `TableInfo.IsLocked`: a lock some session still holds.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.meta
            .read()
            .lock
            .as_ref()
            .is_some_and(|lock| !lock.read().sessions.is_empty())
    }



    /// Enables table caching after the two source admission checks.
    ///
    /// # Errors
    ///
    /// `ErrOptOnCacheTable` for a partitioned table or a
    /// table whose encoded keys and values exceed TiDB's 64 MiB limit, and
    /// [`KvTableError::Storage`] when the backend scan fails.
    pub fn enable_cache(&mut self, store: &mut dyn tidb_kv::storage::TableStorage) -> Result<(), KvTableError> {
        if self.is_cached() {
            return Ok(());
        }
        if self.partition.is_some() {
            return Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                        .generate_with_stack_by_args(&["partition mode".into()]),
                ),
            ));
        }
        if self.cache_storage_size_exceeds(store, CACHE_TABLE_SIZE_LIMIT)? {
            return Err(KvTableError::Mysql(
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                        .generate_with_stack_by_args(&["table too large".into()]),
                ),
            ));
        }
        self.update_meta(|meta| {
            meta.table_cache_status_type = tidb_model::TableCacheStatusType::ENABLE;
        });
        Ok(())
    }

    /// Go `checkCacheTableSize`: the table's keys and values in `store`
    /// against `limit`.
    fn cache_storage_size_exceeds(
        &self,
        store: &mut dyn tidb_kv::storage::TableStorage,
        limit: usize,
    ) -> Result<bool, KvTableError> {
        let prefix = Key::from_bytes(tidb_tablecodec::table_key::gen_table_prefix(self.table_id));
        let upper_bound = prefix.prefix_next();
        let mut iterator = store
            .iter(Some(&prefix), Some(&upper_bound))
            .map_err(KvTableError::from)?;
        let mut size = 0usize;
        while iterator.valid() {
            size = size
                .saturating_add(iterator.key().as_bytes().len())
                .saturating_add(iterator.value().len());
            if size > limit {
                iterator.close();
                return Ok(true);
            }
            if let Err(error) = iterator.next() {
                iterator.close();
                return Err(KvTableError::from(error));
            }
        }
        iterator.close();
        Ok(false)
    }
}

/// Go `cachedTableSizeLimit`: a cached table larger than this refuses writes.
pub const CACHED_TABLE_SIZE_LIMIT: i64 = 64 * (1 << 20);

/// Go `cacheTableWriteLease`.
pub const CACHE_TABLE_WRITE_LEASE: std::time::Duration = std::time::Duration::from_secs(5);

/// Go `cacheData`: the loaded rows and the lease they are valid under.
#[derive(Debug)]
pub struct CacheData {
    /// The start ts of the transaction the rows were read in.
    pub start: u64,
    /// The read lease.
    pub lease: u64,
    /// The rows; `None` while a background load is still running.
    pub mem_buffer: Option<std::sync::Arc<tidb_kv::mem_buffer::MemBufferSnapshot>>,
}

/// Go `cachedTable`'s runtime beside its `TableCommon`: the cache, its size,
/// and the one lock-meta handle (Go `tokenLimit`).
pub struct CachedTable {
    table_id: i64,
    cache_data: std::sync::Mutex<Option<std::sync::Arc<CacheData>>>,
    total_size: std::sync::atomic::AtomicI64,
    token: std::sync::Mutex<Option<Box<dyn super::state_remote::StateRemote>>>,
    token_returned: std::sync::Condvar,
}

impl std::fmt::Debug for CachedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedTable")
            .field("table_id", &self.table_id)
            .field("cache_data", &self.load_cache_data())
            .finish_non_exhaustive()
    }
}

impl CachedTable {
    /// Go `newCachedTable`.
    #[must_use]
    pub fn new(table_id: i64) -> Self {
        Self {
            table_id,
            cache_data: std::sync::Mutex::new(None),
            total_size: std::sync::atomic::AtomicI64::new(0),
            token: std::sync::Mutex::new(None),
            token_returned: std::sync::Condvar::new(),
        }
    }

    /// Go `Init`: the lock-meta handle over the internal session it was given.
    pub fn init(&self, handle: Box<dyn super::state_remote::StateRemote>) {
        self.put_state_remote_handle(handle);
    }

    /// Go `TakeStateRemoteHandle`: waits for the handle.
    fn take_state_remote_handle(&self) -> Box<dyn super::state_remote::StateRemote> {
        let mut token = self.token.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(handle) = token.take() {
                return handle;
            }
            token = self
                .token_returned
                .wait(token)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Go `TakeStateRemoteHandleNoWait`.
    fn take_state_remote_handle_no_wait(&self) -> Option<Box<dyn super::state_remote::StateRemote>> {
        self.token.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
    }

    /// Go `PutStateRemoteHandle`.
    fn put_state_remote_handle(&self, handle: Box<dyn super::state_remote::StateRemote>) {
        *self.token.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        self.token_returned.notify_one();
    }

    fn load_cache_data(&self) -> Option<std::sync::Arc<CacheData>> {
        self.cache_data.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn store_cache_data(&self, data: CacheData) {
        *self.cache_data.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(std::sync::Arc::new(data));
    }

    /// Go `TryReadFromCache`: the cached rows when `ts` reads inside the
    /// lease, and whether a load is still running. Near the lease's end it
    /// renews the lease in the background.
    pub fn try_read_from_cache(
        self: &std::sync::Arc<Self>,
        ts: u64,
        lease_duration: std::time::Duration,
    ) -> (Option<std::sync::Arc<tidb_kv::mem_buffer::MemBufferSnapshot>>, bool) {
        let Some(data) = self.load_cache_data() else {
            return (None, false);
        };
        if ts >= data.start && ts < data.lease {
            let lease_time = super::state_remote::get_time_from_ts(data.lease);
            let now_time = super::state_remote::get_time_from_ts(ts);
            let distance = lease_time.saturating_sub(now_time);
            if distance <= super::state_remote::duration_millis(lease_duration / 2) {
                if let Some(handle) = self.take_state_remote_handle_no_wait() {
                    let table = std::sync::Arc::clone(self);
                    let data = std::sync::Arc::clone(&data);
                    std::thread::spawn(move || table.renew_lease(handle, ts, &data, lease_duration));
                }
            }
            // Data present without rows: a background load is running.
            return (data.mem_buffer.clone(), data.mem_buffer.is_none());
        }
        (None, false)
    }

    /// Go `loadDataFromOriginalTable`.
    fn load_data_from_original_table(
        &self,
        store: &dyn tidb_kv::storage::KvStorage,
    ) -> Result<(tidb_kv::mem_buffer::MemBufferSnapshot, u64, i64), tidb_kv::storage::StorageError> {
        let mut buffer = tidb_kv::mem_buffer::MemBufferSnapshot::default();
        let mut start_ts = 0;
        let mut total_size = 0_i64;
        let prefix = Key::from_bytes(tidb_tablecodec::table_key::gen_table_prefix(self.table_id));
        let upper_bound = prefix.prefix_next();
        store.run_in_new_txn(&mut |ts, txn| {
            start_ts = ts;
            buffer.entries.clear();
            total_size = 0;
            let mut iterator = txn.iter(Some(&prefix), Some(&upper_bound))?;
            while iterator.valid() && iterator.key().as_bytes().starts_with(prefix.as_bytes()) {
                let key = iterator.key().as_bytes().to_vec();
                let value = iterator.value().to_vec();
                total_size += (key.len() + value.len()) as i64;
                buffer.entries.insert(key, value);
                if let Err(error) = iterator.next() {
                    iterator.close();
                    return Err(error);
                }
            }
            iterator.close();
            Ok(())
        })?;
        Ok((buffer, start_ts, total_size))
    }

    /// Go `UpdateLockForRead`: takes the read lock and loads the rows in the
    /// background, unless the handle is busy.
    pub fn update_lock_for_read(
        self: &std::sync::Arc<Self>,
        store: std::sync::Arc<dyn tidb_kv::storage::KvStorage>,
        ts: u64,
        lease_duration: std::time::Duration,
    ) {
        if let Some(handle) = self.take_state_remote_handle_no_wait() {
            let table = std::sync::Arc::clone(self);
            std::thread::spawn(move || table.update_lock_for_read_with(handle, store, ts, lease_duration));
        }
    }

    /// Go `updateLockForRead`.
    fn update_lock_for_read_with(
        self: &std::sync::Arc<Self>,
        mut handle: Box<dyn super::state_remote::StateRemote>,
        store: std::sync::Arc<dyn tidb_kv::storage::KvStorage>,
        ts: u64,
        lease_duration: std::time::Duration,
    ) {
        let lease = super::state_remote::lease_from_ts(ts, lease_duration);
        let locked = handle.lock_for_read(self.table_id, lease);
        self.put_state_remote_handle(handle);
        match locked {
            Err(error) => tracing::warn!(%error, "lock cached table for read"),
            Ok(true) => {
                self.store_cache_data(CacheData {
                    start: ts,
                    lease,
                    // Loaded asynchronously, set below.
                    mem_buffer: None,
                });
                // The load runs on its own, so a load longer than the lease
                // cannot keep staling and restarting forever.
                let table = std::sync::Arc::clone(self);
                std::thread::spawn(move || {
                    let started = std::time::Instant::now();
                    let loaded = table.load_data_from_original_table(store.as_ref());
                    tidb_metrics::server::LOAD_TABLE_CACHE_SECONDS
                        .observe(started.elapsed().as_secs_f64());
                    let (buffer, start_ts, total_size) = match loaded {
                        Ok(loaded) => loaded,
                        Err(error) => {
                            tracing::info!(%error, "load data from table fail");
                            return;
                        }
                    };
                    let mut slot = table
                        .cache_data
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(current) = slot.as_ref().filter(|current| current.start == ts) {
                        *slot = Some(std::sync::Arc::new(CacheData {
                            start: start_ts,
                            lease: current.lease,
                            mem_buffer: Some(std::sync::Arc::new(buffer)),
                        }));
                        table
                            .total_size
                            .store(total_size, std::sync::atomic::Ordering::SeqCst);
                    }
                });
            }
            // The current state does not suit caching.
            Ok(false) => {}
        }
    }

    /// Go `AddRecord` / `UpdateRecord`'s check: a table already too large
    /// refuses further writes.
    ///
    /// # Errors
    ///
    /// `ErrOptOnCacheTable` "table too large".
    pub fn check_write_size(&self) -> Result<(), KvTableError> {
        if self.total_size.load(std::sync::atomic::Ordering::SeqCst) > CACHED_TABLE_SIZE_LIMIT {
            return Err(KvTableError::Mysql(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_OPT_ON_CACHE_TABLE
                    .generate_with_stack_by_args(&["table too large".into()]),
            )));
        }
        Ok(())
    }

    /// Go `renewLease`.
    fn renew_lease(
        &self,
        mut handle: Box<dyn super::state_remote::StateRemote>,
        ts: u64,
        data: &CacheData,
        lease_duration: std::time::Duration,
    ) {
        let lease = super::state_remote::lease_from_ts(ts, lease_duration);
        let renewed = handle.renew_read_lease(self.table_id, data.lease, lease);
        self.put_state_remote_handle(handle);
        match renewed {
            Err(error) => {
                if !is_txn_retryable(&*error) {
                    tracing::warn!(%error, "Renew read lease error");
                }
            }
            Ok(new_lease) if new_lease > 0 => self.store_cache_data(CacheData {
                start: data.start,
                lease: new_lease,
                mem_buffer: data.mem_buffer.clone(),
            }),
            Ok(_) => {}
        }
    }

    /// Go `WriteLockAndKeepAlive`: takes the write lock, reports through
    /// `wg`, then renews the lease every half lease until `exit`.
    pub fn write_lock_and_keep_alive(
        &self,
        exit: &std::sync::mpsc::Receiver<()>,
        lease: &std::sync::atomic::AtomicU64,
        wg: &std::sync::mpsc::Sender<Result<(), tidb_sqlexec::SqlExecError>>,
    ) {
        let locked = self.lock_for_write();
        let failed = match locked {
            Ok(write_lock_lease) => {
                lease.store(write_lock_lease, std::sync::atomic::Ordering::SeqCst);
                let _ = wg.send(Ok(()));
                false
            }
            Err(error) => {
                tracing::warn!(category = "cached table", %error, "lock for write lock fail");
                lease.store(0, std::sync::atomic::Ordering::SeqCst);
                let _ = wg.send(Err(error));
                true
            }
        };
        if failed {
            return;
        }
        loop {
            match exit.recv_timeout(CACHE_TABLE_WRITE_LEASE / 2) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.renew(lease) {
                        tracing::warn!(category = "cached table", %error, "renew write lock lease fail");
                        return;
                    }
                }
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Go `renew`.
    fn renew(&self, lease: &std::sync::atomic::AtomicU64) -> Result<(), tidb_sqlexec::SqlExecError> {
        let old_lease = lease.load(std::sync::atomic::Ordering::SeqCst);
        let new_lease = super::state_remote::go_time_to_ts(
            super::state_remote::get_time_from_ts(old_lease)
                + super::state_remote::duration_millis(CACHE_TABLE_WRITE_LEASE),
        );
        let mut handle = self.take_state_remote_handle();
        let renewed = handle.renew_write_lease(self.table_id, new_lease);
        self.put_state_remote_handle(handle);
        if renewed? {
            lease.store(new_lease, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    /// Go `lockForWrite`.
    fn lock_for_write(&self) -> Result<u64, tidb_sqlexec::SqlExecError> {
        let mut handle = self.take_state_remote_handle();
        let locked = handle.lock_for_write(self.table_id, CACHE_TABLE_WRITE_LEASE);
        self.put_state_remote_handle(handle);
        locked
    }
}

/// Go `kv.IsTxnRetryableError`. The internal session reports its error as
/// text, so the retryable classes are told apart by their MySQL messages:
/// ErrWriteConflict and ErrTxnRetryable.
fn is_txn_retryable(error: &(dyn std::error::Error + Send + Sync)) -> bool {
    let message = error.to_string();
    message.contains("try again later") || message.contains("Write conflict")
}


#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn cache_admission_measures_the_encoded_table_image() {
        let column = crate::column::Column {
            info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("id"),
                field_type: FieldType::new(FieldTypeCode::LongLong),
                version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            }),
            generated: None,
            default_value: None,
            origin_default: None,
        };
        let mut table = TableCommon::new(42, vec![column]);
        let mut store = tidb_kv::storage::MemTableStorage::new();
        assert!(!table.cache_storage_size_exceeds(&mut store, 0).unwrap());
        table
            .insert_row(&mut store, &[Datum::Int(1)], &tidb_exprctx::NoColumns)
            .unwrap();
        assert!(table.cache_storage_size_exceeds(&mut store, 0).unwrap());
    }

    use super::super::state_remote::{lease_from_ts, CachedTableLockType, StateRemote};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// A lock meta in memory, recording each call.
    #[derive(Clone, Default)]
    struct FakeRemote {
        calls: Arc<Mutex<Vec<String>>>,
        grant_read: bool,
    }

    impl StateRemote for FakeRemote {
        fn load(&mut self, _tid: i64) -> Result<(CachedTableLockType, u64), tidb_sqlexec::SqlExecError> {
            Ok((CachedTableLockType::None, 0))
        }
        fn lock_for_read(&mut self, tid: i64, lease: u64) -> Result<bool, tidb_sqlexec::SqlExecError> {
            self.calls.lock().unwrap().push(format!("read {tid} {lease}"));
            Ok(self.grant_read)
        }
        fn lock_for_write(&mut self, tid: i64, _d: Duration) -> Result<u64, tidb_sqlexec::SqlExecError> {
            self.calls.lock().unwrap().push(format!("write {tid}"));
            Ok(1 << 40)
        }
        fn renew_read_lease(&mut self, tid: i64, old: u64, new: u64) -> Result<u64, tidb_sqlexec::SqlExecError> {
            self.calls.lock().unwrap().push(format!("renew-read {tid} {old}"));
            Ok(new)
        }
        fn renew_write_lease(&mut self, tid: i64, _new: u64) -> Result<bool, tidb_sqlexec::SqlExecError> {
            self.calls.lock().unwrap().push(format!("renew-write {tid}"));
            Ok(true)
        }
    }

    /// One table's rows at start ts 77.
    #[derive(Debug)]
    struct FakeStore(tidb_kv::storage::MemTableStorage);

    impl tidb_kv::storage::KvStorage for FakeStore {
        fn run_in_new_txn(
            &self,
            f: &mut dyn FnMut(u64, &mut dyn tidb_kv::storage::TableStorage) -> Result<(), tidb_kv::storage::StorageError>,
        ) -> Result<(), tidb_kv::storage::StorageError> {
            let mut view = self.0.clone();
            f(77, &mut view)
        }
    }

    fn wait_until(mut done: impl FnMut() -> bool) {
        let started = std::time::Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(5), "timed out");
            std::thread::yield_now();
        }
    }

    /// Go `TryReadFromCache` / `UpdateLockForRead`: nothing to read until the
    /// read lock is granted; then a loading marker; then the table's rows
    /// under the lease, and nothing at or past it.
    #[test]
    fn a_granted_read_lock_loads_the_rows_and_serves_them_under_the_lease() {
        let mut store = tidb_kv::storage::MemTableStorage::new();
        let row = Key::from_bytes(tidb_tablecodec::table_key::gen_table_record_prefix(42));
        let other = Key::from_bytes(tidb_tablecodec::table_key::gen_table_record_prefix(43));
        tidb_kv::storage::TableStorage::set(&mut store, row.clone(), b"v".to_vec()).unwrap();
        tidb_kv::storage::TableStorage::set(&mut store, other, b"x".to_vec()).unwrap();
        let store: Arc<dyn tidb_kv::storage::KvStorage> = Arc::new(FakeStore(store));
        let lease_duration = Duration::from_secs(3);
        let ts = 1000 << 18;

        let refused = Arc::new(CachedTable::new(42));
        let remote = FakeRemote::default();
        refused.init(Box::new(remote.clone()));
        refused.update_lock_for_read(Arc::clone(&store), ts, lease_duration);
        wait_until(|| remote.calls.lock().unwrap().len() == 1 && refused.token.lock().unwrap().is_some());
        assert_eq!(refused.try_read_from_cache(ts, lease_duration).0.map(|b| b.entries.len()), None);

        let table = Arc::new(CachedTable::new(42));
        let remote = FakeRemote { grant_read: true, ..FakeRemote::default() };
        table.init(Box::new(remote.clone()));
        assert_eq!(table.try_read_from_cache(ts, lease_duration).0.map(|b| b.entries.len()), None);
        table.update_lock_for_read(Arc::clone(&store), ts, lease_duration);
        let lease = lease_from_ts(ts, lease_duration);
        wait_until(|| {
            table
                .load_cache_data()
                .is_some_and(|data| data.mem_buffer.is_some())
        });
        assert_eq!(remote.calls.lock().unwrap()[..], [format!("read 42 {lease}")]);
        // The rows of table 42 alone, read at the load's start ts.
        let (rows, loading) = table.try_read_from_cache(77, lease_duration);
        assert!(!loading);
        assert_eq!(rows.unwrap().entries.keys().cloned().collect::<Vec<_>>(), [row.as_bytes().to_vec()]);
        let (before_start, loading) = table.try_read_from_cache(76, lease_duration);
        assert!(before_start.is_none() && !loading);
        assert!(table.try_read_from_cache(lease, lease_duration).0.is_none());
        assert_eq!(
            table.total_size.load(std::sync::atomic::Ordering::SeqCst),
            (row.as_bytes().len() + 1) as i64
        );
    }

    /// Go `TryReadFromCache`: inside the last half of the lease a read renews
    /// it in the background.
    #[test]
    fn a_read_near_the_lease_end_renews_it() {
        let table = Arc::new(CachedTable::new(42));
        let remote = FakeRemote::default();
        table.init(Box::new(remote.clone()));
        let lease_duration = Duration::from_secs(3);
        let lease = 10_000 << 18;
        table.store_cache_data(CacheData {
            start: 1,
            lease,
            mem_buffer: Some(Arc::new(tidb_kv::mem_buffer::MemBufferSnapshot::default())),
        });
        // Far from the end: no renewal.
        let early = (10_000 - 2_000) << 18;
        assert!(table.try_read_from_cache(early, lease_duration).0.is_some());
        let late = (10_000 - 1_000) << 18;
        assert!(table.try_read_from_cache(late, lease_duration).0.is_some());
        wait_until(|| table.load_cache_data().is_some_and(|data| data.lease != lease));
        assert_eq!(remote.calls.lock().unwrap()[..], [format!("renew-read 42 {lease}")]);
        assert_eq!(table.load_cache_data().unwrap().lease, lease_from_ts(late, lease_duration));
    }

    /// Go `WriteLockAndKeepAlive`: the lease is published before `wg` is
    /// answered, and `exit` ends the keep-alive.
    #[test]
    fn the_write_lock_publishes_its_lease_and_stops_on_exit() {
        let table = Arc::new(CachedTable::new(42));
        let remote = FakeRemote::default();
        table.init(Box::new(remote.clone()));
        let lease = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (exit_tx, exit_rx) = std::sync::mpsc::channel();
        let (wg_tx, wg_rx) = std::sync::mpsc::channel();
        let keeper = {
            let (table, lease) = (Arc::clone(&table), Arc::clone(&lease));
            std::thread::spawn(move || table.write_lock_and_keep_alive(&exit_rx, &lease, &wg_tx))
        };
        wg_rx.recv().unwrap().unwrap();
        assert_eq!(lease.load(std::sync::atomic::Ordering::SeqCst), 1 << 40);
        drop(exit_tx);
        keeper.join().unwrap();
        assert_eq!(remote.calls.lock().unwrap()[..], ["write 42".to_owned()]);
    }

    /// Go `AddRecord` / `UpdateRecord`: a cached table past the size limit
    /// refuses writes.
    #[test]
    fn an_oversized_cached_table_refuses_writes() {
        let table = CachedTable::new(42);
        table.check_write_size().unwrap();
        table
            .total_size
            .store(CACHED_TABLE_SIZE_LIMIT + 1, std::sync::atomic::Ordering::SeqCst);
        assert!(table.check_write_size().is_err());
    }
}
