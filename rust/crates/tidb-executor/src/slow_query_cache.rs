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

//! Go `executor/slow_query_cache.go`: the process-wide plaintext cache of
//! slow-query SQL that `tidb_redact_log` hid from the slow log, read back
//! into `SLOW_QUERY.In_memory_original_query`.
//!
//! Go's `diagnosticmode` checks are absent: this node has no diagnostic
//! mode, so `diagnosticmode.Enabled()` is always false here. Go's lookup
//! takes the statement's `context.Context`; the slow-log reader has none,
//! so a lookup waits for the worker's answer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use crossbeam_channel::{bounded, select, Receiver, Sender, TryRecvError};

/// Go `slowQueryCacheCapacity`: the number of entries retained by the cache.
/// The backing array is allocated when the cache is initialized. The slice
/// grows until this capacity is reached, then writes reuse its oldest slot.
pub(crate) const SLOW_QUERY_CACHE_CAPACITY: usize = 50000;
/// Go `slowQueryCacheMaxMem`: the budget for entries the worker has
/// retained. Plaintext waiting on the write channel is not included. Once
/// appending an entry would pass the budget, the unused slots are abandoned
/// and the ring stays frozen. A later statement can reuse an existing slot;
/// the replaced entry's bytes are released when that write lands. The index
/// map and the preallocated backing array are not counted.
const SLOW_QUERY_CACHE_MAX_MEM: i64 = 50 * 1024 * 1024;
const SLOW_QUERY_CACHE_WRITE_CHAN_SIZE: usize = 10;
const SLOW_QUERY_CACHE_READ_CHAN_SIZE: usize = 100;

/// Go `slowQueryKey`: identifies one slow query in the plaintext cache.
/// Digest is shortened to 10 characters. The three fields are the same ones
/// written to the slow log, so a later read can find the row again.
#[derive(Clone, Debug, Default)]
pub(crate) struct SlowQueryKey {
    txn_start_ts: u64,
    query_time: f64,
    digest: String,
}

// Go compares the key's float with `==`; query times are finite and
// non-negative, where the bit pattern decides equality the same way.
impl PartialEq for SlowQueryKey {
    fn eq(&self, other: &Self) -> bool {
        self.txn_start_ts == other.txn_start_ts
            && self.query_time.to_bits() == other.query_time.to_bits()
            && self.digest == other.digest
    }
}

impl Eq for SlowQueryKey {}

impl std::hash::Hash for SlowQueryKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.txn_start_ts.hash(state);
        self.query_time.to_bits().hash(state);
        self.digest.hash(state);
    }
}

/// Go `slowQuery`: one cached slow query. Only the unredacted SQL is
/// retained; the key is kept so the slot can be removed from the index when
/// it is reused. Every element of `slow_queries` is occupied. A slot that no
/// longer holds its original key is identified by comparing `key` with the
/// lookup key.
struct SlowQuery {
    key: SlowQueryKey,
    unredacted_sql: String,
}

/// Go `slowQueryCacheWriteTask`.
pub(crate) struct SlowQueryCacheWriteTask {
    key: SlowQueryKey,
    sql: String,
}

/// Go `slowQueryCacheReadTask`: one batch lookup. The worker fills the
/// values and sends them back on `done`.
struct SlowQueryCacheReadTask {
    keys: Vec<SlowQueryKey>,
    done: Sender<Vec<String>>,
}

/// The ring and its index, which only the worker touches (Go keeps them on
/// `slowQueryCache` itself, owned by its one goroutine).
struct SlowQueryCacheState {
    slow_queries: Vec<SlowQuery>,
    /// Go `cap(slowQueries)`.
    capacity: usize,
    write_pos: usize,
    /// Stays set after the first insert that cannot be appended. A later
    /// shorter replacement may free memory, but growing again would leave
    /// `write_pos` behind the chronological order of the slots.
    frozen: bool,
    locations: HashMap<SlowQueryKey, usize>,
    memory_usage: i64,
    max_mem_usage: i64,
}

/// Go `slowQueryCache`: owned by one background thread. Callers only send
/// tasks. `memory_usage` is updated only by that thread. A producer drops a
/// statement when the write channel is full, without consulting the memory
/// budget. Readers block until answered. TiDB starts the worker in
/// `Server.Run`.
pub(crate) struct SlowQueryCache {
    write_tx: Sender<SlowQueryCacheWriteTask>,
    write_rx: Receiver<SlowQueryCacheWriteTask>,
    read_tx: Sender<SlowQueryCacheReadTask>,
    read_rx: Receiver<SlowQueryCacheReadTask>,
    running: AtomicBool,
    /// Go `stopCh` / `exited`: dropping the sender stops the worker, and
    /// joining the handle waits for it.
    worker: Mutex<Option<(Sender<()>, std::thread::JoinHandle<()>)>>,
    state: Arc<Mutex<SlowQueryCacheState>>,
}

static GLOBAL_SLOW_QUERY_CACHE: LazyLock<SlowQueryCache> = LazyLock::new(new_slow_query_cache);

/// Go `newSlowQueryCache`.
fn new_slow_query_cache() -> SlowQueryCache {
    new_slow_query_cache_with_limits(
        SLOW_QUERY_CACHE_CAPACITY,
        SLOW_QUERY_CACHE_MAX_MEM,
        SLOW_QUERY_CACHE_WRITE_CHAN_SIZE,
    )
}

/// Go `newSlowQueryCacheWithLimits`.
fn new_slow_query_cache_with_limits(capacity: usize, max_mem: i64, write_chan_size: usize) -> SlowQueryCache {
    let (write_tx, write_rx) = bounded(write_chan_size);
    let (read_tx, read_rx) = bounded(SLOW_QUERY_CACHE_READ_CHAN_SIZE);
    SlowQueryCache {
        write_tx,
        write_rx,
        read_tx,
        read_rx,
        running: AtomicBool::new(false),
        worker: Mutex::new(None),
        state: Arc::new(Mutex::new(SlowQueryCacheState {
            slow_queries: Vec::with_capacity(capacity),
            capacity,
            write_pos: 0,
            frozen: false,
            locations: HashMap::new(),
            memory_usage: 0,
            max_mem_usage: max_mem,
        })),
    }
}

/// Go `StartSlowQueryCache`: starts the process-wide worker that retains
/// plaintext slow-query SQL. `Server.Run` calls it before accepting
/// connections, whether or not `tidb_redact_log` is enabled. That setting
/// only decides whether a slow query is inserted. Lookups always read the
/// cache. Extra calls do not start another worker.
pub fn start_slow_query_cache() {
    GLOBAL_SLOW_QUERY_CACHE.start();
}

/// Go `globalSlowQueryCache.putIntoCache`.
pub(crate) fn put_into_global_cache(key: SlowQueryKey, sql: String) {
    GLOBAL_SLOW_QUERY_CACHE.put_into_cache(key, sql);
}

/// Go `globalSlowQueryCache.getUnredactedQueries`.
pub(crate) fn global_unredacted_queries(keys: Vec<SlowQueryKey>) -> Option<Vec<String>> {
    GLOBAL_SLOW_QUERY_CACHE.get_unredacted_queries(keys)
}

impl SlowQueryCache {
    fn lock(&self) -> std::sync::MutexGuard<'_, SlowQueryCacheState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn start(&self) {
        if self.check_running() {
            return;
        }
        let (stop_tx, stop_rx) = bounded::<()>(0);
        let (state, read_rx, write_rx) = (Arc::clone(&self.state), self.read_rx.clone(), self.write_rx.clone());
        let handle = std::thread::Builder::new()
            .name("slow-query-cache".into())
            .spawn(move || run(&state, &read_rx, &write_rx, &stop_rx))
            .expect("spawn the slow-query cache worker");
        *self.worker.lock().unwrap_or_else(PoisonError::into_inner) = Some((stop_tx, handle));
        self.running.store(true, Ordering::SeqCst);
    }

    /// Go `stop`: closes the worker and waits until it returns. Call it only
    /// after this cache's sends have finished.
    fn stop(&self) {
        if !self.check_running() {
            return;
        }
        let worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((stop_tx, handle)) = worker {
            drop(stop_tx);
            let _ = handle.join();
        }
        self.running.store(false, Ordering::SeqCst);
    }

    fn check_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    fn put_into_cache(&self, key: SlowQueryKey, sql: String) {
        if sql.is_empty() || !self.check_running() {
            return;
        }
        self.try_to_cache(SlowQueryCacheWriteTask { key, sql });
    }

    /// Go `tryToCache`: whether the write was queued. A full write channel
    /// drops the statement. The memory budget is applied later, by the
    /// worker.
    fn try_to_cache(&self, task: SlowQueryCacheWriteTask) -> bool {
        if task.sql.is_empty() {
            return false;
        }
        self.write_tx.try_send(task).is_ok()
    }

    /// Go `getUnredactedQueries`: one value per key from the worker, or
    /// `None` without a worker (Go's nil result).
    fn get_unredacted_queries(&self, keys: Vec<SlowQueryKey>) -> Option<Vec<String>> {
        if keys.is_empty() || !self.check_running() {
            return None;
        }
        let len = keys.len();
        let (done, values) = bounded(1);
        self.read_tx.send(SlowQueryCacheReadTask { keys, done }).ok()?;
        // A worker that panicked mid-read restarts; Go's reader then sees the
        // zero values its task still held.
        Some(values.recv().unwrap_or_else(|_| vec![String::new(); len]))
    }
}

/// Go `run`: one thread owns the ring and the map. It restarts the loop
/// after a recovered panic so a reader waiting on a task cannot block
/// forever. A dropped stop sender ends the worker instead of restarting it.
fn run(
    state: &Mutex<SlowQueryCacheState>,
    read_rx: &Receiver<SlowQueryCacheReadTask>,
    write_rx: &Receiver<SlowQueryCacheWriteTask>,
    stop_rx: &Receiver<()>,
) {
    loop {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_loop(state, read_rx, write_rx, stop_rx);
        }));
        if matches!(stop_rx.try_recv(), Err(TryRecvError::Disconnected)) {
            return;
        }
    }
}

/// Go `loop`.
fn run_loop(
    state: &Mutex<SlowQueryCacheState>,
    read_rx: &Receiver<SlowQueryCacheReadTask>,
    write_rx: &Receiver<SlowQueryCacheWriteTask>,
    stop_rx: &Receiver<()>,
) {
    let lock = || state.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        // Reads already waiting go first. Writes stay queued until the read
        // queue is empty.
        if let Ok(task) = read_rx.try_recv() {
            lock().handle_read(task);
            continue;
        }
        select! {
            recv(stop_rx) -> _ => return,
            recv(read_rx) -> task => {
                if let Ok(task) = task {
                    lock().handle_read(task);
                }
            }
            recv(write_rx) -> task => {
                // Priority of read task is higher.
                if let Ok(read_task) = read_rx.try_recv() {
                    lock().handle_read(read_task);
                }
                if let Ok(task) = task {
                    lock().handle_write(task);
                }
            }
        }
    }
}

impl SlowQueryCacheState {
    fn handle_read(&self, task: SlowQueryCacheReadTask) {
        let values = self.lookup(&task.keys);
        let _ = task.done.send(values);
    }

    /// Go `handleRead`'s fill: one value per key, empty when the key's slot
    /// is gone.
    fn lookup(&self, keys: &[SlowQueryKey]) -> Vec<String> {
        keys.iter()
            .map(|key| {
                self.locations
                    .get(key)
                    .and_then(|&index| self.slow_queries.get(index))
                    .filter(|query| query.key == *key)
                    .map_or_else(String::new, |query| query.unredacted_sql.clone())
            })
            .collect()
    }

    fn handle_write(&mut self, task: SlowQueryCacheWriteTask) {
        let entry_mem_usage = get_slow_query_value_mem_usage(&task.key, &task.sql);
        if task.sql.is_empty() || entry_mem_usage > self.max_mem_usage {
            return;
        }

        if !self.frozen
            && self.slow_queries.len() < self.capacity
            && self.memory_usage + entry_mem_usage <= self.max_mem_usage
        {
            self.memory_usage += entry_mem_usage;
            self.append_slow_query(task.key, task.sql);
            return;
        }

        // Nothing has been stored, or this cache has no slots. An empty ring
        // is not frozen, so a later smaller SQL can still become the first
        // entry.
        if self.slow_queries.is_empty() {
            return;
        }
        // Stay frozen from here, including when this replacement is
        // discarded. A later SQL that fits in the remaining budget reuses
        // `write_pos` instead of appending.
        self.frozen = true;

        let delta = self.get_replacement_delta(self.write_pos, &task.key, &task.sql);
        if self.memory_usage + delta > self.max_mem_usage {
            return;
        }
        self.memory_usage += delta;
        self.overwrite(task.key, task.sql);
    }

    fn append_slow_query(&mut self, key: SlowQueryKey, sql: String) {
        // A repeated key leaves the older slot in place until the ring reuses
        // it. The map always refers to the latest insert and is not part of
        // `memory_usage`. `handle_write` has already charged this statement.
        self.locations.insert(key.clone(), self.slow_queries.len());
        self.slow_queries.push(SlowQuery { key, unredacted_sql: sql });
    }

    fn overwrite(&mut self, key: SlowQueryKey, sql: String) {
        let index = self.write_pos;
        // A duplicate key keeps the older copy until this slot is reused.
        // Drop the index entry only when it still names this slot.
        let old_key = &self.slow_queries[index].key;
        if self.locations.get(old_key) == Some(&index) {
            self.locations.remove(old_key);
        }
        self.locations.insert(key.clone(), index);
        self.slow_queries[index] = SlowQuery { key, unredacted_sql: sql };
        self.advance_write_pos();
    }

    /// Go `getReplacementDelta`: the retained memory change when a slot is
    /// overwritten. The index map is not included.
    fn get_replacement_delta(&self, index: usize, key: &SlowQueryKey, sql: &str) -> i64 {
        let old = &self.slow_queries[index];
        get_slow_query_value_mem_usage(key, sql) - get_slow_query_value_mem_usage(&old.key, &old.unredacted_sql)
    }

    fn advance_write_pos(&mut self) {
        self.write_pos += 1;
        if self.write_pos >= self.slow_queries.len() {
            self.write_pos = 0;
        }
    }
}

/// Go `slowQueryStructSize`, `unsafe.Sizeof(slowQuery{})` on a 64-bit Go
/// build: the key's `uint64`, `float64` and digest string header, and the
/// SQL's string header.
const SLOW_QUERY_STRUCT_SIZE: i64 = 48;

/// Go `getSlowQueryValueMemUsage`: one retained slow query -- the struct,
/// the SQL bytes and the key's digest bytes.
fn get_slow_query_value_mem_usage(key: &SlowQueryKey, sql: &str) -> i64 {
    SLOW_QUERY_STRUCT_SIZE + sql.len() as i64 + key.digest.len() as i64
}

/// Go `slowQueryKeyOf`.
pub(crate) fn slow_query_key_of(txn_start_ts: u64, query_time: f64, digest: &str) -> SlowQueryKey {
    SlowQueryKey {
        txn_start_ts,
        query_time,
        digest: digest.get(..10).unwrap_or(digest).to_owned(),
    }
}

/// Go `slowQueryTimeKey`: a duration as the `Query_time` value written in
/// the slow log, so a lookup parsed back from that field hits the same key.
/// The log formats seconds with `strconv.FormatFloat` precision -1, which
/// parses back to the same `f64`.
pub(crate) fn slow_query_time_key(query_time: Duration) -> f64 {
    query_time.as_secs_f64()
}

/// Go `updateSlowQueryKey`: the key fields one parsed slow-log field sets.
pub(crate) fn update_slow_query_key(key: &mut SlowQueryKey, field: &str, value: &str) {
    match field {
        tidb_variable::slow_log::SLOW_LOG_TXN_START_TS_STR => {
            if let Ok(txn_start_ts) = value.parse::<u64>() {
                key.txn_start_ts = txn_start_ts;
            }
        }
        tidb_variable::slow_log::SLOW_LOG_QUERY_TIME_STR => {
            if let Ok(query_time) = value.parse::<f64>() {
                key.query_time = query_time;
            }
        }
        tidb_variable::slow_log::SLOW_LOG_DIGEST_STR => {
            key.digest = value.get(..10).unwrap_or(value).to_owned();
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    // Go's `TestSlowQueryCacheDiagnosticModeDoesNotStartOrEnqueue` and
    // `TestSlowQueryCacheLookupRespectsContext` have no counterpart: this
    // node has no diagnostic mode, and the lookup takes no context.

    fn handle_write(cache: &SlowQueryCache, key: &SlowQueryKey, sql: &str) {
        cache.lock().handle_write(SlowQueryCacheWriteTask { key: key.clone(), sql: sql.to_owned() });
    }

    fn task(key: &SlowQueryKey, sql: &str) -> SlowQueryCacheWriteTask {
        SlowQueryCacheWriteTask { key: key.clone(), sql: sql.to_owned() }
    }

    /// Go `lookupSlowQueryCache`.
    fn lookup_slow_query_cache(cache: &SlowQueryCache, keys: &[&SlowQueryKey]) -> Vec<String> {
        let keys: Vec<SlowQueryKey> = keys.iter().map(|key| (*key).clone()).collect();
        cache.lock().lookup(&keys)
    }

    /// Go `startSlowQueryCacheForTest`: `Cleanup` stops the worker.
    struct Started<'a>(&'a SlowQueryCache);

    impl Drop for Started<'_> {
        fn drop(&mut self) {
            self.0.stop();
        }
    }

    fn start_slow_query_cache_for_test(cache: &SlowQueryCache) -> Started<'_> {
        cache.start();
        Started(cache)
    }

    /// Go `require.Eventually(cond, time.Second, 5*time.Millisecond)`.
    fn eventually(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while !condition() {
            assert!(Instant::now() < deadline, "condition never held");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn memory_usage(cache: &SlowQueryCache) -> i64 {
        cache.lock().memory_usage
    }

    #[test]
    fn slow_query_cache_read_write() {
        let cache = new_slow_query_cache();
        let key = slow_query_key_of(123, 1.25, "0123456789abcdef");
        handle_write(&cache, &key, "select 123;");

        let values = lookup_slow_query_cache(&cache, &[&key, &slow_query_key_of(456, 2.5, "another")]);
        assert_eq!(values, ["select 123;", ""]);
        assert_eq!(key.digest, "0123456789");
    }

    #[test]
    fn slow_query_cache_duplicate_key_uses_latest_value() {
        let cache = new_slow_query_cache();
        let key = slow_query_key_of(123, 1.25, "0123456789abcdef");
        handle_write(&cache, &key, "select 1;");
        handle_write(&cache, &key, "select 2;");

        assert_eq!(lookup_slow_query_cache(&cache, &[&key]), ["select 2;"]);
        // Both values stay. The index map is not charged.
        assert_eq!(
            memory_usage(&cache),
            get_slow_query_value_mem_usage(&key, "select 1;") + get_slow_query_value_mem_usage(&key, "select 2;")
        );
    }

    #[test]
    fn slow_query_cache_uses_preallocated_ring() {
        let cache = new_slow_query_cache();
        {
            let state = cache.lock();
            assert_eq!(state.slow_queries.len(), 0);
            assert_eq!(state.capacity, SLOW_QUERY_CACHE_CAPACITY);
            assert!(state.slow_queries.capacity() >= SLOW_QUERY_CACHE_CAPACITY);
            assert_eq!(state.write_pos, 0);
        }
        assert_eq!(cache.write_tx.capacity(), Some(10));
        for i in 0..SLOW_QUERY_CACHE_CAPACITY {
            let key = slow_query_key_of(i as u64, 1.0, &i.to_string());
            handle_write(&cache, &key, "x");
        }

        assert_eq!(cache.lock().slow_queries.len(), SLOW_QUERY_CACHE_CAPACITY);
        assert_eq!(cache.lock().write_pos, 0);
        assert_eq!(lookup_slow_query_cache(&cache, &[&slow_query_key_of(0, 1.0, "0")])[0], "x");

        let new_key = slow_query_key_of(
            SLOW_QUERY_CACHE_CAPACITY as u64,
            1.0,
            &SLOW_QUERY_CACHE_CAPACITY.to_string(),
        );
        handle_write(&cache, &new_key, "new");
        assert_eq!(lookup_slow_query_cache(&cache, &[&slow_query_key_of(0, 1.0, "0")])[0], "");
        assert_eq!(lookup_slow_query_cache(&cache, &[&new_key])[0], "new");
        assert_eq!(cache.lock().write_pos, 1);
    }

    #[test]
    fn slow_query_cache_evicts_oldest_and_freezes_ring() {
        let first = slow_query_key_of(1, 1.0, "first-key");
        let second = slow_query_key_of(2, 1.0, "second-key");
        let third = slow_query_key_of(3, 1.0, "third-key");
        let dropped_key = slow_query_key_of(4, 1.0, "dropped");
        let (first_sql, second_sql, third_sql) = ("12345", "12345", "123456");
        let dropped_sql = "123456789012";
        let two_entries =
            get_slow_query_value_mem_usage(&first, first_sql) + get_slow_query_value_mem_usage(&second, second_sql);
        let after_third =
            get_slow_query_value_mem_usage(&second, second_sql) + get_slow_query_value_mem_usage(&third, third_sql);
        let after_dropped = get_slow_query_value_mem_usage(&third, third_sql)
            + get_slow_query_value_mem_usage(&dropped_key, dropped_sql);
        assert!(after_dropped > after_third);

        let cache = new_slow_query_cache_with_limits(4, after_third, 8);
        handle_write(&cache, &first, first_sql);
        handle_write(&cache, &second, second_sql);
        assert_eq!(memory_usage(&cache), two_entries);

        handle_write(&cache, &third, third_sql);
        assert_eq!(memory_usage(&cache), after_third);
        // The two unused slots are abandoned once the memory budget is reached.
        assert_eq!(cache.lock().slow_queries.len(), 2);
        assert_eq!(cache.lock().capacity, 4);
        assert!(cache.lock().frozen);
        assert_eq!(lookup_slow_query_cache(&cache, &[&first, &second, &third]), ["", second_sql, third_sql]);

        // Replacing the next slot would pass the frozen ceiling, so the SQL
        // is discarded.
        let before = memory_usage(&cache);
        let pos = cache.lock().write_pos;
        handle_write(&cache, &dropped_key, dropped_sql);
        assert_eq!(memory_usage(&cache), before);
        assert_eq!(cache.lock().write_pos, pos);
        assert!(cache.lock().frozen);
        assert_eq!(lookup_slow_query_cache(&cache, &[&second]), [second_sql]);

        // Capacity 4, budget 300, one-byte digests, SQL lengths 100, 100, 1,
        // 1, 1, 1, 1 for A through G. C replaces A and frees enough memory
        // that D would fit as an append. Growing there leaves write_pos on
        // the oldest slot while newer slots sit behind it, so G would evict D
        // and keep the older C.
        const SHORT_BUDGET: i64 = 300;
        let short_cache = new_slow_query_cache_with_limits(4, SHORT_BUDGET, 8);
        let a = "a".repeat(100);
        let b = "b".repeat(100);
        let sqls = [a.as_str(), b.as_str(), "c", "d", "e", "f", "g"];
        let keys: Vec<SlowQueryKey> = (0..sqls.len())
            .map(|i| slow_query_key_of(i as u64 + 1, 1.0, &char::from(b'A' + i as u8).to_string()))
            .collect();
        let mem: Vec<i64> = keys.iter().zip(sqls).map(|(key, sql)| get_slow_query_value_mem_usage(key, sql)).collect();
        for key in &keys {
            assert_eq!(key.digest.len(), 1);
        }
        assert!(mem[0] + mem[1] <= SHORT_BUDGET);
        assert!(mem[0] + mem[1] + mem[2] > SHORT_BUDGET);
        assert!(mem[1] + mem[2] + mem[3] <= SHORT_BUDGET);

        let write_at = |i: usize| handle_write(&short_cache, &keys[i], sqls[i]);
        write_at(0);
        write_at(1);
        assert!(!short_cache.lock().frozen);
        assert_eq!(short_cache.lock().slow_queries.len(), 2);
        assert_eq!(short_cache.lock().write_pos, 0);
        assert_eq!(lookup_slow_query_cache(&short_cache, &[&keys[0], &keys[1]]), [sqls[0], sqls[1]]);

        write_at(2);
        assert!(short_cache.lock().frozen);
        assert_eq!(short_cache.lock().slow_queries.len(), 2);
        assert_eq!(short_cache.lock().capacity, 4);
        assert_eq!(short_cache.lock().write_pos, 1);
        assert_eq!(memory_usage(&short_cache), mem[1] + mem[2]);
        assert_eq!(
            lookup_slow_query_cache(&short_cache, &[&keys[0], &keys[1], &keys[2]]),
            ["", sqls[1], sqls[2]]
        );

        for i in 3..keys.len() {
            write_at(i);
            assert!(short_cache.lock().frozen);
            assert_eq!(short_cache.lock().slow_queries.len(), 2);
            assert!(memory_usage(&short_cache) <= SHORT_BUDGET);
        }
        assert_eq!(short_cache.lock().write_pos, 1);
        assert_eq!(memory_usage(&short_cache), mem[5] + mem[6]);
        let all: Vec<&SlowQueryKey> = keys.iter().collect();
        assert_eq!(lookup_slow_query_cache(&short_cache, &all), ["", "", "", "", "", sqls[5], sqls[6]]);
    }

    #[test]
    fn slow_query_cache_duplicate_key_survives_eviction_of_older_copy() {
        let key = slow_query_key_of(1, 1.0, "0123456789zzz");
        let other = slow_query_key_of(2, 1.0, "other-digest");
        let (older_sql, latest_sql, other_sql) = ("0123456789", "abcdefghij", "12345");
        let two_copies =
            get_slow_query_value_mem_usage(&key, older_sql) + get_slow_query_value_mem_usage(&key, latest_sql);
        let after_other =
            get_slow_query_value_mem_usage(&key, latest_sql) + get_slow_query_value_mem_usage(&other, other_sql);
        let cache = new_slow_query_cache_with_limits(4, two_copies, 8);
        handle_write(&cache, &key, older_sql);
        handle_write(&cache, &key, latest_sql);
        handle_write(&cache, &other, other_sql);

        assert_eq!(lookup_slow_query_cache(&cache, &[&key, &other]), [latest_sql, other_sql]);
        assert_eq!(memory_usage(&cache), after_other);

        let replacement = slow_query_key_of(3, 1.0, "replacement");
        handle_write(&cache, &replacement, other_sql);
        assert_eq!(lookup_slow_query_cache(&cache, &[&key, &other, &replacement]), ["", other_sql, other_sql]);
        assert_eq!(
            memory_usage(&cache),
            get_slow_query_value_mem_usage(&other, other_sql) + get_slow_query_value_mem_usage(&replacement, other_sql)
        );
    }

    #[test]
    fn slow_query_cache_drops_oversized_and_unqueued_sql() {
        let fits_key = slow_query_key_of(2, 1.0, "fits");
        let fits_sql = "12345";
        let fits_mem = get_slow_query_value_mem_usage(&fits_key, fits_sql);
        let huge_key = slow_query_key_of(1, 1.0, "huge");
        let huge_sql = "a".repeat(11);
        assert!(get_slow_query_value_mem_usage(&huge_key, &huge_sql) > fits_mem);

        let cache = new_slow_query_cache_with_limits(4, fits_mem, 2);
        handle_write(&cache, &huge_key, &huge_sql);
        assert_eq!(memory_usage(&cache), 0);
        assert_eq!(cache.lock().write_pos, 0);

        handle_write(&cache, &fits_key, fits_sql);
        assert_eq!(memory_usage(&cache), fits_mem);

        // Retained bytes already fill the budget. The producer still queues
        // both statements and leaves memory_usage unchanged.
        assert!(cache.try_to_cache(task(&slow_query_key_of(3, 1.0, "huge"), &"a".repeat(17))));
        assert!(cache.try_to_cache(task(&slow_query_key_of(4, 1.0, "queued"), "one")));
        assert_eq!(memory_usage(&cache), fits_mem);
        assert_eq!(cache.write_tx.len(), 2);

        // The queue is full, so the next statement is dropped. Queuing does
        // not charge memory_usage.
        let roomy = new_slow_query_cache_with_limits(4, fits_mem * 4, 1);
        let queued_key = slow_query_key_of(5, 1.0, "queued");
        assert!(roomy.try_to_cache(task(&queued_key, "12345")));
        assert_eq!(memory_usage(&roomy), 0);
        assert!(!roomy.try_to_cache(task(&slow_query_key_of(6, 1.0, "full"), "12345")));
        assert_eq!(memory_usage(&roomy), 0);
        assert_eq!(roomy.write_tx.len(), 1);
    }

    #[test]
    fn slow_query_cache_accounts_memory_on_worker() {
        let first = slow_query_key_of(1, 1.0, "queued");
        let second = slow_query_key_of(2, 1.0, "also-queued");
        let first_sql = "a".repeat(10);
        let second_sql = "b".repeat(10);
        let first_mem = get_slow_query_value_mem_usage(&first, &first_sql);
        let second_mem = get_slow_query_value_mem_usage(&second, &second_sql);
        let cache = new_slow_query_cache_with_limits(4, first_mem + second_mem, 8);
        assert!(cache.try_to_cache(task(&first, &first_sql)));
        assert!(cache.try_to_cache(task(&second, &second_sql)));
        assert_eq!(memory_usage(&cache), 0);
        assert_eq!(cache.write_tx.len(), 2);

        let _started = start_slow_query_cache_for_test(&cache);
        eventually(|| {
            let got = cache.get_unredacted_queries(vec![first.clone(), second.clone()]);
            cache.write_tx.is_empty() && got.as_deref() == Some(&[first_sql.clone(), second_sql.clone()][..])
        });
        assert_eq!(memory_usage(&cache), first_mem + second_mem);

        // The budget is full. The producer still queues, and the worker
        // replaces the oldest slot because the new SQL is smaller.
        let extra_key = slow_query_key_of(3, 1.0, "extra");
        let extra_sql = "x";
        assert!(cache.try_to_cache(task(&extra_key, extra_sql)));
        eventually(|| {
            let got = cache.get_unredacted_queries(vec![first.clone(), second.clone(), extra_key.clone()]);
            cache.write_tx.is_empty()
                && got.as_deref() == Some(&[String::new(), second_sql.clone(), extra_sql.to_owned()][..])
        });
        assert_eq!(memory_usage(&cache), second_mem + get_slow_query_value_mem_usage(&extra_key, extra_sql));
        assert!(cache.lock().frozen);

        // One statement larger than the whole budget is queued, then
        // discarded by the worker. It leaves memory unchanged.
        let dropped_key = slow_query_key_of(4, 1.0, "dropped");
        let dropped_sql = "c".repeat(12);
        assert!(get_slow_query_value_mem_usage(&dropped_key, &dropped_sql) > first_mem);
        let dropped = new_slow_query_cache_with_limits(4, first_mem, 4);
        assert!(dropped.try_to_cache(task(&dropped_key, &dropped_sql)));
        assert_eq!(dropped.write_tx.len(), 1);
        assert_eq!(memory_usage(&dropped), 0);
        let _dropped_started = start_slow_query_cache_for_test(&dropped);
        eventually(|| {
            let got = dropped.get_unredacted_queries(vec![dropped_key.clone()]);
            dropped.write_tx.is_empty() && got.as_deref() == Some(&[String::new()][..]) && memory_usage(&dropped) == 0
        });

        // Capacity evicts the older statement. Bytes are charged only when
        // the worker stores a slot, then released when that slot is
        // overwritten.
        let older = slow_query_key_of(5, 1.0, "older");
        let newer = slow_query_key_of(6, 1.0, "newer");
        let (older_sql, newer_sql) = ("older-sql", "newer-sql");
        let older_mem = get_slow_query_value_mem_usage(&older, older_sql);
        let newer_mem = get_slow_query_value_mem_usage(&newer, newer_sql);
        let ring = new_slow_query_cache_with_limits(1, older_mem + newer_mem, 4);
        assert!(ring.try_to_cache(task(&older, older_sql)));
        assert!(ring.try_to_cache(task(&newer, newer_sql)));
        assert_eq!(memory_usage(&ring), 0);
        let _ring_started = start_slow_query_cache_for_test(&ring);
        eventually(|| {
            let got = ring.get_unredacted_queries(vec![older.clone(), newer.clone()]);
            ring.write_tx.is_empty() && got.as_deref() == Some(&[String::new(), newer_sql.to_owned()][..])
        });
        assert_eq!(memory_usage(&ring), newer_mem);
    }

    #[test]
    fn slow_query_cache_async_read_write() {
        let cache = new_slow_query_cache();
        let _started = start_slow_query_cache_for_test(&cache);
        // An empty cache still answers through the worker: one empty string
        // per key.
        let missing = slow_query_key_of(1, 1.0, "missing");
        assert_eq!(cache.get_unredacted_queries(vec![missing]), Some(vec![String::new()]));
        assert!(cache.read_tx.is_empty());

        let key = slow_query_key_of(7, 1.5, "0123456789abcdef");
        cache.put_into_cache(key.clone(), "select 1;".to_owned());
        eventually(|| cache.get_unredacted_queries(vec![key.clone()]) == Some(vec!["select 1;".to_owned()]));
    }

    #[test]
    fn slow_query_cache_entrypoints_without_worker() {
        let cache = new_slow_query_cache();
        let key = slow_query_key_of(1, 1.0, "missing");
        cache.put_into_cache(key.clone(), "select 1;".to_owned());
        assert!(!cache.check_running());
        assert!(cache.write_tx.is_empty());

        assert_eq!(cache.get_unredacted_queries(vec![key]), None);
        assert!(!cache.check_running());
        assert!(cache.read_tx.is_empty());
    }

    #[test]
    fn slow_query_cache_stop_joins_worker() {
        let cache = new_slow_query_cache_with_limits(2, 1024, 1);
        let _started = start_slow_query_cache_for_test(&cache);
        assert!(cache.check_running());

        cache.stop();
        assert!(!cache.check_running());
        // Cleanup calls stop again after the test body restarts the worker.
        cache.stop();
        cache.start();
        assert!(cache.check_running());
    }

    #[test]
    fn slow_query_time_key_matches_slow_log_format() {
        let query_time = Duration::from_nanos(123_456_789);
        let formatted = tidb_execdetails::exec_details::format_seconds(query_time);
        let parsed: f64 = formatted.parse().unwrap();
        assert_eq!(parsed.to_bits(), slow_query_time_key(query_time).to_bits());
    }
}
