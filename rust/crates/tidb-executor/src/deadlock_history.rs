// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Recent deadlock history from `pkg/util/deadlockhistory`.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use tidb_datatype::{Collation, Datum, StringDatum, Time, TimeType, MAX_FSP};
use tidb_txnkv::decode_resource_group_tag;
use tidb_txnkv::transaction::DeadlockDetail;

/// `INFORMATION_SCHEMA.DEADLOCKS.DEADLOCK_ID`.
pub const COL_DEADLOCK_ID: &str = "DEADLOCK_ID";
/// `INFORMATION_SCHEMA.DEADLOCKS.OCCUR_TIME`.
pub const COL_OCCUR_TIME: &str = "OCCUR_TIME";
/// `INFORMATION_SCHEMA.DEADLOCKS.RETRYABLE`.
pub const COL_RETRYABLE: &str = "RETRYABLE";
/// `INFORMATION_SCHEMA.DEADLOCKS.TRY_LOCK_TRX_ID`.
pub const COL_TRY_LOCK_TRX_ID: &str = "TRY_LOCK_TRX_ID";
/// `INFORMATION_SCHEMA.DEADLOCKS.CURRENT_SQL_DIGEST`.
pub const COL_CURRENT_SQL_DIGEST: &str = "CURRENT_SQL_DIGEST";
/// `INFORMATION_SCHEMA.DEADLOCKS.CURRENT_SQL_DIGEST_TEXT`.
pub const COL_CURRENT_SQL_DIGEST_TEXT: &str = "CURRENT_SQL_DIGEST_TEXT";
/// `INFORMATION_SCHEMA.DEADLOCKS.KEY`.
pub const COL_KEY: &str = "KEY";
/// `INFORMATION_SCHEMA.DEADLOCKS.KEY_INFO`.
pub const COL_KEY_INFO: &str = "KEY_INFO";
/// `INFORMATION_SCHEMA.DEADLOCKS.TRX_HOLDING_LOCK`.
pub const COL_TRX_HOLDING_LOCK: &str = "TRX_HOLDING_LOCK";

const DEADLOCK_COLUMNS: [&str; 9] = [
    COL_DEADLOCK_ID,
    COL_OCCUR_TIME,
    COL_RETRYABLE,
    COL_TRY_LOCK_TRX_ID,
    COL_CURRENT_SQL_DIGEST,
    COL_CURRENT_SQL_DIGEST_TEXT,
    COL_KEY,
    COL_KEY_INFO,
    COL_TRX_HOLDING_LOCK,
];

/// One edge in a deadlock's wait cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitChainItem {
    /// Lower-case hexadecimal SQL digest, or empty when the tag is absent.
    pub sql_digest: String,
    /// Encoded key on which the transaction is blocked.
    pub key: Vec<u8>,
    /// Digests of every statement in this transaction, when known.
    pub all_sql_digests: Vec<String>,
    /// Transaction trying to acquire the lock.
    pub try_lock_txn: u64,
    /// Transaction currently holding the lock.
    pub txn_holding_lock: u64,
}

/// One detected deadlock and every edge in its wait cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockRecord {
    /// Local wall-clock timestamp at which the deadlock was observed.
    pub occur_time: Time,
    /// Wait-cycle entries, in TiKV detector order.
    pub wait_chain: Vec<WaitChainItem>,
    /// Monotonic ID assigned by [`DeadlockHistory::push`].
    pub id: u64,
    /// Whether the SQL layer may retry the deadlock internally.
    pub is_retryable: bool,
}

impl DeadlockRecord {
    /// Builds a record from TiKV's complete deadlock payload.
    #[must_use]
    pub fn from_deadlock(detail: &DeadlockDetail, is_retryable: bool) -> Self {
        let wait_chain = detail
            .wait_chain
            .iter()
            .map(|raw| {
                let digest = match decode_resource_group_tag(&raw.resource_group_tag) {
                    Ok(Some(digest)) => hex(&digest, false),
                    Ok(None) => String::new(),
                    Err(error) => {
                        tracing::warn!(%error, "failed to decode deadlock resource-group tag");
                        String::new()
                    }
                };
                WaitChainItem {
                    sql_digest: digest,
                    key: raw.key.clone(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: raw.txn,
                    txn_holding_lock: raw.wait_for_txn,
                }
            })
            .collect();
        let mut occur_time = Time::current(TimeType::Timestamp);
        occur_time
            .set_fsp(MAX_FSP)
            .expect("MAX_FSP is always a valid timestamp precision");
        Self {
            occur_time,
            wait_chain,
            id: 0,
            is_retryable,
        }
    }

    /// Returns one package-owned DEADLOCKS column for one wait-cycle edge.
    #[must_use]
    pub fn to_datum(&self, wait_chain_idx: usize, column_name: &str) -> Datum {
        match column_name {
            COL_DEADLOCK_ID => Datum::UInt(self.id),
            COL_OCCUR_TIME => Datum::Time(self.occur_time),
            COL_RETRYABLE => Datum::Int(i64::from(self.is_retryable)),
            COL_TRY_LOCK_TRX_ID => Datum::UInt(self.wait_chain[wait_chain_idx].try_lock_txn),
            COL_CURRENT_SQL_DIGEST => {
                let digest = &self.wait_chain[wait_chain_idx].sql_digest;
                if digest.is_empty() {
                    Datum::Null
                } else {
                    Datum::String(StringDatum::new(
                        digest.as_bytes().to_vec(),
                        Collation::DEFAULT,
                    ))
                }
            }
            COL_KEY => {
                let key = &self.wait_chain[wait_chain_idx].key;
                if key.is_empty() {
                    Datum::Null
                } else {
                    Datum::String(StringDatum::new(
                        hex(key, true).into_bytes(),
                        Collation::DEFAULT,
                    ))
                }
            }
            COL_TRX_HOLDING_LOCK => Datum::UInt(self.wait_chain[wait_chain_idx].txn_holding_lock),
            _ => Datum::Null,
        }
    }
}

#[derive(Debug)]
struct HistoryState {
    capacity: usize,
    deadlocks: VecDeque<Arc<DeadlockRecord>>,
    next_id: u64,
}

/// Thread-safe bounded history of recently detected deadlocks.
#[derive(Debug)]
pub struct DeadlockHistory {
    state: Mutex<HistoryState>,
}

impl DeadlockHistory {
    /// Creates an empty history with `capacity` retained records.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(HistoryState {
                capacity,
                deadlocks: VecDeque::with_capacity(capacity),
                next_id: 1,
            }),
        }
    }

    /// Changes the retained-record limit, preserving the newest records.
    pub fn resize(&self, capacity: usize) {
        let mut state = self.lock();
        while state.deadlocks.len() > capacity {
            state.deadlocks.pop_front();
        }
        state.capacity = capacity;
        state.deadlocks.shrink_to_fit();
        let additional = capacity.saturating_sub(state.deadlocks.len());
        state.deadlocks.reserve(additional);
    }

    /// Assigns an ID and appends `record`; capacity zero ignores it entirely.
    pub fn push(&self, mut record: DeadlockRecord) {
        let mut state = self.lock();
        if state.capacity == 0 {
            return;
        }
        record.id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        if state.deadlocks.len() == state.capacity {
            state.deadlocks.pop_front();
        }
        state.deadlocks.push_back(Arc::new(record));
    }

    /// Returns an ordered snapshot from oldest to newest.
    #[must_use]
    pub fn get_all(&self) -> Vec<Arc<DeadlockRecord>> {
        self.lock().deadlocks.iter().cloned().collect()
    }

    /// Clears records while retaining capacity and the next monotonic ID.
    pub fn clear(&self) {
        self.lock().deadlocks.clear();
    }

    /// Returns rows in `INFORMATION_SCHEMA.DEADLOCKS` column order.
    #[must_use]
    pub fn rows(&self) -> Vec<Vec<Datum>> {
        self.get_all()
            .into_iter()
            .flat_map(|record| {
                (0..record.wait_chain.len())
                    .map(move |idx| {
                        DEADLOCK_COLUMNS
                            .iter()
                            .map(|column| record.to_datum(idx, column))
                            .collect()
                    })
                    .collect::<Vec<Vec<Datum>>>()
            })
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, HistoryState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

static GLOBAL_DEADLOCK_HISTORY: LazyLock<DeadlockHistory> =
    LazyLock::new(|| DeadlockHistory::new(0));
static COLLECT_RETRYABLE: AtomicBool = AtomicBool::new(false);

/// Returns the process-wide deadlock history.
#[must_use]
pub fn global_deadlock_history() -> &'static DeadlockHistory {
    &GLOBAL_DEADLOCK_HISTORY
}

/// Applies the server's deadlock-history policy.
pub fn configure_global_deadlock_history(capacity: usize, collect_retryable: bool) {
    COLLECT_RETRYABLE.store(collect_retryable, Ordering::Release);
    GLOBAL_DEADLOCK_HISTORY.resize(capacity);
}

/// Records one live TiKV deadlock when the configured policy admits it.
pub fn record_deadlock(detail: &DeadlockDetail, is_retryable: bool) {
    if is_retryable && !COLLECT_RETRYABLE.load(Ordering::Acquire) {
        return;
    }
    GLOBAL_DEADLOCK_HISTORY.push(DeadlockRecord::from_deadlock(detail, is_retryable));
}

fn hex(bytes: &[u8], upper: bool) -> String {
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        if upper {
            write!(output, "{byte:02X}").expect("writing to String cannot fail");
        } else {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::thread;

    use tidb_datatype::{Collation, CoreTime, Datum, StringDatum, Time, TimeType};
    use tidb_txnkv::transaction::{DeadlockDetail, DeadlockWaitChainItem};
    use tidb_txnkv::ResourceGroupTagBuilder;

    use super::{
        configure_global_deadlock_history, global_deadlock_history, record_deadlock,
        DeadlockHistory, DeadlockRecord, WaitChainItem,
    };

    static GLOBAL_HISTORY_TEST: Mutex<()> = Mutex::new(());

    struct ResetGlobalHistory;

    impl Drop for ResetGlobalHistory {
        fn drop(&mut self) {
            global_deadlock_history().clear();
            configure_global_deadlock_history(0, false);
        }
    }

    fn record(time: Time) -> DeadlockRecord {
        DeadlockRecord {
            occur_time: time,
            wait_chain: Vec::new(),
            id: 0,
            is_retryable: false,
        }
    }

    fn timestamp(year: u16, month: u8, day: u8, microsecond: u32) -> Time {
        Time::new(
            CoreTime::from_date(year, month, day, 15, 28, 30, microsecond),
            TimeType::Timestamp,
            6,
        )
        .unwrap()
    }

    #[test]
    fn collection_overwrite_clear_and_resize_match_source_order() {
        let history = DeadlockHistory::new(3);
        let time = timestamp(2021, 5, 14, 123_456);
        for _ in 0..9 {
            history.push(record(time));
        }
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [7, 8, 9]
        );

        history.resize(4);
        history.push(record(time));
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [7, 8, 9, 10]
        );

        history.resize(2);
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [9, 10]
        );
        history.clear();
        assert!(history.get_all().is_empty());
        history.push(record(time));
        assert_eq!(history.get_all()[0].id, 11);

        history.resize(0);
        history.push(record(time));
        history.resize(1);
        history.push(record(time));
        assert_eq!(history.get_all()[0].id, 12);
    }

    #[test]
    fn snapshots_share_the_pushed_record() {
        let history = DeadlockHistory::new(1);
        history.push(record(timestamp(2021, 5, 14, 123_456)));
        let first = history.get_all();
        let second = history.get_all();
        assert!(Arc::ptr_eq(&first[0], &second[0]));
    }

    #[test]
    fn concurrent_pushes_keep_one_ordered_bounded_id_sequence() {
        let history = Arc::new(DeadlockHistory::new(64));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let history = Arc::clone(&history);
            workers.push(thread::spawn(move || {
                for _ in 0..25 {
                    history.push(record(timestamp(2021, 5, 14, 123_456)));
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            (37..=100).collect::<Vec<_>>()
        );
    }

    #[test]
    fn datum_rows_preserve_missing_values_and_column_order() {
        let history = DeadlockHistory::new(10);
        history.push(DeadlockRecord {
            occur_time: timestamp(2021, 5, 14, 123_456),
            wait_chain: vec![
                WaitChainItem {
                    sql_digest: "sql1".to_owned(),
                    key: b"k1".to_vec(),
                    all_sql_digests: vec!["sql1".to_owned(), "sql2".to_owned()],
                    try_lock_txn: 101,
                    txn_holding_lock: 102,
                },
                WaitChainItem {
                    sql_digest: String::new(),
                    key: Vec::new(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: 102,
                    txn_holding_lock: 101,
                },
            ],
            id: 0,
            is_retryable: false,
        });
        history.push(DeadlockRecord {
            occur_time: timestamp(2022, 6, 15, 123_457),
            wait_chain: vec![WaitChainItem {
                sql_digest: String::new(),
                key: Vec::new(),
                all_sql_digests: Vec::new(),
                try_lock_txn: 201,
                txn_holding_lock: 202,
            }],
            id: 0,
            is_retryable: true,
        });
        history.push(record(timestamp(2023, 1, 1, 0)));

        let rows = history.rows();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].len(), 9);
        assert_eq!(rows[0][0], Datum::UInt(1));
        assert_eq!(rows[0][1], Datum::Time(timestamp(2021, 5, 14, 123_456)));
        assert_eq!(rows[0][2], Datum::Int(0));
        assert_eq!(rows[0][3], Datum::UInt(101));
        assert_eq!(
            rows[0][4],
            Datum::String(StringDatum::new(b"sql1".to_vec(), Collation::DEFAULT))
        );
        assert_eq!(rows[0][5], Datum::Null);
        assert_eq!(
            rows[0][6],
            Datum::String(StringDatum::new(b"6B31".to_vec(), Collation::DEFAULT))
        );
        assert_eq!(rows[0][7], Datum::Null);
        assert_eq!(rows[0][8], Datum::UInt(102));
        assert_eq!(rows[1][4], Datum::Null);
        assert_eq!(rows[1][6], Datum::Null);
        assert_eq!(rows[2][0], Datum::UInt(2));
        assert_eq!(rows[2][2], Datum::Int(1));

        let empty = record(timestamp(2023, 1, 1, 0));
        assert_eq!(
            empty.to_datum(usize::MAX, super::COL_DEADLOCK_ID),
            Datum::UInt(0)
        );
        assert_eq!(empty.to_datum(usize::MAX, "UNRECOGNIZED"), Datum::Null);
    }

    #[test]
    fn conversion_keeps_wait_chain_keys_transactions_and_sql_digests() {
        let mut tag = ResourceGroupTagBuilder::new(None);
        tag.set_sql_digest(&[0xaa, 0xbb, 0xcc, 0xdd]);
        let detail = DeadlockDetail {
            lock_ts: 101,
            lock_key: b"k1".to_vec(),
            deadlock_key_hash: 1_234_567,
            deadlock_key: b"k1".to_vec(),
            wait_chain: vec![
                DeadlockWaitChainItem {
                    txn: 100,
                    wait_for_txn: 101,
                    key: b"k2".to_vec(),
                    resource_group_tag: tag.encode_tag_with_key(&[]),
                },
                DeadlockWaitChainItem {
                    txn: 101,
                    wait_for_txn: 100,
                    key: b"k1".to_vec(),
                    resource_group_tag: vec![0xff],
                },
            ],
        };

        let record = DeadlockRecord::from_deadlock(&detail, true);
        assert_eq!(record.occur_time.kind(), TimeType::Timestamp);
        assert_eq!(record.occur_time.fsp(), 6);
        assert!(record.is_retryable);
        assert_eq!(
            record.wait_chain,
            [
                WaitChainItem {
                    sql_digest: "aabbccdd".to_owned(),
                    key: b"k2".to_vec(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: 100,
                    txn_holding_lock: 101,
                },
                WaitChainItem {
                    sql_digest: String::new(),
                    key: b"k1".to_vec(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: 101,
                    txn_holding_lock: 100,
                },
            ]
        );
    }

    #[test]
    fn retryable_collection_obeys_the_process_policy() {
        let _serial = GLOBAL_HISTORY_TEST.lock().unwrap();
        let _reset = ResetGlobalHistory;
        let detail = DeadlockDetail {
            lock_ts: 1,
            lock_key: Vec::new(),
            deadlock_key_hash: 0,
            deadlock_key: Vec::new(),
            wait_chain: Vec::new(),
        };

        configure_global_deadlock_history(3, false);
        global_deadlock_history().clear();
        record_deadlock(&detail, true);
        assert!(global_deadlock_history().get_all().is_empty());

        record_deadlock(&detail, false);
        assert_eq!(global_deadlock_history().get_all()[0].id, 1);

        configure_global_deadlock_history(3, true);
        record_deadlock(&detail, true);
        let records = global_deadlock_history().get_all();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].id, 2);
        assert!(records[1].is_retryable);
    }
}
