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

/// Builds a record from TiKV's complete deadlock error payload.
#[must_use]
pub fn err_deadlock_to_deadlock_record(detail: &DeadlockDetail) -> DeadlockRecord {
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
    DeadlockRecord {
        occur_time,
        wait_chain,
        id: 0,
        is_retryable: detail.is_retryable,
    }
}

impl DeadlockRecord {
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

    fn lock(&self) -> MutexGuard<'_, HistoryState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// Process-wide deadlock history used by the executor and information schema.
pub static GLOBAL_DEADLOCK_HISTORY: LazyLock<DeadlockHistory> =
    LazyLock::new(|| DeadlockHistory::new(0));

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
    use std::sync::Arc;

    use tidb_datatype::{Collation, CoreTime, Datum, StringDatum, Time, TimeType};
    use tidb_txnkv::transaction::{DeadlockDetail, DeadlockWaitChainItem};
    use tidb_txnkv::ResourceGroupTagBuilder;

    use super::{err_deadlock_to_deadlock_record, DeadlockHistory, DeadlockRecord, WaitChainItem};

    fn record(time: Time) -> DeadlockRecord {
        DeadlockRecord {
            occur_time: time,
            wait_chain: Vec::new(),
            id: 0,
            is_retryable: false,
        }
    }

    fn timestamp(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    ) -> Time {
        Time::new(
            CoreTime::from_date(year, month, day, hour, minute, second, microsecond),
            TimeType::Timestamp,
            6,
        )
        .unwrap()
    }

    #[test]
    fn test_deadlock_history_collection() {
        let history = DeadlockHistory::new(1);
        assert!(history.get_all().is_empty());
        history.push(record(timestamp(2021, 5, 14, 15, 28, 30, 123_456)));
        let first = history.get_all();
        let second = history.get_all();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, 1);
        assert!(Arc::ptr_eq(&first[0], &second[0]));
        history.push(record(timestamp(2021, 5, 14, 15, 28, 30, 123_456)));
        assert_eq!(history.get_all()[0].id, 2);
        history.clear();
        assert!(history.get_all().is_empty());

        let history = DeadlockHistory::new(3);
        for expected_id in 1..=3 {
            history.push(record(timestamp(2021, 5, 14, 15, 28, 30, 123_456)));
            assert_eq!(history.get_all().last().unwrap().id, expected_id);
        }
        for newest_id in 4..=9 {
            history.push(record(timestamp(2021, 5, 14, 15, 28, 30, 123_456)));
            assert_eq!(
                history
                    .get_all()
                    .iter()
                    .map(|record| record.id)
                    .collect::<Vec<_>>(),
                [newest_id - 2, newest_id - 1, newest_id]
            );
        }
        history.clear();
        assert!(history.get_all().is_empty());
    }

    #[test]
    fn test_get_datum() {
        let history = DeadlockHistory::new(10);
        let time1 = timestamp(2021, 5, 14, 15, 28, 30, 123_456);
        let time2 = timestamp(2022, 6, 15, 16, 29, 31, 123_457);
        history.push(DeadlockRecord {
            occur_time: time1,
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
            occur_time: time2,
            wait_chain: vec![
                WaitChainItem {
                    sql_digest: String::new(),
                    key: Vec::new(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: 201,
                    txn_holding_lock: 202,
                },
                WaitChainItem {
                    sql_digest: String::new(),
                    key: Vec::new(),
                    all_sql_digests: vec!["sql1".to_owned()],
                    try_lock_txn: 202,
                    txn_holding_lock: 201,
                },
            ],
            id: 0,
            is_retryable: true,
        });
        history.push(record(timestamp(2023, 1, 1, 0, 0, 0, 0)));

        let columns = [
            super::COL_DEADLOCK_ID,
            super::COL_OCCUR_TIME,
            super::COL_RETRYABLE,
            super::COL_TRY_LOCK_TRX_ID,
            super::COL_CURRENT_SQL_DIGEST,
            super::COL_CURRENT_SQL_DIGEST_TEXT,
            super::COL_KEY,
            super::COL_KEY_INFO,
            super::COL_TRX_HOLDING_LOCK,
        ];
        let rows = history
            .get_all()
            .into_iter()
            .flat_map(|record| {
                (0..record.wait_chain.len())
                    .map(|idx| {
                        columns
                            .iter()
                            .map(|column| record.to_datum(idx, column))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].len(), 9);
        assert_eq!(rows[0][0], Datum::UInt(1));
        assert_eq!(rows[0][1], Datum::Time(time1));
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
        assert_eq!(rows[2][3], Datum::UInt(201));
        assert_eq!(rows[2][8], Datum::UInt(202));
        assert_eq!(rows[3][0], Datum::UInt(2));
        assert_eq!(rows[3][2], Datum::Int(1));
        assert_eq!(rows[3][3], Datum::UInt(202));
        assert_eq!(rows[3][8], Datum::UInt(201));

        let empty = record(timestamp(2023, 1, 1, 0, 0, 0, 0));
        assert_eq!(
            empty.to_datum(usize::MAX, super::COL_DEADLOCK_ID),
            Datum::UInt(0)
        );
        assert_eq!(empty.to_datum(usize::MAX, "UNRECOGNIZED"), Datum::Null);
    }

    #[test]
    fn test_err_deadlock_to_deadlock_record() {
        let mut tag1 = ResourceGroupTagBuilder::new(None);
        tag1.set_sql_digest(&[0xaa, 0xbb, 0xcc, 0xdd]);
        let mut tag2 = ResourceGroupTagBuilder::new(None);
        tag2.set_sql_digest(&[0xdd, 0xcc, 0xbb, 0xaa]);
        let detail = DeadlockDetail {
            lock_ts: 101,
            lock_key: b"k1".to_vec(),
            deadlock_key_hash: 1_234_567,
            deadlock_key: b"k1".to_vec(),
            is_retryable: true,
            wait_chain: vec![
                DeadlockWaitChainItem {
                    txn: 100,
                    wait_for_txn: 101,
                    key: b"k2".to_vec(),
                    resource_group_tag: tag1.encode_tag_with_key(&[]),
                },
                DeadlockWaitChainItem {
                    txn: 101,
                    wait_for_txn: 100,
                    key: b"k1".to_vec(),
                    resource_group_tag: tag2.encode_tag_with_key(&[]),
                },
            ],
        };

        let record = err_deadlock_to_deadlock_record(&detail);
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
                    sql_digest: "ddccbbaa".to_owned(),
                    key: b"k1".to_vec(),
                    all_sql_digests: Vec::new(),
                    try_lock_txn: 101,
                    txn_holding_lock: 100,
                },
            ]
        );
    }

    #[test]
    fn test_resize() {
        let history = DeadlockHistory::new(2);
        let time = timestamp(2021, 5, 14, 15, 28, 30, 123_456);
        history.push(record(time));
        history.push(record(time));
        history.push(record(time));
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [2, 3]
        );

        history.resize(3);
        history.push(record(time));
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );

        history.resize(2);
        assert_eq!(
            history
                .get_all()
                .iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [3, 4]
        );

        history.resize(0);
        assert!(history.get_all().is_empty());
        history.resize(2);
        history.push(record(time));
        assert_eq!(history.get_all()[0].id, 5);
    }
}
