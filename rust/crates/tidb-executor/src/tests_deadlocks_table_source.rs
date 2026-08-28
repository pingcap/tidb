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

//! Go `pkg/executor/executor_failpoint_test.go:686::TestDeadlocksTable`,
//! ported against the transcreated history in `crate::deadlock_history`
//! (Go `pkg/util/deadlockhistory` + the `INFORMATION_SCHEMA.DEADLOCKS`
//! rendering).
//!
//! Go drives the assertion through `select * from information_schema.deadlocks`
//! after hand-building two `DeadlockRecord`s; the same rows are what
//! `DeadlockHistory::rows` yields for the same records, one row per wait-chain
//! edge in column order (`COL_DEADLOCK_ID` .. `COL_TRX_HOLDING_LOCK`). Go's
//! `sqlDigestRetrieverSkipRetrieveGlobal` failpoint only forces the SQL-digest
//! text lookup to be skipped, which is the fixed behavior here: the digest
//! TEXT and KEY_INFO columns render NULL when no table catalog resolves the
//! key (matching Go's expected `<nil>` cells).
//!
//! The test pins the process-wide history exactly as Go does; nextest runs
//! each test in its own process, and the guard restores the capacity after.

use tidb_datatype::{Collation, Datum, StringDatum, Time, TimeType};

use crate::deadlock_history::{DeadlockRecord, WaitChainItem, global_deadlock_history};

/// Go: `time.Date(2021, 5, 10, 1, 2, 3, 456789000, time.Local)`
/// (pkg/executor/executor_failpoint_test.go:690).
fn occur_time(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    micros: i32,
) -> Time {
    Time::from_date_checked(
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros,
        TimeType::Timestamp,
        6,
    )
    .unwrap()
}

fn string_datum(text: &str) -> Datum {
    Datum::String(StringDatum::new(
        text.as_bytes().to_vec(),
        Collation::DEFAULT,
    ))
}

/// A catalog that resolves nothing, mirroring Go's expected `<nil>` KEY_INFO
/// under the skip-retrieve failpoint: an undecodable key renders NULL.
struct NoTables;

impl crate::keydecoder::KeyInfoCatalog for NoTables {
    fn resolve_physical_table(
        &self,
        _physical_id: i64,
    ) -> Option<crate::keydecoder::KeyInfoTableLookup> {
        None
    }
}

struct ResetHistory;

impl ResetHistory {
    fn new() -> Self {
        global_deadlock_history().clear();
        global_deadlock_history().resize(10);
        Self
    }
}

impl Drop for ResetHistory {
    fn drop(&mut self) {
        global_deadlock_history().clear();
        global_deadlock_history().resize(0);
    }
}

/// Go `pkg/executor/executor_failpoint_test.go:686::TestDeadlocksTable`.
#[test]
fn deadlocks_table_renders_pushed_records_per_wait_chain_edge() {
    let _guard = ResetHistory::new();

    // Go pkg/executor/executor_failpoint_test.go:692-709: one non-retryable deadlock whose
    // two edges carry digests and keys.
    let rec = DeadlockRecord {
        occur_time: occur_time(2021, 5, 10, 1, 2, 3, 456_789),
        is_retryable: false,
        id: 0,
        wait_chain: vec![
            WaitChainItem {
                try_lock_txn: 101,
                sql_digest: "aabbccdd".to_owned(),
                key: b"k1".to_vec(),
                all_sql_digests: Vec::new(),
                txn_holding_lock: 102,
            },
            WaitChainItem {
                try_lock_txn: 102,
                sql_digest: "ddccbbaa".to_owned(),
                key: b"k2".to_vec(),
                all_sql_digests: vec!["sql1".to_owned()],
                txn_holding_lock: 101,
            },
        ],
    };
    // Go pkg/executor/executor_failpoint_test.go:723-740: one retryable deadlock whose
    // edges carry no digest/key at all (the first edge's AllSQLDigests is the
    // empty list, which must render like absence).
    let rec2 = DeadlockRecord {
        occur_time: occur_time(2022, 6, 11, 2, 3, 4, 987_654),
        is_retryable: true,
        id: 0,
        wait_chain: vec![
            WaitChainItem {
                try_lock_txn: 201,
                sql_digest: String::new(),
                key: Vec::new(),
                all_sql_digests: Vec::new(),
                txn_holding_lock: 202,
            },
            WaitChainItem {
                try_lock_txn: 202,
                sql_digest: String::new(),
                key: Vec::new(),
                all_sql_digests: vec!["sql1".to_owned(), "sql2, sql3".to_owned()],
                txn_holding_lock: 203,
            },
            WaitChainItem {
                try_lock_txn: 203,
                sql_digest: String::new(),
                key: Vec::new(),
                all_sql_digests: Vec::new(),
                txn_holding_lock: 201,
            },
        ],
    };
    global_deadlock_history().push(rec);
    global_deadlock_history().push(rec2);

    // Go: "`Push` sets the record's ID, and ID in a single DeadlockHistory is
    // monotonically increasing" (pkg/executor/executor_failpoint_test.go:742-745).
    let all = global_deadlock_history().get_all();
    assert_eq!(all.len(), 2);
    let id1 = all[0].id;
    let id2 = all[1].id;
    assert!(id2 > id1);

    // The Go suite reads these rows through
    // `select * from information_schema.deadlocks` and expects
    // (pkg/executor/executor_failpoint_test.go:757-764):
    //   id1/2021-05-10 01:02:03.456789/0/101/aabbccdd/<nil>/6B31/<nil>/102
    //   id1/2021-05-10 01:02:03.456789/0/102/ddccbbaa/<nil>/6B32/<nil>/101
    //   id2/2022-06-11 02:03:04.987654/1/201/<nil>/<nil>/<nil>/<nil>/202
    //   id2/2022-06-11 02:03:04.987654/1/202/<nil>/<nil>/<nil>/<nil>/203
    //   id2/2022-06-11 02:03:04.987654/1/203/<nil>/<nil>/<nil>/<nil>/201
    // KEY is the UPPER-HEX of the raw key ("k1" -> "6B31"); CURRENT_SQL_DIGEST
    // TEXT and KEY_INFO are NULL, exactly as the Go expectation pins.
    let null = Datum::Null;
    let expected = vec![
        vec![
            Datum::UInt(id1),
            Datum::Time(occur_time(2021, 5, 10, 1, 2, 3, 456_789)),
            Datum::Int(0),
            Datum::UInt(101),
            string_datum("aabbccdd"),
            null.clone(),
            string_datum("6B31"),
            null.clone(),
            Datum::UInt(102),
        ],
        vec![
            Datum::UInt(id1),
            Datum::Time(occur_time(2021, 5, 10, 1, 2, 3, 456_789)),
            Datum::Int(0),
            Datum::UInt(102),
            string_datum("ddccbbaa"),
            null.clone(),
            string_datum("6B32"),
            null.clone(),
            Datum::UInt(101),
        ],
        vec![
            Datum::UInt(id2),
            Datum::Time(occur_time(2022, 6, 11, 2, 3, 4, 987_654)),
            Datum::Int(1),
            Datum::UInt(201),
            null.clone(),
            null.clone(),
            null.clone(),
            null.clone(),
            Datum::UInt(202),
        ],
        vec![
            Datum::UInt(id2),
            Datum::Time(occur_time(2022, 6, 11, 2, 3, 4, 987_654)),
            Datum::Int(1),
            Datum::UInt(202),
            null.clone(),
            null.clone(),
            null.clone(),
            null.clone(),
            Datum::UInt(203),
        ],
        vec![
            Datum::UInt(id2),
            Datum::Time(occur_time(2022, 6, 11, 2, 3, 4, 987_654)),
            Datum::Int(1),
            Datum::UInt(203),
            null.clone(),
            null.clone(),
            null.clone(),
            null.clone(),
            Datum::UInt(201),
        ],
    ];
    assert_eq!(global_deadlock_history().rows(&NoTables), expected);
}
