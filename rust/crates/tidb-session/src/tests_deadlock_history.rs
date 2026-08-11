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

#![cfg(test)]

use std::sync::Mutex;

use tidb_datatype::{Collation, CoreTime, Datum, FieldTypeCode, StringDatum, Time, TimeType};
use tidb_executor::deadlock_history::{
    configure_global_deadlock_history, global_deadlock_history, DeadlockRecord, WaitChainItem,
};

use crate::{DriverError, Session, StmtOutput};

static GLOBAL_HISTORY_TEST: Mutex<()> = Mutex::new(());

struct ResetHistory;

impl Drop for ResetHistory {
    fn drop(&mut self) {
        let history = global_deadlock_history();
        history.clear();
        configure_global_deadlock_history(0, false);
    }
}

#[test]
fn deadlocks_table_exposes_package_rows_and_requires_process() {
    let _serial = GLOBAL_HISTORY_TEST.lock().unwrap();
    let _reset = ResetHistory;
    configure_global_deadlock_history(10, false);
    let history = global_deadlock_history();
    history.clear();
    let occur_time = Time::new(
        CoreTime::from_date(2021, 5, 14, 15, 28, 30, 123_456),
        TimeType::Timestamp,
        6,
    )
    .unwrap();
    history.push(DeadlockRecord {
        occur_time,
        wait_chain: vec![WaitChainItem {
            sql_digest: "aabbccdd".to_owned(),
            key: b"k1".to_vec(),
            all_sql_digests: Vec::new(),
            try_lock_txn: 101,
            txn_holding_lock: 102,
        }],
        id: 0,
        is_retryable: false,
    });

    let schema = tidb_executor::infoschema_meta::table_schema("DEADLOCKS").unwrap();
    assert_eq!(
        schema
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        [
            "DEADLOCK_ID",
            "OCCUR_TIME",
            "RETRYABLE",
            "TRY_LOCK_TRX_ID",
            "CURRENT_SQL_DIGEST",
            "CURRENT_SQL_DIGEST_TEXT",
            "KEY",
            "KEY_INFO",
            "TRX_HOLDING_LOCK",
        ]
    );
    assert_eq!(schema[1].1.code(), FieldTypeCode::Timestamp);
    assert_eq!(schema[1].1.flen(), 26);
    assert_eq!(schema[1].1.decimal(), 6);
    assert_eq!(schema[3].1.flags(), 1 | 32);
    assert_eq!(schema[8].1.flags(), 1 | 32);

    let mut session = Session::new();
    session.set_user("bob@%".to_owned(), "bob@127.0.0.1".to_owned());
    let error = session
        .run_with_columns("SELECT * FROM information_schema.DEADLOCKS")
        .unwrap_err();
    assert!(
        matches!(error, DriverError::SpecificAccessDenied(privilege) if privilege == "PROCESS")
    );

    session.set_process_privilege(true);
    let StmtOutput::Rows { columns, rows } = session
        .run_with_columns(
            "SELECT DEADLOCK_ID, OCCUR_TIME, RETRYABLE, TRY_LOCK_TRX_ID, CURRENT_SQL_DIGEST, \
             CURRENT_SQL_DIGEST_TEXT, KEY, KEY_INFO, TRX_HOLDING_LOCK \
             FROM information_schema.DEADLOCKS",
        )
        .unwrap()
    else {
        panic!("DEADLOCKS must return rows");
    };
    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        [
            "DEADLOCK_ID",
            "OCCUR_TIME",
            "RETRYABLE",
            "TRY_LOCK_TRX_ID",
            "CURRENT_SQL_DIGEST",
            "CURRENT_SQL_DIGEST_TEXT",
            "KEY",
            "KEY_INFO",
            "TRX_HOLDING_LOCK",
        ]
    );
    assert_eq!(
        rows,
        [vec![
            Datum::Int(1),
            Datum::Time(occur_time),
            Datum::Int(0),
            Datum::UInt(101),
            Datum::String(StringDatum::new(
                b"aabbccdd".to_vec(),
                Collation::Utf8Mb4Bin,
            )),
            Datum::Null,
            Datum::String(StringDatum::new(b"6B31".to_vec(), Collation::Utf8Mb4Bin,)),
            Datum::Null,
            Datum::UInt(102),
        ]]
    );
}
