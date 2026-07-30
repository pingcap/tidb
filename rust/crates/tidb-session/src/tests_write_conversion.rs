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

//! The flags a COLUMN WRITE converts under, in both SQL modes.
//!
//! `tidb_executor::StmtContext::write_conversion_flags` mirrors Go
//! `pkg/util/misc.go`'s `GetTypeFlagsForInsert`. The case that made the
//! difference visible is a negative value written into an UNSIGNED column:
//! Go clears `FlagAllowNegativeToUnsigned` for every write, so the value
//! OVERFLOWS (clamping to `0`) instead of reinterpreting its bit pattern.
//!
//! Captured from real TiDB, `t(a INT UNSIGNED)`:
//!
//! ```text
//! default sql_mode   insert into t values (-5)   ERR 1264 Out of range value for column 'a' at row 1
//! sql_mode = ''      insert into t values (-5)   OK  WARN 1264 (same text), stores 0
//! ```
//!
//! The strict half was already right here, which is what made the non-strict
//! half easy to miss: an error path that looks correct beside a stored value
//! that is not.

use super::Session;
use crate::tests_support::row_text;

/// A strict write of a negative value into an UNSIGNED column fails.
#[test]
fn strict_mode_refuses_a_negative_value_in_an_unsigned_column() {
    let mut session = Session::new();
    session.run("CREATE TABLE u1 (a INT UNSIGNED)").unwrap();

    let error = session
        .run("INSERT INTO u1 VALUES (-5)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1264, "Out of range value for column 'a' at row 1")
    );
    assert!(row_text(session.run("SELECT a FROM u1")).is_empty());
}

/// THE CONTROL, and the bug: under `sql_mode = ''` the statement is ACCEPTED,
/// and the value stored is the CLAMP (`0`), not the reinterpreted bit
/// pattern (`4294967295`).
#[test]
fn non_strict_mode_clamps_a_negative_value_rather_than_reinterpreting_it() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session.run("CREATE TABLE u2 (a INT UNSIGNED)").unwrap();

    session.run("INSERT INTO u2 VALUES (-5)").unwrap();
    let warnings: Vec<(u16, String)> = session
        .warnings()
        .iter()
        .map(|w| (w.code, w.message.clone()))
        .collect();
    assert_eq!(
        warnings,
        [(
            1264,
            "Out of range value for column 'a' at row 1".to_owned()
        )]
    );
    assert_eq!(row_text(session.run("SELECT a FROM u2")), [["0"]]);
}
