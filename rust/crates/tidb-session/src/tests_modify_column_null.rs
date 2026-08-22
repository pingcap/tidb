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

//! What a stored NULL becomes when its column is rewritten to `NOT NULL`.

use crate::tests_support::row_text;
use crate::Session;

/// Go `updateColumnWorker.getRowRecord` substitutes the CURRENT TIMESTAMP for
/// a NULL before it casts, guarded on exactly three things: the old value is
/// NULL, the new type is `mysql.TypeTimestamp`, and the new column carries
/// `NotNullFlag` -- "convert null value to timestamp should be substituted
/// with current timestamp if NOT_NULL flag is set".
///
/// Without it the rewrite failed outright, so the column stayed `int` and the
/// row stayed NULL. Source rows: `tests/integrationtest/t/ddl/db_change.test`,
/// where the recorded answer is that the stored instant is within two seconds
/// of the reading statement's own clock.
#[test]
fn a_null_becomes_the_current_timestamp_when_the_column_turns_not_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int, b int)").unwrap();
    session.run("INSERT INTO t VALUES (NULL, NULL)").unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN a timestamp not null")
        .unwrap();

    assert_eq!(
        row_text(session.run(
            "SELECT floor((unix_timestamp() - unix_timestamp(a)) / 2) FROM t"
        )),
        vec![vec!["0"]],
        "the stored instant is the statement clock, not NULL and not the zero time"
    );

    // Go names `mysql.TypeTimestamp` and nothing else, so the sibling
    // temporal type still refuses the NULL rather than inventing a value.
    let error = session
        .run("ALTER TABLE t MODIFY COLUMN b datetime not null")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1265);
    // And the refusal left the column alone.
    assert_eq!(row_text(session.run("SELECT b FROM t")), vec![vec!["NULL"]]);
}
