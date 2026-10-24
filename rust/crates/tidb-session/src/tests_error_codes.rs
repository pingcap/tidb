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

//! Statements both engines refuse, where the refusal's error code must be
//! the one Go raises first. Each case came from the integrationtest
//! error-code survey (`INTEGRATION_COMPARE_ERROR_CODES` over every topic).
#![cfg(test)]

use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

/// Go `executor.CreateForeignKey` refuses a temporary child table with
/// `ErrCannotAddForeign` before resolving its columns
/// (ddl/db_foreign_key `TestTemporaryTableForeignKey`).
#[test]
fn adding_a_foreign_key_to_a_global_temporary_table_is_refused_first() {
    let mut session = Session::new();
    session
        .run("create global temporary table t1_tmp (a int, b int) on commit delete rows")
        .unwrap();
    session.run("create table t2 (a int, b int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t1_tmp add foreign key (c) references t2(a)"
        ),
        (1215, "Cannot add foreign key constraint".to_owned())
    );
}
