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

//! Direct source-shaped coverage for Go's `SHOW ENGINES` parser leaf.

use super::*;

/// The differential parser corpus's source row is the executor `TestShow`
/// statement at `tests/integrationtest/t/executor/executor.test:1660`.
#[test]
fn show_engines_restores_go_source_row() {
    assert_eq!(r("show engines"), "SHOW ENGINES");
}

#[test]
fn show_engines_preserves_the_shared_filter_payload() {
    assert_eq!(
        r("show engines like 'innodb%'"),
        "SHOW ENGINES LIKE _UTF8MB4'innodb%'"
    );
    assert_eq!(
        r("show engines where Engine = 'InnoDB'"),
        "SHOW ENGINES WHERE `Engine`=_UTF8MB4'InnoDB'"
    );

    let statement = parse("show engines where Engine = 'InnoDB'").expect("parse SHOW ENGINES");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowEngines(show) = admin.as_ref() else {
        panic!("expected typed SHOW ENGINES");
    };
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowEnginesFilter::Where(_))
    ));

    for sql in [
        "show engines like",
        "show engines where",
        "show engines like 'x%' where Engine = 'InnoDB'",
    ] {
        assert!(parse(sql).is_err(), "outside this ENGINES leaf: {sql}");
    }
}
