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

//! Direct source rows for Go's `SHOW CHARACTER SET`/`SHOW CHARSET` leaf.

use super::*;

/// Exact `TestDBAStmt` rows at `pkg/parser/parser_test.go:1314-1315`.
#[test]
fn show_character_set_restores_go_aliases() {
    for sql in ["show character set", "show char set", "show charset"] {
        assert_eq!(r(sql), "SHOW CHARSET", "source spelling: {sql}");
    }
}

#[test]
fn show_character_set_preserves_filter_payload() {
    assert_eq!(
        r("show character set like '%utf8mb4%'"),
        "SHOW CHARSET LIKE _UTF8MB4'%utf8mb4%'"
    );
    assert_eq!(
        r("show charset where Charset = 'utf8'"),
        "SHOW CHARSET WHERE `Charset`=_UTF8MB4'utf8'"
    );

    let statement = parse("show character set where Charset = 'utf8'")
        .expect("SHOW CHARACTER SET with WHERE parses");
    let tidb_ast::Stmt::Admin(admin) = statement else {
        panic!("expected Admin envelope");
    };
    let tidb_ast::AdminStmt::ShowCharset(show) = admin.as_ref() else {
        panic!("expected typed SHOW CHARSET");
    };
    assert!(matches!(
        &show.filter,
        Some(tidb_ast::ShowCharsetFilter::Where(_))
    ));

    for sql in [
        "show character set like",
        "show character set where",
        "show charset like 'x%' where Charset = 'utf8'",
    ] {
        assert!(parse(sql).is_err(), "outside this CHARSET leaf: {sql}");
    }
}
