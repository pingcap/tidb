// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Execution boundary for the source-owned `SHOW CHARSET` catalog leaf.

use super::*;

#[test]
fn show_charset_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table show_charset_boundary (id int)"),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into show_charset_boundary values (1)"),
        "OK"
    );

    assert_eq!(
        step(&mut db, "show character set like '%utf8mb4%'",),
        "Unsupported(\"SHOW CHARSET\")"
    );

    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(step(&mut db, "select id from show_charset_boundary"), "RS:");
}
