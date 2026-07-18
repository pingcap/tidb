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

//! Go-source coverage for table-level `ALTER TABLE ... ATTRIBUTES`.
//! `pkg/parser/ddl_alter_handlers.go:parseAlterTableOptions` owns the exact
//! `[=] {DEFAULT|string}` grammar and `ast.AttributesSpec.Restore` owns its
//! canonical equals-sign/single-quoted output.

use super::*;

#[test]
fn alter_table_attributes_match_go_parser_and_restore() {
    for (sql, expected) in [
        (
            "ALTER TABLE t ATTRIBUTES='str'",
            "ALTER TABLE `t` ATTRIBUTES='str'",
        ),
        (
            "ALTER TABLE t ATTRIBUTES=\"str1,str2\"",
            "ALTER TABLE `t` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t ATTRIBUTES 'str1,str2'",
            "ALTER TABLE `t` ATTRIBUTES='str1,str2'",
        ),
        (
            "ALTER TABLE t ATTRIBUTES=DeFaUlT",
            "ALTER TABLE `t` ATTRIBUTES=DEFAULT",
        ),
        (
            "ALTER TABLE t ATTRIBUTES 'it\\'s'",
            "ALTER TABLE `t` ATTRIBUTES='it''s'",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }

    for sql in [
        "ALTER TABLE t ATTRIBUTES",
        "ALTER TABLE t ATTRIBUTES = 1",
        "ALTER TABLE t ATTRIBUTES bare_word",
    ] {
        assert!(parse(sql).is_err(), "Go rejects: {sql}");
    }
}
