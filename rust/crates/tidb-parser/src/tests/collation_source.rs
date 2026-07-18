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

//! Source-owned collation lookup boundaries from Go's
//! `pkg/parser/charset/charset.go` and the DDL parser call sites.

use super::*;

#[test]
fn ddl_collation_aliases_and_full_registry_match_go() {
    assert_eq!(
        r("CREATE TABLE t (a VARCHAR(10) COLLATE utf8mb3_bin)"),
        "CREATE TABLE `t` (`a` VARCHAR(10) COLLATE utf8_bin)"
    );
    assert_eq!(
        r("CREATE TABLE t (a VARCHAR(10) COLLATE utf8mb4_0900_ai_ci)"),
        "CREATE TABLE `t` (`a` VARCHAR(10) COLLATE utf8mb4_0900_ai_ci)"
    );
    assert_eq!(
        r("CREATE TABLE t (a INT) COLLATE utf8mb4_zh_pinyin_tidb_as_cs"),
        "CREATE TABLE `t` (`a` INT) DEFAULT COLLATE = UTF8MB4_ZH_PINYIN_TIDB_AS_CS"
    );
}

#[test]
fn ddl_collation_unknown_names_are_rejected_at_go_lookup_boundary() {
    for sql in [
        "CREATE TABLE t (a VARCHAR(10) COLLATE utf8bin)",
        "CREATE TABLE t (a INT) COLLATE utf8bin",
        "ALTER TABLE t COLLATE utf8bin",
        "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8bin",
    ] {
        assert!(parse(sql).is_err(), "Go rejects unknown collation: {sql}");
    }
}
