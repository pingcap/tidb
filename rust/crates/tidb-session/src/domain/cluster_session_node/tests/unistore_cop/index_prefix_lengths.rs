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


//! Index key-part prefix lengths (Go `pkg/ddl` `checkIndexPrefixLength`,
//! `buildIndexColumns`): the refusals and the recorded `SUB_PART`, over the
//! cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn refused(tk: &mut Session, sql: &str) -> (u16, String) {
    let refused = error(tk, sql);
    (refused.code, refused.message)
}

#[test]
fn a_length_on_a_type_that_has_no_prefix_is_1089() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 670);
    for sql in [
        "CREATE TABLE t (a INT, KEY idx (a(3)))",
        "CREATE TABLE t (a BIGINT, UNIQUE KEY idx (a(3)))",
        "CREATE TABLE t (a DATETIME, KEY idx (a(3)))",
        "CREATE TABLE t (a VARCHAR(5), KEY idx (a(10)))",
        "CREATE TABLE t (a CHAR(5), KEY idx (a(6)))",
    ] {
        assert_eq!(
            refused(&mut tk, sql),
            (1089, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys".to_owned()),
            "{sql}"
        );
    }
}

#[test]
fn a_blob_or_text_key_part_without_a_length_is_1170() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 671);
    for (sql, column) in [
        ("CREATE TABLE t (a BLOB, KEY idx (a))", "a"),
        ("CREATE TABLE t (a TEXT, KEY idx (a))", "a"),
        ("CREATE TABLE t (a LONGTEXT, UNIQUE KEY idx (a))", "a"),
        ("CREATE TABLE t (x INT, b MEDIUMBLOB, KEY idx (x, b))", "b"),
    ] {
        assert_eq!(refused(&mut tk, sql), (1170, format!("BLOB/TEXT column '{column}' used in key specification without a key length")), "{sql}");
    }
}

#[test]
fn a_zero_length_key_part_is_1391() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 672);
    for sql in [
        "CREATE TABLE t (a VARCHAR(20), KEY idx (a(0)))",
        "CREATE TABLE t (a CHAR(20), KEY idx (a(0)))",
        "CREATE TABLE t (a BLOB, KEY idx (a(0)))",
    ] {
        assert_eq!(refused(&mut tk, sql), (1391, "Key part 'a' length cannot be 0".to_owned()), "{sql}");
    }
}

/// The limit counts bytes: 1000 utf8mb4 characters are 4000 bytes, 1000
/// latin1 ones fit.
#[test]
fn a_key_part_longer_than_the_limit_is_1071() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 673);
    assert_eq!(
        refused(&mut tk, "CREATE TABLE t (a VARCHAR(2000), KEY idx (a(1000)))"),
        (1071, "Specified key was too long (4000 bytes); max key length is 3072 bytes".to_owned())
    );
    rows(&mut tk, "CREATE TABLE t (a VARCHAR(2000) CHARACTER SET latin1, KEY idx (a(1000)))");
}

#[test]
fn a_full_length_char_prefix_is_an_ordinary_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 674);
    exec_all(&mut tk, &["CREATE TABLE t (a VARCHAR(10), KEY idx (a(10)))", "INSERT INTO t VALUES ('abcdefghij'), ('x')"]);
    assert_eq!(q(&mut tk, "SELECT a FROM t WHERE a = 'abcdefghij'"), [["abcdefghij"]]);
}

fn sub_parts(tk: &mut Session, index: &str) -> Vec<String> {
    q(tk, &format!("SELECT sub_part FROM information_schema.statistics WHERE table_schema = 'test' AND table_name = 't' AND index_name = '{index}' ORDER BY seq_in_index"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

#[test]
fn a_real_prefix_on_a_secondary_index_is_built_and_recorded() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 675);
    for (sql, expected) in [
        ("CREATE TABLE t (a VARCHAR(20), KEY idx (a(3)))", &["3"][..]),
        ("CREATE TABLE t (a TEXT, KEY idx (a(5)))", &["5"][..]),
        ("CREATE TABLE t (a VARCHAR(20), UNIQUE KEY idx (a(3)))", &["3"][..]),
        ("CREATE TABLE t (a INT, b VARCHAR(20), KEY idx (a, b(4)))", &["NULL", "4"][..]),
    ] {
        exec_all(&mut tk, &["DROP TABLE IF EXISTS t", sql]);
        assert_eq!(sub_parts(&mut tk, "idx"), expected, "{sql}");
    }
}

#[test]
fn a_prefix_primary_key_is_stored_on_the_common_handle() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 676);
    rows(&mut tk, "CREATE TABLE t (a VARCHAR(20), b INT, PRIMARY KEY (a(3), b) CLUSTERED)");
    assert_eq!(q(&mut tk, "SELECT tidb_pk_type FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 't'"), [["CLUSTERED"]]);
    assert_eq!(sub_parts(&mut tk, "PRIMARY"), ["3", "NULL"]);
    exec_all(&mut tk, &["INSERT INTO t VALUES ('abcdef', 1), ('abcxyz', 2)"]);
    assert_eq!(error(&mut tk, "INSERT INTO t VALUES ('abcqqq', 1)").code, 1062);
}
