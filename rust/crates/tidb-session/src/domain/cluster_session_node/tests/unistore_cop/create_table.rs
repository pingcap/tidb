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


//! `CREATE TABLE` through Go's DDL job (`BuildTableInfoWithStmt`): rows round
//! trip, invisible indexes are kept but never planned, IF NOT EXISTS, the
//! integer handle, per-column charset and collation resolution and BINARY
//! padding, over the cop-backed TestKit stack (X11). Expected values are Go's
//! `information_schema` answers.

use super::sql_kit::*;
use super::*;

#[test]
fn create_insert_select_from_sql() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 800);
    rows(&mut tk, "CREATE TABLE t (a BIGINT, b BIGINT UNSIGNED, s VARCHAR(10))");
    assert_eq!(affected(&mut tk, "INSERT INTO t (a, s) VALUES (7, 'x')"), 1);
    assert_eq!(q(&mut tk, "SELECT a, s FROM t"), [["7", "x"]]);
}

#[test]
fn an_index_declared_invisible_is_maintained_but_never_planned() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 801);
    exec_all(&mut tk, &["CREATE TABLE t (a INT, b INT, KEY idx_a (a) INVISIBLE, KEY idx_b (b))", "INSERT INTO t VALUES (1, 2)"]);
    assert_eq!(
        q(&mut tk, "SELECT index_name, is_visible FROM information_schema.statistics WHERE table_schema = 'test' AND table_name = 't' ORDER BY index_name"),
        [["idx_a", "NO"], ["idx_b", "YES"]]
    );
    assert!(plan_lines(&mut tk, "SELECT a FROM t WHERE a = 1").iter().all(|line| !line.contains("idx_a")));
    assert!(plan_lines(&mut tk, "SELECT b FROM t WHERE b = 2").iter().any(|line| line.contains("idx_b")));
    rows(&mut tk, "ADMIN CHECK INDEX t idx_a");
}

#[test]
fn if_not_exists_and_duplicates() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 802);
    rows(&mut tk, "CREATE TABLE t (a INT)");
    assert_eq!(error(&mut tk, "CREATE TABLE t (a INT)").code, 1050);
    rows(&mut tk, "CREATE TABLE IF NOT EXISTS t (a INT)");
    assert_eq!(warning_codes(&mut tk), ["1050"]);
}

/// Only a single integer primary key is the handle: a clustered string key
/// or no key at all leaves the row with a `_tidb_rowid` or common handle.
#[test]
fn primary_key_sets_the_handle_flag_only_for_an_integer_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 803);
    exec_all(&mut tk, &["CREATE TABLE t (a INT, PRIMARY KEY (a))", "CREATE TABLE s (a VARCHAR(4) PRIMARY KEY)", "CREATE TABLE h (a INT)", "INSERT INTO t VALUES (5)"]);
    assert!(plan_lines(&mut tk, "SELECT a FROM t WHERE a = 5")[0].contains("handle:5"));
    assert!(tk.node_conn().execute("SELECT _tidb_rowid FROM t").is_err());
    rows(&mut tk, "SELECT _tidb_rowid FROM h");
}

fn charset_rows(tk: &mut Session, table: &str) -> Vec<Vec<String>> {
    q(tk, &format!(
        "SELECT column_name, IFNULL(character_set_name, 'NULL'), IFNULL(collation_name, 'NULL'), IFNULL(character_maximum_length, 'NULL') \
         FROM information_schema.columns WHERE table_schema = 'test' AND table_name = '{table}' ORDER BY ordinal_position"
    ))
}

#[test]
fn ddl_resolves_charset_and_collation_per_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 804);
    rows(&mut tk, "CREATE TABLE t1 (c_varchar VARCHAR(10), c_char CHAR(10), c_varbinary VARBINARY(10), c_binary BINARY(3), c_blob BLOB, c_text TEXT, c_tinytext TINYTEXT, c_longblob LONGBLOB, c_vc_cs VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, c_vc_bin VARCHAR(10) BINARY, c_enum ENUM('a','B'), c_set SET('a','B'), c_int INT)");
    assert_eq!(
        charset_rows(&mut tk, "t1"),
        [
            ["c_varchar", "utf8mb4", "utf8mb4_bin", "10"],
            ["c_char", "utf8mb4", "utf8mb4_bin", "10"],
            ["c_varbinary", "NULL", "NULL", "10"],
            ["c_binary", "NULL", "NULL", "3"],
            ["c_blob", "NULL", "NULL", "65535"],
            ["c_text", "utf8mb4", "utf8mb4_bin", "65535"],
            ["c_tinytext", "utf8mb4", "utf8mb4_bin", "255"],
            ["c_longblob", "NULL", "NULL", "4294967295"],
            ["c_vc_cs", "utf8mb4", "utf8mb4_general_ci", "10"],
            ["c_vc_bin", "utf8mb4", "utf8mb4_bin", "10"],
            ["c_enum", "utf8mb4", "utf8mb4_bin", "1"],
            ["c_set", "utf8mb4", "utf8mb4_bin", "3"],
            ["c_int", "NULL", "NULL", "NULL"],
        ]
    );
}

#[test]
fn ddl_column_charset_falls_back_to_the_table_default() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 805);
    rows(&mut tk, "CREATE TABLE t2 (a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4, c VARCHAR(10) CHARACTER SET latin1, d VARCHAR(10) CHARACTER SET binary) DEFAULT CHARSET=latin1");
    assert_eq!(
        charset_rows(&mut tk, "t2").into_iter().map(|row| row[..3].to_vec()).collect::<Vec<_>>(),
        [["a", "latin1", "latin1_bin"], ["b", "utf8mb4", "utf8mb4_bin"], ["c", "latin1", "latin1_bin"], ["d", "NULL", "NULL"]]
    );
    assert_eq!(q(&mut tk, "SELECT table_collation FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 't2'"), [["latin1_bin"]]);
}

#[test]
fn ddl_collate_alone_picks_the_charset_and_a_mismatched_pair_is_rejected() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 806);
    rows(&mut tk, "CREATE TABLE t (a VARCHAR(10) COLLATE latin1_bin)");
    assert_eq!(charset_rows(&mut tk, "t")[0][1..3], ["latin1", "latin1_bin"]);
    error(&mut tk, "CREATE TABLE bad (a VARCHAR(10) CHARACTER SET latin1 COLLATE utf8mb4_bin)");
}

#[test]
fn binary_column_zero_pads_to_its_length() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 807);
    exec_all(&mut tk, &["CREATE TABLE tb (b BINARY(3), vb VARBINARY(3))", "INSERT INTO tb VALUES ('ab','ab')"]);
    assert_eq!(q(&mut tk, "SELECT HEX(b), HEX(vb) FROM tb"), [["616200", "6162"]]);
}
