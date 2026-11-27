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


//! `tidb_mem_quota_query` with `tidb_mem_oom_action`: a write over the quota
//! is Go's 8175 and leaves no partial effect, LOG lets it finish, and a
//! runaway cross join is cancelled, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

/// The TestKit stack's global OOM action is LOG; Go's memory tests set
/// CANCEL explicitly, as these do.
fn seeded(tk: &mut Session) {
    exec_all(tk, &[
        "SET GLOBAL tidb_mem_oom_action = 'CANCEL'",
        "CREATE TABLE t (a BIGINT, b BIGINT)",
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
    ]);
}

/// Runs `sql` under a one-byte quota and answers its error code.
fn cancelled(tk: &mut Session, sql: &str) -> u16 {
    rows(tk, "SET tidb_mem_quota_query = 1");
    let code = error(tk, sql).code;
    rows(tk, "SET tidb_mem_quota_query = DEFAULT");
    code
}

fn all(tk: &mut Session) -> Vec<Vec<String>> {
    q(tk, "SELECT a, b FROM t ORDER BY a")
}

#[test]
fn an_update_over_the_quota_is_8175_and_changes_nothing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 680);
    seeded(&mut tk);
    let before = all(&mut tk);
    assert_eq!(cancelled(&mut tk, "UPDATE t SET b = b + 1"), 8175);
    assert_eq!(all(&mut tk), before);
    assert_eq!(affected(&mut tk, "UPDATE t SET b = b + 1"), 3);
    assert_eq!(all(&mut tk), [["1", "11"], ["2", "21"], ["3", "31"]]);
}

#[test]
fn a_delete_over_the_quota_is_8175_and_removes_nothing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 681);
    seeded(&mut tk);
    let before = all(&mut tk);
    assert_eq!(cancelled(&mut tk, "DELETE FROM t WHERE a > 1"), 8175);
    assert_eq!(all(&mut tk), before);
    assert_eq!(affected(&mut tk, "DELETE FROM t WHERE a > 1"), 2);
    assert_eq!(all(&mut tk), [["1", "10"]]);
}

#[test]
fn an_insert_over_the_quota_is_8175_and_writes_nothing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 682);
    seeded(&mut tk);
    let before = all(&mut tk);
    assert_eq!(cancelled(&mut tk, "INSERT INTO t VALUES (4, 40)"), 8175);
    assert_eq!(all(&mut tk), before);
    assert_eq!(affected(&mut tk, "INSERT INTO t VALUES (4, 40)"), 1);
    assert_eq!(all(&mut tk).len(), 4);
}

#[test]
fn a_cancelled_cascade_repoints_no_child_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 683);
    exec_all(&mut tk, &[
        "SET GLOBAL tidb_mem_oom_action = 'CANCEL'",
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        "CREATE TABLE c (id BIGINT PRIMARY KEY, pid BIGINT, FOREIGN KEY (pid) REFERENCES p (id) ON UPDATE CASCADE)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (10, 1), (11, 1)",
    ]);
    let before = q(&mut tk, "SELECT id, pid FROM c ORDER BY id");
    assert_eq!(cancelled(&mut tk, "UPDATE p SET id = 100"), 8175);
    assert_eq!(q(&mut tk, "SELECT id FROM p"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT id, pid FROM c ORDER BY id"), before);
    rows(&mut tk, "UPDATE p SET id = 100");
    assert_eq!(q(&mut tk, "SELECT id, pid FROM c ORDER BY id"), [["10", "100"], ["11", "100"]]);
}

#[test]
fn log_lets_an_over_quota_write_finish() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 684);
    seeded(&mut tk);
    exec_all(&mut tk, &["SET GLOBAL tidb_mem_oom_action = 'LOG'", "SET tidb_mem_quota_query = 1"]);
    assert_eq!(affected(&mut tk, "UPDATE t SET b = b + 1"), 3);
}

/// Selection's cached child chunk is charged to the statement.
#[test]
fn selection_cached_chunk_is_part_of_the_query_quota() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 685);
    seeded(&mut tk);
    assert_eq!(cancelled(&mut tk, "SELECT a FROM t WHERE oct(a) > '0'"), 8175);
    assert_eq!(q(&mut tk, "SELECT a FROM t WHERE oct(a) > '0'"), [["1"], ["2"], ["3"]]);
}

fn table_of(tk: &mut Session, n: i64) {
    rows(tk, "SET GLOBAL tidb_mem_oom_action = 'CANCEL'");
    rows(tk, "CREATE TABLE t (a BIGINT, b BIGINT)");
    let values = (0..n).map(|i| format!("({i}, {})", i * 10)).collect::<Vec<_>>().join(", ");
    rows(tk, &format!("INSERT INTO t VALUES {values}"));
    rows(tk, &format!("SET tidb_mem_quota_query = {}", 1 << 18));
}

#[test]
fn a_runaway_cross_join_is_cancelled_not_run_to_exhaustion() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 686);
    table_of(&mut tk, 48);
    assert_eq!(error(&mut tk, "SELECT t1.a FROM t t1, t t2, t t3").code, 8175);
}

#[test]
fn ordinary_joins_are_unaffected_by_the_same_quota() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 687);
    table_of(&mut tk, 48);
    assert_eq!(rows(&mut tk, "SELECT t1.a FROM t t1 JOIN t t2 ON t1.a = t2.a").len(), 48);
    assert_eq!(rows(&mut tk, "SELECT t1.a FROM t t1, t t2").len(), 48 * 48);
}


/// Go `TestUncompressTracksInflateMemory` (c6e3cf83998) at the statement:
/// UNCOMPRESS charges its declared length to the statement while it
/// inflates, so a payload declaring 64 MiB is Go's 8175 under a 1 MiB quota
/// with CANCEL (gomaster probe), while LOG and the default quota answer the
/// value, row by row and vectorized alike.
#[test]
fn uncompress_charges_its_declared_length_to_the_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 685);
    let declared = "select uncompress(concat(unhex('00000004'), substr(compress('hello'), 5)))";
    let vectorized = "select uncompress(concat(unhex('00000004'), substr(compress(a), 5))) from \
        (select 'hello' a union all select 'world') t order by 1";
    assert_eq!(q(&mut tk, declared), [["hello"]]);
    exec_all(&mut tk, &["SET GLOBAL tidb_mem_oom_action = 'CANCEL'", "SET tidb_mem_quota_query = 1048576"]);
    assert_eq!(error(&mut tk, declared).code, 8175);
    assert_eq!(error(&mut tk, vectorized).code, 8175);
    exec_all(&mut tk, &["SET GLOBAL tidb_mem_oom_action = 'LOG'"]);
    assert_eq!(q(&mut tk, declared), [["hello"]]);
    // Go: the charge is held while inflating and released afterwards.
    let (memory, _) = tk.stmt_ctx_trackers().expect("the statement's trackers");
    assert!(memory.max_consumed() >= 1 << 26, "max {}", memory.max_consumed());
    assert!(memory.bytes_consumed() < 1 << 26, "held {}", memory.bytes_consumed());
    assert_eq!(q(&mut tk, vectorized), [["hello"], ["world"]]);
}
