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

//! Port for `pkg/planner/core/util_test.go:46 TestExtractTableList` — item
//! 1179 of `pkg/planner.part20` (all 1278 `Test*`/`Benchmark*` declarations
//! under `pkg/planner/` on `origin/master`, sorted by file then line,
//! chunked by 60).
//!
//! The function under test is `core.ExtractTableList`
//! (`pkg/planner/core/logical_plan_builder.go:7505-7536`), a wrapper over the
//! `tableListExtractor` AST visitor (:7537-7666) that collects every
//! `ast.TableName`, substituting the alias for the original name when
//! `asName` is set, then dedupes by (schema-lower, name-lower) via nested
//! maps (:7518-7533). This crate's builder explicitly refuses to carry it —
//! `plan_builder.rs` boundary notes name `ExtractTableList`/
//! `tableListExtractor` as a DML/privilege surface with no logical-tree
//! consumer — so the port below is documentary.

/// GO PORT of `pkg/planner/core/util_test.go:46 TestExtractTableList`.
///
/// Contract: 35 table-driven cases (:52-303; two `asName: true` variants at
/// :76 and :127). Each SQL is parsed (:307), wrapped in `resolve.NewNodeW`
/// (:309), and the extraction is compared by LENGTH (:311), then
/// element-wise on (schema-lower, name-lower) AFTER sorting both sides
/// (:312-316; the sort plus the map-based dedupe make extraction order
/// irrelevant). Case inventory with expected extractions:
///
/// - :53 `WITH t AS (SELECT * FROM t2) SELECT * FROM t, t1, mysql.user ...`
///   → t, t1, t2, mysql.user — the CTE name AND the tables inside its body.
/// - :62 `SELECT (SELECT a,b,c FROM t1) AS t ...` → t1 — a TableSource whose
///   body is a SELECT is represented by its FIRST inner table
///   (:7577-7599), never the derived alias.
/// - :68 `SELECT * FROM t, v AS w` → t, v; :75 `asName` → t, w.
/// - :83-100 the AVG-over-derived-table JOIN query → scores, students.
/// - :107 `DELETE FROM x.y z ...` → x.y; :113 `WITH t AS (SELECT * FROM v)
///   DELETE ...` → x.y, v.
/// - :120 `DELETE FROM t1 AS t2 ...` → t1; :126 `asName` → t2.
/// - :133 `UPDATE t1 ... JOIN t2 SET ...` → t1, t2.
/// - :140 `INSERT INTO t ... SELECT ... FROM t1` → t, t1.
/// - :147 `WITH t AS (SELECT * FROM v) SELECT a FROM t UNION SELECT b FROM
///   t1` → v, t, t1.
/// - :155 `LOAD DATA ... INTO TABLE t` → t.
/// - :161 `batch on c limit 10 delete from t ...` → t.
/// - :167 `split table t1 ...` → t1.
/// - :173 `show create table t` → t; :179 `show create database test` →
///   schema-only `test` (ShowStmt DBName branch :7602-7605).
/// - :185 `create database test` → test (CreateDatabaseStmt branch).
/// - :191 `FLASHBACK DATABASE t1 TO t2` → t1, t2 (FlashBackDatabaseStmt).
/// - :198 `flashback table t,t1,test.t2 to timestamp ...` → t, t1, test.t2.
/// - :206 `flashback database test to timestamp ...` → test.
/// - :212 `flashback table t TO t1` → t, t1 (FlashBackTableStmt new-name
///   branch).
/// - :219 `create table t` → t.
/// - :225 `RENAME TABLE t TO t1, test.t2 TO test.t3` → t, t1, test.t2,
///   test.t3.
/// - :234 `drop table test.t, t1` → t1, test.t.
/// - :241 `create view v as (select * from t)` → v, t.
/// - :248 `create sequence ...` → seq.
/// - :254 `CREATE INDEX idx ON t ...` → t.
/// - :260 `LOCK TABLE t1 WRITE, t2 READ` → t1, t2.
/// - :267 `grant select on test.* to u1` → schema-only `test` (GrantStmt
///   DB-level branch :7627-7634).
/// - :273 `BACKUP TABLE a.b,c.d,e TO 'noop://'` → a.b, c.d, e (BRIEStmt
///   schema list).
/// - :281 `TRACE SELECT ...` → t1; :287 `EXPLAIN SELECT ...` → t1; :293
///   `PLAN REPLAYER DUMP EXPLAIN SELECT ...` → t1.
/// - :299 `ALTER TABLE t COMPACT` → t.
///
/// go-parity-gap: the tableListExtractor visitor is explicitly refused by
/// this crate's builder (DML/privilege surface, no logical-tree consumer).
#[test]
#[ignore = "go-parity-gap: ExtractTableList/tableListExtractor is an explicitly refused builder surface"]
fn extract_table_list_35_cases_dedup_and_asname_variants() {}
