# Port Go multi-valued indexes to the Rust TiDB

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.


## Purpose / Big Picture

A multi-valued index (MV index) indexes every element of a JSON array instead of the JSON document as a whole. In SQL it is an expression index whose key part is `CAST(<json> AS <type> ARRAY)`, for example `create table t(a int, j json, index kj((cast(j as signed array))))`. Queries filter it with `<v> MEMBER OF (j)`, `json_contains(j, ...)` and `json_overlaps(j, ...)`, and Go TiDB answers them through an IndexMerge over the MV index.

Today the Rust server refuses the DDL outright ("a multi-valued index (CAST(... AS ... ARRAY)) is not supported yet", raised in `rust/crates/tidb-executor/src/expression_index.rs`). Every later statement on that table then fails with "Table ... doesn't exist", which is why 14 recorded integration topics diverge in cascades (`planner/core/plan_cache`, `planner/core/indexmerge_path`, `planner/core/issuetest/planner_issue`, `planner/core/casetest/pushdown/push_down`, `planner/core/casetest/index/index`, `planner/core/casetest/physicalplantest/physical_plan`, `ddl/db_rename`, `ddl/default_as_expression`, `executor/foreign_key`, `executor/insert`, `executor/index_lookup_pushdown`, `expression/json`, `expression/multi_valued_index`, `globalindex/multi_valued_index`). Worse, a table that Go created with an MV index on a real TiKV cluster is loaded by `rust/crates/tidb-server/src/cluster_session.rs` (it records the key part through `KvTable::set_mv_key_part_source`), but the Rust write path has no MV key expansion, so a Rust INSERT into such a table would write a wrong index entry.

After this work, `create table ... index kj((cast(j as signed array)))` succeeds, INSERT/UPDATE/DELETE maintain one index entry per distinct array element exactly as Go does, `select * from t where 1 member of (j)` plans an IndexMerge over `kj` and answers the right rows, and the topics above stop diverging on MV statements. Observe it with the integration replay (see Validation).


## Progress

- [ ] M1: DDL accepts MV key parts (hidden array column, `IndexInfo.MVIndex`), with Go's validation errors.
- [ ] M2: `CAST(json AS T ARRAY)` evaluates as Go's `castJSONAsArrayFunctionSig`.
- [ ] M3: DML index maintenance expands MV keys (Go `index.getIndexedValue`, `NewMultiValueIndexKVGenerator`), including unique MV indexes and `Exist`.
- [ ] M4: Planner MV IndexMerge paths (`pkg/planner/core/indexmerge_path.go` MV half).
- [ ] M5: Executor reads MV partial paths (IndexMerge handle de-duplication, JSON comparison for lookups).
- [ ] M6: ADMIN CHECK TABLE/INDEX and ANALYZE over MV indexes.
- [ ] M7: Enroll the MV topics that replay clean; record residue.


## Surprises & Discoveries

- Observation: the planner already carries partial MV awareness (`is_multi_valued` on index metadata, the `contain_mv_path` rule in `rust/crates/tidb-planner/src/access_path/index_merge.rs`, MV paths kept by the prefer-range filter), but no MV partial-path generation from `MEMBER OF`/`json_contains`/`json_overlaps`.
  Evidence: `grep -rn json_contains rust/crates/tidb-planner/src` finds nothing outside tests.
- Observation: the only producer of `mv_key_part_sources` is the cluster loader; local DDL never records one.
  Evidence: `grep -rn set_mv_key_part_source rust/crates/*/src`.


## Decision Log

- Decision: port by Go call site, milestone by milestone, DDL first.
  Rationale: every downstream milestone needs a table with an MV index to test against, and the DDL refusal is what cascades through the recorded topics.
  Date/Author: 2026-10-09 / Claude.
- Decision: the write path (M3) lands before any planner work (M4).
  Rationale: a Go-created MV table on a real cluster is already loadable; writing it without MV expansion corrupts the index, which is worse than a missing plan.
  Date/Author: 2026-10-09 / Claude.


## Outcomes & Retrospective

Nothing yet.


## Context and Orientation

Terms. An expression index stores, per row, the value of an expression rather than of a plain column. TiDB implements it by adding a hidden virtual generated column (named like `_V$_kj_0`) whose generation expression is the indexed expression, and indexing that column. An MV index is an expression index whose expression is `CAST(... AS ... ARRAY)`; the hidden column's field type has the array flag (Go `FieldType.IsArray()`, Rust `tidb_datatype::FieldType::is_array` in `rust/crates/tidb-datatype/src/field_type/mod.rs`), and `IndexInfo.MVIndex` is true (Rust model field `mv_index` in `rust/crates/tidb-model/src/index.rs`). A row whose array is `[1, 2, 2]` writes two index keys, one for `1` and one for `2`; a NULL array writes one key with NULL; an empty array writes no key.

Go sources, all in this branch's `pkg/` (the source of truth):

- DDL: `pkg/ddl/create_table.go` (hidden column construction, around line 1649 `colInfo.FieldType.IsArray()`), `pkg/ddl/index.go` (`checkIndexColumn` and friends, lines ~145 and ~249 reject JSON key parts that are not arrays and constrain array ones), `pkg/ddl/generated_column.go` (`hasCastArrayFunc`, `disallowCastArrayFunc`).
- Expression: `pkg/expression/builtin_cast.go` around line 2634 (`tp.IsArray()` selects the cast-as-array signature), `pkg/expression/builtin_json.go` line ~280.
- Planner rewriter gate: `pkg/planner/core/expression_rewriter.go` line ~1762 (`allowBuildCastArray`).
- Write path: `pkg/table/tables/index.go` `(*index).getIndexedValue` (line ~214) expands an MV row into one value tuple per distinct element, hashing elements with `BinaryJSON.HashValue`; `GenIndexKVIter` (line ~682) and `Exist` use it; `pkg/table/index.go` `NewMultiValueIndexKVGenerator`. `pkg/executor/insert_common.go` line ~763 maps array conversion errors.
- Planner paths: `pkg/planner/core/indexmerge_path.go` lines ~410-1200 (`generateMVIndexMergePartialPaths4And`, `generateANDIndexMerge4MVIndex`, `buildPartialPaths4MVIndex`, `buildPartialPath4MVIndex`, `collectFilters4MVIndex`, `CollectFilters4MVIndexMutations`, `cleanAccessPathForMVIndexHint`, `isMVIndexPath`).
- Executor: `pkg/executor/distsql.go` line ~2108 (`tables.CompareIndexAndVal` with the array flag) and the IndexMerge reader.

Rust locations: DDL expression indexes in `rust/crates/tidb-executor/src/expression_index.rs` (the MV refusal is in the `cast.array` arm); index key generation in `rust/crates/tidb-executor/src/kv_table/index_entries.rs` (`index_values`, `index_key`); the table model in `rust/crates/tidb-executor/src/kv_table.rs` (`mv_key_part_sources`); planner access paths in `rust/crates/tidb-planner/src/access_path/index_merge.rs`; IndexMerge physical construction in `rust/crates/tidb-planner/src/find_best_task/index_merge_union.rs` and `index_merge_intersection.rs`; executor readers in `rust/crates/tidb-executor/src/access_path.rs`.


## Plan of Work

M1 (DDL). In `expression_index.rs`, replace the `cast.array` refusal with Go's construction: the hidden column takes the cast's array field type, the index is marked `mv_index`, and `KvTable::set_mv_key_part_source` records the key part's source column exactly as the cluster loader does, so local and loaded tables look identical to the planner. Port Go's validations (a non-array JSON key part, more than one array key part, an array part in a primary or clustered key, unsupported element types) with their exact error codes and messages, checked against a Go oracle run.

M2 (expression). Port `castJSONAsArrayFunctionSig`: evaluate the JSON argument, require an array (Go wraps a scalar into a one-element array; confirm in the oracle), convert each element to the target type with Go's error behaviour, and return the JSON array.

M3 (write path). Add `KvTable::mv_index_values(index, row) -> Vec<Vec<Datum>>` mirroring `getIndexedValue` (dedupe by Go's `HashValue` bytes, NULL gives one tuple, empty array gives none) and route every index write, delete and uniqueness check through it when the index is MV. Unique MV indexes check each expanded key.

M4 (planner). Port the MV half of `indexmerge_path.go` into `rust/crates/tidb-planner/src/access_path/index_merge.rs` (new submodule `mv_index.rs` if it grows past a few hundred lines), keeping Go's function boundaries and names.

M5 (executor). Teach the IndexMerge reader to read MV partial paths (point and range over the array element), de-duplicate handles across partial paths, and compare JSON element values as `CompareIndexAndVal` does.

M6 (maintenance). ADMIN CHECK TABLE/INDEX recomputes MV keys with M3's expansion; ANALYZE treats MV indexes as Go does (confirm whether Go skips them).

M7 (validation). Replay each MV topic, fix residue, enroll clean topics in `rust/difftests/result-tests/src/enrolled_topics.rs`.


## Concrete Steps

All commands run from the repository root `/Users/qiliu/projects/tidb` unless stated.

Replay one topic and list its divergences:

    cd rust && INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC=expression/multi_valued_index cargo test -p difftest-result-tests --test integration_diff -- --ignored --nocapture replay_one_topic

Run a Go oracle query: add a `zz_*_test.go` file to a Go worktree's `pkg/executor/test/cte/` using the `zzRun` helper there, then

    go test -tags=intest,deadlock ./pkg/executor/test/cte/ -run 'TestZZ<Name>$' -count=1 -v

Owning-crate tests while iterating:

    cd rust && cargo nextest run -p tidb-executor -p tidb-session -p tidb-planner


## Validation and Acceptance

M1 is accepted when `create table t(a int, j json, index kj((cast(j as signed array))))` succeeds and `show create table t` prints Go's text. M3 when, after `insert into t values (1, '[1,2,2]')`, a raw index scan of `kj` finds exactly the keys for 1 and 2, and `admin check table t` passes. M4/M5 when `explain select * from t where 1 member of (j)` matches Go's recorded IndexMerge plan and the query returns the row. Each milestone adds a regression test in `rust/crates/tidb-session/src/` that fails before and passes after, and the topics in Purpose lose their MV divergences.

Repository gates (AGENTS.md): `cd rust && cargo build --locked -p tidb-server` before every commit and push.


## Idempotence and Recovery

Every step is a source edit plus tests; rerunning is safe. A milestone that cannot match Go stays behind the existing refusal rather than shipping a partial write path: M1 must not land without M3, because accepting MV DDL while writing wrong index keys corrupts data. Land M1+M2+M3 together.


## Artifacts and Notes

The cascade that motivated the plan, from `planner/core/casetest/physicalplantest/physical_plan`:

    create table t(a int, j json, index kj((cast(j as signed array))));
      => ERR a multi-valued index (CAST(... AS ... ARRAY)) is not supported yet
    insert into t values(1, '[1,2,3]');
      => ERR Table 'test.t' doesn't exist

Go oracle behaviour (this branch, captured 2026-10-09 with a `zz_mv_test.go` probe), which every milestone must reproduce:

    create table t(a int, j json, index kj((cast(j as signed array))))          => ok
    information_schema.columns for t                                            => only a and j (the hidden column is not listed)
    insert into t values (1,'[1,2,2]'),(2,null),(3,'[]'),(4,'[3]'),(5,'7')      => ok (a scalar 7 becomes [7])
    select a from t where 2 member of (j)                                       => 1
    explain select a from t where 2 member of (j)
      Projection / IndexMerge type: union / IndexRangeScan index:kj(cast(`j` as signed array)) range:[2,2] / TableRowIDScan
    explain select /*+ use_index_merge(t, kj) */ a from t where json_contains(j, '[1,2]')
      IndexMerge type: intersection over ranges [1,1] and [2,2]
    explain select /*+ use_index_merge(t, kj) */ a from t where json_overlaps(j, '[1,3]')
      Selection json_overlaps(...) over IndexMerge type: union over [1,1] and [3,3]
    select a from t where json_overlaps(j, '[1,3]') order by a                  => 1, 4
    insert into t values (6, '{"a":1}')
      => [expression:1235]This version of TiDB doesn't yet support 'CAST-ing JSON OBJECT type to array'
    insert into t values (6, '["x"]')
      => [expression:3903]Invalid JSON value for CAST for expression index 'kj'
    select cast('[1,2]' as signed array)
      => [expression:1235]This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'
    create table t2(j json, index k((cast(j as signed array)), (cast(j as unsigned array))))
      => [ddl:1235]This version of TiDB doesn't yet support 'more than one multi-valued key part per index'
    create table t3(j json, primary key((cast(j as signed array))))
      => [ddl:3756]The primary key cannot be an expression index
    create table t4(j json, unique index k((cast(j->'$.a' as char(10) array)))) => ok
    insert into t4 values ('{"a":["x","y"]}'), ('{"a":["y"]}')
      => [kv:1062]Duplicate entry '["y"]' for key 't4.k'
    create table t5(j json, index k((cast(j as double array))))                => ok
    create table t6(j json, index k((cast(j as json array))))
      => [expression:1235]This version of TiDB doesn't yet support 'CAST-ing data to array of json BINARY'
    alter table t add index k2((cast(j as unsigned array)))                     => ok
    update t set j = '[9]' where a = 1; then 1 member of (j) => none, 9 member of (j) => 1
    delete from t where a = 4; admin check table t                              => ok


## Interfaces and Dependencies

In `rust/crates/tidb-executor/src/kv_table/index_entries.rs`, define

    pub(crate) fn mv_index_values(&self, index: &KvIndex, row: &[Datum]) -> Vec<Vec<Datum>>

returning Go `getIndexedValue`'s tuples. In `rust/crates/tidb-planner/src/access_path/index_merge.rs` (or a `mv_index` submodule), port `buildPartialPaths4MVIndex` as

    fn build_partial_paths_for_mv_index(ds: &DataSource, access_filters: &[Expression], idx_cols: &[Column], mv_index: &IndexMeta) -> Result<(Vec<AccessPath>, bool, bool), PlanError>

keeping Go's three results (paths, isIntersection, ok).


Revision note (2026-10-09): initial plan, written when batch 18 of the Go-alignment work found the DDL refusal cascading through `physical_plan`.
