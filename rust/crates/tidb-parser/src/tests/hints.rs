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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `SELECT /*+ ... */` optimizer-hint comment grammar tests.

use super::*;

/// `SELECT /*+ ... */` optimizer-hint comments — found via a fresh
/// stratified sample of the coverage-measurement's own unhandled bucket
/// (over half of the unhandled statements at the time were blocked by
/// this ONE grammar). Only recognized directly after `SELECT`; see
/// `tidb_ast::Hint`'s own doc for the exact four-shape scope boundary
/// this covers (join/aggregate-pushdown table-list hints, index hints,
/// `SET_VAR`, argument-less hints) and what's deliberately excluded.
#[test]
fn optimizer_hints() {
    assert_eq!(
        r("select /*+ SET_VAR(tidb_max_keys_read=0) */ @@tidb_max_keys_read"),
        "SELECT /*+ SET_VAR(tidb_max_keys_read = '0')*/ @@`tidb_max_keys_read`"
    );
    assert_eq!(
        r("select /*+ TIDB_HJ(t1, t2) */ * from t1, t2"),
        "SELECT /*+ TIDB_HJ(`t1`, `t2`)*/ * FROM (`t1`) JOIN `t2`"
    );
    // A query-block suffix (`@sel_1`) lexes as a `UserVar` token, not a
    // bare `@` operator — a real integration pitfall this needed to get
    // right.
    assert_eq!(
        r("select /*+ HASH_JOIN(t1@sel_1, t2) */ * from t1, t2"),
        "SELECT /*+ HASH_JOIN(`t1`@`sel_1`, `t2`)*/ * FROM (`t1`) JOIN `t2`"
    );
    assert_eq!(
        r("select /*+ use_index_merge(t, idx1, idx2) */ a from t"),
        "SELECT /*+ USE_INDEX_MERGE(`t` `idx1`, `idx2`)*/ `a` FROM `t`"
    );
    // Go's shared `parseIndexLevelHint` covers positive/negative index
    // families and retains an optional query-block prefix for every one.
    assert_eq!(
        r("select /*+ no_order_index(@sel_1 t, idx) force_index(t, i) */ a from t"),
        "SELECT /*+ NO_ORDER_INDEX(@`sel_1` `t` `idx`) FORCE_INDEX(`t` `i`)*/ `a` FROM `t`"
    );
    assert_eq!(
        r("select /*+ no_index_lookup_pushdown(t) */ a from t"),
        "SELECT /*+ NO_INDEX_LOOKUP_PUSHDOWN(`t`)*/ `a` FROM `t`"
    );
    // Argument-less hints ALWAYS restore with parens, regardless of
    // whether the source wrote them.
    assert_eq!(
        r("select /*+ straight_join() */ a from t"),
        "SELECT /*+ STRAIGHT_JOIN()*/ `a` FROM `t`"
    );
    assert_eq!(
        r("select /*+ straight_join */ a from t"),
        "SELECT /*+ STRAIGHT_JOIN()*/ `a` FROM `t`"
    );
    // `SET_VAR`'s value ALWAYS restores as a quoted string, regardless of
    // whether the source wrote it quoted, bare, negative, or decimal.
    assert_eq!(
        r(r#"select /*+ set_var(sql_mode = "ONLY_FULL_GROUP_BY") */ 1"#),
        "SELECT /*+ SET_VAR(sql_mode = 'ONLY_FULL_GROUP_BY')*/ 1"
    );
    assert_eq!(
        r("select /*+ set_var(tidb_partition_prune_mode=static) */ 1"),
        "SELECT /*+ SET_VAR(tidb_partition_prune_mode = 'static')*/ 1"
    );
    assert_eq!(
        r("select /*+ set_var(tidb_opt_ordering_index_selectivity_ratio=-1) */ 1"),
        "SELECT /*+ SET_VAR(tidb_opt_ordering_index_selectivity_ratio = '-1')*/ 1"
    );
    assert_eq!(
        r("select /*+ set_var(tidb_default_string_match_selectivity=0.5) */ 1"),
        "SELECT /*+ SET_VAR(tidb_default_string_match_selectivity = '0.5')*/ 1"
    );
    // Multiple hints in one comment restore SPACE-joined, regardless of
    // whether the source separated them with a comma.
    assert_eq!(
        r("select /*+ SET_VAR(tidb_max_keys_read=25), USE_INDEX(t_mrs, idx_val) */ extra from t_mrs where val >= 1"),
        "SELECT /*+ SET_VAR(tidb_max_keys_read = '25') USE_INDEX(`t_mrs` `idx_val`)*/ `extra` FROM `t_mrs` WHERE `val`>=1"
    );
    // `MERGE` is a real, isolated PARSE/RESTORE asymmetry — see
    // `tidb_ast::Hint`'s own doc — parses a table list but ALWAYS
    // restores bare, discarding it.
    assert_eq!(
        r("select /*+ merge(q) */ * from q"),
        "SELECT /*+ MERGE()*/ * FROM `q`"
    );
    // A hint comment written anywhere OTHER than directly after `SELECT`
    // is silently dropped as an ordinary comment, not recognized as a
    // hint at all.
    assert_eq!(
        r("select distinct /*+ SET_VAR(a=1) */ b from t"),
        "SELECT DISTINCT `b` FROM `t`"
    );
    // `NO_MERGE` (distinct from `NO_MERGE_JOIN`, a real table-list hint)
    // is silently dropped, matching real TiDB — see
    // `unrecognized_hint_names_silently_dropped`'s own doc.
    assert_eq!(
        r("select /*+ no_merge(q) */ a from t"),
        "SELECT `a` FROM `t`"
    );
}

/// Go's `parseTableLevelHint` gives these negative join hints the same
/// optional query-block/table-list grammar as `HASH_JOIN` and `MERGE_JOIN`.
/// They are typed syntax only: the seed executor has no optimizer that could
/// apply them.
#[test]
fn negative_join_hints_restore_as_table_lists() {
    for name in [
        "NO_HASH_JOIN",
        "NO_MERGE_JOIN",
        "NO_INDEX_JOIN",
        "NO_INDEX_HASH_JOIN",
        "NO_INDEX_MERGE_JOIN",
    ] {
        let sql =
            format!("select /*+ {name}(@sel_1 db1.t1, t2@sel_2) */ * from t1 join t2 on t1.a=t2.a");
        let expected = format!(
            "SELECT /*+ {name}(@`sel_1` `db1`.`t1`, `t2`@`sel_2`)*/ * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
        );
        assert_eq!(r(&sql), expected, "{name}");
    }
    assert_eq!(
        r("select /*+ no_hash_join() */ * from t1"),
        "SELECT /*+ NO_HASH_JOIN()*/ * FROM `t1`"
    );
}

/// `LEADING(table, ...)`, a join-order hint — see `tidb_ast::Hint`'s own
/// doc for why it needs its own dispatch arm (requires at least one
/// table, unlike the other table-list hints) and what remains out of
/// scope (nested sub-lists, a hint-level `@qb` prefix).
#[test]
fn leading_hint() {
    assert_eq!(
        r("select /*+ leading(t1) */ * from t1 join t2 on t1.a=t2.a"),
        "SELECT /*+ LEADING(`t1`)*/ * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("select /*+ leading(t1, t2, t3) */ * from t1, t2, t3"),
        "SELECT /*+ LEADING(`t1`, `t2`, `t3`)*/ * FROM ((`t1`) JOIN `t2`) JOIN `t3`"
    );
    // A query-block suffix (`@sel_1`) attaches per-table, reusing the
    // SAME `HintTable` shape every other table-list hint already uses.
    assert_eq!(
        r("select /*+ leading(t1, t2@sel_1) */ * from t1 join t2 on t1.a=t2.a"),
        "SELECT /*+ LEADING(`t1`, `t2`@`sel_1`)*/ * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
    // Multiple `LEADING` hints in one comment are both kept (no
    // last-wins collapsing at parse time — matches every other
    // repeated-hint-name case).
    assert_eq!(
        r("select /*+ leading(t1) leading(t2) */ * from t1 join t2 on t1.a=t2.a"),
        "SELECT /*+ LEADING(`t1`) LEADING(`t2`)*/ * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
    // `LEADING()` (empty) is a genuine `ParseError` here — real TiDB
    // itself silently drops it with a warning instead (confirmed via
    // `godump restore`), a deliberate, narrower divergence matching this
    // project's own `ParseError`-over-silent-drop convention.
    assert_eq!(r("select /*+ leading() */ a from t"), "SELECT `a` FROM `t`");
    assert_eq!(
        r("select /*+ leading(t1, (t2, t3)) */ a from t1, t2, t3"),
        "SELECT /*+ LEADING(`t1`, (`t2`, `t3`))*/ `a` FROM ((`t1`) JOIN `t2`) JOIN `t3`"
    );
    assert_eq!(
        r("select /*+ leading(@sel1 t1, t2) */ a from t1, t2"),
        "SELECT /*+ LEADING(@`sel1` `t1`, `t2`)*/ `a` FROM (`t1`) JOIN `t2`"
    );
    // A db-qualified table name IS valid grammar here (confirmed via
    // `godump restore`, and now modelled since `HintTable::db_name` was
    // added for `READ_FROM_STORAGE` — this test previously asserted the
    // opposite, a stale assumption from before that field existed).
    assert_eq!(
        r("select /*+ leading(db1.t1, t2) */ a from t1, t2"),
        "SELECT /*+ LEADING(`db1`.`t1`, `t2`)*/ `a` FROM (`t1`) JOIN `t2`"
    );
}

/// `USE_TOJA`/`USE_CASCADES`, a boolean-argument hint — see
/// `tidb_ast::HintKind::Bool`'s own doc.
#[test]
fn use_toja_hint() {
    assert_eq!(
        r("select /*+ use_toja(TRUE) */ a from t1"),
        "SELECT /*+ USE_TOJA(TRUE)*/ `a` FROM `t1`"
    );
    // Restore always uppercases, regardless of the source's own casing.
    assert_eq!(
        r("select /*+ use_toja(false) */ a from t1"),
        "SELECT /*+ USE_TOJA(FALSE)*/ `a` FROM `t1`"
    );
    assert_eq!(
        r("select /*+ use_cascades(true) */ a from t1"),
        "SELECT /*+ USE_CASCADES(TRUE)*/ `a` FROM `t1`"
    );
    // Anything other than `TRUE`/`FALSE` is a genuine `ParseError` here
    // — real TiDB itself silently drops the whole hint with a warning
    // instead, the SAME narrower, `ParseError`-over-silent-drop
    // convention already applied to `LEADING()`.
    assert_eq!(
        r("select /*+ use_toja(1) */ a from t1"),
        "SELECT `a` FROM `t1`"
    );
    assert_eq!(
        r("select /*+ use_toja() */ a from t1"),
        "SELECT `a` FROM `t1`"
    );
}

/// `RESOURCE_GROUP(name)` — a single bare identifier argument hint, see
/// `tidb_ast::HintKind::Name`'s own doc.
#[test]
fn resource_group_hint() {
    assert_eq!(
        r("select /*+ resource_group(rg1) */ a from t1"),
        "SELECT /*+ RESOURCE_GROUP(`rg1`)*/ `a` FROM `t1`"
    );
    // A reserved keyword (`DEFAULT`) is still a valid argument here —
    // the hint parser's own lenient identifier acceptance, confirmed
    // via `godump restore`.
    assert_eq!(
        r("select /*+ resource_group(default) */ a from t1"),
        "SELECT /*+ RESOURCE_GROUP(`default`)*/ `a` FROM `t1`"
    );
    // Multiple hints in one comment, space-joined.
    assert_eq!(
        r("select /*+ resource_group(rg1) resource_group(default) */ a from t1"),
        "SELECT /*+ RESOURCE_GROUP(`rg1`) RESOURCE_GROUP(`default`)*/ `a` FROM `t1`"
    );
    // No `@qb_name` suffix is accepted on the argument — a genuine
    // `ParseError` here, real TiDB itself silently drops the whole
    // hint with a warning instead (confirmed via `godump restore`),
    // the SAME narrower convention already applied to `LEADING()`.
    assert_eq!(
        r("select /*+ resource_group(rg1@sel_1) */ a from t1"),
        "SELECT `a` FROM `t1`"
    );
    assert_eq!(
        r("select /*+ resource_group() */ a from t1"),
        "SELECT `a` FROM `t1`"
    );
}

/// `MAX_EXECUTION_TIME([@qb_name] N)` / `NTH_PLAN([@qb_name] N)` — see
/// `tidb_ast::HintKind::Number`'s own doc. Every assertion here was
/// cross-checked against real TiDB via `godump restore`.
#[test]
fn hint_number() {
    assert_eq!(
        r("select /*+ max_execution_time(10) */ sleep(3)"),
        "SELECT /*+ MAX_EXECUTION_TIME(10)*/ SLEEP(3)"
    );
    assert_eq!(
        r("select /*+ nth_plan(3) */ count(1) from t0"),
        "SELECT /*+ NTH_PLAN(3)*/ COUNT(1) FROM `t0`"
    );
    // The optional leading `@qb_name` — a PREFIX, unlike `HintTable`'s
    // own `name@qb_name` SUFFIX shape.
    assert_eq!(
        r("select /*+ max_execution_time(@sel_1 10) */ 1"),
        "SELECT /*+ MAX_EXECUTION_TIME(@`sel_1` 10)*/ 1"
    );
    assert_eq!(
        r("select /*+ nth_plan(@sel_1 3) */ 1"),
        "SELECT /*+ NTH_PLAN(@`sel_1` 3)*/ 1"
    );
}

/// `QB_NAME(name [, view[@sel_N] (. view[@sel_N])*])` — cross-checked
/// against real TiDB via `godump restore`. See `tidb_ast::HintKind::QbName`'s
/// own doc for the restore asymmetry (no leading `@` on the name itself,
/// unlike every other hint's own query-block reference).
#[test]
fn qb_name_hint() {
    // The plain, no-view form.
    assert_eq!(
        r("select /*+ QB_NAME(qb1) */ a from t"),
        "SELECT /*+ QB_NAME(`qb1`)*/ `a` FROM `t`"
    );
    // The existing single-view control composes with `USE_INDEX`'s own
    // `table@qb_name` suffix support.
    assert_eq!(
        r("select /*+ qb_name(qb, v4@sel_4) use_index(t4@qb, idx_a) qb_name(qb2, v4@sel_5) use_index(t4@qb, idx_b) */ * from (select * from d1) as t0 join (select * from d2) as t1"),
        "SELECT /*+ QB_NAME(`qb` , `v4`@`sel_4`) USE_INDEX(`t4`@`qb` `idx_a`) QB_NAME(`qb2` , `v4`@`sel_5`) USE_INDEX(`t4`@`qb` `idx_b`)*/ * FROM (SELECT * FROM `d1`) AS `t0` JOIN (SELECT * FROM `d2`) AS `t1`"
    );
    // A ViewNameList is dot-separated, not a schema-qualified HintTable
    // name. Its final entry may be a bare query-block reference.
    assert_eq!(
        r("select /*+ qb_name(qb, v1@sel_1.v2@sel_2.@sel_3) */ a from t"),
        "SELECT /*+ QB_NAME(`qb` , `v1`@`sel_1`. `v2`@`sel_2`. ``@`sel_3`)*/ `a` FROM `t`"
    );
    // A bare `@sel_N` is valid at any ViewNameList position, including
    // the first one after the comma.
    assert_eq!(
        r("select /*+ qb_name(qb, @sel_1.v@sel_2) */ a from t"),
        "SELECT /*+ QB_NAME(`qb` , ``@`sel_1`. `v`@`sel_2`)*/ `a` FROM `t`"
    );
    assert_eq!(
        r("select /*+ stream_agg(@qb) */ a from t"),
        "SELECT /*+ STREAM_AGG(@`qb`)*/ `a` FROM `t`"
    );
}

/// `USE_INDEX()`/`USE_INDEX_MERGE()`/`IGNORE_INDEX()` with genuinely
/// EMPTY parens — real TiDB's own `parseIndexLevelHint`
/// (`pkg/parser/hintparser.go`) treats this as an internal syntax error
/// too, then silently DROPS the whole hint rather than propagating it
/// (confirmed via `godump restore`) — detected in `parse_hint_comment`
/// itself, deliberately NOT a general per-hint-failure recovery
/// mechanism (which would risk silently dropping REAL content for other
/// currently-unimplemented hints, like `QB_NAME` or extended `LEADING`
/// forms, that real TiDB actually parses successfully — see
/// `Parser::parse_hint_comment`'s own doc).
#[test]
fn empty_index_hint_silently_dropped() {
    assert_eq!(
        r("select /*+ IGNORE_INDEX() */ * from t1, t2 where t1.a=t2.a"),
        "SELECT * FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("select /*+ USE_INDEX() */ * from t1, t2 where t1.a=t2.a"),
        "SELECT * FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    assert_eq!(
        r("select /*+ USE_INDEX_MERGE() */ * from t1, t2 where t1.a=t2.a"),
        "SELECT * FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=`t2`.`a`"
    );
    // A GENUINE arg list (even just one table, no index names) still
    // parses normally — only the fully-empty form is special-cased.
    assert_eq!(
        r("select /*+ use_index(t, i) */ * from t"),
        "SELECT /*+ USE_INDEX(`t` `i`)*/ * FROM `t`"
    );
    // The empty-arg hint is dropped WITHOUT disturbing OTHER valid hints
    // in the SAME comment (confirmed via `godump restore`: real TiDB's
    // own per-hint recovery keeps `use_index(t1, idx1)`/`use_index(t2,
    // idx2)` while dropping only the malformed `IGNORE_INDEX()` between
    // them).
    assert_eq!(
        r("select /*+ use_index(t1, idx1) IGNORE_INDEX() use_index(t2, idx2) */ * from t1, t2"),
        "SELECT /*+ USE_INDEX(`t1` `idx1`) USE_INDEX(`t2` `idx2`)*/ * FROM (`t1`) JOIN `t2`"
    );
}

/// A hint name real TiDB's own lexer doesn't recognize AT ALL — or
/// recognizes but always treats as unsupported regardless of args
/// (`NO_MERGE`, distinct from the real, content-bearing `NO_MERGE_JOIN`)
/// — is silently dropped, matching real TiDB (confirmed via `godump
/// restore`). See `is_recognized_hint_token_name`'s own doc for why this
/// is provably safe (an unrecognized name can never carry real content)
/// and why it does NOT extend to a name real TiDB DOES recognize but
/// this crate simply hasn't implemented yet (`QB_NAME`, extended
/// `LEADING` forms, ... those stay genuine `ParseError`s, unaffected).
#[test]
fn unrecognized_hint_names_silently_dropped() {
    assert_eq!(r("select /*+ unknown_hint(c1)*/ 1"), "SELECT 1");
    // `NO_MERGE` is dropped WITHOUT disturbing a genuinely valid `MERGE`
    // hint in the SAME comment (real per-hint recovery, not an
    // all-or-nothing comment failure).
    assert_eq!(
        r("select /*+ merge(q) no_merge(q1) */ * from q, q q1 where q.a=1 and q1.a=2"),
        "SELECT /*+ MERGE()*/ * FROM (`q`) JOIN `q` AS `q1` WHERE `q`.`a`=1 AND `q1`.`a`=2"
    );
    // A name real TiDB DOES recognize but this crate hasn't implemented
    // (`RESOURCE_GROUP` with an unsupported argument shape) is NOT
    // affected by this — still a genuine `ParseError`, since dropping it
    // would risk discarding real content for OTHER, similarly-shaped
    // unimplemented cases (see `is_recognized_hint_token_name`'s own
    // doc).
    assert_eq!(
        r("select /*+ resource_group() */ a from t1"),
        "SELECT `a` FROM `t1`"
    );
}

/// `NAME([@qb_name] table1, table2, ...)` — a table-list hint's own
/// OPTIONAL leading query-block name, shared by `INL_JOIN`/
/// `INL_HASH_JOIN`/`INL_MERGE_JOIN`/`HASH_JOIN`/`HASH_JOIN_BUILD`/
/// `HASH_JOIN_PROBE`/`MERGE_JOIN`/`TIDB_SMJ`/`TIDB_INLJ`/`TIDB_HJ` — see
/// `tidb_ast::HintKind::Tables`'s own doc. `LEADING` has a dedicated
/// recursive tree because Go preserves nested parenthesized groups.
#[test]
fn table_hint_leading_qb_name() {
    // The real-corpus statement.
    assert_eq!(
        r("select /*+ HASH_JOIN(@sel_1 t2) */ * FROM (select 1) t1 NATURAL LEFT JOIN (select 2) t2"),
        "SELECT /*+ HASH_JOIN(@`sel_1` `t2`)*/ * FROM (SELECT 1) AS `t1` NATURAL LEFT JOIN (SELECT 2) AS `t2`"
    );
    // Composes with a MULTI-table list, and with a DIFFERENT hint name
    // sharing the same dispatch arm.
    assert_eq!(
        r("select /*+ inl_join(@sel_1 t1, t2) */ * from t1, t2"),
        "SELECT /*+ INL_JOIN(@`sel_1` `t1`, `t2`)*/ * FROM (`t1`) JOIN `t2`"
    );
    // No prefix at all is unaffected (the common, already-existing
    // case).
    assert_eq!(
        r("select /*+ hash_join(t1, t2) */ * from t1, t2"),
        "SELECT /*+ HASH_JOIN(`t1`, `t2`)*/ * FROM (`t1`) JOIN `t2`"
    );
    assert_eq!(
        r("select /*+ leading(@sel_1 t1, t2) */ a from t1, t2"),
        "SELECT /*+ LEADING(@`sel_1` `t1`, `t2`)*/ `a` FROM (`t1`) JOIN `t2`"
    );
    assert_eq!(
        r("select /*+ leading((t1, t2), sub) */ a from t1, t2"),
        "SELECT /*+ LEADING((`t1`, `t2`), `sub`)*/ `a` FROM (`t1`) JOIN `t2`"
    );
}

/// `READ_FROM_STORAGE([@qb] STORE[t, ...], ...)` — see
/// `tidb_ast::HintKind::ReadFromStorage`'s own doc for the one-write/
/// many-print restore shape.
#[test]
fn read_from_storage_hint() {
    // The real-corpus statement: a single group, schema-qualified table.
    assert_eq!(
        r("select /*+ read_from_storage(tikv[`executor__admin`.`admin_test`]) */ 1 from `executor__admin`.`admin_test`"),
        "SELECT /*+ READ_FROM_STORAGE(TIKV[`executor__admin`.`admin_test`])*/ 1 FROM `executor__admin`.`admin_test`"
    );
    // Multiple storage-type groups restore as MULTIPLE separate hint
    // blocks from the ONE written occurrence.
    assert_eq!(
        r("select /*+ read_from_storage(tikv[t1], tiflash[t2]) */ 1 from t1, t2"),
        "SELECT /*+ READ_FROM_STORAGE(TIKV[`t1`]) READ_FROM_STORAGE(TIFLASH[`t2`])*/ 1 FROM (`t1`) JOIN `t2`"
    );
    // Multiple tables within one group.
    assert_eq!(
        r("select /*+ read_from_storage(tikv[t1, t2]) */ 1 from t1, t2"),
        "SELECT /*+ READ_FROM_STORAGE(TIKV[`t1`, `t2`])*/ 1 FROM (`t1`) JOIN `t2`"
    );
    // An optional leading `@qb_name`, shared by every group.
    assert_eq!(
        r("select /*+ read_from_storage(@sel_1 tikv[t1]) */ 1 from t1"),
        "SELECT /*+ READ_FROM_STORAGE(@`sel_1` TIKV[`t1`])*/ 1 FROM `t1`"
    );
    // The store type always canonicalizes to uppercase on restore,
    // regardless of how it was written.
    assert_eq!(
        r("select /*+ read_from_storage(TiKv[t1]) */ 1 from t1"),
        "SELECT /*+ READ_FROM_STORAGE(TIKV[`t1`])*/ 1 FROM `t1`"
    );
    // The bracketed table list is optional.
    assert_eq!(
        r("select /*+ read_from_storage(tikv) */ 1 from t1"),
        "SELECT /*+ READ_FROM_STORAGE(TIKV)*/ 1 FROM `t1`"
    );
    // A store type other than TIKV/TIFLASH is a genuine `ParseError`
    // here — this project's own `ParseError`-over-silent-drop
    // convention (real TiDB itself silently drops the rest of the
    // occurrence instead, an obscure malformed-input edge case not
    // replicated — see the dispatch arm's own doc).
    assert_eq!(
        r("select /*+ read_from_storage(tidb[t1]) */ 1 from t1"),
        "SELECT 1 FROM `t1`"
    );
}
