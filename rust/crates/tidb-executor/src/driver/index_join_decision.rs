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

//! Which side of a join, if either, can be READ ONCE PER OUTER KEY rather
//! than read whole: Go's `getIndexJoinBuildHelper` reduced to the shapes
//! this tier can both execute and print.
//!
//! # What Go decides here, and what this decides
//!
//! Go asks this question inside `findBestTask`: an `IndexJoinProp` travels
//! down to the inner `DataSource`, which answers with the access path its
//! ranger can build from the outer join keys
//! (`buildDataSource2IndexScanByIndexJoinProp` for an index,
//! `buildDataSource2TableScanByIndexJoinProp` for the clustered handle), and
//! the resulting task is COSTED against the hash and merge alternatives.
//!
//! This tier has no join cost model and no physical-plan IR to cost (see
//! `crate::driver::merge_decision` for the same seam on the merge side), so
//! the choice here is STRUCTURAL, and deliberately narrower than Go's:
//!
//! * the looked-up side is a single base table read whole -- not a derived
//!   table, not a nested join, not a partitioned table, and not a side column
//!   pruning has narrowed (its offsets would no longer be the table's);
//! * every probe column's type must be EXACTLY the indexed column's type, so
//!   the outer value IS the index probe. Go instead converts each outer value
//!   to the inner column's type and drops the row when the round trip changes
//!   it (`constructDatumLookupKey`'s `ConvertTo` + `Compare`); requiring the
//!   types to agree removes that whole branch rather than reimplementing it,
//!   at the cost of refusing the mixed-type joins Go accepts. NAMED RESIDUE.
//! * the looked-up side is never the outer-join-PRESERVED side, which is Go's
//!   rule too: an index join reads its inner side per outer row, and a
//!   preserved side must be read whole.
//!
//! # Why this is not an over-eager chooser
//!
//! The structural conditions above are necessary for Go to pick an index
//! join, but they are NOT sufficient: Go picks between the three strategies
//! by cost, and a join over an indexed key whose outer side is large is
//! usually a hash join in Go. So one more condition is imposed, and it is the
//! one the recordings support: the driving side must NOT itself be a single
//! base table. Every recorded plan this tier can currently reach an
//! `IndexJoin` for drives from a DERIVED table -- a projection over a join,
//! whose join key is a computed expression that only an index probe can use
//! -- and every recorded plan whose two sides are both base tables is a hash
//! or merge join in Go. See `docs` on [`index_join_decision`].

use tidb_datatype::FieldType;

use crate::driver::from::FromScope;
use crate::driver::{Catalog, TableEntry};
use crate::hash_join::EquiKey;
use crate::kv_table::KvTable;

/// What `EXPLAIN` prints for a column no name reaches -- a projected
/// expression. Go's `Column.StringWithCtx` falls back to it when the column
/// has no `OrigName`.
const UNNAMED_COLUMN: &str = "Column";

/// The looked-up side of a join and the object its probes read.
pub(crate) struct IndexJoinDecision {
    /// Whether the looked-up side is the join's left child.
    pub(crate) lookup_is_left: bool,
    /// Offsets into the join's equality keys that decide the probe range, in
    /// the object's own key-column order.
    pub(crate) probe_keys: Vec<usize>,
    /// The table the probes read.
    pub(crate) table: KvTable,
    /// The object the probes read: an index, or the clustered handle.
    pub(crate) object: crate::access_path::LookupObject,
    /// The looked-up table's visible columns, which are this side's whole
    /// row layout (the decision refuses a narrowed side).
    pub(crate) columns: Vec<(String, FieldType)>,
    /// The name this side is written under, for `EXPLAIN`'s `table:`.
    pub(crate) visible: String,
    /// The `EXPLAIN` text of what decided the range: Go's
    /// `indexJoinPathRangeInfo` / `indexJoinIntPKRangeInfo`.
    pub(crate) range_info: String,
}

/// One join side reduced to what the decision reads about it.
pub(crate) struct JoinSide<'a> {
    /// The table this side reads, when it is a single base table read whole.
    pub(crate) table: Option<&'a KvTable>,
    /// The name it is written under.
    pub(crate) visible: String,
    /// Its output column types, in row order.
    pub(crate) types: Vec<FieldType>,
    /// Its output column names qualified as `EXPLAIN` prints them, in row
    /// order. A column no name reaches -- a projected expression -- is Go's
    /// bare `Column`.
    pub(crate) names: Vec<String>,
}

/// The looked-up side of `join`, or `None` when neither side qualifies.
///
/// `keys` are the join's equality conjuncts as
/// [`crate::hash_join::split_equi`] produced them: `left` is an offset in the
/// LEFT child's row and `right` an offset in the RIGHT child's.
pub(crate) fn index_join_decision(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    merge_chosen: bool,
) -> Option<IndexJoinDecision> {
    if !CHOOSER_IS_FAITHFUL {
        return None;
    }
    index_join_candidate(kind, keys, left, right, merge_chosen)
}

/// Whether this tier may CHOOSE the index strategy for a live statement.
///
/// `false`, and the reason is MEASURED rather than assumed. Every structural
/// rule tried here was falsified against
/// `r/planner/core/join_reorder_through_projection.result`, which records the
/// same statements twice -- once under `tidb_opt_join_reorder_through_proj =
/// off` and once under `on`:
///
/// * "the inner side is a base table with an index on the join key" fires on
///   `jt_ab`/`jt_ch` and on `t5`, where TiDB reads the table WHOLE and merge-
///   or hash-joins it. Replay: 13 -> 18 divergences.
/// * "...and the outer join key is a projected EXPRESSION (Go's bare
///   `Column`), the one key no index can pre-sort" is the narrowest rule the
///   recordings suggest, and it is still wrong: `select t1.*, dt.* from t1,
///   (select t2.a, t2.b * 2 from t2 join t3 ...) dt where t1.b =
///   dt.doubled_b` has exactly that shape and TiDB plans a `HashJoin` over a
///   `TableFullScan` of `t1` under BOTH settings of the variable
///   (result:1584 and result:1607). Replay: 13 -> 22 divergences.
///
/// What separates the recorded index joins from the recorded hash joins over
/// the same shape is TiDB's COST comparison (`compareTaskCost` over the
/// `IndexJoinProp` task `buildDataSource2IndexScanByIndexJoinProp` builds),
/// which needs a physical-plan IR and a join cost model this tier does not
/// have -- see `crate::driver::merge_decision` for the same seam. A
/// structural chooser here can only trade one recorded instance of a
/// statement for the other, never reduce the divergence count.
///
/// Everything BELOW this switch is complete and tested: the decision, the
/// executor ([`crate::join::IndexLookupPlan`]) and the plan text. The switch
/// is the one piece the cost model owns.
///
/// # What is now known about that cost model, and what is still missing
///
/// Half of it exists and is validated:
/// [`tidb_planner::plan_cost_ver2`] is Go's `plan_cost_ver2.go` -- the
/// DEFAULT cost model -- reproducing every `estCost` in
/// `r/planner/core/plan_cost_ver2.result` to the printed digit, including
/// `getIndexJoinCostVer24PhysicalIndexJoin` and `compareTaskCost`.
///
/// What is missing is the OTHER half, and it was measured on the recorded
/// statement at `join_reorder_through_projection.result:1169`. Under a live
/// `gorun` binary that statement's two candidates cost:
///
/// ```text
/// HashJoin_16   2492.68 = start(1497) + build(44.03) + probe(643.44) + ...
/// IndexJoin_23  4558.90 = start(1497) + build(643.44) + buildTask(2395.20) + ...
/// ```
///
/// -- and `tidb_planner::plan_cost_ver2::hash_join_cost` reproduces the
/// 2492.68 exactly from those child costs. The decision therefore does not
/// hinge on anything about the join itself: BOTH candidates are dominated by
/// the cost of the OUTER SUBTREE (here `643.44`, a projection over a merge
/// join over two table readers) and by that subtree's row count. This
/// decision function is handed [`JoinSide`]s carrying a table, some names and
/// some types -- no rows, no row sizes, no child cost -- so it cannot form
/// either number.
///
/// The missing piece is therefore named precisely: a recursive plan-cost
/// evaluator over THIS tier's plan tree that carries per-node rows and row
/// sizes, whose result feeds
/// [`tidb_planner::plan_cost_ver2::compare_task_cost`]. Turning this switch
/// on before that exists still only trades one recorded instance of a
/// statement for the other.
pub(crate) const CHOOSER_IS_FAITHFUL: bool = false;

/// The looked-up side [`index_join_decision`] would name if this tier could
/// choose the strategy, which is what the tests exercise.
pub(crate) fn index_join_candidate(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    merge_chosen: bool,
) -> Option<IndexJoinDecision> {
    if keys.is_empty() || merge_chosen {
        return None;
    }
    // The looked-up side is never the preserved one.
    let sides: &[bool] = match kind {
        crate::join::JoinKind::Inner => &[false, true],
        crate::join::JoinKind::Left => &[false],
        crate::join::JoinKind::Right => &[true],
    };
    for lookup_is_left in sides.iter().copied() {
        let (inner, outer) = if lookup_is_left {
            (left, right)
        } else {
            (right, left)
        };
        let Some(table) = inner.table else {
            continue;
        };
        // THE GATE. See the module doc: the structural conditions above are
        // necessary but not sufficient, and this is the one the recordings
        // support -- every outer key must be a column no NAME reaches, which
        // is Go's bare `Column` in `EXPLAIN`.
        //
        // A projected expression is the one join key that has no other
        // strategy available to it: no index provides its order, so no merge
        // join can key on it, and Go's cost model is then choosing between an
        // index join and a hash join over a key it cannot pre-sort. Every
        // recorded index join this tier can currently reach has such a key,
        // and every recorded plan whose outer key is a NAMED column is a
        // merge or hash join -- which the replay measures both ways.
        let outer_at = |key: &EquiKey| if lookup_is_left { key.right } else { key.left };
        if !keys
            .iter()
            .any(|key| outer.names[outer_at(key)] == UNNAMED_COLUMN)
        {
            continue;
        }
        if let Some(decision) = decide_over(table, lookup_is_left, keys, inner, outer) {
            return Some(decision);
        }
    }
    None
}

/// The decision for one candidate side, once it is known to be a base table.
fn decide_over(
    table: &KvTable,
    lookup_is_left: bool,
    keys: &[EquiKey],
    inner: &JoinSide<'_>,
    outer: &JoinSide<'_>,
) -> Option<IndexJoinDecision> {
    // A partitioned table's probe would have to name the partition the key
    // falls in; Go refuses `keepOrder` there and prunes per probe, neither of
    // which this reads. Refuse it whole.
    if table.partition().is_some() {
        return None;
    }
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    // A side column pruning narrowed no longer has the table's own offsets,
    // and the probe would read the wrong column.
    if inner.types.len() != columns.len() {
        return None;
    }
    let inner_at = |key: &EquiKey| if lookup_is_left { key.left } else { key.right };
    let outer_at = |key: &EquiKey| if lookup_is_left { key.right } else { key.left };
    // Which of this side's columns a key probes, and with which key.
    let key_of_column = |column: usize| -> Option<usize> {
        keys.iter().position(|key| {
            inner_at(key) == column
                && probe_compatible(&inner.types[inner_at(key)], &outer.types[outer_at(key)])
        })
    };

    // The clustered integer handle first, as Go does
    // (`buildDataSource2TableScanByIndexJoinProp` is "no worse than" the
    // index): one probe reads exactly one row.
    let int_handle = (0..columns.len()).find(|at| table.is_clustered_handle_column(*at));
    if let Some(pk) = int_handle {
        if let Some(key) = key_of_column(pk) {
            return Some(IndexJoinDecision {
                lookup_is_left,
                probe_keys: vec![key],
                table: table.clone(),
                object: crate::access_path::LookupObject::Handle,
                columns,
                visible: inner.visible.clone(),
                // Go `indexJoinIntPKRangeInfo`: the OUTER keys, bare.
                range_info: format!("[{}]", outer.names[outer_at(&keys[key])]),
            });
        }
    }

    // Otherwise the longest LEADING run of an index's columns that are all
    // join keys -- Go's `indexJoinPathTmpInit` walk, which stops at the first
    // index column no inner key covers.
    let mut best: Option<(i64, Vec<usize>)> = None;
    for index in table.indexes() {
        if !index.visible || index.has_prefix() {
            continue;
        }
        let mut probe_keys = Vec::new();
        for offset in &index.column_offsets {
            let Some(key) = key_of_column(*offset) else {
                break;
            };
            probe_keys.push(key);
        }
        if probe_keys.is_empty() {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|(_, best)| best.len() < probe_keys.len())
        {
            best = Some((index.id, probe_keys));
        }
    }
    let (index_id, probe_keys) = best?;
    let index = table.indexes().iter().find(|i| i.id == index_id)?;
    // Go `indexJoinPathRangeInfo`: `eq(<index column>, <outer key>)` per
    // covered index column, in index order.
    let range_info = format!(
        "[{}]",
        probe_keys
            .iter()
            .enumerate()
            .map(|(at, key)| {
                let column = index.column_offsets[at];
                format!(
                    "eq({}, {})",
                    inner.names[column],
                    outer.names[outer_at(&keys[*key])]
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    );
    Some(IndexJoinDecision {
        lookup_is_left,
        probe_keys,
        table: table.clone(),
        object: crate::access_path::LookupObject::Index(index_id),
        columns,
        visible: inner.visible.clone(),
        range_info,
    })
}

/// Whether an outer value of type `outer` IS a probe of an indexed column of
/// type `inner`, with no conversion in between.
///
/// See the module doc: this replaces Go's `ConvertTo` + `Compare` per value
/// with a type check made once. `flen` and `decimal` are deliberately NOT
/// compared -- an `int` column and `t.b * 2` differ there and encode
/// identically -- while the type code, the signedness and, for strings, the
/// collation decide the bytes an index entry was written with.
fn probe_compatible(inner: &FieldType, outer: &FieldType) -> bool {
    if inner.is_unsigned() != outer.is_unsigned() {
        // An unsigned index column stores its entries under the unsigned
        // encoding; a signed probe would ask the wrong bytes for the same
        // number. Go converts and compares per value instead. NAMED RESIDUE.
        return false;
    }
    if inner.code().is_type_integer() && outer.code().is_type_integer() {
        // Every integer width shares ONE index encoding, so a `BIGINT`
        // expression probes an `INT` column's index with its own value and no
        // conversion. A value the narrower column cannot hold simply has no
        // entry -- which is the same answer Go reaches by dropping the row
        // when `ConvertTo` overflows, arrived at through the index rather
        // than through a check.
        return true;
    }
    inner.code() == outer.code()
        && inner
            .collation_name()
            .eq_ignore_ascii_case(outer.collation_name())
}

/// The two sides of a join as the decision reads them, built from the scope
/// the join produced and the executors its children became.
pub(crate) fn join_sides<'a>(
    join: &tidb_ast::Join,
    scope: &FromScope,
    current_db: &str,
    left_width: usize,
    catalog: &'a Catalog,
    left_types: &[FieldType],
    right_types: &[FieldType],
) -> (JoinSide<'a>, JoinSide<'a>) {
    let left = side_of(
        &join.left, scope, current_db, catalog, left_types, 0, left_width,
    );
    let right = match &join.right {
        Some(node) => side_of(
            node,
            scope,
            current_db,
            catalog,
            right_types,
            left_width,
            scope.width(),
        ),
        None => JoinSide {
            table: None,
            visible: String::new(),
            types: Vec::new(),
            names: Vec::new(),
        },
    };
    (left, right)
}

fn side_of<'a>(
    node: &tidb_ast::JoinNode,
    scope: &FromScope,
    current_db: &str,
    catalog: &'a Catalog,
    types: &[FieldType],
    from: usize,
    to: usize,
) -> JoinSide<'a> {
    let computed = computed_columns(node, to - from);
    let names: Vec<String> = (from..to)
        .map(|offset| {
            if computed.get(offset - from).copied().unwrap_or(false) {
                UNNAMED_COLUMN.to_owned()
            } else {
                crate::driver::from::qualified_scope_column(scope, current_db, offset)
            }
        })
        .collect();
    let (table, visible) = single_table_of(node, catalog, current_db)
        .map_or((None, String::new()), |(kv, visible)| (Some(kv), visible));
    JoinSide {
        table,
        visible,
        types: types.to_vec(),
        names,
    }
}

/// The base table `node` reads whole, with the name it is written under, or
/// `None` for every other shape.
fn single_table_of<'a>(
    node: &tidb_ast::JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<(&'a KvTable, String)> {
    // `FROM a, b` wraps its left relation in a single-child join node, the
    // same peeling `crate::column_prune` does.
    let mut node = node;
    while let tidb_ast::JoinNode::Join(inner) = node {
        if inner.right.is_some() || inner.on.is_some() || !inner.using.is_empty() || inner.natural {
            return None;
        }
        node = &inner.left;
    }
    let tidb_ast::JoinNode::Table(table_ref) = node else {
        return None;
    };
    // A named partition list, an `AS OF`, or an index hint all change what
    // the read is; none is read here, so none may be silently ignored.
    if !table_ref.partitions.is_empty()
        || table_ref.as_of.is_some()
        || !table_ref.hints.is_empty()
        || table_ref.sample.is_some()
    {
        return None;
    }
    let (database, name) =
        crate::driver::catalog::split_table_path(&table_ref.name, current_db).ok()?;
    let entry = catalog.get_in(database, name)?;
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
    Some((kv, visible))
}

/// Which of a side's columns `EXPLAIN` prints as a bare `Column`, one flag
/// per column in row order.
///
/// Go carries `Column.OrigName` from the base column a projection merely
/// passes through, and leaves it EMPTY for a projected expression -- so
/// `t2.a AS key_a` still prints `<db>.t2.a` while `t2.b * 2 AS doubled_b`
/// prints `Column`. This tier's scope keeps only the name a column answers
/// to, so the distinction is read back off the derived table's own select
/// list, which is where it was made.
///
/// Everything that is not a derived table with an explicit, wildcard-free
/// select list of the expected width answers "no column is computed" -- a
/// refusal, since the index-join gate reads this as a REQUIREMENT.
fn computed_columns(node: &tidb_ast::JoinNode, width: usize) -> Vec<bool> {
    let none = vec![false; width];
    let tidb_ast::JoinNode::Derived { subquery, .. } = node else {
        return none;
    };
    let tidb_ast::QueryStmt::Select(select) = &**subquery else {
        return none;
    };
    let fields = select.fields.fields();
    if fields.len() != width {
        return none;
    }
    fields
        .iter()
        .map(|field| match field {
            // A wildcard expands to base columns, none of them computed.
            tidb_ast::SelectField::Wildcard(_) => false,
            // Go keeps `OrigName` through a plain column reference and
            // through nothing else -- not through a cast, not through an
            // arithmetic expression, not through a function call.
            tidb_ast::SelectField::Expr { expr, .. } => !matches!(expr, tidb_ast::Expr::Column(_)),
        })
        .collect()
}
