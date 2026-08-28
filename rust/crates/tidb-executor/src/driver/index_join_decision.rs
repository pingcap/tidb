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

//! Lowers an exact physical IndexJoin receipt into executor lookup metadata.
//!
//! The shared planner owns join-family, inner-child, access-object, order,
//! aggregation-family, and cost decisions. This module must not enumerate or
//! re-cost alternatives. It only validates that the selected table/index can
//! be represented by the executor and builds Go-compatible range metadata.
//! Probe values are converted at runtime to the inner key type and retained
//! only when comparison with the original value is equal, matching Go's
//! `constructDatumLookupKey`.

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::rewrite_expr_resolved;

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
    /// The complete object key assembled from dynamic join keys and static
    /// access conditions.
    pub(crate) probe_parts: Vec<crate::access_path::LookupProbePart>,
    /// The table the probes read.
    pub(crate) table: KvTable,
    /// The object the probes read: an index, or the clustered handle.
    pub(crate) object: crate::access_path::LookupObject,
    /// The root reader selected on the completed physical inner task.
    pub(crate) reader: super::planner_bridge::AccessReader,
    /// The looked-up table's visible columns, which are this side's whole
    /// physical row layout.
    pub(crate) columns: Vec<(String, FieldType)>,
    /// The physical database owning `table`. Inner filters use a table-local
    /// column layout and must not be qualified through the surrounding join.
    pub(crate) database: String,
    /// Physical table offsets emitted by a bare lookup, in the narrowed
    /// child's logical output order. A retained aggregation consumes its
    /// physical-width input instead and does not use this projection.
    pub(crate) output_offsets: Vec<usize>,
    /// The name this side is written under, for `EXPLAIN`'s `table:`.
    pub(crate) visible: String,
    /// The `EXPLAIN` text of what decided the range: Go's
    /// `indexJoinPathRangeInfo` / `indexJoinIntPKRangeInfo`.
    pub(crate) range_info: String,
    /// Every predicate local to the looked-up leaf, in statement form for
    /// EXPLAIN and rewritten form for execution.
    pub(crate) filters: Vec<tidb_ast::Expr>,
    pub(crate) filter_exprs: Vec<Expression>,
    /// Selectivity of the filter residue not already represented by a static
    /// object-key part.
    pub(crate) filter_selectivity: f64,
    /// Selectivity of every predicate on the base-table source, including
    /// predicates represented by static lookup-key parts. Go uses this when
    /// an IndexJoin runtime property crosses a retained aggregation: the
    /// aggregate output expectation must be scaled back to source rows.
    pub(crate) source_filter_selectivity: f64,
    /// A grouped derived table retained above the re-seeded base-table
    /// reader. Bare table lookup sides leave this absent.
    pub(crate) aggregation: Option<crate::join::IndexLookupAggregation>,
    /// Go's physical aggregation payload for that retained aggregation.
    pub(crate) aggregation_info: Option<String>,
    /// Go's final aggregation payload when a cop partial aggregation is
    /// retained below a double-read lookup.
    pub(crate) aggregation_final_info: Option<String>,
    /// Go's partial aggregation payload pushed onto the table side of a
    /// double-read lookup.
    pub(crate) aggregation_partial_info: Option<String>,
    /// The aggregation family selected on the physical inner subtree. This
    /// is a planner receipt, not an executor inference from lookup order.
    pub(crate) aggregation_family: Option<super::planner_bridge::AggregationFamily>,
    /// The lookup target is nested below another join on this side. The
    /// executor rebuilds that subtree with a shared probe channel instead of
    /// replacing the whole side with a bare table reader.
    pub(crate) composite: bool,
    /// Whether those leaf filters plus the join equalities cover the complete
    /// written WHERE.
    pub(crate) consumes_where: bool,
    /// Go's `rule_join_key_type_cast` probe: the outer key is
    /// `cast(str AS SIGNED)` computed behind the rule's guard rather than a
    /// bare outer column, and the equality it belongs to is not in the
    /// split keys. See [`crate::driver::join_key_cast`].
    pub(crate) probe_cast: Option<crate::join::IndexProbeCast>,
    /// Outer-derived comparisons on the object-key column just past
    /// `probe_parts` -- Go's `CompareFilters` (`ColWithCmpFuncManager`). The
    /// executor evaluates each against one outer row and extends that probe's
    /// point range with the result, which is what turns Go's
    /// `range: decided by [... ge(col, outer-expr) ...]` from text into rows
    /// actually not read. Empty keeps the point-probe path.
    pub(crate) probe_bounds: Vec<crate::access_path::LookupProbeBound>,
}

impl IndexJoinDecision {
    /// Records the evicted statistics Go touches while costing this physical
    /// lookup candidate. `ColumnStatsIsInvalid` owns clustered handles;
    /// `IndexStatsIsInvalid` owns common handles and secondary indexes.
    pub(crate) fn record_stats_access(&self, stats: Option<&crate::access_cost::TableStatistics>) {
        let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
            return;
        };
        let mut columns = std::collections::BTreeSet::new();
        let mut indexes = std::collections::BTreeSet::new();
        match self.object {
            crate::access_path::LookupObject::Handle => {
                if let Some(column) = self
                    .table
                    .pk_handle_offset()
                    .and_then(|offset| self.table.columns.get(offset))
                {
                    columns.insert(column.id);
                }
            }
            crate::access_path::LookupObject::CommonHandle => {
                if let Some(index) = self
                    .table
                    .indexes()
                    .iter()
                    .find(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
                {
                    indexes.insert(index.id);
                }
            }
            crate::access_path::LookupObject::Index(id) => {
                indexes.insert(id);
            }
        }
        stats.mark_accessed_statistics(columns, indexes);
    }

    /// Go's `maxOneRow`: only an integer handle or every column of a unique
    /// object key guarantees at most one row. A partial common-handle prefix is
    /// a table range and can return every suffix below it.
    pub(crate) fn max_one_row(&self) -> bool {
        match self.object {
            crate::access_path::LookupObject::Handle => true,
            crate::access_path::LookupObject::CommonHandle => {
                self.probe_parts.len() == self.table.common_handle_offsets().len()
            }
            crate::access_path::LookupObject::Index(id) => self
                .table
                .indexes()
                .iter()
                .find(|index| index.id == id)
                .is_some_and(|index| {
                    index.unique && self.probe_parts.len() == index.column_offsets.len()
                }),
        }
    }

    /// The looked-up object's key columns, in key order.
    fn key_offsets(&self) -> Vec<usize> {
        match self.object {
            crate::access_path::LookupObject::Handle => self
                .table
                .pk_handle_offset()
                .map(|offset| vec![offset])
                .unwrap_or_default(),
            crate::access_path::LookupObject::CommonHandle => {
                self.table.common_handle_offsets().to_vec()
            }
            crate::access_path::LookupObject::Index(id) => self
                .table
                .indexes()
                .iter()
                .find(|index| index.id == id)
                .map(|index| index.column_offsets.clone())
                .unwrap_or_default(),
        }
    }

    /// The object-key columns a leaf constant fixed, so that the rebuilt
    /// range encodes them beside the outer key.
    ///
    /// Go's `indexJoinPathFindUsefulEQIn` returns such an equality as
    /// `notKeyEqAndIn`, which becomes an ACCESS condition rather than a
    /// remained one (`pkg/planner/core/index_join_path.go:176`, `:187`), and
    /// `constructDS2TableScanTask` receives `chosenRemained` as its filter
    /// conditions (`pkg/planner/core/exhaust_physical_plans.go:754`, `:756`)
    /// and derives `countAfterAccess = rowCount / selectivity` from just
    /// those (`:864`, `:871`). The explicit probe-side `Selection` Go prints
    /// for the access predicates is appended only afterwards (`:907`,
    /// `:915`), so the equality never divides the estimate.
    pub(crate) fn static_key_columns(&self) -> Vec<usize> {
        self.key_offsets()
            .into_iter()
            .zip(&self.probe_parts)
            .filter(|(_, part)| matches!(part, crate::access_path::LookupProbePart::Constant(_)))
            .map(|(offset, _)| offset)
            .collect()
    }
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
    /// `<database>.<table>` for the base table this side reads, written under
    /// the table's OWN name rather than the alias it is written under.
    ///
    /// Go's `Column.StringWithCtx` prints `OrigName`, which a table alias does
    /// NOT rename: the recorded plan for `from t1 outer_t, (...)` carries
    /// `table:outer_t` in the access object and
    /// `<db>.t1.b` in the same row's `range: decided by [...]`. Qualifying the
    /// range's inner column off the scope would print the alias in both.
    pub(crate) origin: Option<String>,
    /// The base table name printed by the lookup reader.  This differs from
    /// `visible` when the join side is a derived table alias.
    pub(crate) source_visible: String,
    /// For each side output, the base-table column it carries. Computed
    /// aggregate outputs have no source offset.
    pub(crate) output_to_source: Vec<Option<usize>>,
    /// Predicates written inside a grouped derived table, evaluated by the
    /// re-seeded base-table reader before aggregation.
    pub(crate) source_filters: Vec<tidb_ast::Expr>,
    /// The executable grouped derived-table transformation, if any.
    pub(crate) aggregation: Option<crate::join::IndexLookupAggregation>,
    pub(crate) aggregation_info: Option<String>,
    pub(crate) aggregation_final_info: Option<String>,
    pub(crate) aggregation_partial_info: Option<String>,
    /// Whether `table` is a target leaf discovered below a composite side.
    pub(crate) composite: bool,
}

/// Lowers the exact inner access selected by the shared physical planner.
///
/// Go carries this choice in `IndexJoinInfo` from the inner `DataSource` to
/// `completePhysicalIndexJoin`.  Once that receipt exists, executor lowering
/// must not enumerate the other child or another handle/index and choose a
/// look-alike candidate locally.
#[allow(clippy::too_many_arguments)]
pub(crate) fn index_join_decision_for_planner(
    kind: crate::join::JoinKind,
    keys: &[EquiKey],
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    inner_child_idx: usize,
    inner_table_id: Option<i64>,
    inner_index_id: Option<i64>,
    inner_reader: Option<super::planner_bridge::AccessReader>,
    inner_aggregation: Option<super::planner_bridge::AggregationFamily>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    catalog: Option<&crate::driver::Catalog>,
    ctx: &crate::StmtContext,
) -> Option<IndexJoinDecision> {
    if keys.is_empty() || inner_child_idx > 1 {
        return None;
    }
    let side_has_table = |side: &JoinSide<'_>, table_id: i64| {
        side.table.is_some_and(|table| table.table_id == table_id)
    };
    let lookup_is_left = inner_table_id.map_or(inner_child_idx == 0, |table_id| {
        match (
            side_has_table(left, table_id),
            side_has_table(right, table_id),
        ) {
            (true, false) => true,
            (false, true) => false,
            _ => inner_child_idx == 0,
        }
    });
    let side_is_admitted = match kind {
        crate::join::JoinKind::Inner => true,
        crate::join::JoinKind::Left => !lookup_is_left,
        crate::join::JoinKind::Right => lookup_is_left,
        crate::join::JoinKind::Semi
        | crate::join::JoinKind::LeftOuterSemi
        | crate::join::JoinKind::AntiSemi => !lookup_is_left,
    };
    if !side_is_admitted {
        return None;
    }
    let (inner, outer) = if lookup_is_left {
        (left, right)
    } else {
        (right, left)
    };
    let table = inner.table?;
    if inner_table_id.is_some_and(|table_id| table.table_id != table_id) {
        return None;
    }
    let object = match inner_index_id {
        Some(index_id) => crate::access_path::LookupObject::Index(index_id),
        None if table.pk_handle_offset().is_some() => crate::access_path::LookupObject::Handle,
        None if !table.common_handle_offsets().is_empty() => {
            crate::access_path::LookupObject::CommonHandle
        }
        None => return None,
    };
    let reader = inner_reader?;
    if !matches!(
        (&object, reader),
        (
            crate::access_path::LookupObject::Index(_),
            super::planner_bridge::AccessReader::Index
                | super::planner_bridge::AccessReader::IndexLookup
        ) | (
            crate::access_path::LookupObject::Handle
                | crate::access_path::LookupObject::CommonHandle,
            super::planner_bridge::AccessReader::Table
        )
    ) {
        return None;
    }
    let statistics =
        catalog.and_then(|catalog| catalog.table_statistics(table.stats_physical_id()));
    lower_selected_access(
        table,
        lookup_is_left,
        keys,
        inner,
        outer,
        rows,
        statistics.as_deref().map(AsRef::as_ref),
        ctx,
        &object,
        reader,
        inner_aggregation,
    )
}

/// Lowers one exact access object from the shared planner's index-join
/// receipt. It validates that the object can be probed by the received join
/// keys and never enumerates an alternative side, handle, or index.
#[allow(clippy::too_many_arguments)]
fn lower_selected_access(
    table: &KvTable,
    lookup_is_left: bool,
    keys: &[EquiKey],
    inner: &JoinSide<'_>,
    outer: &JoinSide<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    statistics: Option<&crate::access_cost::TableStatistics>,
    ctx: &crate::StmtContext,
    selected_object: &crate::access_path::LookupObject,
    selected_reader: super::planner_bridge::AccessReader,
    selected_aggregation: Option<super::planner_bridge::AggregationFamily>,
) -> Option<IndexJoinDecision> {
    // A partitioned table's probe would have to name the partition the key
    // falls in; Go refuses `keepOrder` there and prunes per probe, neither of
    // which this reads. Refuse it whole.
    if table.partition().is_some() {
        return None;
    }
    let Some(database) = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned())
    else {
        return None;
    };
    // Every bare output must map back to the physical table. A grouped
    // derived side may also contain computed aggregate outputs, which are
    // rebuilt after the lookup and therefore have no source offset.
    //
    // These admission checks are pure metadata and fire for most statements,
    // so they must run BEFORE the per-column name/type clones below (the
    // unconditional build profiled at several percent of process CPU).
    if inner.output_to_source.len() != inner.types.len() {
        return None;
    }
    let output_offsets = inner
        .output_to_source
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>();
    if inner.aggregation.is_none() && output_offsets.is_none() && !inner.composite {
        return None;
    }
    let output_offsets = output_offsets.unwrap_or_default();
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let inner_at = |key: &EquiKey| if lookup_is_left { key.left } else { key.right };
    let outer_at = |key: &EquiKey| if lookup_is_left { key.right } else { key.left };
    // Which of this side's columns a key probes, and with which key.
    let key_of_column = |column: usize| -> Option<usize> {
        keys.iter().position(|key| {
            inner
                .output_to_source
                .get(inner_at(key))
                .is_some_and(|offset| *offset == Some(column))
        })
    };
    let outer_filters = rows
        .and_then(|rows| rows.filters_for(&inner.visible))
        .unwrap_or_default()
        .to_vec();
    let outer_filters = if inner.aggregation.is_some() {
        let Some(filters) = outer_filters
            .iter()
            .map(|filter| rewrite_derived_filter_to_source(filter, inner, &columns))
            .collect::<Option<Vec<_>>>()
        else {
            return None;
        };
        filters
    } else {
        outer_filters
    };
    let mut filters = inner
        .source_filters
        .iter()
        .cloned()
        .chain(outer_filters)
        .collect::<Vec<_>>();
    for filter in &mut filters {
        if normalize_source_filter(filter, &inner.source_visible, &columns).is_none() {
            return None;
        }
    }
    filters.dedup();
    let Some(filter_exprs) = rewrite_inner_filters(inner, &columns, &filters, ctx) else {
        return None;
    };
    let source_filter_selectivity = residual_filter_selectivity(
        &filters,
        &[],
        &columns,
        table,
        &inner.source_visible,
        statistics,
        &ctx.session_zone(),
    );
    let constants = static_equalities(&columns, &filters, ctx);
    let consumes_where = rows
        .is_some_and(crate::driver::join_reorder::RowSource::all_where_is_leaf_or_join_equality);

    // Builds the object's complete leading key from dynamic equality columns
    // and leaf-local constants. At least one part must be dynamic -- a wholly
    // static key is a point get, not an index join.
    let probe_for = |offsets: &[usize]| {
        let mut probe_keys = Vec::new();
        let mut probe_parts = Vec::new();
        let mut dynamic_info = Vec::new();
        let mut static_info = Vec::new();
        let mut static_columns = Vec::new();
        for offset in offsets {
            if let Some(key) = key_of_column(*offset) {
                let dynamic = probe_keys.len();
                probe_keys.push(key);
                probe_parts.push(crate::access_path::LookupProbePart::Dynamic(dynamic));
                dynamic_info.push(format!(
                    "eq({}, {})",
                    inner_column_name(inner, *offset),
                    if inner.composite {
                        outer.names[outer_at(&keys[key])].clone()
                    } else {
                        physical_outer_column_name(outer, outer_at(&keys[key]))
                    }
                ));
            } else if let Some(value) = constants.get(*offset).and_then(Clone::clone) {
                probe_parts.push(crate::access_path::LookupProbePart::Constant(value.clone()));
                static_columns.push(*offset);
                static_info.push(format!(
                    "eq({}, {})",
                    inner_column_name(inner, *offset),
                    datum_text(&value)
                ));
            } else {
                break;
            }
        }
        (!probe_keys.is_empty()).then_some((
            probe_keys,
            probe_parts,
            dynamic_info,
            static_info,
            static_columns,
        ))
    };

    let build =
        |object, probe_keys, probe_parts, static_columns: &[usize], range_info| IndexJoinDecision {
            lookup_is_left,
            probe_keys,
            probe_parts,
            table: table.clone(),
            object,
            reader: selected_reader,
            filter_selectivity: residual_filter_selectivity(
                &filters,
                static_columns,
                &columns,
                table,
                &inner.source_visible,
                statistics,
                &ctx.session_zone(),
            ),
            source_filter_selectivity,
            aggregation: inner.aggregation.clone(),
            aggregation_info: inner.aggregation_info.clone(),
            aggregation_final_info: inner.aggregation_final_info.clone(),
            aggregation_partial_info: inner.aggregation_partial_info.clone(),
            aggregation_family: selected_aggregation,
            composite: inner.composite,
            columns: columns.clone(),
            database: database.clone(),
            output_offsets: output_offsets.clone(),
            visible: inner.source_visible.clone(),
            range_info,
            filters: filters.clone(),
            filter_exprs: filter_exprs.clone(),
            consumes_where,
            probe_cast: None,
            probe_bounds: Vec::new(),
        };

    match selected_object {
        crate::access_path::LookupObject::Handle => {
            let handle = (0..columns.len()).find(|at| table.is_clustered_handle_column(*at))?;
            let key = key_of_column(handle)?;
            let range_info = format!(
                "[{}]",
                if inner.composite {
                    outer.names[outer_at(&keys[key])].clone()
                } else {
                    physical_outer_column_name(outer, outer_at(&keys[key]))
                }
            );
            Some(build(
                crate::access_path::LookupObject::Handle,
                vec![key],
                vec![crate::access_path::LookupProbePart::Dynamic(0)],
                &[],
                range_info,
            ))
        }
        crate::access_path::LookupObject::CommonHandle => {
            let offsets = table.common_handle_offsets();
            if offsets.is_empty() {
                return None;
            }
            let (probe_keys, probe_parts, dynamic, static_parts, static_columns) =
                probe_for(offsets)?;
            let range_info = format!(
                "[{}]",
                dynamic
                    .into_iter()
                    .chain(static_parts)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            Some(build(
                crate::access_path::LookupObject::CommonHandle,
                probe_keys,
                probe_parts,
                &static_columns,
                range_info,
            ))
        }
        crate::access_path::LookupObject::Index(index_id) => {
            let index = table.indexes().iter().find(|index| index.id == *index_id)?;
            if !index.visible
                || index.has_prefix()
                || (index.name.eq_ignore_ascii_case("PRIMARY")
                    && (table.pk_handle_offset().is_some()
                        || !table.common_handle_offsets().is_empty()))
            {
                return None;
            }
            let (probe_keys, probe_parts, dynamic, static_parts, static_columns) =
                probe_for(&index.column_offsets)?;
            let range_info = format!(
                "[{}]",
                dynamic
                    .into_iter()
                    .chain(static_parts)
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            Some(build(
                crate::access_path::LookupObject::Index(*index_id),
                probe_keys,
                probe_parts,
                &static_columns,
                range_info,
            ))
        }
    }
}

/// One rewritten mismatched equality mapped onto a concrete lookup side --
/// [`crate::driver::join_key_cast`]'s product, in the child-local offsets the
/// executor reads.
pub(crate) struct CastLookupKey {
    /// Child-local offset of the INT column in the LOOKUP side's output.
    pub(crate) inner_offset: usize,
    /// Child-local offset of the STRING column in the OUTER side's output.
    pub(crate) outer_offset: usize,
    /// The computed key: `cast(str AS SIGNED)` and Go's guard.
    pub(crate) rewrite: crate::driver::join_key_cast::RewrittenEquality,
}

/// The lookup decision for Go's `rule_join_key_type_cast` shape: an INNER
/// join whose only usable equality pairs a signed INT column (the lookup
/// side's clustered handle) with a STRING column, made probeable by the
/// rule's `cast(str AS SIGNED)` key.
///
/// Deliberately narrower than ordinary receipt lowering, each refusal
/// fail-closed:
///
/// * INNER joins only. Go's rule skips a preserved string side, and the
///   lookup refuses a preserved lookup side, so an outer join never
///   qualifies on both counts at once.
/// * the probed column must be the clustered INT handle -- Go's recorded
///   shape (`TableRangeScan range: decided by [Column#N]`); a secondary
///   index over the int column is NAMED RESIDUE.
/// * no leaf filters on the lookup side: the plain-column path threads them
///   into the probe with their selectivity, and this path would silently
///   drop them.
pub(crate) fn cast_lookup_decision(
    kind: crate::join::JoinKind,
    lookup_is_left: bool,
    key: CastLookupKey,
    inner: &JoinSide<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
    selected_reader: super::planner_bridge::AccessReader,
) -> Option<IndexJoinDecision> {
    if kind != crate::join::JoinKind::Inner
        || selected_reader != super::planner_bridge::AccessReader::Table
    {
        return None;
    }
    let table = inner.table?;
    if table.partition().is_some()
        || inner.aggregation.is_some()
        || inner.composite
        || !inner.source_filters.is_empty()
    {
        return None;
    }
    if rows.is_some_and(|rows| {
        rows.filters_for(&inner.visible)
            .is_some_and(|filters| !filters.is_empty())
    }) {
        return None;
    }
    let database = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned())?;
    if inner.output_to_source.len() != inner.types.len() {
        return None;
    }
    let output_offsets = inner
        .output_to_source
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>()?;
    // The probed column must be the table's clustered INT handle. This
    // refusal fires for every join whose equality is not the int-handle cast
    // shape -- most statements -- so it must run BEFORE the per-column clone
    // below (profiled at ~5% of process CPU when built unconditionally).
    let source_offset = *output_offsets.get(key.inner_offset)?;
    if !table.is_clustered_handle_column(source_offset) {
        return None;
    }
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    Some(IndexJoinDecision {
        lookup_is_left,
        probe_keys: Vec::new(),
        probe_parts: vec![crate::access_path::LookupProbePart::Dynamic(0)],
        table: table.clone(),
        object: crate::access_path::LookupObject::Handle,
        reader: selected_reader,
        filter_selectivity: 1.0,
        source_filter_selectivity: 1.0,
        aggregation: None,
        aggregation_info: None,
        aggregation_final_info: None,
        aggregation_partial_info: None,
        aggregation_family: None,
        composite: false,
        columns,
        database,
        output_offsets,
        visible: inner.source_visible.clone(),
        // Go `indexJoinIntPKRangeInfo` prints the OUTER key column, which is
        // the rule's injected cast column and has no `OrigName`. The caller
        // patches in the numbered `Column#N` form when a plan trace carries
        // the statement's Go plan-column stream; this bare fallback is what
        // `format='plan_tree'` recordings show.
        range_info: format!("[{UNNAMED_COLUMN}]"),
        filters: Vec::new(),
        filter_exprs: Vec::new(),
        consumes_where: rows.is_some_and(
            crate::driver::join_reorder::RowSource::all_where_is_leaf_or_join_equality,
        ),
        probe_cast: Some(crate::join::IndexProbeCast {
            outer_offset: key.outer_offset,
            inner_offset: key.inner_offset,
            cast: key.rewrite.cast,
            guard: key.rewrite.guard,
            str_type: key.rewrite.str_type,
        }),
        probe_bounds: Vec::new(),
    })
}

/// Pushes a predicate on carried derived outputs down to the base row the
/// lookup source returns. A reference to a computed aggregate output refuses
/// the index strategy; applying it before aggregation would be wrong.
fn rewrite_derived_filter_to_source(
    filter: &tidb_ast::Expr,
    inner: &JoinSide<'_>,
    columns: &[(String, FieldType)],
) -> Option<tidb_ast::Expr> {
    struct Rewriter<'a, 'b> {
        inner: &'a JoinSide<'b>,
        columns: &'a [(String, FieldType)],
        failed: bool,
    }
    impl tidb_ast::Visitor for Rewriter<'_, '_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Subquery(_) | tidb_ast::Expr::Exists { .. } => {
                    self.failed = true;
                    true
                }
                tidb_ast::Expr::Column(path) => {
                    let Some(name) = path.last() else {
                        self.failed = true;
                        return true;
                    };
                    let output = self.inner.names.iter().position(|candidate| {
                        candidate
                            .rsplit('.')
                            .next()
                            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(name))
                    });
                    let Some(source) = output
                        .and_then(|output| self.inner.output_to_source.get(output))
                        .copied()
                        .flatten()
                    else {
                        self.failed = true;
                        return true;
                    };
                    *path = vec![
                        self.inner.source_visible.clone(),
                        self.columns[source].0.clone(),
                    ];
                    true
                }
                _ => false,
            }
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewritten = filter.clone();
    let mut rewriter = Rewriter {
        inner,
        columns,
        failed: false,
    };
    tidb_ast::Visitable::accept(&mut rewritten, &mut rewriter);
    (!rewriter.failed).then_some(rewritten)
}

/// Gives every base-column reference one spelling so a predicate copied out
/// of a derived table and the same predicate inferred above it deduplicate.
fn normalize_source_filter(
    filter: &mut tidb_ast::Expr,
    visible: &str,
    columns: &[(String, FieldType)],
) -> Option<()> {
    struct Rewriter<'a> {
        visible: &'a str,
        columns: &'a [(String, FieldType)],
        failed: bool,
    }
    impl tidb_ast::Visitor for Rewriter<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(tidb_ast::Expr::Column(path)) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            let Some(column) = path.last().and_then(|name| {
                self.columns
                    .iter()
                    .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            }) else {
                self.failed = true;
                return true;
            };
            *path = vec![self.visible.to_owned(), column.0.clone()];
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewriter = Rewriter {
        visible,
        columns,
        failed: false,
    };
    tidb_ast::Visitable::accept(filter, &mut rewriter);
    (!rewriter.failed).then_some(())
}

/// Rewrites leaf-local predicates against the full table row the lookup
/// source returns. `RowSource` has already classified them as belonging to
/// this leaf; a rewrite failure therefore means the physical and logical
/// scopes disagree, and the index strategy is refused.
fn rewrite_inner_filters(
    inner: &JoinSide<'_>,
    columns: &[(String, FieldType)],
    filters: &[tidb_ast::Expr],
    ctx: &crate::StmtContext,
) -> Option<Vec<Expression>> {
    let database = inner
        .origin
        .as_deref()
        .and_then(|origin| origin.rsplit_once('.'))
        .map(|(database, _)| database.to_owned());
    let mut scope = crate::plan_trace::PlanTrace::single_table_scope(
        &inner.source_visible,
        database,
        columns.to_vec(),
    );
    scope.zone = ctx.session_zone();
    let resolver = crate::driver::from::ScopeResolver { scope: &scope };
    filters
        .iter()
        .map(|filter| {
            let mut expression = rewrite_expr_resolved(filter, &resolver).ok()?;
            tidb_expr::builtin_compare::refine_comparisons(&mut expression, ctx).ok()?;
            Some(expression)
        })
        .collect()
}

/// Constant equalities available to the looked-up table, in table-column
/// order and already converted into each column's storage domain.
fn static_equalities(
    columns: &[(String, FieldType)],
    filters: &[tidb_ast::Expr],
    ctx: &crate::StmtContext,
) -> Vec<Option<Datum>> {
    let mut values = vec![None; columns.len()];
    for filter in filters {
        let mut pairs = Vec::new();
        if !crate::driver::access::name_value_pairs(filter, &mut pairs, &ctx.session_zone())
            || pairs.len() != 1
            || !crate::driver::access::convert_pairs_to_column_domain(&mut pairs, columns)
        {
            continue;
        }
        let pair = &pairs[0];
        if let Some(offset) = columns
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(pair.column()))
        {
            values[offset] = Some(pair.value().clone());
        }
    }
    values
}

pub(crate) fn residual_filter_selectivity(
    filters: &[tidb_ast::Expr],
    static_columns: &[usize],
    columns: &[(String, FieldType)],
    table: &KvTable,
    visible: &str,
    statistics: Option<&crate::access_cost::TableStatistics>,
    zone: &tidb_datatype::SessionTimeZone,
) -> f64 {
    let residual: Vec<&tidb_ast::Expr> = filters
        .iter()
        .filter(|filter| {
            let mut pairs = Vec::new();
            if !crate::driver::access::name_value_pairs(filter, &mut pairs, zone)
                || pairs.len() != 1
            {
                return true;
            }
            !static_columns.iter().any(|offset| {
                columns
                    .get(*offset)
                    .is_some_and(|(name, _)| pairs[0].column().eq_ignore_ascii_case(name))
            })
        })
        .collect();
    if residual.is_empty() {
        1.0
    } else {
        let mut scope =
            crate::plan_trace::PlanTrace::single_table_scope(visible, None, columns.to_vec());
        scope.zone = zone.clone();
        let resolver = crate::driver::from::ScopeResolver { scope: &scope };
        let selectivity =
            crate::access_cost::selectivity_of_conjuncts(&residual, table, &resolver, statistics);
        // Go's cardinality.Selectivity applies the session SelectionFactor to a
        // residual predicate that has no usable statistics (for example the
        // q4 column-to-column date comparison). Keep the access-before-filter
        // estimate distinct from the logical rows that survive the Selection.
        if selectivity >= 1.0 {
            crate::plan_trace::SELECTIVITY_FACTOR
        } else {
            selectivity
        }
    }
}

fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Bytes(value) => format!("\"{}\"", String::from_utf8_lossy(value)),
        other => other.sql_string().unwrap_or_else(|_| format!("{other:?}")),
    }
}

/// One looked-up column as `EXPLAIN` prints it INSIDE a range, which is Go's
/// `OrigName`: `<database>.<table>.<column>` written under the TABLE's own
/// name, never the alias the side is read under.
///
/// The scope-qualified name is the fallback for a side whose origin was not
/// resolved; a decision is only ever reached for a side that HAS one, so the
/// fallback is unreachable in practice rather than a second spelling.
fn inner_column_name(inner: &JoinSide<'_>, column: usize) -> String {
    match (&inner.origin, inner.table) {
        (Some(origin), Some(table)) => format!(
            "{}.{}",
            origin.to_lowercase(),
            table.columns[column].name.to_lowercase()
        ),
        _ => inner.names[column].clone(),
    }
}

/// Go's dynamic IndexJoin range text uses the outer column's `OrigName`, not
/// the relation alias through which SQL name resolution reached it.
pub(crate) fn physical_outer_column_name(outer: &JoinSide<'_>, output: usize) -> String {
    outer
        .origin
        .as_ref()
        .zip(outer.table)
        .zip(outer.output_to_source.get(output).copied().flatten())
        .and_then(|((origin, table), source)| {
            table
                .visible_columns()
                .get(source)
                .map(|column| format!("{}.{}", origin.to_lowercase(), column.name.to_lowercase()))
        })
        .unwrap_or_else(|| outer.names[output].clone())
}

/// The two sides of a join as the decision reads them, built from the scope
/// the join produced and the executors its children became.
pub(crate) fn join_sides<'a>(
    join: &tidb_ast::Join,
    keys: &[EquiKey],
    scope: &FromScope,
    current_db: &str,
    left_width: usize,
    catalog: &'a Catalog,
    left_types: &[FieldType],
    right_types: &[FieldType],
    planner_inner: Option<(i64, Option<&str>)>,
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
            origin: None,
            source_visible: String::new(),
            output_to_source: Vec::new(),
            source_filters: Vec::new(),
            aggregation: None,
            aggregation_info: None,
            aggregation_final_info: None,
            aggregation_partial_info: None,
            composite: false,
        },
    };
    (
        discover_composite_target(
            left,
            &join.left,
            &keys.iter().map(|key| key.left).collect::<Vec<_>>(),
            catalog,
            current_db,
            planner_inner,
        ),
        discover_composite_target(
            right,
            join.right.as_ref().expect("right side exists"),
            &keys.iter().map(|key| key.right).collect::<Vec<_>>(),
            catalog,
            current_db,
            planner_inner,
        ),
    )
}

/// Finds the physical table leaf that carries a join key through a composite
/// side. The side keeps its full output names, while `output_to_source` marks
/// only columns that can be mapped back to this target table.
fn discover_composite_target<'a>(
    mut side: JoinSide<'a>,
    node: &tidb_ast::JoinNode,
    key_outputs: &[usize],
    catalog: &'a Catalog,
    current_db: &str,
    planner_inner: Option<(i64, Option<&str>)>,
) -> JoinSide<'a> {
    if side.table.is_some() || planner_inner.is_none() {
        return side;
    }
    let (target_table_id, target_relation) = planner_inner.expect("checked above");
    let mut tables = Vec::new();
    collect_table_refs(node, &mut tables);
    for table_ref in tables {
        let Ok((database, name)) =
            crate::driver::catalog::split_table_path(&table_ref.name, current_db)
        else {
            continue;
        };
        let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
            continue;
        };
        let visible = table_ref.alias.as_deref().unwrap_or(name);
        if table.table_id != target_table_id
            || target_relation.is_some_and(|relation| !relation.eq_ignore_ascii_case(visible))
        {
            continue;
        }
        let origin = format!("{database}.{name}");
        let matches = side
            .names
            .iter()
            .enumerate()
            .filter_map(|(output, rendered)| {
                let suffix = rendered
                    .strip_prefix(&format!("{origin}."))
                    .or_else(|| rendered.strip_prefix(&format!("{}.", origin.to_lowercase())))?;
                let source = table
                    .visible_columns()
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(suffix))?;
                Some((output, source))
            })
            .collect::<Vec<_>>();
        if !matches
            .iter()
            .any(|(output, _)| key_outputs.contains(output))
        {
            continue;
        }
        side.table = Some(table);
        let visible = visible.to_owned();
        side.visible.clone_from(&visible);
        side.source_visible = visible;
        side.origin = Some(origin);
        side.output_to_source = vec![None; side.names.len()];
        for (output, source) in matches {
            side.output_to_source[output] = Some(source);
        }
        side.composite = true;
        return side;
    }
    side
}

/// Collects syntax leaves so the exact physical scan receipt can be mapped
/// back to the executor's source object. Operator admission has already
/// happened in the shared planner; this walk makes no planning decision.
fn collect_table_refs<'a>(node: &'a tidb_ast::JoinNode, out: &mut Vec<&'a tidb_ast::TableRef>) {
    match node {
        tidb_ast::JoinNode::Table(table) => out.push(table),
        tidb_ast::JoinNode::Join(join) => {
            collect_table_refs(&join.left, out);
            if let Some(right) = &join.right {
                collect_table_refs(right, out);
            }
        }
        tidb_ast::JoinNode::Derived { subquery, .. } => {
            if let tidb_ast::QueryStmt::Select(select) = &**subquery {
                if let Some(from) = &select.from {
                    collect_table_refs(&from.left, out);
                    if let Some(right) = &from.right {
                        collect_table_refs(right, out);
                    }
                }
            }
        }
    }
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
    let mut names: Vec<String> = (from..to)
        .map(|offset| {
            if computed.get(offset - from).copied().unwrap_or(false) {
                UNNAMED_COLUMN.to_owned()
            } else {
                crate::driver::from::qualified_scope_column(scope, current_db, offset)
            }
        })
        .collect();
    let relation_visible = relation_visible_name(node);
    let derived = grouped_aggregate_of(node, catalog, current_db);
    let read = derived
        .as_ref()
        .map(|derived| {
            (
                derived.table,
                derived.source_visible.clone(),
                derived.origin.clone(),
            )
        })
        .or_else(|| single_table_of(node, catalog, current_db));
    let visible = relation_visible.or_else(|| {
        read.as_ref()
            .map(|(_, source_visible, _)| source_visible.clone())
    });
    let origin = read.as_ref().map(|(_, _, origin)| origin.clone());
    let source_visible = read
        .as_ref()
        .map_or_else(String::new, |(_, visible, _)| visible.clone());
    let output_to_source = derived.as_ref().map_or_else(
        || {
            read.as_ref().map_or_else(
                || vec![None; types.len()],
                |(table, ..)| {
                    (from..to)
                        .map(|output| {
                            let (name, _) = scope.column_at(output)?;
                            table
                                .visible_columns()
                                .iter()
                                .position(|column| column.name.eq_ignore_ascii_case(name))
                        })
                        .collect()
                },
            )
        },
        |derived| derived.output_to_source.clone(),
    );
    if let Some(derived) = &derived {
        for (output, source) in output_to_source.iter().copied().enumerate() {
            if let Some(source) = source {
                names[output] = format!(
                    "{}.{}",
                    derived.origin,
                    derived.table.visible_columns()[source].name
                );
            }
        }
    }
    for (output, name) in names.iter_mut().enumerate() {
        let Some(path) = scope.qualified_path(from + output) else {
            continue;
        };
        let [.., relation, column] = path.as_slice() else {
            continue;
        };
        if let Some(physical) = super::merge_decision::physical_column_trace_name(
            node,
            &super::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            },
            catalog,
            current_db,
        ) {
            *name = physical;
        }
    }
    JoinSide {
        table: read.map(|(kv, ..)| kv),
        visible: visible.unwrap_or_default(),
        types: types.to_vec(),
        names,
        origin,
        source_visible,
        output_to_source,
        source_filters: derived
            .as_ref()
            .map_or_else(Vec::new, |derived| derived.filters.clone()),
        aggregation: derived.as_ref().map(|derived| derived.aggregation.clone()),
        aggregation_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_info.clone()),
        aggregation_final_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_final_info.clone()),
        aggregation_partial_info: derived
            .as_ref()
            .map(|derived| derived.aggregation_partial_info.clone()),
        composite: false,
    }
}

fn relation_visible_name(node: &tidb_ast::JoinNode) -> Option<String> {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            table.alias.clone().or_else(|| table.name.last().cloned())
        }
        tidb_ast::JoinNode::Derived { alias, .. } => alias.clone(),
        tidb_ast::JoinNode::Join(join)
            if join.right.is_none()
                && join.on.is_none()
                && join.using.is_empty()
                && !join.natural =>
        {
            relation_visible_name(&join.left)
        }
        tidb_ast::JoinNode::Join(_) => None,
    }
}

struct GroupedAggregateLookup<'a> {
    table: &'a KvTable,
    source_visible: String,
    origin: String,
    output_to_source: Vec<Option<usize>>,
    filters: Vec<tidb_ast::Expr>,
    aggregation: crate::join::IndexLookupAggregation,
    aggregation_info: String,
    aggregation_final_info: String,
    aggregation_partial_info: String,
}

/// A grouped derived side Go can rebuild over a re-seeded index reader.
///
/// This is intentionally the smallest truthful rule: one base table, plain
/// non-null integer group keys, carried group columns, and the COUNT, MAX, or
/// exact decimal SUM aggregates used by the Go TPCC plans. Unsupported clauses
/// or expressions keep the ordinary materialized join.
fn grouped_aggregate_of<'a>(
    node: &tidb_ast::JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<GroupedAggregateLookup<'a>> {
    let tidb_ast::JoinNode::Derived {
        subquery,
        lateral: false,
        column_names,
        ..
    } = node
    else {
        return None;
    };
    if !column_names.is_empty() {
        return None;
    }
    let tidb_ast::QueryStmt::Select(select) = &**subquery else {
        return None;
    };
    if select.with.is_some()
        || !select.values.is_empty()
        || select.distinct
        || select.rollup
        || select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return None;
    }
    let from = select.from.as_ref()?;
    if from.right.is_some() || from.on.is_some() || !from.using.is_empty() || from.natural {
        return None;
    }
    let (table, source_visible, origin) = single_table_of(&from.left, catalog, current_db)?;
    let column_offset = |path: &[String]| {
        let name = path.last()?;
        table
            .visible_columns()
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    };
    let group_offsets = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            tidb_ast::Expr::Column(path) => column_offset(path),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if group_offsets.iter().any(|offset| {
        let field_type = &table.visible_columns()[*offset].field_type;
        !field_type.code().is_type_integer()
            || !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
    }) {
        return None;
    }
    let group_set = group_offsets
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut input_offsets = std::collections::BTreeSet::new();
    let mut output_to_source = Vec::with_capacity(select.fields.fields().len());
    let mut outputs = Vec::with_capacity(select.fields.fields().len());
    let mut aggregate_functions = Vec::new();
    let mut final_aggregate_functions = Vec::new();
    let mut first_row_functions = Vec::new();
    let physical_name =
        |offset: usize| format!("{origin}.{}", table.visible_columns()[offset].name);
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Expr {
                expr: tidb_ast::Expr::Column(path),
                ..
            } => {
                let offset = column_offset(path)?;
                if !group_set.contains(&offset) {
                    return None;
                }
                output_to_source.push(Some(offset));
                outputs.push(crate::join::IndexLookupAggregateOutput::Column(offset));
                let name = physical_name(offset);
                first_row_functions.push((offset, format!("funcs:firstrow({name})->{name}")));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("COUNT") => {
                let offset = match args.as_slice() {
                    [tidb_ast::Expr::Int(_)] => None,
                    [tidb_ast::Expr::Column(path)] => Some(column_offset(path)?),
                    _ => return None,
                };
                if let Some(offset) = offset {
                    input_offsets.insert(offset);
                }
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::Count(offset));
                let input = offset.map_or_else(|| "1".to_owned(), physical_name);
                aggregate_functions.push(format!(
                    "funcs:count({input})->Column#{}",
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:count(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("MAX") => {
                let [tidb_ast::Expr::Column(path)] = args.as_slice() else {
                    return None;
                };
                let offset = column_offset(path)?;
                input_offsets.insert(offset);
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::Max {
                    offset,
                    collation: table.visible_columns()[offset].field_type.collation(),
                });
                aggregate_functions.push(format!(
                    "funcs:max({})->Column#{}",
                    physical_name(offset),
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:max(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            tidb_ast::SelectField::Expr {
                expr:
                    tidb_ast::Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                ..
            } if name.eq_ignore_ascii_case("SUM") => {
                let [tidb_ast::Expr::Column(path)] = args.as_slice() else {
                    return None;
                };
                let offset = column_offset(path)?;
                if table.visible_columns()[offset].field_type.code()
                    != tidb_datatype::FieldTypeCode::NewDecimal
                {
                    return None;
                }
                input_offsets.insert(offset);
                output_to_source.push(None);
                outputs.push(crate::join::IndexLookupAggregateOutput::DecimalSum(offset));
                aggregate_functions.push(format!(
                    "funcs:sum({})->Column#{}",
                    physical_name(offset),
                    aggregate_functions.len()
                ));
                final_aggregate_functions.push(format!(
                    "funcs:sum(Column#{0})->Column#{0}",
                    final_aggregate_functions.len()
                ));
            }
            _ => return None,
        }
    }
    let mut groups = group_offsets
        .iter()
        .map(|offset| physical_name(*offset))
        .collect::<Vec<_>>();
    groups.sort_unstable();
    let aggregate_precedes_carriers = select
        .fields
        .fields()
        .iter()
        .position(|field| {
            matches!(field, tidb_ast::SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
        })
        .zip(select.fields.fields().iter().position(|field| {
            matches!(field,
                tidb_ast::SelectField::Expr { expr: tidb_ast::Expr::Column(path), .. }
                if select.group_by.iter().any(|item| {
                    matches!(&item.expr, tidb_ast::Expr::Column(group) if group == path)
                })
            )
        }))
        .is_some_and(|(aggregate, carrier)| aggregate < carrier);
    if !aggregate_precedes_carriers {
        first_row_functions.sort_by_key(|(offset, _)| *offset);
    }
    let first_row_functions = first_row_functions
        .into_iter()
        .map(|(_, function)| function)
        .collect::<Vec<_>>();
    let aggregation_partial_info = if aggregate_functions.is_empty() {
        format!("group by:{}", groups.join(", "))
    } else {
        format!(
            "group by:{}, {}",
            groups.join(", "),
            aggregate_functions.join(", ")
        )
    };
    let order_functions = |mut aggregates: Vec<String>, mut carriers: Vec<String>| {
        if super::derived_agg_pruning::has_pruned_row_count(select) {
            carriers.extend(aggregates);
            carriers
        } else {
            aggregates.extend(carriers);
            aggregates
        }
    };
    let functions = order_functions(aggregate_functions, first_row_functions.clone());
    let final_functions = order_functions(final_aggregate_functions, first_row_functions);
    let aggregation_info = format!("group by:{}, {}", groups.join(", "), functions.join(", "));
    let aggregation_final_info = format!(
        "group by:{}, {}",
        groups.join(", "),
        final_functions.join(", ")
    );
    let mut filters = Vec::new();
    if let Some(predicate) = &select.where_clause {
        ast_conjuncts(predicate, &mut filters);
    }
    Some(GroupedAggregateLookup {
        table,
        source_visible,
        origin,
        output_to_source,
        filters,
        aggregation: crate::join::IndexLookupAggregation {
            group_offsets,
            input_offsets: input_offsets.into_iter().collect(),
            outputs,
            pruned_row_count: super::derived_agg_pruning::has_pruned_row_count(select),
        },
        aggregation_info,
        aggregation_final_info,
        aggregation_partial_info,
    })
}

fn ast_conjuncts(expr: &tidb_ast::Expr, out: &mut Vec<tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) => {
            ast_conjuncts(left, out);
            ast_conjuncts(right, out);
        }
        expr => out.push(expr.clone()),
    }
}

/// The base table `node` reads whole, the name it is written under, and its
/// own `<database>.<table>` qualification -- or `None` for every other shape.
fn single_table_of<'a>(
    node: &tidb_ast::JoinNode,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<(&'a KvTable, String, String)> {
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
    Some((kv, visible, format!("{database}.{name}")))
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
    let select = match &**subquery {
        tidb_ast::QueryStmt::Select(select) => select,
        // A SET OPERATION's outputs are its OWN columns, never a
        // pass-through of any term's: `buildProjection4Union`
        // (`logical_plan_builder.go:2053`) allocates one fresh
        // `*expression.Column` per output through `AllocPlanColumnID` and
        // gives every child a clone of that schema, so none of them carries
        // an `OrigName` and `EXPLAIN` prints each as a bare `Column`.
        tidb_ast::QueryStmt::SetOpr(_) => return vec![true; width],
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
