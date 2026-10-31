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

//! `FROM` and `JOIN`: the half of the SELECT spine batch 6a completed as typed
//! logical operators.
//!
//! Go sources, by symbol:
//!
//! | Rust | Go `logical_plan_builder.go` |
//! | --- | --- |
//! | [`PlanBuilder::build_table_refs`] | `buildTableRefs` (:420) |
//! | [`PlanBuilder::build_derived_source`] | `buildResultSetNode`'s `*ast.TableSource` arm (:441-576) |
//! | [`PlanBuilder::build_join`] | `buildJoin` (:736) |
//! | [`PlanBuilder::build_lateral_join`] | `buildLateralJoin` (:956) |
//! | [`PlanBuilder::build_using_clause`] | `buildUsingClause` (:1104) |
//! | [`PlanBuilder::build_natural_join`] | `buildNaturalJoin` (:1128) |
//! | [`PlanBuilder::coalesce_common_columns`] | `coalesceCommonColumns` (:1142) |
//! | [`PlanBuilder::build_mem_table`] | `buildMemTable` (:5372) |
//! | [`PlanBuilder::check_recursive_view`] | `checkRecursiveView` (:5487) |
//! | [`PlanBuilder::build_data_source_from_view`] | `BuildDataSourceFromView` (:5509) |
//! | [`PlanBuilder::build_proj_upon_view`] | `buildProjUponView` (:5646) |
//! | [`find_join_full_schema`] | `findJoinFullSchema` (:645) |
//! | [`contains_lateral_table_source`] | `containsLateralTableSource` (:676) |
//! | [`is_immediate_lateral_table_source`] | `isImmediateLateralTableSource` (:721) |
//! | [`set_preferred_store_type`] | `setPreferredStoreType` (:586) |
//!
//! plus `buildSelectLock` (`planbuilder.go:1610`) as
//! [`PlanBuilder::build_select_lock`], and `checkNonUniqTableAlias`
//! (`preprocess.go:1139`) / `isTableAliasDuplicate` (`preprocess.go:1158`) as
//! [`check_non_uniq_table_alias`] / [`is_table_alias_duplicate`], which a
//! survey confirmed were not ported anywhere in the workspace.
//!
//! Join method selection belongs to physical planning. This builder owns
//! source schemas, names, conditions, view expansion and derived aliases.
//! [`ViewBuildGuard`] follows Go's name-based recursion check; the retired
//! executor materializer's thread-local depth limit is no longer used.
//!
//! # Section 3 of [`super`], everywhere
//!
//! `buildJoin` is where the read-after-move rule bites hardest: Go reads
//! `leftPlan.Schema()` in eleven places AFTER `joinPlan.SetChildren(leftPlan,
//! rightPlan)`. Every one of those reads is served here from a
//! [`snapshot_schema_and_names`] taken before the move, and the snapshots are
//! what [`PlanBuilder::coalesce_common_columns`] takes as parameters rather
//! than the two child plans Go passes.
//!
//! # Boundaries, by exact Go symbol
//!
//! * `hint.PlanHints` / `hint.tidb_planner_util::misc::HintedTable`'s query-block matching is retained
//!   for the SELECT builder's preorder offsets and `sel_N` names. View-path
//!   hint resolution remains owned by `BuildDataSourceFromView`, as in Go.
//! * `coreusage.ExtractCorColumnsBySchema4LogicalPlan`. Present, as
//!   [`crate::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan`];
//!   used unchanged.
//! * CORRELATED resolution inside a `LATERAL` body. Go's `b.rewrite` consults
//!   `b.outerSchemas` and produces an `*expression.CorrelatedColumn`;
//!   [`PlanBuilder::rewrite_scalar`] is 6a's SUBQUERY-FREE rewrite and resolves
//!   against ONE plan's schema, so a `LATERAL (SELECT t1.a)` body reports an
//!   unresolved column. The apply is still built — Go's condition (a), an
//!   immediately-`LATERAL` right operand, does not depend on any correlation
//!   being found — and [`extract_cor_columns_by_schema_4_logical_plan`] is
//!   called exactly where Go calls it, so closing 6a's rewrite seam closes
//!   this with no change here.
//! * `setIsInApplyForCTE` (`:5763`). `LogicalCTE`'s `IsInApply` is a batch-6d
//!   field on the CTE producer; the walk is a one-liner there and would have
//!   no reader before that batch lands, so [`PlanBuilder::build_lateral_join`]
//!   names it rather than walking a tree whose CTE nodes cannot yet exist.
//! * `b.Build(ctx, nodeW)` inside `BuildDataSourceFromView`. The view body is
//!   built with [`PlanBuilder::build_select`], which is the SELECT statement
//!   Go's `Build` dispatches to; a view whose body is a `UNION` reaches
//!   `buildSetOpr`, batch 6d, and is refused by name.
//! * `aliasChecker` (`preprocess.go:2357`). Its whole body is the DELETE
//!   statement's `IsAlias` tagging — `getTableRefsAlias` writes
//!   `table.IsAlias = true` back into the AST for `DELETE t FROM t AS ...`.
//!   There is no DELETE builder and `tidb_ast`'s table reference carries no
//!   `is_alias` bit to write, so [`table_refs_alias`] ports the READ half
//!   (`getTableRefsAlias`, `:2389`) and the tagging arm is named here.
//!
//! # Narrowings, by name
//!
//! * `cteInfo.recursiveRef` — `buildTableRefs`' deferred reset walks
//!   `b.outerCTEs` clearing it. 6a's [`OuterCte`](super::OuterCte) narrowed
//!   the field away (it gates `buildCte`'s recursive-reference detection,
//!   batch 6d), so the reset has nothing to clear and is a no-op named in
//!   [`PlanBuilder::build_table_refs`].
//! * `b.inUpdateStmt` / `b.inDeleteStmt` — DROPPED by 6a. Both guard the same
//!   arm of `buildUsingClause`/`buildNaturalJoin`/`coalesceCommonColumns`:
//!   UPDATE and DELETE restore the merged child schema instead of the
//!   coalesced one. With no DML builder the guard is constant-false, so the
//!   coalesced schema always stands — which is the SELECT behaviour Go has.
//! * `b.ctx.GetSessionVars().StmtCtx.ViewDepth` is the builder's
//!   [`PlanBuilder::view_depth`]; recursion is refused by the name-keyed
//!   stack, and the counter's only reader is the `ErrViewNoExplain` arm.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use tidb_ast::{Join, JoinNode, JoinType, QueryStmt};
use tidb_datatype::{
    FieldName, FieldNameMetadata, FieldType, FieldTypeCode, FieldTypeFlags, IdentifierMetadata,
};
use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::into_cnf_items;
use tidb_expr::expr_util::{FunctionBuilder, RealFunctionBuilder, SubstituteOptions};
use tidb_expr::expression::Expression;
use tidb_expr::schema::{merge_schema, Schema};
use tidb_exprctx::Columns;

use crate::expression_rewriter::{extract_cor_columns_by_schema_4_logical_plan, ClauseCode};
use tidb_planner_core_base::LogicalJoinType;
use tidb_planner_core_operator_logicalop::apply::LogicalApply;
use tidb_planner_core_operator_logicalop::join::LogicalJoin;
use tidb_planner_core_operator_logicalop::lock::{LogicalLock, SelectLockType};
use tidb_planner_core_operator_logicalop::mem_table::{LogicalMemTable, MemTableColumn};
use tidb_planner_core_operator_logicalop::projection::LogicalProjection;
use tidb_planner_core_rule::flags;
use tidb_planner_core_operator_logicalop::selection::LogicalSelection;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_base::PlanError;

use tidb_infoschema::table_source::TableSource;
use tidb_infoschema::table_source::SourceView;
use tidb_infoschema::table_source::SourceTable;
use super::handle_col_helper::PlanHandleCols;
use super::marker::MarkerKind;
use super::{
    lower_name_matches, snapshot_schema_and_names, PlanBuilder, EXTRA_COMMIT_TS_NAME,
    EXTRA_HANDLE_NAME,
};

/// Go `model.ExtraPhysTblIDName` (`meta/model/table.go:95`), which
/// [`PlanBuilder::coalesce_common_columns`] excludes from `NATURAL`/`USING`
/// matching beside the other two extra columns.
pub const EXTRA_PHYS_TBL_ID_NAME: &str = "_tidb_tid";

/// Go `LogicalJoin.SetPreferredJoinTypeAndOrder(hintInfo)`
/// (`logical_join.go:1596`), over the two children's already-snapshotted
/// output names.
///
/// Go reads the aliases off `p.Children()`; per [`super`]'s section 3 the
/// children have moved by then, so the names arrive as parameters.
pub fn set_preferred_join_type_and_order(
    join: &mut LogicalJoin,
    hints: &Rc<tidb_planner_util::misc::JoinHints>,
    (left_names, left_offset): (&[FieldName], i32),
    (right_names, right_offset): (&[FieldName], i32),
    ctx: &dyn tidb_exprctx::Columns,
) {
    use tidb_hint::join_hint_flags as f;

    let lhs = tidb_planner_util::misc::extract_table_alias(left_names).map(|alias| tidb_planner_util::misc::HintedTable {
        select_offset: hints.alias_select_offset(left_offset),
        ..alias
    });
    let rhs = tidb_planner_util::misc::extract_table_alias(right_names).map(|alias| tidb_planner_util::misc::HintedTable {
        select_offset: hints.alias_select_offset(right_offset),
        ..alias
    });
    // Every arm is Go's: the symmetric hints set the same bit on the join and
    // on the side that named it; the index-join hints set the bit naming the
    // OTHER side as inner.
    let symmetric = [
        f::MERGE_JOIN,
        f::NO_MERGE_JOIN,
        f::BC_JOIN,
        f::SHUFFLE_JOIN,
        f::HASH_JOIN,
        f::NO_HASH_JOIN,
        f::NO_INDEX_JOIN,
        f::NO_INDEX_HASH_JOIN,
        f::NO_INDEX_MERGE_JOIN,
    ];
    for flag in symmetric {
        // go `addMPPHintCalls`-era semantics: the broadcast/shuffle join
        // hints request MPP pushdown, which a tier without the MPP engine
        // cannot serve — go warns `The join can not push down to the MPP
        // side, the X() hint is invalid` (Warning 1815) and ignores the
        // hint entirely.
        if matches!(flag, f::BC_JOIN | f::SHUFFLE_JOIN) {
            // Run the table-name matching FIRST (go MatchTableName marks the
            // hinted entries), so a MATCHED MPP hint warns the invalid-
            // push-down warning while an unmatched name keeps its own
            // no-matching-table warning.
            // BOTH sides must run their matching: the || short-circuit
            // would skip the rhs pass when the lhs matched, leaving the
            // rhs hint entry unmarked and firing go's no-matching-table
            // warning beside the MPP warning.
            let matched_lhs = hints.prefers(lhs.as_ref(), flag);
            let matched_rhs = hints.prefers(rhs.as_ref(), flag);
            let matched_any = matched_lhs || matched_rhs;
            if matched_any {
                ctx.append_go_warning(
                    &tidb_plannererrors::ERR_INTERNAL,
                    &format!(
                        "The join can not push down to the MPP side, the {}() hint is invalid",
                        if flag == f::BC_JOIN {
                            "broadcast_join"
                        } else {
                            "shuffle_join"
                        }
                    ),
                );
            }
            continue;
        }
        if hints.prefers(lhs.as_ref(), flag) {
            join.prefer_join_type |= flag;
            join.left_prefer_join_type |= flag;
        }
        if hints.prefers(rhs.as_ref(), flag) {
            join.prefer_join_type |= flag;
            join.right_prefer_join_type |= flag;
        }
    }
    let inner_side = [
        (f::INLJ, f::LEFT_AS_INLJ_INNER, f::RIGHT_AS_INLJ_INNER),
        (f::INLHJ, f::LEFT_AS_INLHJ_INNER, f::RIGHT_AS_INLHJ_INNER),
        (f::INLMJ, f::LEFT_AS_INLMJ_INNER, f::RIGHT_AS_INLMJ_INNER),
        (f::HJ_BUILD, f::LEFT_AS_HJ_BUILD, f::RIGHT_AS_HJ_BUILD),
        (f::HJ_PROBE, f::LEFT_AS_HJ_PROBE, f::RIGHT_AS_HJ_PROBE),
    ];
    for (hinted, left_bit, right_bit) in inner_side {
        if hints.prefers(lhs.as_ref(), hinted) {
            join.prefer_join_type |= left_bit;
            join.left_prefer_join_type |= hinted;
        }
        if hints.prefers(rhs.as_ref(), hinted) {
            join.prefer_join_type |= right_bit;
            join.right_prefer_join_type |= hinted;
        }
    }
    let straight_join_order = hints
        .canonical
        .as_ref()
        .is_some_and(|canonical| canonical.borrow().straight_join_order);
    let has_conflict = if !hints.advanced_join_hint || straight_join_order {
        contain_different_join_types(join.prefer_join_type)
    } else {
        contain_different_join_types(join.left_prefer_join_type)
            || contain_different_join_types(join.right_prefer_join_type)
    };
    if has_conflict {
        ctx.append_go_warning(
            &tidb_plannererrors::ERR_INTERNAL,
            "Join hints are conflict, you can only specify one type of join",
        );
        join.prefer_join_type = 0;
    }
    // Go `if hintInfo.LeadingJoinOrder != nil { p.PreferJoinOrder =
    // hintInfo.MatchTableName(...) }`: ANY of the join's aliases in the
    // LEADING tables (by database, name and query block) marks it; the
    // unlisted aliases float freely. ParsePlanHints empties the tables when
    // several LEADING hints or STRAIGHT_JOIN void them, so no join is marked
    // although the first hint's LeadingList stays.
    if let Some(canonical) = &hints.canonical {
        let aliases = [lhs.as_ref(), rhs.as_ref()]
            .into_iter()
            .flatten()
            .map(|alias| tidb_hint::HintedTable {
                database_name: hints.alias_database(alias),
                table_name: alias.table_name.clone(),
                select_offset: alias.select_offset,
                ..tidb_hint::HintedTable::default()
            })
            .collect::<Vec<_>>();
        let candidates = aliases.iter().map(Some).collect::<Vec<_>>();
        let mut canonical = canonical.borrow_mut();
        join.prefer_join_order = tidb_hint::PlanHints::match_table_names(
            &candidates,
            &mut canonical.leading_join_order,
        );
    }
    if join.prefer_join_type != 0 || join.prefer_join_order {
        join.hint_info = Some(Rc::clone(hints));
    }
}

/// Go `containDifferentJoinTypes` (`logical_join.go:2087`).
fn contain_different_join_types(prefer_join_type: u32) -> bool {
    use tidb_hint::join_hint_flags as f;
    let prefer_join_type = prefer_join_type
        & !f::NO_HASH_JOIN
        & !f::NO_MERGE_JOIN
        & !f::NO_INDEX_JOIN
        & !f::NO_INDEX_HASH_JOIN
        & !f::NO_INDEX_MERGE_JOIN;
    let inl_mask = f::RIGHT_AS_INLJ_INNER ^ f::LEFT_AS_INLJ_INNER;
    let inlhj_mask = f::RIGHT_AS_INLHJ_INNER ^ f::LEFT_AS_INLHJ_INNER;
    let inlmj_mask = f::RIGHT_AS_INLMJ_INNER ^ f::LEFT_AS_INLMJ_INNER;
    let hj_right_build_mask = f::RIGHT_AS_HJ_BUILD ^ f::LEFT_AS_HJ_PROBE;
    let hj_left_build_mask = f::LEFT_AS_HJ_BUILD ^ f::RIGHT_AS_HJ_PROBE;
    let mpp_mask = f::SHUFFLE_JOIN ^ f::BC_JOIN;
    let mask = inl_mask ^ inlhj_mask ^ inlmj_mask ^ hj_right_build_mask ^ hj_left_build_mask;
    let ones_count = (prefer_join_type & !mask & !mpp_mask).count_ones();
    if ones_count > 1 || (ones_count == 1 && prefer_join_type & mask > 0) {
        return true;
    }
    [inl_mask, inlhj_mask, inlmj_mask, hj_left_build_mask, hj_right_build_mask]
        .into_iter()
        .filter(|side_mask| prefer_join_type & side_mask > 0)
        .count()
        > 1
}

/// Go `findJoinFullSchema(p)` (`:645`): the `FullSchema`/`FullNames` of the
/// join under any chain of single-child `LogicalSelection` wrappers.
///
/// Go's own comment states the rule this reproduces: a `LogicalSelection` from
/// an `ON` clause is transparent, but a `LogicalProjection` is a DERIVED TABLE
/// BOUNDARY and walking through it would leak the inner tables' aliases.
#[must_use]
pub fn find_join_full_schema(plan: &LogicalPlan) -> Option<(&Schema, &[FieldName])> {
    let mut plan = plan;
    loop {
        match plan {
            LogicalPlan::Join(join) => {
                return join
                    .full_schema
                    .as_ref()
                    .map(|schema| (schema, join.full_names.as_slice()))
            }
            LogicalPlan::Apply(apply) => {
                return apply
                    .join
                    .full_schema
                    .as_ref()
                    .map(|schema| (schema, apply.join.full_names.as_slice()))
            }
            LogicalPlan::Selection(selection) => match selection.base.children() {
                [child] => plan = child,
                _ => return None,
            },
            _ => return None,
        }
    }
}

/// Go `containsLateralTableSource(node)` (`:676`): is there a `LATERAL` table
/// source ANYWHERE in this subtree?
///
/// Go descends into `*ast.SelectStmt`'s `From` and every `*ast.SetOprStmt`
/// operand; the Rust AST reaches both through [`QueryStmt`].
#[must_use]
pub fn contains_lateral_table_source(node: &JoinNode) -> bool {
    match node {
        JoinNode::Derived {
            lateral, subquery, ..
        } => *lateral || query_contains_lateral(subquery),
        JoinNode::Join(join) => contains_lateral_in_join(join),
        JoinNode::Table(_) => false,
    }
}

/// The `*ast.Join` arm of the above, split out because `SelectStmt::from` is a
/// [`Join`] (Go's `TableRefsClause.TableRefs`) rather than a [`JoinNode`].
#[must_use]
pub fn contains_lateral_in_join(join: &Join) -> bool {
    match &join.right {
        // The parenthesized single-table form.
        None => contains_lateral_table_source(&join.left),
        Some(right) => {
            contains_lateral_table_source(&join.left) || contains_lateral_table_source(right)
        }
    }
}

/// The `*ast.SelectStmt` / `*ast.SetOprStmt` arms of the above.
fn query_contains_lateral(query: &QueryStmt) -> bool {
    match query {
        QueryStmt::Select(select) => select.from.as_ref().is_some_and(contains_lateral_in_join),
        QueryStmt::SetOpr(set_opr) => set_opr.terms.iter().any(|term| match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => {
                select.from.as_ref().is_some_and(contains_lateral_in_join)
            }
            tidb_ast::SetOprTermBody::Nested(nested) => {
                query_contains_lateral(&QueryStmt::SetOpr(nested.clone()))
            }
        }),
    }
}

/// Go `isImmediateLateralTableSource(node)` (`:721`): is the TOP-LEVEL node
/// itself a `LATERAL` source? A multi-table join on the right is not.
#[must_use]
pub fn is_immediate_lateral_table_source(node: &JoinNode) -> bool {
    match node {
        JoinNode::Derived { lateral, .. } => *lateral,
        JoinNode::Join(join) if join.right.is_none() => {
            is_immediate_lateral_table_source(&join.left)
        }
        _ => false,
    }
}

/// Go `getTableRefsAlias(tableRefs)` (`preprocess.go:2389`), the READ half of
/// `aliasChecker`; the DELETE-tagging half is a boundary above.
#[must_use]
pub fn table_refs_alias(node: &JoinNode) -> Option<&str> {
    match node {
        JoinNode::Join(join) => table_refs_alias(&join.left),
        JoinNode::Table(table_ref) => table_ref.alias.as_deref(),
        JoinNode::Derived { alias, .. } => alias.as_deref(),
    }
}

/// Go `tableAliasKey` (`preprocess.go`): a bare alias, or the qualified name a
/// schema-carrying `TableName` with no alias falls back to.
type TableAliasKey = (String, String);

/// Go `isTableAliasDuplicate(node, tableAliases)` (`preprocess.go:1158`).
///
/// # Errors
///
/// `ErrNonUniqTable` when the alias is already in `table_aliases`.
pub fn is_table_alias_duplicate(
    node: &JoinNode,
    table_aliases: &mut BTreeMap<TableAliasKey, String>,
) -> Result<(), PlanError> {
    // Go's `if ts, ok := node.(*ast.TableSource)`: only a table SOURCE has an
    // alias to collide. A nested `*ast.Join` is walked by the caller.
    let (key, display) = match node {
        JoinNode::Table(table_ref) => match table_ref.alias.as_deref() {
            Some(alias) if !alias.is_empty() => {
                ((String::new(), tidb_mysql::to_lowercase(alias)), alias)
            }
            _ => match table_ref.name.as_slice() {
                // `newQualifiedTableAliasKey(Schema, Name)` when the reference
                // is schema-qualified, `newTableAliasKey(Name)` otherwise.
                [db, table] => (
                    (
                        tidb_mysql::to_lowercase(db),
                        tidb_mysql::to_lowercase(table),
                    ),
                    table.as_str(),
                ),
                [table] => (
                    (String::new(), tidb_mysql::to_lowercase(table)),
                    table.as_str(),
                ),
                _ => return Ok(()),
            },
        },
        JoinNode::Derived { alias, .. } => match alias.as_deref() {
            Some(alias) if !alias.is_empty() => {
                ((String::new(), tidb_mysql::to_lowercase(alias)), alias)
            }
            // Go's `tabName.L == ""` with a non-`TableName` source leaves the
            // key empty and `len(tabName.L) != 0` false, so nothing is
            // recorded and nothing can collide.
            _ => return Ok(()),
        },
        JoinNode::Join(_) => return Ok(()),
    };
    if let Some(existing) = table_aliases.get(&key) {
        let existing = if existing.is_empty() {
            display
        } else {
            existing
        };
        return Err(PlanError::from(
            tidb_plannererrors::ERR_NON_UNIQ_TABLE
                .generate_with_stack_by_args(&[existing.into()]),
        ));
    }
    table_aliases.insert(key, display.to_owned());
    Ok(())
}

/// Go `preprocessor.checkNonUniqTableAlias(stmt)` (`preprocess.go:1139`).
///
/// Go threads `p.flag&parentIsJoin` and a STACK of alias maps so that one
/// `ast.Join` tree shares one map while a nested query block starts a fresh
/// one. The traversal here is that stack made explicit: one call covers one
/// join tree, recursing through nested `Join` nodes into the same map and
/// stopping at a derived table, which is its own block.
///
/// Go skips the whole check under `SQLMode` `ORACLE`; that session mode has no
/// carrier on this seam, so `oracle_mode` is a parameter.
///
/// # Errors
///
/// `ErrNonUniqTable` for the first duplicate alias found.
pub fn check_non_uniq_table_alias(node: &JoinNode, oracle_mode: bool) -> Result<(), PlanError> {
    if oracle_mode {
        return Ok(());
    }
    let mut aliases = BTreeMap::new();
    check_join_aliases(node, &mut aliases)
}

/// [`check_non_uniq_table_alias`] over a query block's whole `FROM` join.
///
/// # Errors
///
/// `ErrNonUniqTable` for the first duplicate alias found.
pub fn check_non_uniq_table_alias_in_join(join: &Join, oracle_mode: bool) -> Result<(), PlanError> {
    if oracle_mode {
        return Ok(());
    }
    let mut aliases = BTreeMap::new();
    check_join_aliases(&join.left, &mut aliases)?;
    match &join.right {
        Some(right) => check_join_aliases(right, &mut aliases),
        None => Ok(()),
    }
}

fn check_join_aliases(
    node: &JoinNode,
    aliases: &mut BTreeMap<TableAliasKey, String>,
) -> Result<(), PlanError> {
    match node {
        JoinNode::Join(join) => {
            check_join_aliases(&join.left, aliases)?;
            match &join.right {
                Some(right) => check_join_aliases(right, aliases),
                None => Ok(()),
            }
        }
        other => is_table_alias_duplicate(other, aliases),
    }
}

/// The RAII form of Go `checkRecursiveView`'s returned `func()`.
///
/// Go returns a closure the caller `defer`s; a `Drop` impl is the same thing
/// and cannot be forgotten. The stack it pops lives on the builder, so the
/// guard carries the key and the caller re-enters the builder to release it.
#[derive(Clone, Debug)]
#[must_use = "the guard must outlive the view body, or the stack pops too early"]
pub struct ViewBuildGuard {
    key: super::SchemaTableKey,
}

impl ViewBuildGuard {
    /// Releases the guard against the builder that issued it, which is Go's
    /// `delete(b.buildingViewStack, viewFullName)`.
    pub fn release<S: TableSource, C: Columns>(self, builder: &mut PlanBuilder<'_, S, C>) {
        builder.building_view_stack.remove(&self.key);
    }
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildTableRefs(ctx, from)` (`:420`).
    ///
    /// # Errors
    ///
    /// Whatever the `FROM` clause's own build returns.
    pub fn build_table_refs(&mut self, from: Option<&Join>) -> Result<LogicalPlan, PlanError> {
        let Some(join) = from else {
            return Ok(self.build_table_dual());
        };
        // Go's preprocessor runs `checkNonUniqTableAlias` over each query
        // block's join tree before the block is built.
        check_non_uniq_table_alias_in_join(join, self.oracle_mode)?;
        // Go `buildTableRefs` enters `buildResultSetNode(ctx, from, false)`.
        self.is_cte = false;
        let result = self.build_join(join);
        // Go resets this per query block. Multiple recursive SELECT blocks
        // may each reference the CTE once; a second reference in one block
        // is still rejected before this deferred reset runs.
        for cte in &mut self.outer_ctes {
            cte.recursive_ref = false;
        }
        result
    }

    /// Go `buildResultSetNode`'s `*ast.TableSource` arm over a derived table
    /// (`:441-576`): the `LATERAL` scoping, the alias rename, the
    /// `AS dt(c1, c2)` column list and the duplicate-name check.
    ///
    /// # Errors
    ///
    /// `ErrViewWrongList` when the column-alias list does not match the
    /// subquery's visible column count, `ErrDupFieldName` for a repeated
    /// output name, or the subquery's own error.
    pub fn build_derived_source(
        &mut self,
        subquery: &QueryStmt,
        alias: Option<&str>,
        lateral: bool,
        column_names: &[String],
    ) -> Result<LogicalPlan, PlanError> {
        // `:441` The LATERAL scoping. A NON-lateral derived table must not see
        // the outer schemas `buildJoin` pushed for its LATERAL siblings, so
        // they are hidden for the duration; a LATERAL one ADOPTS them into
        // ordinary correlated scope, so a plain subquery nested inside it can
        // still resolve them.
        let hidden = (self.lateral_outer_count > 0 && !lateral).then(|| {
            let keep = self.outer_schemas.len() - self.lateral_outer_count;
            let saved_schemas = self.outer_schemas.split_off(keep);
            let saved_names = self.outer_names.split_off(keep);
            let saved_count = std::mem::take(&mut self.lateral_outer_count);
            (saved_schemas, saved_names, saved_count)
        });
        let adopted = (self.lateral_outer_count > 0 && lateral)
            .then(|| std::mem::take(&mut self.lateral_outer_count));
        // Go sets `buildingLateralSubquery` on that same LATERAL arm, scoped
        // to this table source, so a recursive CTE part may ORDER BY / LIMIT
        // inside it.
        let saved_building_lateral = self.building_lateral_subquery;
        if adopted.is_some() {
            self.building_lateral_subquery = true;
        }

        let modified_ctes = self.prepare_cte_check_for_subquery();
        let built = match subquery {
            QueryStmt::Select(select) => {
                // `:482` "b.optFlag |= rule.FlagConstantPropagation".
                self.opt_flag |= flags::CONSTANT_PROPAGATION;
                self.build_select(select).map(|(plan, _)| plan)
            }
            // `buildResultSetNode`'s `*ast.SetOprStmt` arm (`:579`), which
            // landed in 6d as [`super::set_opr`].
            QueryStmt::SetOpr(set_opr) => {
                self.opt_flag |= flags::CONSTANT_PROPAGATION;
                self.build_set_opr(set_opr)
            }
        };
        self.reset_cte_check_for_subquery(&modified_ctes);

        if let Some((saved_schemas, saved_names, saved_count)) = hidden {
            self.outer_schemas.extend(saved_schemas);
            self.outer_names.extend(saved_names);
            self.lateral_outer_count = saved_count;
        }
        if let Some(saved_count) = adopted {
            self.lateral_outer_count = saved_count;
        }
        self.building_lateral_subquery = saved_building_lateral;
        let mut plan = built?;

        // `:497` The alias rename. A HIDDEN name is passed through untouched,
        // and a derived table's `DBName` is CLEARED so an error message reads
        // "alias.col" rather than "db.alias.col".
        let mut names = plan.output_names().to_vec();
        if let Some(alias) = alias.filter(|alias| !alias.is_empty()) {
            for name in &mut names {
                if name.hidden {
                    continue;
                }
                name.names.database = IdentifierMetadata::default();
                name.names.table = IdentifierMetadata::new(alias);
            }
        }

        // `:556` "TableName is not a select block": a derived table's block
        // records its alias, which join-hint aliases read.
        if let Some(first) = names.first() {
            self.select_block_as_names
                .borrow_mut()
                .insert(plan.query_block_offset(), first.names.table.lower.clone());
        }

        // `:530` The `AS dt(c1, c2, ...)` column list, harvested from
        // `driver/from.rs:1484` `rename_derived_columns` (whose
        // `ViewWrongList` error is this same one).
        if !column_names.is_empty() {
            let visible = names.iter().filter(|name| !name.hidden).count();
            if visible != column_names.len() {
                return Err(PlanError::from(
                    tidb_dbterror::ERR_VIEW_WRONG_LIST.generate_with_stack_by_args(&[]),
                ));
            }
            let mut aliases = column_names.iter();
            for name in names.iter_mut().filter(|name| !name.hidden) {
                let Some(alias) = aliases.next() else { break };
                name.names.column = IdentifierMetadata::new(alias);
                name.names.original_column = IdentifierMetadata::new(alias);
            }
        }

        // `:567` "Duplicate column name in one table is not allowed."
        let mut seen = BTreeSet::new();
        for name in &names {
            if !seen.insert(name.names.column.original.clone()) {
                return Err(PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
                        .generate_with_stack_by_args(&[(&name.names.column.original).into()]),
                ));
            }
        }
        // NOT `LogicalPlan::set_output_names`: that is Go
        // `BaseLogicalPlan.SetOutputNames`, which FORWARDS to `children[0]`.
        // A derived table's alias belongs to the projection at its root.
        plan.base_mut().base.set_output_names(names);
        Ok(plan)
    }

    /// Go `buildJoin(ctx, joinNode)` (`:736`).
    ///
    /// Ported: the single-operand unwrap, the `LATERAL` outer-schema push and
    /// its two-condition apply decision, the opt-flag set, the handle-map
    /// merge, the schema/name merge, the three join types with their
    /// `ResetNotNullFlag`, the `FullSchema`/`FullNames` merge with the RIGHT
    /// join's side swap, the hint preference, and the
    /// `NATURAL`/`USING`/`ON` dispatch — including the rule that an INNER join
    /// with an `ON` clause returns a [`LogicalSelection`] ABOVE the join
    /// rather than attaching the conditions to it.
    ///
    /// # Errors
    ///
    /// Any child's error, an `ON` clause containing a subquery, or a
    /// `NATURAL`/`USING` clause's own ambiguity/unknown-column errors.
    pub fn build_join(&mut self, join_node: &Join) -> Result<LogicalPlan, PlanError> {
        // `:738` "For this scenario joinNode.Right is nil and we only build
        // the left ResultSetNode."
        let Some(right_node) = join_node.right.as_ref() else {
            return self.build_result_set_node(&join_node.left);
        };

        let is_lateral = contains_lateral_table_source(right_node);

        self.opt_flag |= flags::PREDICATE_PUSH_DOWN | flags::JOIN_KEY_TYPE_CAST;
        // "Don't enable join reorder for LATERAL (similar to StraightJoin).
        // LATERAL has order dependencies: right side can reference left side."
        if !is_lateral {
            self.opt_flag |= flags::JOIN_REORDER;
        }
        self.opt_flag |= flags::PREDICATE_SIMPLIFICATION | flags::EMPTY_SELECTION_ELIMINATOR;

        let left_plan = self.build_result_set_node(&join_node.left)?;

        // `:765` For LATERAL, the left side's schema becomes an OUTER schema
        // while the right side is built, so its columns resolve as correlated
        // ones. A `USING`/`NATURAL` left side contributes its FULL schema,
        // which still holds the coalesced-away duplicates.
        let pushed_lateral = if is_lateral {
            let (outer_schema, outer_names) = match find_join_full_schema(&left_plan) {
                Some((schema, names)) => (schema.clone(), names.to_vec()),
                None => snapshot_schema_and_names(&left_plan),
            };
            self.outer_schemas.push(outer_schema);
            self.outer_names.push(outer_names);
            self.lateral_outer_count += 1;
            true
        } else {
            false
        };

        let right_built = self.build_result_set_node(right_node);
        if pushed_lateral {
            // Go's single `defer`: popped on the error path too.
            self.outer_schemas.pop();
            self.outer_names.pop();
            self.lateral_outer_count -= 1;
        }
        let mut right_plan = right_built?;

        // `:806` The apply decision, which is deliberately TIGHTER than the
        // push decision: only an immediately-LATERAL right operand, or a right
        // plan that really does correlate with THIS left side.
        if is_lateral {
            let outer_schema = match find_join_full_schema(&left_plan) {
                Some((schema, _)) => schema.clone(),
                None => left_plan.schema().cloned().unwrap_or_default(),
            };
            let cor_cols =
                extract_cor_columns_by_schema_4_logical_plan(&mut right_plan, &outer_schema);
            if is_immediate_lateral_table_source(right_node) || !cor_cols.is_empty() {
                return self.build_lateral_join(left_plan, right_plan, join_node);
            }
        }

        // `:829` "The recursive part in CTE must not be on the right side of a
        // LEFT JOIN."
        if let LogicalPlan::CTETable(cte) = &right_plan {
            if join_node.tp == JoinType::Left {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_CTERECURSIVE_FORBIDDEN_JOIN_ORDER
                        .generate_with_stack_by_args(&[(cte.name.clone()).into()]),
                ));
            }
        }

        self.handle_helper.pop_two_and_merge().ok_or_else(|| {
            PlanError::internal("handleHelper does not hold both join children's maps")
        })?;

        // Section 3: everything Go reads off the children AFTER `SetChildren`.
        let (left_schema, left_names) = snapshot_schema_and_names(&left_plan);
        let (right_schema, right_names) = snapshot_schema_and_names(&right_plan);
        let left_len = left_schema.columns.len();
        let left_full = find_join_full_schema(&left_plan)
            .map(|(schema, names)| (schema.clone(), names.to_vec()));
        let right_full = find_join_full_schema(&right_plan)
            .map(|(schema, names)| (schema.clone(), names.to_vec()));

        let mut join_plan = LogicalJoin::new(
            self.base(LogicalJoin::TYPE),
            // The join type is set below; `InnerJoin` is Go's zero value.
            LogicalJoinType::Inner,
        );
        join_plan.straight_join = join_node.straight || self.in_straight_join;
        join_plan.base.set_children(vec![left_plan, right_plan]);

        let mut schema = merge_schema(Some(&left_schema), Some(&right_schema)).unwrap_or_default();
        let mut names: Vec<FieldName> = left_names.to_vec();
        names.extend(right_names.iter().cloned());
        let schema_len = schema.columns.len();

        // `:843` The join type, and the nullability the outer side loses.
        match join_node.tp {
            JoinType::Left => {
                self.opt_flag |= flags::ELIMINATE_OUTER_JOIN | flags::OUTER_JOIN_TO_SEMI_JOIN;
                join_plan.join_type = LogicalJoinType::LeftOuter;
                tidb_planner_util::null_misc::reset_not_null_flag(&mut schema, left_len, schema_len);
            }
            JoinType::Right => {
                self.opt_flag |= flags::ELIMINATE_OUTER_JOIN | flags::OUTER_JOIN_TO_SEMI_JOIN;
                join_plan.join_type = LogicalJoinType::RightOuter;
                tidb_planner_util::null_misc::reset_not_null_flag(&mut schema, 0, left_len);
            }
            JoinType::Cross => join_plan.join_type = LogicalJoinType::Inner,
        }
        join_plan.base.base.set_schema(Some(schema));
        join_plan.base.base.set_output_names(names);

        // `:860` The `FullSchema`/`FullNames` merge. A child that already
        // carries one contributes it; otherwise its own schema is its full
        // schema. For a RIGHT join the two are SWAPPED first so that `l`
        // always means the OUTER side, which is what the `Redundant` marking
        // in [`Self::coalesce_common_columns`] then relies on.
        let (mut l_full_schema, mut l_full_names) =
            left_full.unwrap_or_else(|| (left_schema.clone(), left_names.clone()));
        let (mut r_full_schema, mut r_full_names) =
            right_full.unwrap_or_else(|| (right_schema.clone(), right_names.clone()));
        if join_node.tp == JoinType::Right {
            std::mem::swap(&mut l_full_schema, &mut r_full_schema);
            std::mem::swap(&mut l_full_names, &mut r_full_names);
        }
        let l_full_len = l_full_schema.columns.len();
        let mut full_schema =
            merge_schema(Some(&l_full_schema), Some(&r_full_schema)).unwrap_or_default();
        if matches!(join_node.tp, JoinType::Left | JoinType::Right) {
            let full_len = full_schema.columns.len();
            tidb_planner_util::null_misc::reset_not_null_flag(&mut full_schema, l_full_len, full_len);
        }
        join_plan.full_schema = Some(full_schema);
        join_plan.full_names = l_full_names
            .iter()
            .chain(r_full_names.iter())
            .cloned()
            .collect();

        // `:900` "Set preferred join algorithm if some join hints is specified
        // by user."
        let join_hints = self.join_hints.clone();
        let child_offset =
            |index: usize, plan: &LogicalJoin| plan.base.children()[index].query_block_offset();
        let (left_offset, right_offset) = (child_offset(0, &join_plan), child_offset(1, &join_plan));
        set_preferred_join_type_and_order(
            &mut join_plan,
            &join_hints,
            (&left_names, left_offset),
            (&right_names, right_offset),
            self.ctx,
        );

        if join_node.natural {
            self.build_natural_join(
                &mut join_plan,
                (&left_schema, &left_names),
                (&right_schema, &right_names),
                join_node.tp,
            )?;
        } else if !join_node.using.is_empty() {
            self.build_using_clause(
                &mut join_plan,
                (&left_schema, &left_names),
                (&right_schema, &right_names),
                join_node,
            )?;
        } else if let Some(on) = join_node.on.as_ref() {
            self.cur_clause = ClauseCode::On;
            // boundary: Go's `b.rewrite(...)` may REPLACE the plan with an
            // apply when the ON clause holds a subquery, and rejects that with
            // "ON condition doesn't support subqueries yet" (`:923`).
            // [`PlanBuilder::rewrite_scalar`] is the subquery-free rewrite, so
            // a subquery surfaces as an unresolved-column error instead.
            let on_plan = LogicalPlan::Join(join_plan.clone());
            let on_expr = self.rewrite_scalar_with_plan(
                &Self::clause_scratch(on),
                &on_plan,
                &BTreeMap::new(),
            )?;
            let on_condition = into_cnf_items(on_expr);
            // `:930` "Keep these expressions as a LogicalSelection upon the
            // inner join, in order to apply possible decorrelate
            // optimizations. The ON clause is actually treated as a WHERE
            // clause now."
            if join_plan.join_type == LogicalJoinType::Inner {
                let mut selection =
                    LogicalSelection::new(self.base(LogicalSelection::TYPE), on_condition);
                selection
                    .base
                    .set_children(vec![LogicalPlan::Join(join_plan)]);
                return Ok(LogicalPlan::Selection(selection));
            }
            let builder = RealFunctionBuilder::new(self.ctx);
            let opts = SubstituteOptions::new(&builder);
            join_plan.attach_on_conds(&on_condition, &left_schema, &right_schema, &opts);
        }
        Ok(LogicalPlan::Join(join_plan))
    }

    /// Go `buildLateralJoin(ctx, leftPlan, rightPlan, joinNode)` (`:956`):
    /// a `LATERAL` derived table is a `LogicalApply` with `InnerJoin` or
    /// `LeftOuterJoin`.
    ///
    /// # Errors
    ///
    /// `ErrInvalidLateralJoin` for `NATURAL`, `USING`, or `RIGHT JOIN` beside
    /// `LATERAL`, or the `ON` clause's own error.
    pub fn build_lateral_join(
        &mut self,
        left_plan: LogicalPlan,
        mut right_plan: LogicalPlan,
        join_node: &Join,
    ) -> Result<LogicalPlan, PlanError> {
        // `:961` "NATURAL JOIN and USING clauses are not supported with
        // LATERAL derived tables."
        if join_node.natural {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_INVALID_LATERAL_JOIN.generate_with_stack_by_args(&[
                    "NATURAL JOIN is not supported with LATERAL".into(),
                ]),
            ));
        }
        if !join_node.using.is_empty() {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_INVALID_LATERAL_JOIN.generate_with_stack_by_args(&[
                    "USING clause is not supported with LATERAL".into(),
                ]),
            ));
        }
        // `:984` "Currently supports INNER JOIN and comma syntax".
        let join_type = match join_node.tp {
            JoinType::Left => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_INVALID_LATERAL_JOIN
                        .generate_with_stack_by_args(&[
                            "LEFT JOIN is not supported with LATERAL".into()
                        ]),
                ))
            }
            JoinType::Right => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_INVALID_LATERAL_JOIN
                        .generate_with_stack_by_args(&[
                            "RIGHT JOIN is not supported with LATERAL".into()
                        ]),
                ))
            }
            // Comma syntax and an explicit `INNER JOIN` are the same node.
            JoinType::Cross => LogicalJoinType::Inner,
        };

        let outer_schema = match find_join_full_schema(&left_plan) {
            Some((schema, _)) => schema.clone(),
            None => left_plan.schema().cloned().unwrap_or_default(),
        };
        let cor_cols = extract_cor_columns_by_schema_4_logical_plan(&mut right_plan, &outer_schema);

        self.opt_flag |= flags::PREDICATE_PUSH_DOWN
            | flags::BUILD_KEY_INFO
            | flags::DECORRELATE
            | flags::CONSTANT_PROPAGATION;

        let (left_schema, left_names) = snapshot_schema_and_names(&left_plan);
        let (right_schema, right_names) = snapshot_schema_and_names(&right_plan);
        let left_full = find_join_full_schema(&left_plan)
            .map(|(schema, names)| (schema.clone(), names.to_vec()));
        let right_full = find_join_full_schema(&right_plan)
            .map(|(schema, names)| (schema.clone(), names.to_vec()));

        let mut apply = LogicalApply::new(self.base(LogicalApply::TYPE), join_type);
        apply.cor_cols = cor_cols;
        // "Allow decorrelation; optimizer will decide if safe."
        apply.no_decorrelate = false;
        // "Mark as LATERAL join to prevent unsafe elimination in PruneColumns."
        apply.is_lateral = true;
        apply.join.base.set_children(vec![left_plan, right_plan]);

        let mut schema = merge_schema(Some(&left_schema), Some(&right_schema)).unwrap_or_default();
        if join_type == LogicalJoinType::LeftOuter {
            let schema_len = schema.len();
            tidb_planner_util::null_misc::reset_not_null_flag(&mut schema, left_schema.len(), schema_len);
        }
        apply.join.base.base.set_schema(Some(schema));
        // `:1017` The names are CLONED and the `DBName` is deliberately NOT
        // overridden: a real table inside the right subtree must keep its own
        // so that `ORDER BY test.t2.a` still resolves.
        let mut names: Vec<FieldName> = left_names.clone();
        names.extend(right_names.iter().cloned());
        apply.join.base.base.set_output_names(names);

        // `:1029` The same `FullSchema`/`FullNames` merge `buildJoin` does.
        // There is no RIGHT-join swap because `LATERAL` refuses `RIGHT JOIN`
        // above. LEFT JOIN null-extends the inner side, so its FullSchema
        // columns lose NOT NULL just like the visible schema above.
        let (l_full_schema, l_full_names) =
            left_full.unwrap_or_else(|| (left_schema.clone(), left_names.clone()));
        let (r_full_schema, r_full_names) =
            right_full.unwrap_or_else(|| (right_schema.clone(), right_names.clone()));
        let mut full_schema =
            merge_schema(Some(&l_full_schema), Some(&r_full_schema)).unwrap_or_default();
        if join_type == LogicalJoinType::LeftOuter {
            let full_len = full_schema.len();
            tidb_planner_util::null_misc::reset_not_null_flag(&mut full_schema, l_full_schema.len(), full_len);
        }
        apply.join.full_schema = Some(full_schema);
        apply.join.full_names = l_full_names
            .iter()
            .chain(r_full_names.iter())
            .cloned()
            .collect();

        // boundary: `setIsInApplyForCTE(rightPlan, ap.FullSchema)` (`:1066`);
        // see this module's boundaries.

        if let Some(on) = join_node.on.as_ref() {
            self.cur_clause = ClauseCode::On;
            let on_plan = LogicalPlan::Apply(apply.clone());
            let on_expr = self.rewrite_scalar_with_plan(
                &Self::clause_scratch(on),
                &on_plan,
                &BTreeMap::new(),
            )?;
            let builder = RealFunctionBuilder::new(self.ctx);
            let opts = SubstituteOptions::new(&builder);
            apply.join.attach_on_conds(
                &into_cnf_items(on_expr),
                &left_schema,
                &right_schema,
                &opts,
            );
        }

        self.handle_helper.pop_two_and_merge().ok_or_else(|| {
            PlanError::internal("handleHelper does not hold both join children's maps")
        })?;

        let join_hints = self.join_hints.clone();
        let left_offset = apply.join.base.children()[0].query_block_offset();
        let right_offset = apply.join.base.children()[1].query_block_offset();
        set_preferred_join_type_and_order(
            &mut apply.join,
            &join_hints,
            (&left_names, left_offset),
            (&right_names, right_offset),
            self.ctx,
        );
        Ok(LogicalPlan::Apply(apply))
    }

    /// Go `buildUsingClause(p, leftPlan, rightPlan, join)` (`:1104`).
    ///
    /// The UPDATE/DELETE restore arm is a dropped narrowing; see this module.
    ///
    /// # Errors
    ///
    /// `ErrAmbiguous` for a `USING` column appearing twice on one side, or
    /// `ErrUnknownColumn` for one that is not common to both.
    pub fn build_using_clause(
        &self,
        p: &mut LogicalJoin,
        left: (&Schema, &[FieldName]),
        right: (&Schema, &[FieldName]),
        join: &Join,
    ) -> Result<(), PlanError> {
        let mut filter: BTreeMap<String, bool> = join
            .using
            .iter()
            .map(|column| (tidb_mysql::to_lowercase(column), true))
            .collect();
        self.coalesce_common_columns(p, left, right, join.tp, Some(&mut filter))
    }

    /// Go `buildNaturalJoin(p, leftPlan, rightPlan, join)` (`:1128`): the same
    /// coalescing with NO filter, so every common column matches.
    ///
    /// # Errors
    ///
    /// `ErrAmbiguous` when a common column name is not unique on one side.
    pub fn build_natural_join(
        &self,
        p: &mut LogicalJoin,
        left: (&Schema, &[FieldName]),
        right: (&Schema, &[FieldName]),
        join_tp: JoinType,
    ) -> Result<(), PlanError> {
        self.coalesce_common_columns(p, left, right, join_tp, None)
    }

    /// Go `coalesceCommonColumns(p, leftPlan, rightPlan, joinTp, filter)`
    /// (`:1142`), the whole of it.
    ///
    /// The standard's column order — the coalesced common columns first, in
    /// LEFT order, then the rest of the left, then the rest of the right — is
    /// produced exactly as Go produces it: by ROTATING each matched column to
    /// position `commonLen` in both sides' cloned column and name vectors.
    ///
    /// For a `RIGHT` join the two sides are SWAPPED first, so "left" below
    /// means the join's OUTER side throughout, and the `Redundant` flag then
    /// lands on the side the outer join does not keep. That is the swap
    /// [`Self::build_join`]'s `FullSchema` merge already performed, and the
    /// two must agree — the index this writes into `FullNames` is an index
    /// into the SWAPPED full schema.
    ///
    /// # Errors
    ///
    /// `ErrAmbiguous` and `ErrUnknownColumn`, as Go raises them.
    pub fn coalesce_common_columns(
        &self,
        p: &mut LogicalJoin,
        left: (&Schema, &[FieldName]),
        right: (&Schema, &[FieldName]),
        join_tp: JoinType,
        filter: Option<&mut BTreeMap<String, bool>>,
    ) -> Result<(), PlanError> {
        let mut lsc = left.0.clone();
        let mut rsc = right.0.clone();
        // The outer join's inner side is nullable in the coalesced output.
        match join_tp {
            JoinType::Left => {
                let len = rsc.columns.len();
                tidb_planner_util::null_misc::reset_not_null_flag(&mut rsc, 0, len);
            }
            JoinType::Right => {
                let len = lsc.columns.len();
                tidb_planner_util::null_misc::reset_not_null_flag(&mut lsc, 0, len);
            }
            JoinType::Cross => {}
        }
        // Go aliases `lColumns`/`rColumns` onto `lsc`/`rsc`'s slices and then
        // swaps the ALIASES for a RIGHT join while `lsc`/`rsc` keep their
        // original meaning — the final `conds` loop reads `lsc.Columns[i]` and
        // `rsc.Columns[i]`, NOT the aliases. Both are kept here for the same
        // reason.
        let swapped = join_tp == JoinType::Right;
        let (mut l_columns, mut r_columns) = if swapped {
            (rsc.columns.clone(), lsc.columns.clone())
        } else {
            (lsc.columns.clone(), rsc.columns.clone())
        };
        let (mut l_names, mut r_names) = if swapped {
            (right.1.to_vec(), left.1.to_vec())
        } else {
            (left.1.to_vec(), right.1.to_vec())
        };

        match filter.as_ref() {
            Some(filter) => {
                // "Check using clause with ambiguous columns."
                for names in [&l_names, &r_names] {
                    let mut seen = BTreeSet::new();
                    for name in names {
                        if !filter.contains_key(&name.names.column.lower) {
                            continue;
                        }
                        if !seen.insert(name.names.column.lower.clone()) {
                            return Err(ambiguous(&name.names.column.lower));
                        }
                    }
                }
            }
            None => {
                // "(t3 cross join t4) natural join t1": a cross join can
                // present the same name twice, and then no common column of a
                // NATURAL join is well defined.
                let mut l_name_map: BTreeMap<String, usize> = BTreeMap::new();
                let mut r_name_map: BTreeMap<String, usize> = BTreeMap::new();
                let mut common_names = Vec::new();
                for (index, name) in l_names.iter().enumerate() {
                    if is_extra_column_name(&name.names.column.lower)
                        || l_columns.get(index).is_some_and(|column| column.is_hidden)
                    {
                        continue;
                    }
                    *l_name_map
                        .entry(name.names.column.lower.clone())
                        .or_insert(0) += 1;
                }
                for (index, name) in r_names.iter().enumerate() {
                    if is_extra_column_name(&name.names.column.lower)
                        || r_columns.get(index).is_some_and(|column| column.is_hidden)
                    {
                        continue;
                    }
                    *r_name_map
                        .entry(name.names.column.lower.clone())
                        .or_insert(0) += 1;
                    if let Some(count) = l_name_map.get(&name.names.column.lower) {
                        if *count > 1 {
                            return Err(ambiguous(&name.names.column.lower));
                        }
                        common_names.push(name.names.column.lower.clone());
                    }
                }
                for common in &common_names {
                    if r_name_map.get(common).copied().unwrap_or(0) > 1 {
                        return Err(ambiguous(common));
                    }
                }
            }
        }

        // "Find out all the common columns and put them ahead."
        let mut filter = filter;
        let mut common_len = 0usize;
        for i in 0..l_names.len() {
            if is_extra_column_name(&l_names[i].names.column.lower)
                || l_columns.get(i).is_some_and(|column| column.is_hidden)
            {
                continue;
            }
            for j in common_len..r_names.len() {
                if r_columns.get(j).is_some_and(|column| column.is_hidden) {
                    continue;
                }
                if l_names[i].names.column.lower != r_names[j].names.column.lower {
                    continue;
                }
                if let Some(filter) = filter.as_deref_mut() {
                    if !filter.is_empty() {
                        match filter.get_mut(&l_names[i].names.column.lower) {
                            // The USING list does not name this column, so it
                            // is NOT common: Go breaks out of the inner loop,
                            // leaving the left column where it is.
                            None | Some(false) => break,
                            // "Mark this column exist."
                            Some(slot) => *slot = false,
                        }
                    }
                }
                // Go's two `copy`s per side are a rotation of the element at
                // `i` (resp. `j`) down to `common_len`.
                l_columns[common_len..=i].rotate_right(1);
                l_names[common_len..=i].rotate_right(1);
                r_columns[common_len..=j].rotate_right(1);
                r_names[common_len..=j].rotate_right(1);
                common_len += 1;
                break;
            }
        }

        if let Some(filter) = filter.as_deref() {
            if !filter.is_empty() && filter.len() != common_len {
                if let Some((column, _)) = filter.iter().find(|(_, not_exist)| **not_exist) {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(
                            &[tidb_hack::go_to_lower(column).into(), "from clause".into()],
                        ),
                    ));
                }
            }
        }

        // The coalesced output: every left column (the common ones now first),
        // then the right columns the coalescing did NOT absorb.
        let mut schema_columns = l_columns.clone();
        schema_columns.extend_from_slice(&r_columns[common_len.min(r_columns.len())..]);
        let mut names = l_names.clone();
        names.extend_from_slice(&r_names[common_len.min(r_names.len())..]);

        let builder = RealFunctionBuilder::new(self.ctx);
        let mut conditions = Vec::with_capacity(common_len);
        let mut redundant_mappings = Vec::with_capacity(common_len);
        for i in 0..common_len {
            // Go reads `lsc.Columns[i]`/`rsc.Columns[i]` here — the ROTATED
            // clones, not the swapped aliases.
            // Go reads `lsc.Columns[i]`/`rsc.Columns[i]`: the ORIGINAL left
            // and right sides. The RIGHT-join swap moved the ALIASES
            // `lColumns`/`rColumns`, not `lsc`/`rsc`, so it is undone here.
            let (Some(lc), Some(rc)) = (if swapped {
                (r_columns.get(i), l_columns.get(i))
            } else {
                (l_columns.get(i), r_columns.get(i))
            }) else {
                break;
            };
            let condition = builder
                .new_function(
                    "eq",
                    Some(FieldType::new(FieldTypeCode::Tiny)),
                    vec![
                        Expression::Column(lc.clone()),
                        Expression::Column(rc.clone()),
                    ],
                )
                .map_err(|error| PlanError::internal(format!("{error:?}")))?;
            conditions.push(condition);
            if let Some(full_schema) = p.full_schema.as_ref() {
                // "since FullSchema is derived from left and right schema in
                // upper layer, so rc/lc must be in FullSchema."
                if swapped {
                    // "Right join keeps right side as canonical output for
                    // USING/NATURAL common columns."
                    let index = full_schema.column_index(lc);
                    if let Ok(index) = usize::try_from(index) {
                        if let Some(name) = p.full_names.get_mut(index) {
                            name.redundant = true;
                        }
                    }
                    redundant_mappings.push((lc.clone(), rc.clone()));
                } else {
                    // "For inner/left join, left side is the canonical visible
                    // output."
                    let index = full_schema.column_index(rc);
                    if let Ok(index) = usize::try_from(index) {
                        if let Some(name) = p.full_names.get_mut(index) {
                            name.redundant = true;
                        }
                    }
                    redundant_mappings.push((rc.clone(), lc.clone()));
                }
            }
        }

        let coalesced_schema = Schema::new(schema_columns);
        p.base.base.set_schema(Some(coalesced_schema.clone()));
        p.base.base.set_output_names(names);
        if self.in_update_or_delete_stmt {
            // "We do not need to coalesce columns for update and delete."
            // (`buildUsingClause`/`buildNaturalJoin`): restore the merged
            // child schema, and skip the redundant mapping registration,
            // whose output indexes would be stale (`coalesceCommonColumns`).
            let merged = Schema::new(
                left.0
                    .columns
                    .iter()
                    .chain(right.0.columns.iter())
                    .cloned()
                    .collect(),
            );
            p.base.base.set_schema(Some(merged));
            p.base
                .base
                .set_output_names(left.1.iter().chain(right.1.iter()).cloned().collect());
        } else {
            for (redundant, visible) in &redundant_mappings {
                p.register_redundant_column_mapping(redundant, visible, &coalesced_schema);
            }
        }
        conditions.extend(std::mem::take(&mut p.other_conditions));
        p.other_conditions = conditions;
        Ok(())
    }

    /// Go `buildMemTable(ctx, dbName, tableInfo)` (`:5372`), over
    /// [`SourceTable`].
    ///
    /// "We can use the `TableInfo.Columns` directly because the memory table
    /// has a stable schema and there is no online DDL on the memory table."
    /// The `Extractor` switch is
    /// [`crate::memtable_predicate_extractor::MemTableExtractor::for_table`].
    pub fn build_mem_table(
        &mut self,
        db_name: &str,
        table: &SourceTable,
        alias: Option<&str>,
    ) -> LogicalPlan {
        let visible_table = alias.unwrap_or(&table.table_name);
        let mut schema_columns = Vec::with_capacity(table.columns.len());
        let mut names = Vec::with_capacity(table.columns.len());
        let mut handle_cols: Option<PlanHandleCols> = None;
        for source_column in &table.columns {
            names.push(FieldName {
                names: FieldNameMetadata {
                    database: IdentifierMetadata::new(db_name),
                    table: IdentifierMetadata::new(visible_table),
                    original_table: IdentifierMetadata::new(&table.table_name),
                    column: IdentifierMetadata::new(source_column.read().name.original()),
                    original_column: IdentifierMetadata::new(source_column.read().name.original()),
                },
                ..FieldName::default()
            });
            let mut column = Column::new(self.column_ids.alloc(), source_column.read().field_type.clone());
            column.id = source_column.read().id;
            if table.pk_is_handle && source_column.read().field_type.has_flag(FieldTypeFlags::PRI_KEY) {
                handle_cols = Some(self.plan_handle_cols(std::slice::from_ref(&column), true));
            }
            schema_columns.push(column);
        }

        // Go pushes a nil map when the table has no handle, which is
        // `push_empty`.
        match handle_cols {
            Some(handle) => {
                let mut map = super::handle_col_helper::HandleColMap::new();
                map.insert(table.table_id, vec![handle]);
                self.handle_helper.push_map(map);
            }
            None => self.handle_helper.push_empty(),
        }

        let columns: Vec<MemTableColumn> = table
            .columns
            .iter()
            .map(|source_column| MemTableColumn {
                id: source_column.read().id,
                name: source_column.read().name.original().to_owned(),
            })
            .collect();
        let mut mem_table = LogicalMemTable::new(
            self.base(LogicalMemTable::TYPE),
            db_name,
            table.table_name.clone(),
        );
        mem_table.table_columns.clone_from(&columns);
        mem_table.columns = columns;
        // Some memory tables can receive some predicates.
        mem_table.extractor =
            crate::memtable_predicate_extractor::MemTableExtractor::for_table(
                db_name,
                &table.table_name,
            )
            .map(|extractor| {
                Box::new(extractor) as Box<dyn tidb_planner_core_base::MemTablePredicateExtractor>
            });
        // Go sets the summary tables' `QueryTimeRange` with their extractors.
        if db_name.eq_ignore_ascii_case("information_schema")
            && matches!(
                table.table_name.to_ascii_uppercase().as_str(),
                "INSPECTION_RESULT"
                    | "INSPECTION_SUMMARY"
                    | "METRICS_SUMMARY"
                    | "METRICS_SUMMARY_BY_LABEL"
            )
        {
            mem_table.query_time_range = self.time_range_for_summary_table();
        }
        mem_table
            .base
            .base
            .set_schema(Some(Schema::new(schema_columns)));
        mem_table.base.base.set_output_names(names);
        LogicalPlan::MemTable(mem_table)
    }

    /// Go `checkRecursiveView(dbName, tableName)` (`:5487`), returning the
    /// RAII form of the `func()` Go's caller defers.
    ///
    /// The `renameView` arm is a dropped 6a narrowing (`b.capFlag` and
    /// `b.renamingViewName` have no producer on the SELECT path).
    ///
    /// # Errors
    ///
    /// `ErrViewRecursive` when the view is already on the building stack.
    pub fn check_recursive_view(
        &mut self,
        db_name: &str,
        table_name: &str,
    ) -> Result<ViewBuildGuard, PlanError> {
        let key: super::SchemaTableKey = (
            tidb_mysql::to_lowercase(db_name),
            tidb_mysql::to_lowercase(table_name),
        );
        // "If this view has already been on the building stack, it means this
        // view contains a recursive definition."
        if self.building_view_stack.contains(&key) {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_VIEW_RECURSIVE
                    .generate_with_stack_by_args(&[db_name.into(), table_name.into()]),
            ));
        }
        // "If the view is being renamed, we return the mysql compatible error
        // message."
        if self.renaming_view_name.as_ref() == Some(&key) {
            return Err(PlanError::from(
                tidb_plannererrors::ERR_NO_SUCH_TABLE
                    .generate_with_stack_by_args(&[db_name.into(), table_name.into()]),
            ));
        }
        self.building_view_stack.insert(key.clone());
        Ok(ViewBuildGuard { key })
    }

    /// Go `BuildDataSourceFromView(ctx, dbName, tableInfo, qbNameMap4View,
    /// viewHints)` (`:5509`).
    ///
    /// Ported: the recursion guard, view-hint query-block conversion, the view
    /// body's parse and build, the CTE save/restore around it, the body's
    /// visit info (verified against a `SQL SECURITY DEFINER` view's definer,
    /// otherwise the reader's), the EXPLAIN arms (`ErrViewNoExplain`), the
    /// column-count check, and the projection over the result.
    ///
    /// # Errors
    ///
    /// `ErrViewRecursive`, a parse error, or `ErrViewInvalid` when the body's
    /// column count no longer matches the view's.
    pub fn build_data_source_from_view(
        &mut self,
        view: &SourceView,
    ) -> Result<LogicalPlan, PlanError> {
        self.build_data_source_from_view_with_hints(
            view,
            None,
            tidb_hint::ViewHintContext::default(),
        )
    }

    pub(crate) fn build_data_source_from_view_with_hints(
        &mut self,
        view: &SourceView,
        alias: Option<&str>,
        inherited_hints: tidb_hint::ViewHintContext,
    ) -> Result<LogicalPlan, PlanError> {
        let view_depth = self.view_depth;
        self.view_depth += 1;
        let guard = self.check_recursive_view(&view.db_name, &view.view_name)?;
        // Go relaxes truncation only while folding this view's constant
        // predicates; the outer statement keeps its own semantics.
        let outer_ignore_truncate = std::mem::replace(
            &mut self.ignore_truncate_err_for_view_predicate_folding,
            true,
        );
        // Go keeps the view body's visit info apart (`originalVisitInfo`): a
        // DEFINER view's is checked against its definer and then dropped,
        // an INVOKER view's is appended for the invoker to hold.
        let original_visit_info = std::mem::take(&mut self.visit_info);
        // Under `canExpandAST` Go expands this body's freshly parsed AST too
        // and drops it with the parse; its query-block offsets restart at 1,
        // so its records must not reach the statement's (see
        // [`super::create_view`]).
        let outer_expanded = std::mem::take(&mut self.expanded_selects);
        let built = self.build_view_body(view, inherited_hints);
        self.expanded_selects = outer_expanded;
        self.ignore_truncate_err_for_view_predicate_folding = outer_ignore_truncate;
        guard.release(self);
        let body_visit_info = std::mem::replace(&mut self.visit_info, original_visit_info);
        let mut plan = built?;
        let (db_lower, view_lower) = (
            tidb_mysql::to_lowercase(&view.db_name),
            tidb_mysql::to_lowercase(&view.view_name),
        );
        // Under EXPLAIN every view after the statement's first needs SELECT
        // of the session's own account.
        if view_depth != 0 && self.in_explain_stmt {
            if let Some(verifier) = self.source.privilege_verifier() {
                if !verifier.request_verification(
                    &db_lower,
                    &view_lower,
                    "",
                    tidb_mysql::SelectPriv,
                ) {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_VIEW_NO_EXPLAIN
                            .generate_with_stack_by_args(&[]),
                    ));
                }
            }
        }
        if view.security_definer {
            if let Some(verifier) = self.source.privilege_verifier() {
                for visit in &body_visit_info {
                    if !verifier.verify_with_user(
                        &view.definer_user,
                        &view.definer_host,
                        &visit.database,
                        &visit.table,
                        &visit.column,
                        visit.privilege,
                    ) {
                        return Err(PlanError::from(
                            tidb_plannererrors::ERR_VIEW_INVALID
                                .generate_with_stack_by_args(&[
                                    (&view.db_name).into(),
                                    (&view.view_name).into(),
                                ]),
                        ));
                    }
                }
            }
        } else {
            self.visit_info.extend(body_visit_info);
        }
        if self.in_explain_stmt {
            self.visit_info.push(super::visit_info::VisitInfo {
                privilege: tidb_mysql::ShowViewPriv,
                database: db_lower,
                table: view_lower,
                column: String::new(),
                error: super::visit_info::VisitAuthError::ViewNoExplain,
                dynamic_privs: Vec::new(),
                dynamic_with_grant: false,
            });
        }
        if let Some(alias) = alias.filter(|alias| !alias.is_empty()) {
            let mut names = plan.output_names().to_vec();
            for name in &mut names {
                if !name.hidden {
                    name.names.table = IdentifierMetadata::new(alias);
                }
            }
            plan.base_mut().base.set_output_names(names);
        }
        Ok(plan)
    }

    fn build_view_body(
        &mut self,
        view: &SourceView,
        inherited_hints: tidb_hint::ViewHintContext,
    ) -> Result<LogicalPlan, PlanError> {
        // Go parses the body with the session's charset and parser switches
        // (`GetCharsetInfo`, `BuildParserConfig`) on a fresh parser.
        let (charset, collation) = self.ctx.connection_charset_info();
        let mut statement = tidb_parser::parse_with_config_and_connection(
            &view.select_sql,
            self.parser_config,
            charset,
            collation,
        )
        .map_err(|error| PlanError::internal(error.compatibility_message(&view.select_sql)))?;
        let (view_hint_handler, view_hint_state) =
            tidb_hint::QBHintHandler::for_view_body(&mut statement, inherited_hints);
        let tidb_ast::Stmt::Query(query) = statement else {
            return Err(PlanError::internal(format!(
                "View '{}.{}' body is not a query",
                view.db_name, view.view_name
            )));
        };

        // `:5545` "For the case that views appear in CTE queries, we need to
        // save the CTEs after the views are established."
        let saved_ctes = std::mem::take(&mut self.outer_ctes);
        let saved_building_cte = std::mem::replace(&mut self.building_cte, false);
        let outer_hint_handler = self.qb_hint_handler.replace(view_hint_handler);
        let outer_hint_state = self.qb_hint_state.replace(view_hint_state);
        // Go reads each block's `QueryBlockOffset`, which the view body's own
        // `QBHintHandler` walk numbered from 1, so the body's blocks restart
        // the numbering its hints were resolved against.
        let outer_next_qb_offset = std::mem::replace(&mut self.next_qb_offset, 0);
        // Go gives the body a fresh `PlannerSelectBlockAsName`.
        let outer_block_as_names = std::mem::take(&mut self.select_block_as_names);
        let built = match query.as_ref() {
            QueryStmt::Select(select) => self.build_select(select).map(|(plan, _)| plan),
            QueryStmt::SetOpr(set_operation) => self.build_set_opr(set_operation),
        };
        self.next_qb_offset = outer_next_qb_offset;
        self.select_block_as_names = outer_block_as_names;
        self.flush_hint_build_warnings();
        self.qb_hint_handler = outer_hint_handler;
        self.qb_hint_state = outer_hint_state;
        self.outer_ctes = saved_ctes;
        self.building_cte = saved_building_cte;
        let plan = built.map_err(|error| {
            use tidb_planner_core_base::PlanErrorKind;
            use tidb_plannererrors as pe;
            // Go BuildDataSourceFromView preserves these planner errors;
            // ordinary missing dependencies instead invalidate the view.
            // A message-only refusal of this tier is `ErrInternal`'s kind.
            if matches!(error.kind(), PlanErrorKind::Internal)
                || [
                    &pe::ERR_VIEW_RECURSIVE,
                    &pe::ERR_NO_SUCH_TABLE,
                    &pe::ERR_INTERNAL,
                    &pe::ERR_FIELD_NOT_IN_GROUP_BY,
                    &pe::ERR_MIX_OF_GROUP_FUNC_AND_FIELDS,
                    &pe::ERR_VIEW_NO_EXPLAIN,
                    &pe::ERR_NOT_SUPPORTED_YET,
                ]
                .into_iter()
                .any(|proto| error.equal(proto))
            {
                error
            } else {
                view_invalid(&view.db_name, &view.view_name)
            }
        })?;

        let (schema, names) = snapshot_schema_and_names(&plan);
        if view.columns.len() != schema.columns.len() {
            return Err(view_invalid(&view.db_name, &view.view_name));
        }
        self.build_proj_upon_view(view, plan, &schema, &names)
    }

    /// Go `buildProjUponView(_, dbName, tableInfo, selectLogicalPlan)`
    /// (`:5646`): the projection that renames the body's output to the VIEW's
    /// own column names.
    ///
    /// Go reads `selectLogicalPlan.Schema()` after `SetChildren`; per
    /// [`super`]'s section 3 the snapshot arrives as a parameter.
    ///
    /// # Errors
    ///
    /// `ErrViewInvalid` when a stored `View.Cols` name is not in the body's
    /// output.
    pub fn build_proj_upon_view(
        &mut self,
        view: &SourceView,
        plan: LogicalPlan,
        schema: &Schema,
        names: &[FieldName],
    ) -> Result<LogicalPlan, PlanError> {
        // "In the old version of VIEW implementation, TableInfo.View.Cols is
        // used to store the origin columns' names of the underlying SelectStmt
        // used when creating the view."
        let (columns, underlying_names): (Vec<Column>, Vec<FieldName>) =
            if view.view_cols.is_empty() {
                (schema.columns.clone(), names.to_vec())
            } else {
                let mut columns = Vec::with_capacity(view.columns.len());
                let mut underlying = Vec::with_capacity(view.columns.len());
                for info in &view.columns {
                    // Go `expression.FindFieldNameIdxByColName(names, info.Name.L)`.
                    let index = names
                        .iter()
                        .position(|name| lower_name_matches(&name.names.column.lower, info.read().name.original()))
                        .ok_or_else(|| view_invalid(&view.db_name, &view.view_name))?;
                    let (Some(column), Some(name)) = (schema.columns.get(index), names.get(index))
                    else {
                        return Err(view_invalid(&view.db_name, &view.view_name));
                    };
                    columns.push(column.clone());
                    underlying.push(name.clone());
                }
                (columns, underlying)
            };

        let mut proj_schema = Vec::with_capacity(view.columns.len());
        let mut proj_exprs = Vec::with_capacity(view.columns.len());
        let mut proj_names = Vec::with_capacity(view.columns.len());
        for (i, name) in underlying_names.iter().enumerate() {
            let Some(column_info) = view.columns.get(i) else {
                break;
            };
            let orig_column = view
                .view_cols
                .get(i)
                .map_or_else(|| name.names.column.original.clone(), Clone::clone);
            proj_names.push(FieldName {
                names: FieldNameMetadata {
                    database: IdentifierMetadata::new(&view.db_name),
                    // "TblName is the of view instead of the name of the
                    // underlying table."
                    table: IdentifierMetadata::new(&view.view_name),
                    original_table: name.names.original_table.clone(),
                    column: IdentifierMetadata::new(column_info.read().name.original()),
                    original_column: IdentifierMetadata::new(orig_column),
                },
                ..FieldName::default()
            });
            let Some(column) = columns.get(i) else { break };
            // Go re-uses the UniqueID and takes the STATIC type, so the
            // projection's column is the body column's identity with a frozen
            // type.
            let mut projected = column.clone();
            projected.index = i as i64;
            proj_schema.push(Column::new(
                column.unique_id,
                column
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified)),
            ));
            proj_exprs.push(Expression::Column(column.clone()));
        }

        let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), proj_exprs);
        projection.base.set_children(vec![plan]);
        projection
            .base
            .base
            .set_schema(Some(Schema::new(proj_schema)));
        projection.base.base.set_output_names(proj_names);
        Ok(LogicalPlan::Projection(projection))
    }

    /// Go setExtraPhysTblIDColsOnDataSource/addExtraPhysTblIDColumn4DS.
    /// The synthetic value belongs to the record reader, not to SQL data.
    pub(super) fn set_extra_phys_tbl_id_cols(
        &mut self,
        plan: &mut LogicalPlan,
        columns: &mut BTreeMap<i64, Column>,
    ) {
        if let LogicalPlan::DataSource(source) = plan {
            if source.partition_definition_ids.is_empty() {
                return;
            }
            if let Some(column) = source
                .table_columns
                .iter()
                .rev()
                .find(|column| column.id == tidb_model::column::EXTRA_PHYS_TBL_ID)
            {
                columns.insert(source.table_id, column.clone());
                return;
            }
            let mut column = Column::new(
                self.column_ids.alloc(),
                FieldType::new(FieldTypeCode::LongLong),
            );
            column.id = tidb_model::column::EXTRA_PHYS_TBL_ID;
            column.orig_name = format!(
                "{}.{}.{}",
                source.db_name, source.table_name, EXTRA_PHYS_TBL_ID_NAME
            );
            let name = self.table_field_name(
                &source.db_name,
                &source.table_name,
                source
                    .table_as_name
                    .as_deref()
                    .unwrap_or(&source.table_name),
                EXTRA_PHYS_TBL_ID_NAME,
                false,
                false,
            );
            source
                .columns
                .push(tidb_model::GoShared::new(
                    tidb_model::ColumnInfo::new_extra_phys_tbl_id_col_info(),
                ));
            let mut schema = source.base.base.schema().cloned().unwrap_or_default();
            schema.append([column.clone()]);
            source.base.base.set_schema(Some(schema));
            let mut names = source.base.base.output_names().to_vec();
            names.push(name);
            source.base.base.set_output_names(names);
            source.table_columns.push(column.clone());
            columns.insert(source.table_id, column);
        } else {
            for child in plan.base_mut().children_mut() {
                self.set_extra_phys_tbl_id_cols(child, columns);
            }
        }
    }

    /// Go `buildSelectLock(src, lock)` (`planbuilder.go:1610`).
    ///
    /// Go's `TblID2Handle` is `map[int64][]util.HandleCols`; 6a's
    /// [`PlanHandleCols`] carries handle IDENTITIES rather than whole
    /// [`Column`]s, and [`LogicalLock`] wants the columns, so each identity is
    /// resolved back against the child's schema by unique id. Losing an
    /// identity is a planning error, never permission to omit a row lock.
    ///
    /// # Errors
    ///
    /// An empty `handleHelper` stack, where Go panics.
    pub fn build_select_lock(
        &mut self,
        mut src: LogicalPlan,
        lock_type: SelectLockType,
        wait_sec: u64,
    ) -> Result<LogicalPlan, PlanError> {
        let (schema, _) = snapshot_schema_and_names(&src);
        let tail = self
            .handle_helper
            .tail_map()
            .ok_or_else(|| PlanError::internal("handleHelper is empty at buildSelectLock"))?;
        let mut tbl_id_to_handle_cols: BTreeMap<i64, Vec<Column>> = BTreeMap::new();
        for (table_id, handles) in tail {
            let mut columns = Vec::new();
            for handle in handles {
                for unique_id in handle_unique_ids(handle) {
                    let column = schema
                        .columns
                        .iter()
                        .find(|column| column.unique_id == unique_id)
                        .ok_or_else(|| {
                            PlanError::internal(format!(
                                "SelectLock lost handle column {unique_id} for table {table_id}"
                            ))
                        })?;
                    columns.push(column.clone());
                }
            }
            if !columns.is_empty() {
                tbl_id_to_handle_cols.insert(*table_id, columns);
            }
        }
        let mut physical_columns = BTreeMap::new();
        self.set_extra_phys_tbl_id_cols(&mut src, &mut physical_columns);
        let mut lock = LogicalLock::new(self.base(LogicalLock::TYPE), lock_type);
        lock.tbl_id_to_phys_tbl_id_col = physical_columns;
        lock.wait_sec = wait_sec;
        lock.tbl_id_to_handle_cols = tbl_id_to_handle_cols;
        lock.base.set_children(vec![src]);
        Ok(LogicalPlan::Lock(lock))
    }
}

/// The unique ids of whichever handle identity this is.
fn handle_unique_ids(handle: &PlanHandleCols) -> Vec<i64> {
    match handle {
        PlanHandleCols::Int(identity) => identity.column_unique_ids(),
        PlanHandleCols::Common(identity) => identity.column_unique_ids(),
    }
}

/// Go's three extra columns, which `NATURAL`/`USING` matching skips.
fn is_extra_column_name(lower: &str) -> bool {
    lower == EXTRA_HANDLE_NAME || lower == EXTRA_COMMIT_TS_NAME || lower == EXTRA_PHYS_TBL_ID_NAME
}

/// Go `plannererrors.ErrAmbiguous.GenWithStackByArgs(name, "from clause")`.
fn ambiguous(column: &str) -> PlanError {
    PlanError::from(
        tidb_plannererrors::ERR_AMBIGUOUS
            .generate_with_stack_by_args(&[column.into(), "from clause".into()]),
    )
}

/// Go `plannererrors.ErrViewInvalid.GenWithStackByArgs(dbName.O, name.O)`.
fn view_invalid(db_name: &str, view_name: &str) -> PlanError {
    PlanError::from(
        tidb_plannererrors::ERR_VIEW_INVALID
            .generate_with_stack_by_args(&[db_name.into(), view_name.into()]),
    )
}

/// A `MarkerKind`-keyed empty map, for a clause with no marker producer.
///
/// `ON` clauses can hold no aggregate, window function or select-list
/// reference, so [`Self::build_join`]'s rewrite binds no marker at all — this
/// names that rather than leaving a bare `BTreeMap::new()` in the body.
#[must_use]
pub fn no_markers() -> BTreeMap<MarkerKind, Vec<Column>> {
    BTreeMap::new()
}
