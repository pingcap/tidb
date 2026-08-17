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

//! Go `pkg/planner/core/operator/logicalop/logical_show.go`: `LogicalShow`,
//! `ShowContents`, and the `SHOW STATS_META` predicate extraction that file
//! also declares.
//!
//! SEED of `pkg/planner/core`. `LogicalShow` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! The crate's `logical_show` identity leaf is KEPT rather than merged:
//! `difftests/planner-tests/tests/logical_show.rs` consumes its
//! `LogicalShowIdentity`/`ShowColumnIdentity` from OUTSIDE this crate.
//!
//! # Narrowings, by name
//!
//! * `ShowContents.Tp ast.ShowStmtType`. `pkg/parser/ast`'s show-statement
//!   enumeration is not transcreated. Exactly ONE of its values is a decision in
//!   this file — `ast.ShowStatsMeta`, which gates the whole predicate
//!   extraction — so [`ShowContents::is_stats_meta`] carries that bit and the
//!   rest of the statement kind stays with the caller as
//!   [`ShowContents::show_type`], an opaque tag.
//! * `Table *resolve.TableNameW`, `Column *ast.ColumnName`, `Limit *ast.Limit`,
//!   `User *auth.UserIdentity`, `Roles []*auth.RoleIdentity`. All are AST or
//!   resolver handles that no body in this file reads; they are carried as
//!   names and counts, which is what `MemoryUsage` sizes and what an EXPLAIN
//!   would print.
//! * `MemoryUsage` sizes a Go struct with `unsafe.Sizeof`; a Rust struct has no
//!   corresponding constant, so it has no counterpart here.
//! * `getStringValueFromConstant`'s `ParamMarker.GetUserVar(evalCtx)` needs a
//!   session's user variables. Without one, a constant carrying a param marker
//!   yields NO value here, which drops that predicate from the extraction and
//!   leaves it to be evaluated normally — the same direction Go takes when the
//!   lookup errors.
//! * `collate.WildcardPattern` and the rest of `base.ShowPredicateExtractor`
//!   beyond `StatsMetaDBFilters`/`StatsMetaTableFilters` are not transcreated;
//!   [`ShowStatsMetaPredicateExtractor`] carries the two filter sets, which are
//!   the extractor's whole output here.

use std::collections::BTreeSet;

use tidb_datatype::FieldName;
use tidb_expr::expr_util::normal_form::split_dnf_items;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::BaseLogicalPlan;
use crate::stats_info::StatsInfo;

/// Go `logicalop.ShowStatsMetaPredicateExtractor` (`logical_show.go:43`): the
/// db and table filters `SHOW STATS_META ... WHERE ...` gave up to the plan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShowStatsMetaPredicateExtractor {
    /// Go `DB`, as `StatsMetaDBFilters()` returns it.
    pub db: BTreeSet<String>,
    /// Go `Table`, as `StatsMetaTableFilters()` returns it.
    pub table: BTreeSet<String>,
}

/// Go `logicalop.ShowContents` (`logical_show.go:79`).
#[derive(Clone, Debug, Default)]
pub struct ShowContents {
    /// Go `Tp`, as an opaque tag; see this module's header.
    pub show_type: i64,
    /// Whether `Tp == ast.ShowStatsMeta`, which is the only value this file
    /// branches on.
    pub is_stats_meta: bool,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `Table.Name.O`, for `SHOW COLUMNS`.
    pub table_name: Option<String>,
    /// Go `Partition`.
    pub partition: String,
    /// Go `Column.Name.O`, the `DESC table column` selector.
    pub column_name: Option<String>,
    /// Go `IndexName`.
    pub index_name: String,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `Flag`, e.g. the parsed `FULL`.
    pub flag: i64,
    /// Go `User.String()`, for `SHOW GRANTS`.
    pub user: Option<String>,
    /// Go `Roles`, by name.
    pub roles: Vec<String>,
    /// Go `CountWarningsOrErrors`.
    pub count_warnings_or_errors: bool,
    /// Go `Full`.
    pub full: bool,
    /// Go `IfNotExists`, for `SHOW CREATE DATABASE IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// Go `GlobalScope`, used by `SHOW VARIABLES`.
    pub global_scope: bool,
    /// Go `Extended`, for `SHOW EXTENDED COLUMNS FROM ...`.
    pub extended: bool,
    /// Go `ImportJobID`.
    pub import_job_id: Option<i64>,
    /// Go `ImportGroupKey`.
    pub import_group_key: String,
    /// Go `DistributionJobID`.
    pub distribution_job_id: Option<i64>,
}

impl ShowContents {
    /// Go `p.Tp == ast.ShowStatsMeta`; see this module's header.
    #[must_use]
    pub const fn is_stats_meta(&self) -> bool {
        self.is_stats_meta
    }
}

/// Go `logicalop.LogicalShow` (`logical_show.go:35`).
#[derive(Clone, Debug, Default)]
pub struct LogicalShow {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go's embedded `ShowContents`.
    pub contents: ShowContents,
    /// Go `Extractor base.ShowPredicateExtractor`, in the one form this file
    /// installs.
    pub extractor: Option<ShowStatsMetaPredicateExtractor>,
}

impl LogicalShow {
    /// Go `plancodec.TypeShow`.
    pub const TYPE: &'static str = "Show";

    /// Go `LogicalShow.Init(ctx)` (`logical_show.go:120`), which fixes the
    /// query-block offset at 0.
    #[must_use]
    pub fn new(base: BaseLogicalPlan, contents: ShowContents) -> Self {
        Self {
            base,
            contents,
            extractor: None,
        }
    }

    /// Go `LogicalShow.PredicatePushDown(predicates)`
    /// (`logical_show.go:130`): only `SHOW STATS_META` gives predicates to the
    /// plan, and only `db_name`/`table_name` equality, `IN`, and disjunctions of
    /// those.
    ///
    /// The two extractions run in SEQUENCE — the table filter sees what the db
    /// filter did not claim — and the extractor is installed only if something
    /// was actually claimed, so a `SHOW STATS_META` with an unrelated `WHERE`
    /// keeps its plain filtering.
    ///
    /// Returns the remaining predicates; the extractor, when one was installed,
    /// is on [`Self::extractor`].
    pub fn predicate_push_down(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        if !self.contents.is_stats_meta() {
            return predicates;
        }
        let (remained, db_filters) =
            extract_stats_meta_filters(schema, names, predicates.clone(), "db_name", true);
        let (remained, table_filters) =
            extract_stats_meta_filters(schema, names, remained, "table_name", false);
        if remained.len() != predicates.len() {
            self.extractor = Some(ShowStatsMetaPredicateExtractor {
                db: db_filters.unwrap_or_default(),
                table: table_filters.unwrap_or_default(),
            });
        }
        remained
    }

    /// Go `LogicalShow.DeriveStats(_, selfSchema, _, reloads)`
    /// (`logical_show.go:163`): Go's own words, "a fake count, just to avoid
    /// panic now" — see [`get_fake_stats`].
    pub fn derive_stats(&mut self, self_schema: &Schema, reloads: &[bool]) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let profile = get_fake_stats(self_schema);
        self.base.base.set_stats(Some(profile.clone()));
        (profile, true)
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            contents: self.contents.clone(),
            extractor: self.extractor.clone(),
        }
    }
}

/// Go `getFakeStats(schema)` (`logical_show.go:199`): one row, every column at
/// NDV 1. Shared with [`crate::logical::LogicalShowDDLJobs`], as Go's own
/// `todo: merge getFakeStats with the one in logical_show_ddl_jobs.go` asks
/// for — the two bodies are identical and this is the merged one.
#[must_use]
pub fn get_fake_stats(schema: &Schema) -> StatsInfo {
    StatsInfo::new(1.0, schema.columns.iter().map(|col| (col.unique_id, 1.0)))
}

/// Go `extractStatsMetaFilters(...)` (`logical_show.go:210`): the predicates
/// that survive, and the INTERSECTION of the values every claimed predicate
/// allowed for `col_name`.
///
/// Two refusals are deliberate, and both keep the original predicates:
///
/// * a column that the schema does not name is not filterable at all;
/// * an EMPTY intersection is not "no rows" but a contradiction, which Go keeps
///   as a normal predicate "to preserve semantics for contradictory filters" —
///   handing an empty filter set to the extractor would read as "no filter".
#[must_use]
pub fn extract_stats_meta_filters(
    schema: &Schema,
    names: &[FieldName],
    predicates: Vec<Expression>,
    col_name: &str,
    to_lower: bool,
) -> (Vec<Expression>, Option<BTreeSet<String>>) {
    let col_ids = find_show_column_ids(schema, names, col_name);
    if col_ids.is_empty() {
        return (predicates, None);
    }
    let mut extracted_idx = Vec::with_capacity(predicates.len());
    let mut intersection: Option<BTreeSet<String>> = None;
    for (i, expr) in predicates.iter().enumerate() {
        let Some(vals) = extract_stats_meta_filter_values(expr, &col_ids) else {
            continue;
        };
        extracted_idx.push(i);
        let val_set: BTreeSet<String> = vals
            .into_iter()
            .map(|val| if to_lower { val.to_lowercase() } else { val })
            .collect();
        intersection = Some(match intersection {
            None => val_set,
            Some(previous) => previous.intersection(&val_set).cloned().collect(),
        });
    }
    let Some(intersection) = intersection.filter(|set| !set.is_empty()) else {
        return (predicates, None);
    };
    if extracted_idx.is_empty() {
        return (predicates, None);
    }
    let remained = predicates
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !extracted_idx.contains(i))
        .map(|(_, expr)| expr)
        .collect();
    (remained, Some(intersection))
}

/// Go `findShowColumnIDs(schema, names, colName)` (`logical_show.go:266`): the
/// unique ids of the schema columns whose OUTPUT NAME is `col_name`.
///
/// The name list and the schema are parallel, and Go stops at the shorter of
/// the two rather than indexing past the schema.
#[must_use]
pub fn find_show_column_ids(schema: &Schema, names: &[FieldName], col_name: &str) -> BTreeSet<i64> {
    names
        .iter()
        .zip(&schema.columns)
        .filter(|(name, _)| name.names.column.lower == col_name)
        .map(|(_, column)| column.unique_id)
        .collect()
}

/// Go `extractStatsMetaFilterValues(ctx, expr, colIDs)`
/// (`logical_show.go:279`): the string values one predicate allows for the
/// column, or `None` when the predicate is not of an extractable shape.
///
/// A disjunction is extractable only if EVERY branch is, which is what makes
/// the union of the branches sound: one unextractable branch could admit
/// anything.
#[must_use]
pub fn extract_stats_meta_filter_values(
    expr: &Expression,
    col_ids: &BTreeSet<i64>,
) -> Option<Vec<String>> {
    let Expression::ScalarFunction(fn_expr) = expr else {
        return None;
    };
    match fn_expr.func_name.lowercase() {
        "eq" => extract_stats_meta_eq_value(&fn_expr.args, col_ids).map(|value| vec![value]),
        "in" => extract_stats_meta_in_values(&fn_expr.args, col_ids),
        "or" => {
            let dnf_items = split_dnf_items(expr);
            if dnf_items.is_empty() {
                return None;
            }
            let mut result = Vec::with_capacity(dnf_items.len());
            for item in &dnf_items {
                result.extend(extract_stats_meta_filter_values(item, col_ids)?);
            }
            Some(result)
        }
        _ => None,
    }
}

/// Go `extractStatsMetaEQValue(ctx, fn, colIDs)` (`logical_show.go:321`): the
/// constant on the OTHER side of an equality whose one side is the column.
fn extract_stats_meta_eq_value(args: &[Expression], col_ids: &BTreeSet<i64>) -> Option<String> {
    let [left, right] = args else {
        return None;
    };
    let col_idx = [left, right].into_iter().position(
        |arg| matches!(arg, Expression::Column(col) if col_ids.contains(&col.unique_id)),
    )?;
    get_string_value_from_constant(if col_idx == 0 { right } else { left })
}

/// Go `extractStatsMetaINValues(ctx, fn, colIDs)` (`logical_show.go:349`).
///
/// Unlike the equality case the column must be the FIRST argument, because that
/// is where `IN`'s rewrite puts it, and EVERY remaining argument must be a
/// usable constant — one that is not makes the whole list unextractable.
fn extract_stats_meta_in_values(
    args: &[Expression],
    col_ids: &BTreeSet<i64>,
) -> Option<Vec<String>> {
    let (first, rest) = args.split_first()?;
    if rest.is_empty() {
        return None;
    }
    match first {
        Expression::Column(col) if col_ids.contains(&col.unique_id) => {}
        _ => return None,
    }
    rest.iter().map(get_string_value_from_constant).collect()
}

/// Go `getStringValueFromConstant(ctx, expr)` (`logical_show.go:377`): a plain
/// literal's string form.
///
/// A DEFERRED constant is refused because its value is not known at plan time;
/// see this module's header for the param-marker narrowing. Go's `Datum.ToString`
/// lands here as `Datum::to_bytes`, whose one difference is that it returns the
/// octets rather than a Go string, so a non-UTF-8 value is refused instead of
/// being carried through.
#[must_use]
pub fn get_string_value_from_constant(expr: &Expression) -> Option<String> {
    let Expression::Constant(constant) = expr else {
        return None;
    };
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return None;
    }
    String::from_utf8(constant.value.to_bytes().ok()?).ok()
}
