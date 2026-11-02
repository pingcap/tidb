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

//! Access-path identities from `pkg/planner/util/path.go`.
//!
//! Go decides point-get eligibility and derives `AccessPath.CountAfterAccess`
//! before physical index conversion. Both are explicit at this boundary, so a
//! generic Rust candidate cannot silently become an index scan.

use tidb_planner_util::path::{IndexLookUpPushDownByType, AccessPath};
use tidb_planner_core_operator_logicalop::data_source::PossibleAccessPath;
use tidb_sessionctx::{IndexLookupPushDownPolicy, IndexLookupPushDownSession};

pub(crate) mod index_merge;
pub(crate) mod mv_index;
pub(crate) mod ordinary;

/// Go `statistics.PrepareCols4MVIndex` (`PrepareIdxColsAndUnwrapArrayType`
/// with its one-ARRAY-column check), which `GenerateHistCollFromColumnInfo`
/// stores as `HistColl.MVIdx2Columns`.
#[must_use]
pub fn prepare_cols_for_mv_index(
    ds: &tidb_planner_core_operator_logicalop::DataSource,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
) -> Option<Vec<tidb_expr::column::Column>> {
    mv_index::prepare_idx_cols_and_unwrap_array_type(ds, index, true)
}

/// Statement inputs to logical access-path derivation, independent of physical
/// properties, plan allocation and task costing. Range fallback side effects
/// remain attached to the same statement as the expression evaluator.
pub(crate) struct AccessPathDerivationContext<'a> {
    pub opt_prefix_index_single_scan: bool,
    pub expr_pushdown_blacklist: &'a tidb_expr::infer_pushdown::ExprPushDownBlacklist,
    pub selectivity_factor: f64,
    pub estimator_options: tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
    pub range_max_size: i64,
    pub range_fallback_handler: Option<&'a tidb_context::RangeFallbackHandler>,
    pub expression_evaluator: &'a tidb_ranger::points::ExpressionEvaluator<'a>,
    /// Go `sctx.SetSkipPlanCache` from the ranger; `None` unless the
    /// statement uses the plan cache (Go's tracker ignores it otherwise).
    pub plan_cache_marker: Option<&'a dyn tidb_planner_core_base::PlanCacheMarker>,
    /// Go `recordUsedItemStatsStatus`; `None` outside the statement's
    /// statistics derivation.
    pub used_stats_recorder: Option<&'a dyn tidb_planner_core_operator_logicalop::plan_context::UsedStatsRecorder>,
}

impl AccessPathDerivationContext<'_> {
    pub(crate) fn detach_index_range(
        &self,
        conditions: &[tidb_expr::expression::Expression],
        columns: &[tidb_expr::column::Column],
        lengths: &[i64],
    ) -> Result<tidb_ranger::detacher::DetachRangeResult, tidb_ranger::points::PointBuilderError>
    {
        let options = tidb_ranger::detacher::RangerOptions {
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
            ..self.estimator_options.ranger_options()
        };
        let detached = match self.range_fallback_handler {
            Some(handler) => tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
                conditions,
                columns,
                lengths,
                self.range_max_size,
                handler,
                self.expression_evaluator,
                options,
            ),
            None => tidb_ranger::detacher::detach_cond_and_build_range_for_index_in(
                conditions,
                columns,
                lengths,
                self.range_max_size,
                self.expression_evaluator,
                options,
            ),
        }?;
        if let (Some(marker), Some(reason)) = (
            self.plan_cache_marker,
            detached.skip_plan_cache_reason.as_deref(),
        ) {
            marker.set_skip_plan_cache(reason);
        }
        Ok(detached)
    }
}

pub(crate) fn derive_access_paths(
    source: &mut tidb_planner_core_operator_logicalop::DataSource,
    context: &AccessPathDerivationContext<'_>,
    index_merge_enabled: bool,
    no_index_merge_hint: bool,
    prefix_single_scan: bool,
    use_plan_cache: bool,
) -> Result<(Vec<PossibleAccessPath>, Option<f64>, Option<String>), tidb_planner_core_base::PlanError> {
    // Go `deriveTablePathStats` over the TiKV table path.
    if let Some(position) = source.all_possible_access_paths.iter().position(|path| path.is_tikv_table_path()) {
        let filled = ordinary::fill_table_path(
            source,
            source.all_possible_access_paths[position].index.as_ref(),
            context,
        )?;
        source.all_possible_access_paths[position].filled_table = Some(filled);
    }
    let table_path = source.filled_table_path().cloned();
    ordinary::fill_ordinary_index_paths(source, context, prefix_single_scan)?;
    // Go's `PossibleAccessPaths` holds the same path pointers as
    // `AllPossibleAccessPaths` until the index-merge paths join it.
    let mut paths = (0..source.all_possible_access_paths.len())
        .map(PossibleAccessPath::Shared)
        .collect::<Vec<_>>();
    let mut min_selectivity = source
        .table_stats
        .as_ref()
        .filter(|stats| stats.row_count() > 0.0)
        .and_then(|stats| {
            source
                .all_possible_access_paths
                .iter()
                .filter_map(|path| {
                    if path.is_tiflash_path() {
                        // The TiFlash path is the table path under another
                        // store; only a common handle's PRIMARY estimate
                        // reaches it.
                        return table_path
                            .as_ref()
                            .filter(|_| source.is_common_handle() && !source.pushed_down_conds.is_empty())
                            .and_then(|path| path.count_after_access);
                    }
                    if path.is_table_path() {
                        return table_path.as_ref().and_then(|path| path.count_after_access);
                    }
                    path.filled
                        .as_ref()
                        .and_then(|filled| filled.count_after_index)
                        .or_else(|| path.count_after_access())
                })
                .map(|rows| rows / stats.row_count())
                .reduce(f64::min)
        })
        .unwrap_or(source.access_path_min_selectivity)
        .min(1.0);
    // Go `generateIndexMergePath`'s warning, reported only for a hinted table
    // (which then loses its hints).
    let mut index_merge_warning = None;
    let mut index_merge_max_count = None;
    // Go `generateIndexMergePath`: `(EnableIndexMerge || len(IndexMergeHints)
    // > 0) && !NoIndexMergeHint`.
    if !((index_merge_enabled || !source.index_merge_hints.is_empty()) && !no_index_merge_hint) {
        index_merge_warning = Some(
            "IndexMerge is inapplicable or disabled. Got no_index_merge hint or tidb_enable_index_merge is off.",
        );
    } else if source.is_local_temporary() {
        index_merge_warning =
            Some("IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table.");
    } else {
        // Go `generateOtherIndexMerge`'s `isPossibleIdxMerge`, over
        // `indexMergeConds` (= `AllConds`).
        if source.all_conds.is_empty() || source.all_possible_access_paths.len() <= 1 {
            index_merge_warning = Some(
                "IndexMerge is inapplicable or disabled. No available filter or available index.",
            );
        }
        let mut merges = Vec::new();
        merges.extend(index_merge::prepare_union_index_merge_paths(source, context, use_plan_cache)?);
        merges.extend(index_merge::generate_and_index_merge_4_normal_index(
            source,
            context,
            use_plan_cache,
        )?);
        // Go `generateOtherIndexMerge`'s `needConsiderIndexMerge`: without
        // index-merge hints, the merges just built are kept only when every
        // condition can be pushed down and, with fix control 52869 OFF, no
        // other path has access conditions.
        if source.index_merge_hints.is_empty() {
            let mut need_consider_index_merge = true;
            if !source.index_merge_skip_range_scan_check {
                need_consider_index_merge =
                    !source
                        .all_possible_access_paths
                        .iter()
                        .skip(1)
                        .any(|path| match path.index_info() {
                            None => table_path
                                .as_ref()
                                .is_some_and(|path| !path.detached.access_conds.is_empty()),
                            Some(_) => path
                                .filled
                                .as_ref()
                                .is_some_and(|filled| !filled.detached.access_conds.is_empty()),
                        });
            }
            if need_consider_index_merge
                && !tidb_expr::infer_pushdown::can_exprs_push_down(
                    &source.all_conds,
                    tidb_kv::store_type::StoreType::Unspecified,
                    context.expr_pushdown_blacklist,
                )
            {
                need_consider_index_merge = false;
            }
            if !need_consider_index_merge {
                // Go deletes a merge in place and still advances its index,
                // so the merge after each deleted one is never examined and
                // survives; `containMVPath` is never reset, so every merge
                // after the first one over an MV index survives too.
                let mut contain_mv_path = false;
                let mut i = 0;
                while i < merges.len() {
                    contain_mv_path |= merges[i]
                        .partial_alternative_index_paths
                        .iter()
                        .flatten()
                        .any(|alternative| index_merge::is_mv_alternative(alternative));
                    if !contain_mv_path {
                        merges.remove(i);
                    }
                    i += 1;
                }
                if merges.is_empty() && index_merge_warning.is_none() {
                    index_merge_warning = Some("IndexMerge is inapplicable or disabled. ");
                }
            }
        }
        // Go's second and third entry points read `AllConds`: an IndexMerge
        // over one MV index, then an intersection composing MV and ordinary
        // indexes.
        for path in mv_index::generate_and_index_merge_4_mv_index(
            source,
            context,
            &source.all_conds,
            use_plan_cache,
        )? {
            merges.push(path);
        }
        let composed = mv_index::generate_and_index_merge_4_composed_index(
            source,
            context,
            &source.all_conds,
            use_plan_cache,
        )?;
        if source.all_conds.len() > source.pushed_down_conds.len() {
            index_merge_max_count = merges
                .iter()
                .chain(composed.iter())
                .map(|path| path.count_after_access().unwrap_or(0.0))
                .reduce(f64::max);
        }
        if !source.index_merge_hints.is_empty() {
            // A hinted composed path prunes every other path, merges
            // included; otherwise the merges prune the ordinary paths.
            if composed.is_some() {
                merges.clear();
            }
            if composed.is_some() || !merges.is_empty() {
                paths.clear();
                min_selectivity = 1.0;
            }
        }
        merges.extend(composed);
        if let Some(stats) = source
            .table_stats
            .as_ref()
            .filter(|stats| stats.row_count() > 0.0)
        {
            for path in &merges {
                // Go's unfinished PartialAlternativeIndexPaths reads
                // CountAfterIndex (still zero) in getGeneralAttributesFromPaths.
                // Convergence later creates concrete PartialIndexPaths whose
                // CountAfterAccess is used for physical conversion.
                let rows = if path.partial_alternative_index_paths.is_empty() {
                    path.count_after_access().unwrap_or(0.0)
                } else {
                    0.0
                };
                min_selectivity = min_selectivity.min(rows / stats.row_count());
            }
        }
        if merges.is_empty() && index_merge_warning.is_none() {
            index_merge_warning = Some("IndexMerge is inapplicable");
        }
        paths.extend(merges.into_iter().map(PossibleAccessPath::IndexMerge));
        // Go `cleanAccessPathForMVIndexHint`.
        mv_index::clean_access_path_for_mv_index_hint(source, &mut paths);
    }
    let index_merge_warning = index_merge_warning
        .filter(|_| !source.index_merge_hints.is_empty())
        .map(|message| {
            source.index_merge_hints.clear();
            message.to_owned()
        });
    // Go `getGeneralAttributesFromPaths`' `AccessPathMinSelectivity`, which
    // includes merge estimates before ordered-index expected-count
    // adjustment.
    source.access_path_min_selectivity = min_selectivity;
    // Go `generateIndexMergePath`'s `maxRowCount` (the largest index-merge
    // `CountAfterAccess`, when merges saw more conditions than the
    // statistics did) goes back with the paths for the caller to cap the
    // DataSource's rows.
    Ok((paths, index_merge_max_count, index_merge_warning))
}

/// Go `getPossibleAccessPaths` (`planbuilder.go:1320-1441`) without hints —
/// the enumeration Go performs before any hint filtering.
///
/// Reproduced, in Go's order:
/// * the table path first, with `fillContentForTablePath`'s int-vs-common
///   handle split (`planbuilder.go:1520-1532`);
/// * one path per PUBLIC index (`index.State == model.StatePublic`);
/// * an invisible index is skipped unless
///   `OptimizerUseInvisibleIndexes` (`:1378`);
/// * a common-handle table's PRIMARY index is skipped as an index path
///   (`:1382`) — it already rode in on the table path;
/// * a columnar index is skipped because this access-path representation does
///   not yet carry Go's TiFlash index-path identity (`:1399-1404`).
///
/// Named boundaries, absent rather than approximated:
/// * TiFlash columnar-index paths (`:1399-1404`) — only TiFlash table paths
///   are represented today;
/// * `kv.TiDB` for cluster tables (`:1324-1326`) — no cluster-table flag;
/// * inverted indexes (`:1396-1398`) — no inverted flag on
///   [`tidb_model::GoShared<tidb_model::IndexInfo>`], so an inverted index
///   would enumerate here where Go skips it; the catalog does not model one
///   today;
/// * an EXPLAIN's hypothetical TiFlash replica (`:1331-1346`) adds the
///   TiFlash path, and its hypothetical indexes (`:1419-1441`) are public
///   paths after the real ones, appended as they are; neither joins the
///   table's own metadata;
/// Hint filtering (`:1443` onward) is the separate
/// [`apply_table_index_hints`] stage, matching Go's enumeration-then-filter
/// structure. Comment-style hints must join that same stage through the
/// query-block hint owner; they are not approximated here.
#[must_use]
pub fn get_possible_access_paths(
    table: &tidb_infoschema::table_source::SourceTable,
    optimizer_use_invisible_indexes: bool,
    latest_index_schema: Option<&tidb_infoschema::table_source::LatestIndexSchema>,
    connection_id: Option<u64>,
    repeatable_read: bool,
    is_for_update_read: bool,
    hypo_indexes: &[tidb_model::GoShared<tidb_model::IndexInfo>],
    hypo_tiflash_replica: bool,
) -> Result<Vec<AccessPath>, tidb_planner_core_base::PlanError> {
    // Go splits ONLY on `tblInfo.IsCommonHandle` (`planbuilder.go:1521`):
    // a table with no explicit handle column is still an int-handle path,
    // through the implicit `_tidb_rowid`. `handle_is_int()` would misread
    // that case — the first version of this port did, and the test caught it.
    let is_common_handle = table.is_common_handle();
    let mut paths = Vec::with_capacity(table.table_indices().len() + 1);
    let primary_index = table.table_indices().iter().find(|index| index.read().primary).cloned();
    paths.push(AccessPath::table_path(
        tidb_kv::store_type::StoreType::TiKv,
        is_common_handle,
        primary_index.clone(),
    ));
    if table.has_tiflash_replica() || hypo_tiflash_replica {
        // Go `genTiFlashPath`.
        paths.push(AccessPath::table_path(
            tidb_kv::store_type::StoreType::TiFlash,
            is_common_handle,
            primary_index,
        ));
    }
    let mut check_latest_schema =
        (is_for_update_read || !repeatable_read) && connection_id.is_some_and(|id| id > 0);
    let mut latest_indexes = None;
    for index in &table.table_indices() {
        if !index.read().is_public() {
            continue;
        }
        if check_latest_schema && latest_indexes.is_none() {
            let (indexes, changed) =
                tidb_planner_util_domainmisc::get_latest_index_info(latest_index_schema, table.table_id(), 0)
                    .map_err(|error| tidb_planner_core_base::PlanError::internal(error.to_string()))?;
            latest_indexes = indexes;
            check_latest_schema = changed;
        }
        if check_latest_schema
            && latest_indexes.as_ref().is_none_or(|indexes| {
                indexes
                    .get(&index.read().id)
                    .is_none_or(|latest| !latest.read().is_public())
            })
        {
            continue;
        }
        if !optimizer_use_invisible_indexes && index.read().invisible {
            continue;
        }
        if is_common_handle && index.read().primary {
            continue;
        }
        if index.read().is_columnar_index() {
            continue;
        }
        paths.push(tidb_planner_util::path::AccessPath::index_path(index.clone()));
    }
    paths.extend(hypo_indexes.iter().map(|index| tidb_planner_util::path::AccessPath::index_path(index.clone())));
    Ok(paths)
}


/// Core's `getPossibleAccessPaths` hint resolution, registered for the rules
/// (Go `utilfuncp.GetPossibleAccessPaths`).
#[derive(Clone, Copy, Debug, Default)]
pub struct CoreAccessPathResolver;

impl tidb_planner_core_operator_logicalop::plan_context::AccessPathResolver for CoreAccessPathResolver {
    fn apply_table_index_hints(
        &self,
        table: &tidb_infoschema::table_source::SourceTable,
        public_paths: &[AccessPath],
        hints: &[tidb_ast::IndexHint],
        comment_hints: &[tidb_planner_core_operator_logicalop::data_source::DataSourceIndexHint],
        remove_global_indexes: bool,
        force_no_index_lookup_push_down: bool,
        session: IndexLookupPushDownSession,
        tikv_in_isolation_read: bool,
        isolation_read_engines_value: &str,
        warnings: &mut dyn FnMut(u16, String),
    ) -> Result<Vec<AccessPath>, tidb_planner_core_base::PlanError> {
        apply_table_index_hints(
            table,
            public_paths,
            hints,
            comment_hints,
            remove_global_indexes,
            force_no_index_lookup_push_down,
            session,
            tikv_in_isolation_read,
            isolation_read_engines_value,
            warnings,
        )
    }
}

/// Go `getPathByIndexName`: the position of the path `name` names -- the
/// TiKV table path for PRIMARY of a clustered table, an exact index name, or
/// the only index whose name `name` prefixes.
fn path_by_index_name(paths: &[AccessPath], name: &str, has_clustered_index: bool) -> Option<usize> {
    let mut prefix_path = None;
    let mut prefix_matches = 0;
    for (position, path) in paths.iter().enumerate() {
        // Only accept TiKV's primary key table path.
        if path.is_tikv_table_path() && tidb_mysql::lowercase_equal(name, "primary") && has_clustered_index {
            return Some(position);
        }
        // If it's not a TiKV table path and the index is nil, it could not
        // be any index path.
        let Some(index) = &path.index else {
            continue;
        };
        if tidb_mysql::lowercase_equal(index.read().name.original(), name) {
            return Some(position);
        }
        if tidb_mysql::to_lowercase(index.read().name.original()).starts_with(&tidb_mysql::to_lowercase(name)) {
            prefix_path = Some(position);
            prefix_matches += 1;
        }
    }
    // Return only unique prefix matches.
    if prefix_matches == 1 { prefix_path } else { None }
}

/// Applies Go `getPossibleAccessPaths`' table-syntax and already-matched
/// comment-style index-hint tail to an enumerated public-path list, returning
/// Go's `available` paths. The warnings Go appends to the statement context
/// go to `warnings` as (code, message), in Go's order.
pub fn apply_table_index_hints(
    table: &tidb_infoschema::table_source::SourceTable,
    public_paths: &[AccessPath],
    hints: &[tidb_ast::IndexHint],
    comment_hints: &[tidb_planner_core_operator_logicalop::data_source::DataSourceIndexHint],
    remove_global_indexes: bool,
    force_no_index_lookup_push_down: bool,
    session: IndexLookupPushDownSession,
    tikv_in_isolation_read: bool,
    isolation_read_engines_value: &str,
    warnings: &mut dyn FnMut(u16, String),
) -> Result<Vec<AccessPath>, tidb_planner_core_base::PlanError> {
    use tidb_ast::{IndexHintKind, IndexHintScope};

    // Go hands out pointers into `publicPaths`: a hint's flags land on the
    // shared path, and `available` lists those paths. Here `available` holds
    // positions into this working copy.
    let mut public_paths = public_paths.to_vec();
    let table_path = public_paths
        .iter()
        .position(|path| path.is_tikv_table_path())
        .expect("getPossibleAccessPaths always creates a table path");
    let mut has_scan_hint = false;
    let mut has_use_or_force = false;
    let mut available: Vec<usize> = Vec::new();
    let mut ignored = std::collections::BTreeSet::new();

    let unsupported_reason = |metadata: &tidb_model::GoShared<tidb_model::IndexInfo>| {
        if table.is_common_handle() && table.common_handle_version() < 1 {
            Some("common handle table with old encoding version is not supported")
        } else if metadata.read().global {
            Some("the global index in partition table is not supported")
        } else if table.is_temporary() {
            Some("temporary table is not supported")
        } else if table.is_cached() {
            Some("cached table is not supported")
        } else if metadata.read().mv_index {
            Some("multi-valued index is not supported")
        } else if !session.repeatable_read {
            Some("transaction isolation level is not REPEATABLE-READ")
        } else if !session.leader_read {
            Some("only leader read is supported")
        } else if session.staleness {
            Some("stale read is not supported")
        } else if session.historical_read {
            Some("historical read is not supported")
        } else if session.max_keys_read > 0 {
            Some("tidb_max_keys_read is set")
        } else {
            None
        }
    };
    if !force_no_index_lookup_push_down
        && matches!(
            session.policy,
            IndexLookupPushDownPolicy::Force | IndexLookupPushDownPolicy::AffinityForce
        )
        && (session.policy != IndexLookupPushDownPolicy::AffinityForce || table.has_affinity())
    {
        for path in &mut public_paths {
            if path.index_info().is_some_and(|metadata| unsupported_reason(metadata).is_none()) {
                path.index_lookup_push_down_by = IndexLookUpPushDownByType::SysVar;
            }
        }
    }

    let table_hints = hints.iter().filter_map(|hint| {
        if hint.scope != IndexHintScope::All {
            return None;
        }
        Some((
            hint.kind,
            hint.indexes.as_slice(),
            false,
            false,
            false,
            false,
        ))
    });
    let comment_hints = comment_hints.iter().map(|hint| {
        (
            hint.kind,
            hint.index_names.as_slice(),
            true,
            hint.force_keep_order,
            hint.force_no_keep_order,
            hint.push_down_lookup,
        )
    });
    for (kind, indexes, comment_style, force_keep_order, force_no_keep_order, push_down_lookup) in
        table_hints.chain(comment_hints)
    {
        has_scan_hint = true;
        // Go checks this before the ordinary empty-list handling. With TiKV
        // disabled, `USE INDEX ()` must leave the public TiFlash paths
        // available rather than manufacture a TiKV table path that the
        // isolation filter immediately removes.
        if !tikv_in_isolation_read && indexes.is_empty() {
            continue;
        }
        if indexes.is_empty() && kind != IndexHintKind::Ignore {
            // Go `getTablePath`: the first table path.
            if let Some(position) = public_paths.iter().position(AccessPath::is_table_path) {
                has_use_or_force = true;
                public_paths[position].forced = true;
                available.push(position);
            }
        }
        for name in indexes {
            // Go `getPathByIndexName`.
            let path = path_by_index_name(&public_paths, name, table.pk_is_handle() || table.is_common_handle());
            let Some(position) = path else {
                if comment_style {
                    // Go appends `ErrKeyDoesNotExist` as a warning for a
                    // comment-style hint.
                    warnings(1176, format!("Key '{name}' doesn't exist in table '{}'", table.table_name()));
                    continue;
                }
                return Err(tidb_planner_core_base::PlanError::from(
                    tidb_plannererrors::ERR_KEY_DOES_NOT_EXIST
                        .generate_with_stack_by_args(&[name.into(), (&table.table_name()).into()]),
                ));
            };
            let path = &mut public_paths[position];
            if kind == IndexHintKind::Ignore {
                // Go `removeIgnoredPaths` matches the resolved paths' `Index`.
                if let Some(index) = &path.index {
                    ignored.insert(index.read().id);
                }
                continue;
            }
            // Go `isTiKVIndexByName`.
            let is_tikv_index = tidb_mysql::lowercase_equal(name, "primary") && table.pk_is_handle()
                || path
                    .index
                    .as_ref()
                    .is_some_and(|index| !index.read().is_columnar_index());
            if is_tikv_index && !tikv_in_isolation_read {
                let message = format!(
                    "TiDB doesn't support index '{name}' in the isolation read engines(value: '{isolation_read_engines_value}')"
                );
                if comment_style {
                    warnings(1105, message);
                    continue;
                }
                return Err(tidb_planner_core_base::PlanError::internal(message));
            }
            has_use_or_force = true;
            path.forced = true;
            if force_keep_order {
                path.force_keep_order = true;
            }
            if force_no_keep_order {
                path.force_no_keep_order = true;
            }
            if let Some(metadata) = path.index.clone() {
                let metadata = &metadata;
                if push_down_lookup {
                    if force_no_index_lookup_push_down {
                        warnings(
                            1815,
                            "hint INDEX_LOOKUP_PUSHDOWN cannot be inapplicable, NO_INDEX_LOOKUP_PUSHDOWN is specified".to_owned(),
                        );
                        continue;
                    }
                    if let Some(reason) = unsupported_reason(metadata) {
                        warnings(1815, format!("hint INDEX_LOOKUP_PUSHDOWN is inapplicable, {reason}"));
                        continue;
                    }
                    path.index_lookup_push_down_by = IndexLookUpPushDownByType::Hint;
                }
            }
            available.push(position);
        }
    }

    if !has_scan_hint || !has_use_or_force {
        available = (0..public_paths.len()).collect();
    }
    // Go `removeIgnoredPaths`.
    available.retain(|&position| {
        let path = &public_paths[position];
        path.is_tikv_table_path()
            || path.is_tiflash_simple_table_path()
            || !path.index.as_ref().is_some_and(|index| ignored.contains(&index.read().id))
    });
    if remove_global_indexes {
        // Go `removeGlobalIndexPaths`.
        available.retain(|&position| {
            !public_paths[position].index.as_ref().is_some_and(|index| index.read().global)
        });
    }
    if available.is_empty() {
        available.push(table_path);
    }
    if available.iter().all(|&position| public_paths[position].is_undetermined()) {
        available.push(table_path);
    }
    Ok(available.into_iter().map(|position| public_paths[position].clone()).collect())
}

/// Pinned Go `indexIsAvailableByHints`, the deliberately smaller hint check
/// used only while trying a point/batch-point fast plan.
///
/// Unlike ordinary `getPossibleAccessPaths`, the fast check recognizes only
/// `USE_INDEX`, `FORCE_INDEX`, and `IGNORE_INDEX`, matches index names exactly
/// (not by unique prefix), and performs no warning or name validation. A
/// rejected fast candidate falls back to ordinary planning, which owns those
/// full behaviors.
pub fn fast_index_is_available_by_hints(
    current_db: &str,
    db_name: &str,
    table_alias: &str,
    index_name: Option<&str>,
    comment_hints: &[tidb_ast::Hint],
    table_hints: &[tidb_ast::IndexHint],
) -> bool {
    use tidb_ast::{HintKind, IndexHintKind, IndexHintScope};

    let matches_target = |name: &str| match index_name {
        Some(index) => tidb_mysql::lowercase_equal(index, name),
        None => tidb_mysql::lowercase_equal(name, "primary"),
    };
    let mut is_ignore = false;
    // Go skips any hint whose scope is not `HintForScan` before it can mark
    // anything (`hint.HintScope != ast.HintForScan -> continue`), so a
    // `FOR JOIN`/`FOR ORDER BY`/`FOR GROUP BY` hint never makes a point read
    // fall back, exactly as if it were absent.
    let mut saw_hint = table_hints
        .iter()
        .any(|hint| hint.scope == IndexHintScope::All);
    let mut apply = |kind: IndexHintKind, scope: IndexHintScope, indexes: &[String]| {
        if scope != IndexHintScope::All {
            return None;
        }
        if kind == IndexHintKind::Ignore && !indexes.is_empty() {
            is_ignore = true;
            if indexes.iter().any(|name| matches_target(name)) {
                return Some(false);
            }
        }
        if matches!(kind, IndexHintKind::Use | IndexHintKind::Force)
            && !indexes.is_empty()
            && indexes.iter().any(|name| matches_target(name))
        {
            return Some(true);
        }
        None
    };

    for hint in table_hints {
        if let Some(available) = apply(hint.kind, hint.scope, &hint.indexes) {
            return available;
        }
    }
    for hint in comment_hints {
        let HintKind::Index { table, indexes, .. } = &hint.kind else {
            continue;
        };
        // Go switches on `h.HintName.L`.
        let kind = match hint.lower_name().as_str() {
            "use_index" => IndexHintKind::Use,
            "ignore_index" => IndexHintKind::Ignore,
            "force_index" => IndexHintKind::Force,
            _ => continue,
        };
        let hint_db = table.db_name.as_deref().unwrap_or(current_db);
        if !tidb_mysql::lowercase_equal(&table.name, table_alias)
            || !(tidb_mysql::lowercase_equal(hint_db, db_name) || hint_db == "*")
        {
            continue;
        }
        saw_hint = true;
        if let Some(available) = apply(kind, IndexHintScope::All, indexes) {
            return available;
        }
    }
    if !saw_hint {
        return true;
    }
    is_ignore
}

#[cfg(test)]
mod enumeration_tests {
    use std::collections::BTreeMap;

    use super::{get_possible_access_paths as enumerate_possible_access_paths};

    use tidb_planner_util::path::{AccessPath};
    use tidb_infoschema::table_source::LatestIndexSchema;
    use tidb_infoschema::table_source::SourceTable;

    // All WRITTEN: Go's coverage of getPossibleAccessPaths is
    // planner-integration bound. Each expectation cites the Go line it
    // checks.

    fn index(name: &str) -> tidb_model::GoShared<tidb_model::IndexInfo> {
        tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 1,
            name: tidb_ast::CiString::new(name.to_string()),
            columns: (Vec::new()).into(),
            unique: false,
            primary: false,
            state: tidb_model::SchemaState::PUBLIC,
            mv_index: false,
            global: false,
            condition_expr_string: String::new(),
            ..tidb_model::IndexInfo::default()
        })
    }

    fn table(indexes: Vec<tidb_model::GoShared<tidb_model::IndexInfo>>) -> SourceTable {
        let mut table = SourceTable::default();
        table.set_table_indices(indexes);
        table
    }

    fn get_possible_access_paths(
        table: &SourceTable,
        optimizer_use_invisible_indexes: bool,
    ) -> Vec<AccessPath> {
        enumerate_possible_access_paths(
            table,
            optimizer_use_invisible_indexes,
            None,
            None,
            true,
            false,
            &[],
            false,
        )
        .unwrap()
    }

    #[test]
    fn the_table_path_comes_first_and_is_int_handle_by_default() {
        // `publicPaths[0]` is the table path (`planbuilder.go:1327-1329`),
        // int-handle when the table is not common-handle (`:1530`).
        let paths = get_possible_access_paths(&table(vec![index("i1")]), false);
        assert_eq!(
            paths[0],
            AccessPath::table_path(tidb_kv::store_type::StoreType::TiKv, false, None)
        );
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn an_invisible_index_needs_the_session_flag() {
        // `!optimizerUseInvisibleIndexes && index.Invisible` skips
        // (`planbuilder.go:1378`).
        let hidden = index("hidden");
        hidden.write().invisible = true;
        let t = table(vec![hidden]);
        assert_eq!(get_possible_access_paths(&t, false).len(), 1);
        assert_eq!(get_possible_access_paths(&t, true).len(), 2);
    }

    #[test]
    fn a_non_public_index_never_enumerates() {
        // `index.State == model.StatePublic` guards the whole arm
        // (`planbuilder.go:1376`).
        let building = index("building");
        building.write().state = tidb_model::SchemaState::WRITE_REORGANIZATION;
        assert_eq!(
            get_possible_access_paths(&table(vec![building]), true).len(),
            1
        );
    }

    #[test]
    fn a_columnar_index_is_skipped_without_a_tiflash_index_path_identity() {
        // Go admits a columnar index only as a TiFlash index path
        // (`planbuilder.go:1399-1404`); this path type is not represented yet.
        let columnar = index("v");
        columnar.write().vector_info =
            Some(tidb_model::GoShared::new(tidb_model::VectorIndexInfo::default()));
        assert_eq!(
            get_possible_access_paths(&table(vec![columnar]), false).len(),
            1
        );
    }

    #[test]
    fn read_committed_connected_session_uses_only_latest_public_indexes() {
        let mut source = table(vec![
            index("removed"),
            {
                let public = index("public");
                public.write().id = 2;
                public
            },
        ]);
        source.set_table_id(42);
        let latest = LatestIndexSchema {
            schema_meta_version: 8,
            table_indexes: BTreeMap::from([(
                42,
                vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 2,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::IndexInfo::default()
                })],
            )]),
        };

        let paths =
            enumerate_possible_access_paths(&source, false, Some(&latest), Some(7), false, false, &[], false)
                .unwrap();

        assert_eq!(
            paths,
            vec![
                AccessPath::table_path(tidb_kv::store_type::StoreType::TiKv, false, None),
                tidb_planner_util::path::AccessPath::index_path(source.table_indices()[1].clone()),
            ]
        );
    }

    /// Go `getPossibleAccessPaths` (`planbuilder.go:1419-1441`): an
    /// EXPLAIN's hypothetical indexes follow the real ones as they are,
    /// unvetted by the latest schema, and its hypothetical TiFlash replica
    /// adds the TiFlash path.
    #[test]
    fn hypothetical_indexes_and_replica_join_only_the_paths() {
        let mut source = table(vec![index("real")]);
        source.set_table_id(42);
        let latest = LatestIndexSchema {
            schema_meta_version: 8,
            table_indexes: BTreeMap::from([(42, vec![source.table_indices()[0].clone()])]),
        };
        let hypo = index("hypo");
        hypo.write().id = -1;
        let paths = enumerate_possible_access_paths(
            &source,
            false,
            Some(&latest),
            Some(7),
            false,
            false,
            std::slice::from_ref(&hypo),
            true,
        )
        .unwrap();
        assert_eq!(
            paths,
            vec![
                AccessPath::table_path(tidb_kv::store_type::StoreType::TiKv, false, None),
                AccessPath::table_path(tidb_kv::store_type::StoreType::TiFlash, false, None),
                tidb_planner_util::path::AccessPath::index_path(source.table_indices()[0].clone()),
                tidb_planner_util::path::AccessPath::index_path(hypo),
            ]
        );
        assert_eq!(source.table_indices().len(), 1);
    }

    #[test]
    fn internal_session_does_not_consult_the_latest_domain_schema() {
        let source = table(vec![index("i1")]);
        let paths =
            enumerate_possible_access_paths(&source, false, None, None, false, false, &[], false).unwrap();
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn repeatable_read_for_update_uses_the_latest_schema() {
        let mut source = table(vec![index("removed")]);
        source.set_table_id(42);
        let latest = LatestIndexSchema {
            schema_meta_version: 8,
            table_indexes: BTreeMap::from([(42, Vec::new())]),
        };

        let paths =
            enumerate_possible_access_paths(&source, false, Some(&latest), Some(7), true, true, &[], false)
                .unwrap();
        assert_eq!(paths.len(), 1);
    }
}

#[cfg(test)]
mod index_range_collation_tests {
    use tidb_charset::Collation;
    use tidb_datatype::Datum;
    use tidb_ranger::types::Range;

    use tidb_planner_cardinality::row_count_estimator::ordinal_of_range_cond;

    #[test]
    fn index_range_prefix_comparison_retains_ranger_collation() {
        let collation = Collation::Utf8Mb4GeneralCi;
        let range = Range {
            collators: vec![collation],
            low_val: vec![Datum::new_collation_string("A", collation)],
            high_val: vec![Datum::new_collation_string("a", collation)],
            low_exclude: false,
            high_exclude: false,
        };
        assert_eq!(ordinal_of_range_cond(&range), 1);
        assert!(tidb_planner_core_operator_logicalop::data_source_paths::only_point_ranges(
            tidb_planner_core_operator_logicalop::data_source_paths::PointRangePath::Index { column_count: 1 },
            std::slice::from_ref(&range),
        ));
        let mut binary_range = range.clone();
        binary_range.collators[0] = Collation::Binary;
        assert_eq!(ordinal_of_range_cond(&binary_range), 0);
        assert!(!tidb_planner_core_operator_logicalop::data_source_paths::only_point_ranges(
            tidb_planner_core_operator_logicalop::data_source_paths::PointRangePath::Index { column_count: 1 },
            &[binary_range],
        ));
    }

    #[test]
    fn range_prefix_and_point_checks_use_their_source_collator_positions() {
        let range = Range {
            low_val: vec![Datum::Int(1), Datum::new_string("A")],
            high_val: vec![Datum::Int(1), Datum::new_string("a")],
            collators: vec![Collation::Binary, Collation::Utf8Mb4GeneralCi],
            low_exclude: false,
            high_exclude: false,
        };
        assert_eq!(ordinal_of_range_cond(&range), 1);
        assert!(tidb_planner_core_operator_logicalop::data_source_paths::only_point_ranges(
            tidb_planner_core_operator_logicalop::data_source_paths::PointRangePath::Index { column_count: 2 },
            &[range],
        ));
    }
}
