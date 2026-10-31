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

//! `ANALYZE TABLE`: the `AdminStmt::AnalyzeTable` arm of
//! `crate::admin::AdminExec::admin_next`.
//!
//! What an `ANALYZE` computes is [`crate::analyze`]'s, shared with the
//! cluster tier; what it means for THIS session is here: which tables the
//! statement names, and where the result is published.
//!
//! # Why this used to be refused, and what changed
//!
//! Statistics are cluster state, so a session over cluster storage routes the
//! statement to a node that can write `mysql.stats_*`
//! (`Session::statement_stored_state_change` still says so, and
//! `tidb_server`'s convergence node still routes it there). An IN-PROCESS
//! session has no cluster to write to and no peer to tell -- its catalog IS
//! the whole world -- so the honest answer is not a refusal but the analysis
//! itself, published into the same catalog the planner reads from. Until it
//! was, every `EXPLAIN` in this tier printed `stats:pseudo` even for a table
//! the script had just analyzed, and every `ANALYZE` answered "not supported".
//!
//! # What is deliberately not here
//!
//! * **Privileges.** Go gates `ANALYZE` on INSERT *and* SELECT for each named
//!   table (`planbuilder.go`'s `requireInsertAndSelectPriv`), which the
//!   convergence node enforces. This tier applies no table privileges to
//!   ordinary reads either, so a check on this one statement would be the
//!   only one and would refuse scripts nothing else refuses.
//! * **Transaction interaction.** The analysis reads the catalog THIS
//!   statement sees, including its transaction's rows, while statistics are
//!   published through the process-wide catalog cache. This is Go's split:
//!   analyze workers read the statement snapshot and `SaveAnalyzeResultToStorage`
//!   uses a stats-handle internal session, so a user `ROLLBACK` cannot discard
//!   the published statistics.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::analyze::kv::{
    analyze_kv_table_columns, analyze_kv_table_independent_index, is_special_global_index,
};
use crate::analyze::panic_recovery::recover_analyze_panic;
use crate::analyze::{
    resolve_analyze_options, AnalyzeColumnChoice, AnalyzeStatement, PhysicalAnalyzeOptions,
    SampleMemoryQuota, SavedAnalyzeOptions, MEM_QUOTA_ANALYZE_VARIABLE,
};
use crate::analyze::ClusterAnalyzeReport;
use crate::{DriverError, TableEntry};

use tidb_hack::GoToLower;
use tidb_hack::go_to_lower;

/// Publish merged ANALYZE items as a real cache table. Go's storage loader
/// derives the table format from histogram rows; these local results bypass
/// that reload and must refresh the same metadata after replacing items.
fn finish_analyze_publication(statistics: &mut tidb_stats::Table) {
    let mut version = 0;
    statistics.hist_coll.for_each_column(|_, item| {
        version = version.max(item.stats_version);
        false
    });
    statistics.hist_coll.for_each_index(|_, item| {
        version = version.max(item.stats_version);
        false
    });
    if version != 0 {
        statistics.hist_coll.stats_version = i32::try_from(version).unwrap_or(i32::MAX);
    }
    statistics.hist_coll.pseudo = false;
}

fn merge_analyzed_statistics(
    old: Option<Arc<tidb_stats::Table>>,
    mut fresh: tidb_stats::Table,
    replace_counts: bool,
) -> tidb_stats::Table {
    let Some(old) = old else {
        finish_analyze_publication(&mut fresh);
        return fresh;
    };
    let mut merged = old.copy_as(tidb_stats::CopyIntent::BothMapsWritable);
    if replace_counts {
        merged.hist_coll.realtime_count = fresh.hist_coll.realtime_count;
        merged.hist_coll.modify_count = fresh.hist_coll.modify_count;
    }
    merged.version = fresh.version;
    merged.last_analyze_version = merged.last_analyze_version.max(fresh.last_analyze_version);
    merged.last_stats_hist_version = merged
        .last_stats_hist_version
        .max(fresh.last_stats_hist_version);
    merged.is_pk_handle = fresh.is_pk_handle;
    for (id, column) in analyzed_columns(&fresh) {
        merged.hist_coll.set_column(id, column);
        insert_existence(&merged, id, false);
    }
    for (id, index) in analyzed_indexes(&fresh) {
        merged.hist_coll.set_index(id, index);
        insert_existence(&merged, id, true);
    }
    finish_analyze_publication(&mut merged);
    merged
}

fn analyzed_columns(statistics: &tidb_stats::Table) -> Vec<(i64, tidb_stats::column::Column)> {
    let mut columns = Vec::new();
    statistics.hist_coll.for_each_column(|id, column| {
        columns.push((id, column.clone()));
        false
    });
    columns
}

fn analyzed_indexes(statistics: &tidb_stats::Table) -> Vec<(i64, tidb_stats::index::Index)> {
    let mut indexes = Vec::new();
    statistics.hist_coll.for_each_index(|id, index| {
        indexes.push((id, index.clone()));
        false
    });
    indexes
}

/// Records one freshly analyzed item in Go's `ColAndIdxExistenceMap`.
fn insert_existence(statistics: &tidb_stats::Table, id: i64, is_index: bool) {
    if let Some(map) = &statistics.existence_map {
        let mut map = map.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if is_index {
            map.insert_index(id, true);
        } else {
            map.insert_column(id, true);
        }
    }
}

fn merge_partial_statistics(
    old: Option<Arc<tidb_stats::Table>>,
    fresh: tidb_stats::Table,
    partial: bool,
) -> tidb_stats::Table {
    merge_analyzed_statistics(if partial { old } else { None }, fresh, true)
}

fn merge_independent_index_statistics(
    old: Option<Arc<tidb_stats::Table>>,
    fresh: tidb_stats::Table,
) -> tidb_stats::Table {
    // An independent index task updates histogram rows without changing
    // stats_meta row/modify counts, unlike a full column-sampling task.
    merge_analyzed_statistics(old, fresh, false)
}

/// Rebuilds dynamic-mode global statistics from the physical partition
/// statistics, the same source used by Go's `MergePartitionStats2GlobalStats`.
fn merge_partitioned_global_statistics(
    table: &tidb_table::kv_table::KvTable,
    partitions: &[(i64, Arc<tidb_stats::Table>)],
    previous: Option<Arc<tidb_stats::Table>>,
    options: &crate::analyze::AnalyzeOptions,
    time_zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_stats::Table, DriverError> {
    let row_count = partitions
        .iter()
        .fold(0_i64, |count, (_, stats)| {
            count.saturating_add(stats.hist_coll.realtime_count.max(0))
        });
    let modify_count = partitions
        .iter()
        .fold(0_i64, |count, (_, stats)| {
            count.saturating_add(stats.hist_coll.modify_count.max(0))
        });
    let mut global = previous.map_or_else(
        || tidb_stats::Table::from_analyzed_items(table.table_id(), 0, 0, [], []),
        |previous| previous.copy_as(tidb_stats::CopyIntent::BothMapsWritable),
    );
    global.hist_coll.realtime_count = row_count;
    global.hist_coll.modify_count = modify_count;
    global.version = partitions.iter().map(|(_, stats)| stats.version).max().unwrap_or(0);
    global.last_analyze_version = partitions
        .iter()
        .map(|(_, stats)| stats.last_analyze_version)
        .max()
        .unwrap_or(0);
    global.hist_coll.pseudo = false;
    let meta = table.meta();
    let meta = meta.read();

    let bucket_count = usize::try_from(options.num_buckets.max(1)).unwrap_or(256);
    let topn_count = u32::try_from(options.num_topn.max(0)).unwrap_or(0);
    let killer = tidb_sqlkiller::SqlKiller::default();

    for column in table.visible_columns() {
        let items = partitions
            .iter()
            .filter_map(|(_, stats)| {
                let column_stats = stats.hist_coll.get_column(column.id)?;
                let column_stats = column_stats
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                Some(column_stats)
            })
            .collect::<Vec<_>>();
        if items.len() != partitions.len() || items.is_empty() {
            continue;
        }
        let partition_items = items
            .iter()
            .map(|stats| tidb_stats_handle_globalstats::PartitionStatsItem {
                histogram: stats.histogram.clone(),
                cmsketch: stats.cmsketch.clone(),
                topn: stats.top_n.clone(),
                fm_sketch: stats.fm_sketch.clone(),
            })
            .collect();
        let source = &items[0];
        let merged = tidb_stats_handle_globalstats::merge_partition_stats_item(
            Some(time_zone),
            source.stats_version,
            topn_count,
            bucket_count,
            row_count,
            &column.field_type,
            false,
            tidb_stats_handle_globalstats::GlobalStatsMergeMode::Blocking,
            1,
            partition_items,
            &killer,
        )
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
        if let Some(mut histogram) = merged.histogram {
            if crate::analyze::kv::is_local_unique(table, false, column.id) {
                histogram.ndv = tidb_stats_handle_globalstats::unique_global_ndv(
                    items.iter().map(|stats| &stats.histogram),
                    row_count,
                );
            }
            histogram.round_trip_through_storage(&column.field_type);
            global.hist_coll.set_column(
                column.id,
                tidb_stats::column::Column {
                    histogram,
                    top_n: merged.topn,
                    cmsketch: merged.cmsketch,
                    stats_version: source.stats_version,
                    stats_loaded_status: source.stats_loaded_status,
                    info: meta.find_column_by_id(column.id),
                    physical_id: table.table_id(),
                    is_handle: source.is_handle,
                    ..Default::default()
                },
            );
            insert_existence(&global, column.id, false);
        }
    }

    for index in table.indexes() {
        let items = partitions
            .iter()
            .filter_map(|(_, stats)| {
                let index_stats = stats.hist_coll.get_index(index.id)?;
                let index_stats = index_stats
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                Some(index_stats)
            })
            .collect::<Vec<_>>();
        if items.len() != partitions.len() || items.is_empty() {
            continue;
        }
        let partition_items = items
            .iter()
            .map(|stats| tidb_stats_handle_globalstats::PartitionStatsItem {
                histogram: stats.histogram.clone(),
                cmsketch: stats.cmsketch.clone(),
                topn: stats.top_n.clone(),
                fm_sketch: stats.fm_sketch.clone(),
            })
            .collect();
        let source = &items[0];
        let field_type = index
            .column_offsets
            .first()
            .and_then(|offset| table.columns().get(*offset))
            .map(|column| &column.field_type)
            .ok_or_else(|| DriverError::unsupported("global index has no leading column"))?;
        let merged = tidb_stats_handle_globalstats::merge_partition_stats_item(
            Some(time_zone),
            source.stats_version,
            topn_count,
            bucket_count,
            row_count,
            field_type,
            true,
            tidb_stats_handle_globalstats::GlobalStatsMergeMode::Blocking,
            1,
            partition_items,
            &killer,
        )
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
        if let Some(mut histogram) = merged.histogram {
            if crate::analyze::kv::is_local_unique(table, true, index.id) {
                histogram.ndv = tidb_stats_handle_globalstats::unique_global_ndv(
                    items.iter().map(|stats| &stats.histogram),
                    row_count,
                );
            }
            histogram.round_trip_through_storage(&tidb_datatype::FieldType::new(
                tidb_datatype::FieldTypeCode::Blob,
            ));
            global.hist_coll.set_index(
                index.id,
                tidb_stats::index::Index {
                    histogram,
                    top_n: merged.topn,
                    cmsketch: merged.cmsketch,
                    stats_version: source.stats_version,
                    stats_loaded_status: source.stats_loaded_status,
                    info: meta.find_index_by_id(index.id),
                    physical_id: table.table_id(),
                    ..Default::default()
                },
            );
            insert_existence(&global, index.id, true);
        }
    }
    finish_analyze_publication(&mut global);
    Ok(global)
}

struct AnalyzeIndexTasks {
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
}

fn select_index_tasks(
    table: &tidb_table::kv_table::KvTable,
    statement: &AnalyzeStatement,
) -> Result<AnalyzeIndexTasks, DriverError> {
    let Some(names) = &statement.index_names else {
        return Ok(AnalyzeIndexTasks {
            run_full_sampling: true,
            independent_index_ids: if statement.partitions.is_empty() {
                table
                    .indexes()
                    .iter()
                    .filter(|index| is_special_global_index(table, index))
                    .map(|index| index.id)
                    .collect()
            } else {
                Vec::new()
            },
        });
    };
    let selected = if names.is_empty() {
        table.indexes().iter().collect::<Vec<_>>()
    } else {
        names
            .iter()
            .map(|name| {
                table
                    .indexes()
                    .iter()
                    .find(|index| go_to_lower(&index.name) == go_to_lower(name))
                    .ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "Index '{name}' in field list does not exist in table '{}'",
                            table.name()
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if selected
        .iter()
        .all(|index| is_special_global_index(table, index))
    {
        if let Some(index) = selected.iter().find(|_| !statement.partitions.is_empty()) {
            return Err(DriverError::unsupported(format!(
                "Analyze global index '{}' can't work with analyze specified partitions",
                index.name
            )));
        }
        // Pinned Go deliberately iterates the explicitly named list here.
        // Therefore `ANALYZE TABLE t INDEX` with no names and only special
        // global indexes creates no task.
        return Ok(AnalyzeIndexTasks {
            run_full_sampling: false,
            independent_index_ids: names
                .iter()
                .filter_map(|name| {
                    selected
                        .iter()
                        .find(|index| go_to_lower(&index.name) == go_to_lower(name))
                        .map(|index| index.id)
                })
                .collect(),
        });
    }
    Ok(AnalyzeIndexTasks {
        run_full_sampling: true,
        independent_index_ids: if statement.partitions.is_empty() {
            table
                .indexes()
                .iter()
                .filter(|index| is_special_global_index(table, index))
                .map(|index| index.id)
                .collect()
        } else {
            Vec::new()
        },
    })
}

fn analyze_partition_ids(
    table: &tidb_table::kv_table::KvTable,
    requested: &[String],
) -> Result<Vec<i64>, DriverError> {
    let Some(partition) = table.partition() else {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        return Err(DriverError::from(
            tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    if requested.is_empty() {
        return Ok(partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect());
    }
    let mut ids = Vec::with_capacity(requested.len());
    for requested_name in requested {
        let definition = partition
            .definitions
            .iter()
            .find(|definition| go_to_lower(&definition.name) == go_to_lower(requested_name))
            .ok_or_else(|| {
                DriverError::unsupported(format!(
                    "can not found the specified partition name {requested_name} in the table definition"
                ))
            })?;
        ids.push(definition.id);
    }
    Ok(ids)
}

fn effective_column_choice(
    choice: &AnalyzeColumnChoice,
    default: &AnalyzeColumnChoice,
) -> AnalyzeColumnChoice {
    if *choice == AnalyzeColumnChoice::Default {
        default.clone()
    } else {
        choice.clone()
    }
}

fn selected_column_ids(
    table: &tidb_table::kv_table::KvTable,
    choice: &AnalyzeColumnChoice,
    default_choice: &AnalyzeColumnChoice,
    predicate_ids: &HashSet<(i64, i64)>,
    table_id: i64,
    schema: &str,
    table_name: &str,
    context: &crate::StmtContext,
    predicate_warning_emitted: &mut bool,
    explicit_warning_emitted: &mut bool,
) -> Result<Option<HashSet<i64>>, DriverError> {
    let choice = effective_column_choice(choice, default_choice);
    let mut selected = match &choice {
        AnalyzeColumnChoice::All | AnalyzeColumnChoice::Default => return Ok(None),
        AnalyzeColumnChoice::Predicate => Some(
            predicate_ids
                .iter()
                .filter_map(|(usage_table, column)| (*usage_table == table_id).then_some(*column))
                .collect::<HashSet<_>>(),
        ),
        AnalyzeColumnChoice::Explicit(names) => {
            let mut ids = HashSet::new();
            for name in names {
                let column = table
                    .columns()
                    .iter()
                    .find(|column| go_to_lower(&column.name) == go_to_lower(name))
                    .ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "column `{name}` does not exist in `{schema}`.`{table_name}`"
                        ))
                    })?;
                ids.insert(column.id);
            }
            Some(ids)
        }
    };
    let selected_ids = selected.as_mut().expect("the all-columns case returned");
    let explicitly_selected = selected_ids.clone();
    if choice == AnalyzeColumnChoice::Predicate
        && selected_ids.is_empty()
        && !*predicate_warning_emitted
    {
        context.append_warning_parts(
            1105,
            &format!(
                "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                go_to_lower(schema),
                go_to_lower(table_name)
            ),
        );
        *predicate_warning_emitted = true;
    }
    for index in table.indexes() {
        for offset in &index.column_offsets {
            if let Some(column) = table.columns().get(*offset) {
                selected_ids.insert(column.id);
            }
        }
    }
    if let Some(offset) = table.pk_handle_offset() {
        if let Some(column) = table.columns().get(offset) {
            selected_ids.insert(column.id);
        }
    }
    loop {
        let before = selected_ids.len();
        for column in table.columns() {
            if !selected_ids.contains(&column.id) {
                continue;
            }
            if let Some(generated) = &column.generated {
                for dependency in &generated.dependencies {
                    if let Some(base) = table
                        .columns()
                        .iter()
                        .find(|base| go_to_lower(&base.name) == go_to_lower(dependency))
                    {
                        selected_ids.insert(base.id);
                    }
                }
            }
        }
        if selected_ids.len() == before {
            break;
        }
    }
    if matches!(choice, AnalyzeColumnChoice::Explicit(_))
        && !*explicit_warning_emitted
        && selected_ids != &explicitly_selected
    {
        let missing = table
            .columns()
            .iter()
            .filter(|column| {
                selected_ids.contains(&column.id) && !explicitly_selected.contains(&column.id)
            })
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        context.append_warning_parts(
            1105,
            &format!(
                "Columns {} are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
                missing.join(",")
            ),
        );
        *explicit_warning_emitted = true;
    }
    Ok(selected)
}

/// Go `filterSkipColumnTypes`: drop the columns whose type is in
/// `tidb_analyze_skip_column_types` (and every vector column) unless an index
/// or the integer handle needs their sample values, then the generated columns
/// that depend on a skipped one.
fn skip_column_types(
    table: &tidb_table::kv_table::KvTable,
    selected: Option<HashSet<i64>>,
    skip_types: &std::collections::BTreeSet<String>,
) -> Option<HashSet<i64>> {
    let columns = table.columns();
    let mut selected =
        selected.unwrap_or_else(|| columns.iter().map(|column| column.id).collect::<HashSet<_>>());
    // Go `getMustAnalyzedColumns`: public non-multi-valued index columns, the
    // integer handle, and what a virtual generated one of them is built from.
    let mut mandatory = HashSet::new();
    for index in table.indexes() {
        if table.is_mv_index(index) {
            continue;
        }
        for offset in &index.column_offsets {
            if let Some(column) = columns.get(*offset) {
                mandatory.insert(column.id);
            }
        }
    }
    let mut roots = mandatory.clone();
    if let Some(column) = table.pk_handle_offset().and_then(|offset| columns.get(offset)) {
        mandatory.insert(column.id);
    }
    loop {
        let before = roots.len();
        for column in columns {
            let Some(generated) = &column.generated else {
                continue;
            };
            if !roots.contains(&column.id) || generated.stored {
                continue;
            }
            for dependency in &generated.dependencies {
                if let Some(base) = columns
                    .iter()
                    .find(|base| go_to_lower(&base.name) == go_to_lower(dependency))
                {
                    roots.insert(base.id);
                    mandatory.insert(base.id);
                }
            }
        }
        if roots.len() == before {
            break;
        }
    }
    let mut skipped_names = HashSet::new();
    for column in columns {
        let vector = column.field_type.code() == tidb_datatype::FieldTypeCode::VectorFloat32;
        let skipped = skip_types.contains(tidb_datatype::type_to_str(
            column.field_type.code(),
            column.field_type.charset_name(),
        ));
        if vector || (skipped && !mandatory.contains(&column.id)) {
            selected.remove(&column.id);
            skipped_names.insert(column.name.go_to_lower());
        }
    }
    for column in columns {
        let Some(generated) = &column.generated else {
            continue;
        };
        if !selected.contains(&column.id) {
            continue;
        }
        // A stored generated column that must be analyzed is read from
        // storage, so its skipped dependencies do not matter.
        if generated.stored && mandatory.contains(&column.id) {
            continue;
        }
        if generated
            .dependencies
            .iter()
            .any(|dependency| skipped_names.contains(&dependency.go_to_lower()))
        {
            selected.remove(&column.id);
        }
    }
    (selected.len() != columns.len()).then_some(selected)
}

fn saved_options(
    table: &tidb_table::kv_table::KvTable,
    options: &PhysicalAnalyzeOptions,
    selected: Option<&HashSet<i64>>,
) -> SavedAnalyzeOptions {
    let columns = match &options.columns {
        AnalyzeColumnChoice::Explicit(_) => AnalyzeColumnChoice::Explicit(
            table
                .columns()
                .iter()
                .filter(|column| selected.is_some_and(|selected| selected.contains(&column.id)))
                .map(|column| column.name.clone())
                .collect(),
        ),
        choice => choice.clone(),
    };
    SavedAnalyzeOptions {
        raw: options.raw,
        columns,
    }
}

/// Go `getSavedAnalyzeOpts`'s row: `sample_num, sample_rate, buckets, topn,
/// column_choice, column_ids` of `mysql.analyze_options`, with a LIST's
/// column ids resolved against `table` (an id it no longer has is dropped).
fn saved_options_from_row(
    row: &[tidb_datatype::Datum],
    table: &tidb_table::kv_table::KvTable,
) -> SavedAnalyzeOptions {
    let text = |index: usize| row.get(index).and_then(tidb_datatype::datum_text::sysvar_datum_text).unwrap_or_default();
    let int = |index: usize| text(index).parse::<i64>().unwrap_or(0);
    let sample_num = int(0);
    let sample_rate = text(1).parse::<f64>().unwrap_or(0.0);
    let buckets = int(2);
    let topn = int(3);
    let raw = crate::analyze::AnalyzeOptionOverrides {
        num_buckets: (buckets > 0).then(|| buckets as isize),
        num_topn: (topn >= 0).then(|| topn as isize),
        num_samples: (sample_num > 0).then(|| sample_num as usize),
        sample_rate: (sample_rate > 0.0).then_some(sample_rate),
    };
    let columns = match text(4).as_str() {
        "ALL" => AnalyzeColumnChoice::All,
        "LIST" => AnalyzeColumnChoice::Explicit(
            text(5)
                .split(',')
                .filter_map(|id| id.parse::<i64>().ok())
                .filter_map(|id| table.columns().iter().find(|column| column.id == id))
                .map(|column| column.name.clone())
                .collect(),
        ),
        "PREDICATE" => AnalyzeColumnChoice::Predicate,
        _ => AnalyzeColumnChoice::Default,
    };
    SavedAnalyzeOptions { raw, columns }
}

/// Go `AnalyzeExec.saveAnalyzeOptions`' `REPLACE INTO mysql.analyze_options`
/// over `saved`, a LIST's column names stored as `table`'s column ids.
fn save_analyze_options_sql(
    saved: &[(i64, SavedAnalyzeOptions)],
    table: &tidb_table::kv_table::KvTable,
) -> String {
    let rows = saved
        .iter()
        .map(|(physical_id, options)| {
            let (choice, ids) = match &options.columns {
                AnalyzeColumnChoice::Default => ("DEFAULT", String::new()),
                AnalyzeColumnChoice::All => ("ALL", String::new()),
                AnalyzeColumnChoice::Predicate => ("PREDICATE", String::new()),
                AnalyzeColumnChoice::Explicit(names) => (
                    "LIST",
                    names
                        .iter()
                        .filter_map(|name| {
                            table
                                .columns()
                                .iter()
                                .find(|column| column.name.go_to_lower() == name.go_to_lower())
                                .map(|column| column.id.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            };
            format!(
                "({physical_id},{},{},{},{},'{choice}','{ids}')",
                options.raw.num_samples.map_or(0, |value| value as i64),
                tidb_goruntime::go_strconv::format_float(
                    options.raw.sample_rate.unwrap_or(0.0),
                    b'g',
                    -1,
                    64,
                ),
                options.raw.num_buckets.map_or(0, |value| value as i64),
                options.raw.num_topn.map_or(-1, |value| value as i64),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "REPLACE INTO mysql.analyze_options (table_id,sample_num,sample_rate,buckets,topn,column_choice,column_ids) VALUES {rows}"
    )
}

/// Go `AnalyzeExec` for the tables this session's catalog holds.
pub trait AnalyzeExec: crate::show::exec::ShowExec {
    /// Go `pdhelper.GlobalPDHelper`.
    fn approximate_table_counts(
        &self,
    ) -> std::sync::Arc<dyn tidb_pdhelper::ApproximateTableCountProvider>;
    /// Moves the statement context's warnings onto the session.
    fn drain_context_warnings(&mut self, ctx: &crate::StmtContext);

    /// Runs one `ANALYZE` through the session catalog when its resolved table
    /// is temporary.
    ///
    /// A cluster session normally delegates `ANALYZE` to the TiKV-backed
    /// executor. Temporary rows are the exception: LOCAL table metadata and
    /// both kinds' row storage live in this session's catalog overlay, so the
    /// shared cluster catalog cannot resolve or sample them. Go still builds
    /// an ordinary analyze task for these tables; this narrow entry point lets
    /// the cluster route preserve that behavior without creating a second
    /// temporary-table analyzer.
    fn analyze_temporary_table(
        &mut self,
        statement: &AnalyzeStatement,
    ) -> Result<Option<(i64, Arc<tidb_stats::Table>)>, DriverError> {
        let temporary = self.with_catalog_mut(|catalog| {
            Ok(
                match catalog.table_in(&statement.schema, &statement.table) {
                    Some(TableEntry::Kv(table)) if table.is_temporary() => Some(table.clone()),
                    _ => None,
                },
            )
        })?;
        let Some(table) = temporary else {
            return Ok(None);
        };
        let table_id = table.table_id();
        let statistics = self
            .analyze_one_table(statement)?
            .remove(&table_id)
            .ok_or_else(|| DriverError::unsupported("temporary ANALYZE produced no statistics"))?;
        Ok(Some((
            table_id,
            statistics,
        )))
    }

    /// Analyzes one named table and publishes its statistics.
    fn analyze_one_table(
        &mut self,
        statement: &AnalyzeStatement,
    ) -> Result<
        std::collections::HashMap<i64, Arc<tidb_stats::Table>>,
        DriverError,
    > {
        let stats_handle = self.stats_handle();
        let schema = statement.schema.clone();
        let name = statement.table.clone();
        let ctx = self.statement_context(false);
        let usage_provider = self.column_stats_usage_provider();
        let session_time_zone = self.session_time_zone();
        let resource_group = self.active_resource_group().to_owned();
        let approximate_table_counts = self.approximate_table_counts();
        // Go `getSavedAnalyzeOpts`, per physical table, by restricted SQL.
        let mut persisted = std::collections::HashMap::new();
        if statement.persist_options {
            let target = self.with_catalog_mut(|catalog| {
                Ok(match catalog.table_in(&schema, &name) {
                    Some(TableEntry::Kv(kv)) => Some((
                        kv.clone(),
                        analyze_partition_ids(kv, &statement.partitions)?,
                    )),
                    _ => None,
                })
            })?;
            if let Some((table, partition_ids)) = target {
                for physical_id in
                    std::iter::once(table.table_id()).chain(partition_ids.iter().copied())
                {
                    let rows = self.exec_restricted_sql(&format!(
                        "select sample_num,sample_rate,buckets,topn,column_choice,column_ids \
                         from mysql.analyze_options where table_id = {physical_id}"
                    ))?;
                    if let Some(row) = rows.first() {
                        persisted.insert(physical_id, saved_options_from_row(row, &table));
                    }
                }
            }
        }
        let mut to_save = Vec::new();
        let mut saved_table = None;
        let result = self.with_catalog_mut(|catalog| {
            let (table_id, partition_ids, table) = match catalog.table_in(&schema, &name) {
                Some(TableEntry::Kv(kv)) => {
                    let partition_ids = analyze_partition_ids(kv, &statement.partitions)?;
                    (kv.table_id(), partition_ids, kv.clone())
                }
                // Go raises 1146 for a name that is not a table, and
                // `ErrAnalyzeMissColumn`-adjacent refusals for a view or a
                // sequence; both are "there is nothing here to analyze", and
                // naming the object is what a caller can act on.
                Some(_) => {
                    return Err(DriverError::unsupported(format!(
                        "`{schema}`.`{name}` is not a table whose rows this node can analyze"
                    )));
                }
                None => {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .generate_with_stack_by_args(&[
                                schema.clone().into(),
                                name.clone().into(),
                            ]),
                    ));
                }
            };
            let index_tasks = select_index_tasks(&table, statement)?;
            if statement.index_names.is_some() && index_tasks.run_full_sampling {
                ctx.append_warning_parts(
                    1105,
                    "The version 2 would collect all statistics not only the selected indexes",
                );
            }
            if matches!(statement.columns, AnalyzeColumnChoice::Explicit(_)) {
                let mut suppress_predicate_warning = true;
                let mut explicit_warning_emitted = false;
                selected_column_ids(
                    &table,
                    &statement.columns,
                    &AnalyzeColumnChoice::All,
                    &HashSet::new(),
                    table_id,
                    &schema,
                    &name,
                    &ctx,
                    &mut suppress_predicate_warning,
                    &mut explicit_warning_emitted,
                )?;
            }
            let mut resolution = resolve_analyze_options(
                table_id,
                &partition_ids,
                statement.raw_options,
                &statement.columns,
                &persisted,
                !statement.persist_options || statement.partitions.is_empty(),
                !ctx.static_partition_prune(),
            );
            if resolution.ignored_partition_overrides {
                ctx.append_warning_parts(
                    1105,
                    "Ignore columns and options when analyze partition in dynamic mode",
                );
            }
            let needs_predicate = resolution.physical.iter().any(|options| {
                effective_column_choice(&options.columns, &statement.default_columns)
                    == AnalyzeColumnChoice::Predicate
            });
            let predicate_ids = if needs_predicate {
                match &usage_provider {
                    Some(provider) => provider
                        .load_column_stats_usage(&session_time_zone, &resource_group)
                        .map_err(DriverError::unsupported)?
                        .keys()
                        .filter(|item| item.table_id != 0 && !item.is_index)
                        .map(|item| (item.table_id, item.id))
                        .collect::<HashSet<_>>(),
                    None => HashSet::new(),
                }
            } else {
                HashSet::new()
            };
            let mut predicate_warning_emitted = false;
            let mut explicit_warning_emitted = true;
            let mut selections = std::collections::HashMap::new();
            let mut executed = std::collections::HashMap::new();
            for options in &resolution.physical {
                let selected = selected_column_ids(
                    &table,
                    &options.columns,
                    &statement.default_columns,
                    &predicate_ids,
                    table_id,
                    &schema,
                    &name,
                    &ctx,
                    &mut predicate_warning_emitted,
                    &mut explicit_warning_emitted,
                )?;
                // Go persists the options from `genV2AnalyzeOptions` and only
                // then applies `filterSkipColumnTypes` to what executes.
                executed.insert(
                    options.physical_id,
                    skip_column_types(&table, selected.clone(), &statement.skip_column_types),
                );
                selections.insert(options.physical_id, selected);
            }
            let realtime_count = |physical_id| {
                // Go's `getAdjustedSampleRate` reads the CURRENT
                // `mysql.stats_meta.count` of the physical table being
                // analyzed, which here is whatever its last analysis
                // published.
                catalog
                    .table_statistics_in(&stats_handle, physical_id)
                    .map(|statistics| statistics.hist_coll.realtime_count)
                    // Go GetPhysicalTableStats supplies a PseudoTable on a
                    // cache miss; it is not the unavailable-stats branch.
                    .or(Some(tidb_stats::PSEUDO_ROW_COUNT))
            };
            let partition_counts = partition_ids
                .iter()
                .map(|physical_id| (*physical_id, realtime_count(*physical_id)))
                .collect::<Vec<_>>();
            let global_count = realtime_count(table_id);
            // Go `buildAnalyzeSamplingPushdown`: a column task with neither
            // SAMPLES nor SAMPLERATE samples at `getAdjustedSampleRate`'s
            // rate, and its reason is a note. The tasks are the partitions of
            // a partitioned table, the table itself otherwise.
            for options in resolution
                .physical
                .iter_mut()
                .filter(|_| index_tasks.run_full_sampling)
            {
                if options.is_partition == partition_ids.is_empty()
                    || options.effective.num_samples != 0
                    || options.effective.sample_rate.is_some()
                {
                    continue;
                }
                let partition_name = table
                    .partition()
                    .and_then(|partition| {
                        partition
                            .definitions
                            .iter()
                            .find(|definition| definition.id == options.physical_id)
                    })
                    .map_or("", |definition| definition.name.as_str());
                // Go `getAdjustedSampleRate`'s
                // `GlobalPDHelper.GetApproximateTableCountFromStorage`.
                let (approximate, has_pd) = approximate_table_counts.approximate_table_count(
                    &resource_group,
                    options.physical_id,
                    &schema,
                    &name,
                    partition_name,
                );
                let approximate = has_pd.then_some(approximate);
                let realtime = realtime_count(options.physical_id);
                let rate =
                    tidb_stats::row_sample_collector::adjusted_sample_rate(realtime, approximate);
                let reason =
                    crate::analyze::sample_rate_reason(realtime, approximate, rate);
                options.effective.sample_rate = Some(rate);
                let target = if partition_name.is_empty() {
                    format!("{schema}.{name}")
                } else {
                    format!("{schema}.{name}'s partition {partition_name}")
                };
                ctx.append_note_parts(
                    1105,
                    &format!(
                        "Analyze use auto adjusted sample rate {rate:.6} for table {target}, \
                         reason to use this rate is \"{reason}\""
                    ),
                );
            }
            let mut analyzed = std::collections::HashMap::new();
            let execution: Result<(), DriverError> = recover_analyze_panic(|| {
                if index_tasks.run_full_sampling {
                    if partition_ids.is_empty() {
                        let mut scan_table = (*table).clone();
                        let options = &resolution.physical[0];
                        let mut effective = options.effective;
                        effective.memory_quota = statement.options.memory_quota;
                        let selected = executed
                            .get(&table_id)
                            .expect("the logical table selection exists");
                        let statistics = analyze_kv_table_columns(
                            &mut scan_table,
                            &effective,
                            global_count,
                            &ctx,
                            selected.as_ref(),
                        )
                        .map_err(DriverError::from)?;
                        let statistics = merge_partial_statistics(
                            catalog.table_statistics_in(&stats_handle, table_id),
                            statistics,
                            selected.is_some(),
                        );
                        analyzed.insert(table_id, Arc::new(statistics));
                    } else {
                        let mut partition_statistics = Vec::with_capacity(partition_counts.len());
                        for (physical_id, realtime_count) in partition_counts {
                            let options = resolution
                                .physical
                                .iter()
                                .find(|options| options.physical_id == physical_id)
                                .expect("every requested partition has options");
                            let mut effective = options.effective;
                            effective.memory_quota = statement.options.memory_quota;
                            let selected = executed
                                .get(&physical_id)
                                .expect("every requested partition has a selection");
                            let mut partition = (*table).clone();
                            partition.restrict_read_to_partitions(&[physical_id]);
                            let statistics = analyze_kv_table_columns(
                                &mut partition,
                                &effective,
                                realtime_count,
                                &ctx,
                                selected.as_ref(),
                            )
                            .map_err(DriverError::from)?;
                            let statistics = merge_partial_statistics(
                                catalog.table_statistics_in(&stats_handle, physical_id),
                                statistics,
                                selected.is_some(),
                            );
                            partition_statistics.push((physical_id, Arc::new(statistics)));
                        }

                        // Static pruning analyzes physical partitions only. Dynamic
                        // pruning merges those partition statistics into the logical
                        // table, including untouched partitions during a partial
                        // `ANALYZE TABLE ... PARTITION`.
                        let global_statistics = if ctx.static_partition_prune() {
                            None
                        } else {
                            let definitions = &table
                                .partition()
                                .expect("partition statistics require partition metadata")
                                .definitions;
                            let all_partition_statistics = definitions
                                .iter()
                                .filter_map(|definition| {
                                    partition_statistics
                                        .iter()
                                        .find(|(id, _)| *id == definition.id)
                                        .map(|(_, stats)| (definition.id, Arc::clone(stats)))
                                        .or_else(|| {
                                            catalog
                                                .table_statistics_in(&stats_handle, definition.id)
                                                .map(|stats| (definition.id, stats))
                                        })
                                })
                                .collect::<Vec<_>>();
                            if all_partition_statistics.len() != definitions.len() {
                                catalog.table_statistics_in(&stats_handle, table_id)
                            } else {
                                let options = &resolution.physical[0].effective;
                                Some(Arc::new(merge_partitioned_global_statistics(
                                    &table,
                                    &all_partition_statistics,
                                    catalog.table_statistics_in(&stats_handle, table_id),
                                    options,
                                    &session_time_zone,
                                )?))
                            }
                        };
                        for (physical_id, statistics) in partition_statistics {
                            analyzed.insert(physical_id, statistics);
                        }
                        if let Some(statistics) = global_statistics {
                            analyzed.insert(table_id, statistics);
                        }
                    }
                }

                let independent_options = resolution
                    .physical
                    .first()
                    .expect("an ANALYZE plan has physical options");
                let mut effective = independent_options.effective;
                effective.memory_quota = statement.options.memory_quota;
                for index_id in &index_tasks.independent_index_ids {
                    let mut index_table = (*table).clone();
                    let statistics =
                        analyze_kv_table_independent_index(&mut index_table, *index_id, &effective)
                            .map_err(DriverError::from)?;
                    let statistics = merge_independent_index_statistics(
                        analyzed
                            .get(&table_id)
                            .cloned()
                            .or_else(|| catalog.table_statistics_in(&stats_handle, table_id)),
                        statistics,
                    );
                    analyzed.insert(table_id, Arc::new(statistics));
                }
                Ok(())
            })
            .map_err(|error| DriverError::unsupported(error.rendered_message().to_owned()))?;
            execution?;
            if statement.persist_options {
                for options in &resolution.physical {
                    if options.is_partition && !ctx.static_partition_prune() {
                        continue;
                    }
                    let selected = selections
                        .get(&options.physical_id)
                        .and_then(Option::as_ref);
                    to_save.push((options.physical_id, saved_options(&table, options, selected)));
                }
                saved_table = Some(table.clone());
            }
            Ok(analyzed)
        });
        self.drain_context_warnings(&ctx);
        let analyzed = result?;
        // Go `saveAnalyzeOptions`, by restricted SQL after the analysis.
        if let (false, Some(table)) = (to_save.is_empty(), saved_table) {
            self.exec_restricted_sql(&save_analyze_options_sql(&to_save, &table))?;
        }
        Ok(analyzed)
    }

    /// Go's analyze memory quota, as one `ANALYZE` reads it.
    ///
    /// `tidb_mem_quota_analyze` is process-wide and read at execution time
    /// (`variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`,
    /// `pkg/executor/select.go:141`), so the value in force is whatever the
    /// last `SET GLOBAL` stored. Its default, `-1`, is no bound.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.session_vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }

    /// Go `AnalyzeExec.Next`'s `FLUSH STATS_DELTA` of this session's own
    /// pending deltas before sampling.
    fn publish_table_delta(&self);
    /// Starts the statement's own warnings.
    fn begin_routed_statement_warnings(&mut self);
    /// Appends one warning of a routed statement.
    fn append_routed_warning(&mut self, code: u16, message: String);
    /// Appends one note of a routed statement.
    fn append_routed_note(&mut self, code: u16, message: String);
    /// The session's catalog, read even after a panic poisoned its lock.
    fn catalog_ignoring_poison(&self) -> std::sync::MutexGuard<'_, crate::Catalog>;

    /// Go `AnalyzeExec.Next`: runs `ANALYZE TABLE` over `tables` through
    /// `service`, after `Compile` checked their privileges. The user's
    /// transaction is left alone, as Go's executor leaves it.
    fn run_analyze_statements(
        &mut self,
        tables: &[AnalyzeStatement],
        service: &dyn AnalyzeService,
    ) -> Result<(), DriverError> {
        // `Compile` already checked every named table (`buildAnalyze`'s visit
        // infos), so `ANALYZE TABLE ok, no` stores nothing at all.
        // Go `AnalyzeExec.Next` broadcasts `FLUSH STATS_DELTA` before
        // sampling; this session's pending deltas are published first.
        self.publish_table_delta();
        let column_task_tables = tables
            .iter()
            .filter(|statement| service.builds_column_task(statement))
            .cloned()
            .collect::<Vec<_>>();
        let target_ids = self.analyze_stats_delta_target_ids(&column_task_tables);
        if !target_ids.is_empty() {
            service.flush_stats_delta(&target_ids)?;
        }
        let memory_quota = self.analyze_memory_quota();
        self.begin_routed_statement_warnings();
        let mut successful_table_ids = BTreeSet::new();
        let mut cache_update_physical_ids: Option<Vec<i64>> = None;
        for statement in tables {
            let mut statement = self.analyze_statement_with_session_options(statement);
            statement.options.memory_quota = memory_quota;
            let statement = &statement;
            // Go resolves and samples temporary tables through the session's
            // temporary-table overlay, which the shared cluster analyzer
            // cannot see; the session analyzer runs them.
            if let Some((table_id, statistics)) = self.analyze_temporary_table(statement)? {
                service.publish_temporary_table_stats(table_id, statistics);
                continue;
            }
            let analyzed = service.execute(statement, self.historical_stats_enabled());
            self.drain_context_warnings(&statement.eval_context);
            let report = analyzed?;
            self.append_analyze_report_warnings(statement, &report);
            successful_table_ids.extend(report.historical_stats_table_ids());
            cache_update_physical_ids
                .get_or_insert_with(Vec::new)
                .extend_from_slice(report.cache_update_physical_ids());
        }
        // Go collects `results.TableID.GetStatisticsID()` only from successful
        // results and enqueues that set after every save worker has finished.
        if self.historical_stats_enabled() {
            for table_id in successful_table_ids {
                service.dump_historical_stats(table_id);
            }
        }
        // Go ends `AnalyzeExec.Next` with `return statsHandle.Update(...)`, so
        // a failed cache update fails the statement although every table's
        // statistics are already stored. Temporary tables never reach it.
        if let Some(physical_ids) = cache_update_physical_ids {
            service.update_stats_cache(&physical_ids)?;
        }
        Ok(())
    }

    /// Go `collectStatsDeltaFlushObjectsForAnalyze`: one table-scope object
    /// per table with a column task, whatever partitions it names, resolved
    /// as `FLUSH STATS_DELTA` resolves it (the table and every partition).
    fn analyze_stats_delta_target_ids(&self, tables: &[AnalyzeStatement]) -> Vec<i64> {
        let catalog = self.catalog_ignoring_poison();
        let mut seen = BTreeSet::new();
        let mut ids = Vec::new();
        for statement in tables {
            if !seen.insert((
                go_to_lower(&statement.schema),
                go_to_lower(&statement.table),
            )) {
                continue;
            }
            let Some(crate::TableEntry::Kv(table)) =
                catalog.table_in(&statement.schema, &statement.table)
            else {
                continue;
            };
            ids.push(table.table_id());
            if let Some(partition) = table.partition() {
                ids.extend(partition.definitions.iter().map(|definition| definition.id));
            }
        }
        ids
    }

    fn historical_stats_enabled(&self) -> bool {
        self.session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

    /// The statement with the options Go reads from session variables when
    /// it builds the analyze plan.
    fn analyze_statement_with_session_options(
        &self,
        statement: &AnalyzeStatement,
    ) -> AnalyzeStatement {
        let mut statement = statement.clone();
        let on = |name: &str| {
            self.session_vars()
                .get_system(name)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        };
        statement.persist_options = on(tidb_vardef::tidb_vars::TIDB_PERSIST_ANALYZE_OPTIONS);
        statement.default_columns = if self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("PREDICATE"))
        {
            crate::analyze::AnalyzeColumnChoice::Predicate
        } else {
            crate::analyze::AnalyzeColumnChoice::All
        };
        statement.skip_column_types = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
            .map(|value| tidb_variable::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        statement.dynamic_partition_prune = !self
            .session_vars()
            .get_system("tidb_partition_prune_mode")
            .is_ok_and(|value| value.eq_ignore_ascii_case("static"));
        statement.skip_missing_partition_stats =
            on(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS);
        statement.analyze_snapshot = on(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT);
        statement.enable_async_merge_global_stats =
            on(tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS);
        statement.partition_merge_concurrency = self
            .session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        statement.scan_concurrency = self
            .session_vars()
            .get_system(analyze_scan_concurrency_variable(statement.auto_analyze))
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY);
        statement.store_batch_size = self.session_vars().analyze_store_batch_size();
        statement.eval_context = self.analyze_statement_context().into();
        statement
    }

    /// The warnings and notes Go's planner and analyze executor raise for
    /// one finished table, in Go's order.
    fn append_analyze_report_warnings(
        &mut self,
        statement: &AnalyzeStatement,
        report: &ClusterAnalyzeReport,
    ) {
        // Go's planner raises these while it builds the plan, before any
        // execution diagnostic.
        for (_, code, warning) in report.build_diagnostics.iter().filter(|(note, ..)| !note) {
            self.append_routed_warning(*code, warning.clone());
        }
        if report.predicate_columns_empty {
            self.append_routed_warning(
                1105,
                format!(
                    "No predicate column has been collected yet for table {}.{}, so only indexes and the columns composing the indexes will be analyzed",
                    go_to_lower(&statement.schema),
                    go_to_lower(&statement.table)
                ),
            );
        }
        if report.ignored_partition_overrides {
            self.append_routed_warning(
                1105,
                "Ignore columns and options when analyze partition in dynamic mode".to_owned(),
            );
        }
        if report.collected_all_for_index_target {
            self.append_routed_warning(
                1105,
                "The version 2 would collect all statistics not only the selected indexes"
                    .to_owned(),
            );
        }
        for index in &report.skipped_columnar_indexes {
            self.append_routed_warning(
                1105,
                format!("analyzing columnar index is not supported, skip {index}"),
            );
        }
        if let Some(warning) = &report.option_save_warning {
            self.append_routed_warning(1105, warning.clone());
        }
        for (code, warning) in &report.global_stats_warnings {
            self.append_routed_warning(*code, warning.clone());
        }
        // Go `builder.go:3185`/`3194`: the sample-rate notice is a NOTE.
        for (_, code, note) in report.build_diagnostics.iter().filter(|(note, ..)| *note) {
            self.append_routed_note(*code, note.clone());
        }
    }
}

/// Samples and persists the statistics an `ANALYZE` builds.
pub trait AnalyzeService {
    /// Go `AnalyzeExec.Next`'s `FLUSH STATS_DELTA` before sampling, over the
    /// physical ids the session resolved (never empty: empty would mean
    /// every table).
    fn flush_stats_delta(&self, target_ids: &[i64]) -> Result<(), DriverError>;
    /// Whether `statement` builds a column task, the tables Go flushes
    /// deltas for before sampling (`collectStatsDeltaFlushObjectsForAnalyze`).
    fn builds_column_task(&self, statement: &AnalyzeStatement) -> bool;
    /// Runs and persists one table's analysis.
    fn execute(
        &self,
        statement: &AnalyzeStatement,
        historical_stats_enabled: bool,
    ) -> Result<ClusterAnalyzeReport, DriverError>;
    /// Go `AnalyzeExec.Next`'s closing `statsHandle.Update(ctx, infoSchema,
    /// tableAndPartitionIDs...)` over every analyzed table's physical ids;
    /// its failure is the statement's error.
    fn update_stats_cache(&self, physical_ids: &[i64]) -> Result<(), DriverError>;
    /// Publishes a temporary table's statistics, which the session analyzed
    /// itself (its rows live in the session's overlay).
    fn publish_temporary_table_stats(&self, table_id: i64, statistics: Arc<tidb_stats::Table>);
    /// Queues one successful table for the historical-stats dump.
    fn dump_historical_stats(&self, table_id: i64);
}

/// The scan-concurrency variable an analyze reads: an auto-analyze uses the
/// system-process one.
#[must_use]
pub fn analyze_scan_concurrency_variable(auto_analyze: bool) -> &'static str {
    if auto_analyze {
        tidb_vardef::tidb_vars::TIDB_SYS_PROC_SCAN_CONCURRENCY
    } else {
        tidb_vardef::tidb_vars::TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY
    }
}
