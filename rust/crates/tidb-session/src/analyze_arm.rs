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
//! [`crate::Session::dispatch_admin_stmt`].
//!
//! What an `ANALYZE` computes is [`tidb_executor::analyze`]'s, shared with the
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

use std::collections::HashSet;
use std::sync::Arc;

use tidb_executor::analyze::kv::{
    analyze_kv_table_columns, analyze_kv_table_independent_index, is_special_global_index,
};
use tidb_executor::analyze::panic_recovery::recover_analyze_panic;
use tidb_executor::analyze::{
    lower_analyze_admin, resolve_analyze_options, AnalyzeColumnChoice, AnalyzeStatement,
    PhysicalAnalyzeOptions, SampleMemoryQuota, SavedAnalyzeOptions, MEM_QUOTA_ANALYZE_VARIABLE,
};
use tidb_executor::{DriverError, SchemaErrorKind, TableEntry};

use crate::{Session, StmtOutput};

/// The source goroutine boundary at which a test injects its one-shot panic.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AnalyzePanicPhase {
    /// Go's analyze worker, before it computes statistics.
    Worker,
    /// Go's result handler, after statistics are computed but before publish.
    Result,
}

fn merge_partial_statistics(
    old: Option<Arc<tidb_executor::access_cost::TableStatistics>>,
    mut fresh: tidb_executor::access_cost::TableStatistics,
    partial: bool,
) -> tidb_executor::access_cost::TableStatistics {
    if !partial {
        return fresh;
    }
    let Some(old) = old else {
        return fresh;
    };
    let mut merged = (*old).clone();
    merged.row_count = fresh.row_count;
    merged.modify_count = fresh.modify_count;
    merged.version = fresh.version;
    merged.last_analyze_version = fresh.last_analyze_version;
    merged.columns.append(&mut fresh.columns);
    merged.indexes.append(&mut fresh.indexes);
    merged
        .column_load_status
        .append(&mut fresh.column_load_status);
    merged
        .index_load_status
        .append(&mut fresh.index_load_status);
    merged
        .column_stats_existence
        .append(&mut fresh.column_stats_existence);
    merged
        .index_stats_existence
        .append(&mut fresh.index_stats_existence);
    merged.pseudo = merged.row_count == 0
        || (merged.column_stats_existence.values().all(|exists| !exists)
            && merged.index_stats_existence.values().all(|exists| !exists));
    merged
}

fn merge_independent_index_statistics(
    old: Option<Arc<tidb_executor::access_cost::TableStatistics>>,
    fresh: tidb_executor::access_cost::TableStatistics,
) -> tidb_executor::access_cost::TableStatistics {
    let Some(old) = old else {
        return fresh;
    };
    let mut merged = (*old).clone();
    merged.version = fresh.version;
    merged.last_analyze_version = fresh.last_analyze_version;
    merged.indexes.extend(fresh.indexes);
    merged.index_load_status.extend(fresh.index_load_status);
    merged
        .index_stats_existence
        .extend(fresh.index_stats_existence);
    merged.pseudo = merged.row_count == 0
        || (merged.column_stats_existence.values().all(|exists| !exists)
            && merged.index_stats_existence.values().all(|exists| !exists));
    merged
}

struct AnalyzeIndexTasks {
    run_full_sampling: bool,
    independent_index_ids: Vec<i64>,
}

fn select_index_tasks(
    table: &tidb_executor::kv_table::KvTable,
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
                    .find(|index| index.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        DriverError::unsupported(format!(
                            "Index '{name}' in field list does not exist in table '{}'",
                            table.name
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
                        .find(|index| index.name.eq_ignore_ascii_case(name))
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
    table: &tidb_executor::kv_table::KvTable,
    requested: &[String],
) -> Result<Vec<i64>, DriverError> {
    let Some(partition) = table.partition() else {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        return Err(DriverError::unsupported(
            "Partition management on a not partitioned table is not possible".to_owned(),
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
            .find(|definition| definition.name.eq_ignore_ascii_case(requested_name))
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
    table: &tidb_executor::kv_table::KvTable,
    choice: &AnalyzeColumnChoice,
    default_choice: &AnalyzeColumnChoice,
    predicate_ids: &HashSet<(i64, i64)>,
    table_id: i64,
    schema: &str,
    table_name: &str,
    context: &tidb_executor::StmtContext,
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
                    .find(|column| column.name.eq_ignore_ascii_case(name))
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
                schema.to_lowercase(),
                table_name.to_lowercase()
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
                        .find(|base| base.name.eq_ignore_ascii_case(dependency))
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

fn saved_options(
    table: &tidb_executor::kv_table::KvTable,
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

#[cfg(test)]
std::thread_local! {
    /// A one-shot, thread-local equivalent of the two Go failpoints in
    /// `pkg/executor/test/analyzetest/panictest`.
    ///
    /// Keeping the injection local to the test thread makes the real
    /// `Session::run` path deterministic while other session tests execute in
    /// parallel.
    static ANALYZE_PANIC_FOR_TEST: std::cell::Cell<Option<(AnalyzePanicPhase, &'static str)>> =
        const { std::cell::Cell::new(None) };
}

/// Arms the in-process analyze path's one-shot source-shaped panic injection.
#[cfg(test)]
pub(crate) fn panic_next_analyze_for_test(phase: AnalyzePanicPhase, message: &'static str) {
    ANALYZE_PANIC_FOR_TEST.with(|pending| pending.set(Some((phase, message))));
}

#[cfg(test)]
fn inject_analyze_panic_for_test(phase: AnalyzePanicPhase) {
    ANALYZE_PANIC_FOR_TEST.with(|pending| {
        if let Some((pending_phase, message)) = pending.get() {
            if pending_phase == phase {
                pending.set(None);
                panic!("{message}");
            }
        }
    });
}

impl Session {
    /// Runs `ANALYZE TABLE t [, u ...]`, one table at a time.
    ///
    /// Returns `None` when the statement is not an `ANALYZE` this arm owns, so
    /// the caller falls through to the rest of the admin dispatch.
    pub(crate) fn analyze_stmt(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(tables) = lower_analyze_admin(admin, &self.current_db)
            .map_err(|error| DriverError::unsupported(error.to_string()))?
        else {
            return Ok(None);
        };
        let memory_quota = self.analyze_memory_quota();
        let persist_options = self
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PERSIST_ANALYZE_OPTIONS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("ON") || value == "1");
        let default_columns = if self
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_ANALYZE_COLUMN_OPTIONS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("PREDICATE"))
        {
            AnalyzeColumnChoice::Predicate
        } else {
            AnalyzeColumnChoice::All
        };
        for statement in &tables {
            let mut statement = statement.clone();
            statement.persist_options = persist_options;
            statement.default_columns = default_columns.clone();
            statement.options.memory_quota = memory_quota;
            self.analyze_one_table(&statement)?;
        }
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(Some(StmtOutput::Affected(0)))
    }

    /// Analyzes one named table and publishes its statistics.
    fn analyze_one_table(&mut self, statement: &AnalyzeStatement) -> Result<(), DriverError> {
        let schema = statement.schema.clone();
        let name = statement.table.clone();
        let ctx = self.statement_context(false);
        let usage_provider = self.column_stats_usage.clone();
        let session_time_zone = self.session_time_zone();
        let resource_group = self.active_resource_group.clone();
        let result = self.with_catalog_mut(|catalog| {
            let (table_id, partition_ids, table) = match catalog.table_in(&schema, &name) {
                Some(TableEntry::Kv(kv)) => {
                    let partition_ids = analyze_partition_ids(kv, &statement.partitions)?;
                    (kv.table_id, partition_ids, kv.clone())
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
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                        name.clone(),
                    )));
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
            let mut persisted = std::collections::HashMap::new();
            if statement.persist_options {
                for physical_id in std::iter::once(table_id).chain(partition_ids.iter().copied()) {
                    if let Some(options) = catalog.analyze_options(physical_id) {
                        persisted.insert(physical_id, options);
                    }
                }
            }
            let resolution = resolve_analyze_options(
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
            for options in &resolution.physical {
                selections.insert(
                    options.physical_id,
                    selected_column_ids(
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
                    )?,
                );
            }
            let realtime_count = |physical_id| {
                // Go's `getAdjustedSampleRate` reads the CURRENT
                // `mysql.stats_meta.count` of the physical table being
                // analyzed, which here is whatever its last analysis
                // published.
                catalog
                    .table_statistics(physical_id)
                    .map(|statistics| statistics.row_count)
            };
            let partition_counts = partition_ids
                .iter()
                .map(|physical_id| (*physical_id, realtime_count(*physical_id)))
                .collect::<Vec<_>>();
            let global_count = realtime_count(table_id);
            let execution: Result<(), DriverError> = recover_analyze_panic(|| {
                #[cfg(test)]
                inject_analyze_panic_for_test(AnalyzePanicPhase::Worker);

                if index_tasks.run_full_sampling {
                    if partition_ids.is_empty() {
                        let mut scan_table = table.clone();
                        let options = &resolution.physical[0];
                        let mut effective = options.effective;
                        effective.memory_quota = statement.options.memory_quota;
                        let selected = selections
                            .get(&table_id)
                            .expect("the logical table selection exists");
                        let statistics = analyze_kv_table_columns(
                            &mut scan_table,
                            &effective,
                            global_count,
                            &ctx,
                            selected.as_ref(),
                        )
                        .map_err(|error| DriverError::unsupported(error.to_string()))?;
                        #[cfg(test)]
                        inject_analyze_panic_for_test(AnalyzePanicPhase::Result);
                        let statistics = merge_partial_statistics(
                            catalog.table_statistics(table_id),
                            statistics,
                            selected.is_some(),
                        );
                        catalog.set_table_statistics(table_id, Arc::new(statistics));
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
                            let selected = selections
                                .get(&physical_id)
                                .expect("every requested partition has a selection");
                            let mut partition = table.clone();
                            partition.restrict_read_to_partitions(&[physical_id]);
                            let statistics = analyze_kv_table_columns(
                                &mut partition,
                                &effective,
                                realtime_count,
                                &ctx,
                                selected.as_ref(),
                            )
                            .map_err(|error| DriverError::unsupported(error.to_string()))?;
                            let statistics = merge_partial_statistics(
                                catalog.table_statistics(physical_id),
                                statistics,
                                selected.is_some(),
                            );
                            partition_statistics.push((physical_id, Arc::new(statistics)));
                        }

                        // Go's static pruning mode analyzes the physical partitions
                        // and deliberately does not merge a logical-table histogram.
                        // Dynamic pruning performs that merge; analyzing the same
                        // complete row set here gives its planner the same global
                        // distribution without inventing a second statistics store.
                        let global_statistics = if ctx.static_partition_prune() {
                            None
                        } else {
                            let mut global = table.clone();
                            let options = &resolution.physical[0];
                            let mut effective = options.effective;
                            effective.memory_quota = statement.options.memory_quota;
                            let selected = selections
                                .get(&table_id)
                                .expect("the logical table selection exists");
                            let statistics = analyze_kv_table_columns(
                                &mut global,
                                &effective,
                                global_count,
                                &ctx,
                                selected.as_ref(),
                            )
                            .map_err(|error| DriverError::unsupported(error.to_string()))?;
                            Some(Arc::new(merge_partial_statistics(
                                catalog.table_statistics(table_id),
                                statistics,
                                selected.is_some(),
                            )))
                        };
                        #[cfg(test)]
                        inject_analyze_panic_for_test(AnalyzePanicPhase::Result);
                        for (physical_id, statistics) in partition_statistics {
                            catalog.set_table_statistics(physical_id, statistics);
                        }
                        if let Some(statistics) = global_statistics {
                            catalog.set_table_statistics(table_id, statistics);
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
                    let mut index_table = table.clone();
                    let statistics =
                        analyze_kv_table_independent_index(&mut index_table, *index_id, &effective)
                            .map_err(|error| DriverError::unsupported(error.to_string()))?;
                    let statistics = merge_independent_index_statistics(
                        catalog.table_statistics(table_id),
                        statistics,
                    );
                    catalog.set_table_statistics(table_id, Arc::new(statistics));
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
                    catalog.set_analyze_options(
                        options.physical_id,
                        saved_options(&table, options, selected),
                    );
                }
            }
            Ok(())
        });
        self.drain_context_warnings(&ctx);
        result
    }

    /// Go's analyze memory quota, as one `ANALYZE` reads it.
    ///
    /// `tidb_mem_quota_analyze` is process-wide and read at execution time
    /// (`variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`,
    /// `pkg/executor/select.go:141`), so the value in force is whatever the
    /// last `SET GLOBAL` stored. Its default, `-1`, is no bound.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }
}
