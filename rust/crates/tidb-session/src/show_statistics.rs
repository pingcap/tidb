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

//! SHOW statistics and analyze-status result sets.

use super::*;

use tidb_hack::GoCaseCompare;

impl Session {
    /// Pinned Go `ShowExec.fetchShowColumnStatsUsage`: load the persisted map
    /// once, then walk every schema, table, logical/global table ID, physical
    /// partition ID, and column in infoschema order.
    pub(super) fn column_stats_usage_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let usage = match self.column_stats_usage.clone() {
            Some(provider) => provider
                .load_column_stats_usage(&self.session_time_zone(), &self.active_resource_group)
                .map_err(DriverError::unsupported)?,
            None => std::collections::HashMap::new(),
        };
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let Some(names) = catalog.table_names(&database) else {
                    continue;
                };
                for name in names {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let mut targets = vec![(
                        table.table_id,
                        tidb_executor::show_stats::column_stats_usage_label(
                            table.partition().is_some(),
                            None,
                        ),
                    )];
                    if let Some(partition) = table.partition() {
                        targets.extend(partition.definitions.iter().map(|definition| {
                            (
                                definition.id,
                                tidb_executor::show_stats::column_stats_usage_label(
                                    false,
                                    Some(&definition.name),
                                ),
                            )
                        }));
                    }
                    for (physical_id, partition) in targets {
                        for column in table.columns() {
                            let item = tidb_model::TableItemID {
                                table_id: physical_id,
                                id: column.id,
                                is_index: false,
                                is_sync_load_failed: false,
                            };
                            let Some((last_used_at, last_analyzed_at)) = usage.get(&item) else {
                                continue;
                            };
                            rows.push(tidb_executor::show_stats::column_stats_usage_row(
                                &database,
                                &name,
                                &partition,
                                &column.name,
                                *last_used_at,
                                *last_analyzed_at,
                            ));
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `dataForAnalyzeStatusHelper`, which both `SHOW ANALYZE STATUS` and
    /// `information_schema.ANALYZE_STATUS` answer with: the recorded analyze
    /// jobs the session may see, with a running job's progress estimate.
    pub(crate) fn analyze_status_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let provider = self.analyze_status.clone();
        let resource_group = self.active_resource_group.clone();
        let visibility = self.schema_visibility();
        let zone = self.session_time_zone();
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        self.with_catalog_mut(|catalog| {
            analyze_status_rows_of(
                provider.as_deref(),
                &resource_group,
                &visibility,
                &zone,
                &stats_handle,
                catalog,
            )
        })
    }

    /// Go `ShowExec.fetchShowStatsMeta` (`pkg/executor/show_stats.go:36`):
    /// one row per table or partition whose statistics this session has
    /// loaded, with the two TSOs of the stored `mysql.stats_meta` row
    /// rendered as DATETIMEs.
    ///
    /// Go walks the infoschema and asks the stats handle for each physical ID;
    /// the walk here is the catalog and the handle is
    /// [`Catalog::table_statistics`], where `None` is exactly Go's
    /// `GetNonPseudoPhysicalTableStats` miss: no `mysql.stats_meta` row was
    /// ever loaded for that physical table. A LOADED row is shown even when
    /// it carries no histograms -- Go shows an un-analyzed table too, with a
    /// NULL `Last_analyze_time`, which is the branch
    /// `appendTableForStatsMeta` makes on `IsAnalyzed()`.
    pub(super) fn stats_meta_rows(
        &mut self,
        extractor: Option<&tidb_planner::logical::ShowPredicateExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        // Go renders through `oracle.GetTimeFromTS`, which builds the
        // `time.Time` in the process's LOCAL zone -- not the session zone --
        // so both servers on one host render identical strings.
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                // Go `fetchShowStatsMeta` walks `SchemaSimpleTableInfos`, whose
                // rows come out in TABLE-ID (creation) order; an alphabetical
                // walk reorders `e` ahead of `t` where Go keeps `t` first.
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            tidb_executor::TableEntry::Kv(table) => (table.table_id, name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions: Vec<(i64, String)> = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if let Some(row) = tidb_executor::show_stats::table_statistics_meta_row(
                            &database,
                            &name,
                            &target.label,
                            &statistics,
                            &chrono::Local,
                        ) {
                            rows.push(row);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows
            .into_iter()
            .filter(|row| stats_row_admitted(extractor, row))
            .collect())
    }

    /// Go `ShowExec.fetchShowStatsLocked`: enumerate every physical table in
    /// the current prune mode, intersect it with the persisted lock rows, and
    /// emit in ascending physical-ID order.
    pub(super) fn stats_locked_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let context = self.statement_context(false);
        let rows = {
            let catalog = self
                .catalog
                .lock()
                .map_err(|_| DriverError::CatalogPoisoned)?;
            let locked =
                tidb_executor::stats_lock::query_catalog_locked_tables(&catalog, &context)?;
            let mut physical = std::collections::BTreeMap::new();
            for database in catalog.database_names() {
                let Some(names) = catalog.table_names(&database) else {
                    continue;
                };
                for name in names {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        physical.insert(
                            target.physical_id,
                            (database.clone(), name.clone(), target.label),
                        );
                    }
                }
            }
            physical
                .into_iter()
                .filter(|(physical_id, _)| locked.contains(physical_id))
                .map(|(_, (database, table, partition))| {
                    tidb_executor::show_stats::stats_locked_row(&database, &table, &partition)
                })
                .collect()
        };
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsHealthy`: one health percentage for every
    /// non-pseudo physical statistics object visible under this prune mode.
    pub(super) fn stats_healthy_rows(
        &mut self,
        extractor: Option<&tidb_planner::logical::ShowPredicateExtractor>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            tidb_executor::TableEntry::Kv(table) => (table.table_id, name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if let Some(row) = tidb_executor::show_stats::healthy_row(
                            &database,
                            &name,
                            &target.label,
                            &statistics,
                        ) {
                            rows.push(row);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows
            .into_iter()
            .filter(|row| stats_row_admitted(extractor, row))
            .collect())
    }

    /// Go `ShowExec.fetchShowStatsHistogram`: expose every initialized
    /// resident column and index statistics object through the ordinary
    /// statistics cache traversal.
    pub(super) fn stats_histograms_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = chrono::Local;
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            tidb_executor::TableEntry::Kv(table) => (table.table_id, name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.is_synthetic_pseudo() {
                            continue;
                        }
                        for (offset, column) in table.columns().iter().enumerate() {
                            let Some(column_stats) = statistics.columns.get(&column.id) else {
                                continue;
                            };
                            let Some(status) = statistics.column_load_status.get(&column.id) else {
                                continue;
                            };
                            if !status.stats_initialized() {
                                continue;
                            }
                            let row_size =
                                tidb_planner::cardinality::row_size::RowSizeColumnStats::new(
                                    tidb_planner::cardinality::row_size::RowSizeType::from_field_type_code(
                                        column.field_type.code(),
                                    ),
                                    column_stats.histogram.tot_col_size,
                                    column_stats.histogram.null_count,
                                    column_stats.total_row_count(),
                                    table.pk_handle_offset() == Some(offset),
                                );
                            let avg_col_size =
                                tidb_planner::cardinality::row_size::avg_col_size(
                                    &row_size,
                                    statistics.row_count,
                                    false,
                                );
                            rows.push(tidb_executor::show_stats::histogram_row(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                false,
                                &column_stats.histogram,
                                avg_col_size,
                                status.status_to_string(),
                                column_stats.memory_usage().into(),
                                &zone,
                            ));
                        }
                        for index in table.indexes() {
                            let Some(index_stats) = statistics.indexes.get(&index.id) else {
                                continue;
                            };
                            let Some(status) = statistics.index_load_status.get(&index.id) else {
                                continue;
                            };
                            if !status.stats_initialized() {
                                continue;
                            }
                            rows.push(tidb_executor::show_stats::histogram_row(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                true,
                                &index_stats.histogram,
                                0.0,
                                status.status_to_string(),
                                index_stats.memory_usage().into(),
                                &zone,
                            ));
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsTopN`: render every resident TopN value
    /// through the session-aware statistics value decoder.
    pub(super) fn stats_topn_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            tidb_executor::TableEntry::Kv(table) => (table.table_id, name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.is_synthetic_pseudo() {
                            continue;
                        }
                        for (id, column_stats) in &statistics.columns {
                            let Some(column) =
                                table.columns().iter().find(|column| column.id == *id)
                            else {
                                continue;
                            };
                            let column_types = [column.field_type.code()];
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(tidb_executor::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(tidb_executor::show_stats::topn_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                1,
                                false,
                                column_stats.topn.as_ref(),
                                &column_types,
                                &mut render,
                            )?);
                        }
                        for (id, index_stats) in &statistics.indexes {
                            let Some(index) = table.indexes().iter().find(|index| index.id == *id)
                            else {
                                continue;
                            };
                            let column_types = index
                                .column_offsets
                                .iter()
                                .filter_map(|offset| table.columns().get(*offset))
                                .map(|column| column.field_type.code())
                                .collect::<Vec<_>>();
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(tidb_executor::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(tidb_executor::show_stats::topn_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                index.column_offsets.len(),
                                true,
                                index_stats.topn.as_ref(),
                                &column_types,
                                &mut render,
                            )?);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowStatsBuckets`: expose the resident cumulative
    /// histogram buckets in stable column/index ID order.
    pub(super) fn stats_buckets_rows(&mut self) -> Result<Vec<Vec<Datum>>, DriverError> {
        let stats_handle = std::sync::Arc::clone(&self.stats_handle);
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
        let rows = self.with_catalog_mut(|catalog| {
            let mut rows = Vec::new();
            for database in catalog.database_names() {
                let mut named = catalog
                    .table_names(&database)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|name| {
                        catalog.table_in(&database, &name).map(|entry| match entry {
                            tidb_executor::TableEntry::Kv(table) => (table.table_id, name),
                            _ => (i64::MAX, name),
                        })
                    })
                    .collect::<Vec<_>>();
                named.sort_by_key(|(id, _)| *id);
                for (_, name) in named {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let partitions = table
                        .partition()
                        .map(|partition| {
                            partition
                                .definitions
                                .iter()
                                .map(|definition| (definition.id, definition.name.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    for target in tidb_executor::show_stats::PartitionTargets::for_table(
                        table.table_id,
                        &partitions,
                        dynamic_partition_prune,
                    ) {
                        let Some(statistics) = catalog.table_statistics_in(&stats_handle, target.physical_id) else {
                            continue;
                        };
                        if statistics.is_synthetic_pseudo() {
                            continue;
                        }
                        for (id, column_stats) in &statistics.columns {
                            let Some(column) =
                                table.columns().iter().find(|column| column.id == *id)
                            else {
                                continue;
                            };
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(tidb_executor::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(tidb_executor::show_stats::buckets_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &column.name,
                                0,
                                &column_stats.histogram,
                                &[],
                                &mut render,
                            )?);
                        }
                        for (id, index_stats) in &statistics.indexes {
                            let Some(index) = table.indexes().iter().find(|index| index.id == *id)
                            else {
                                continue;
                            };
                            let column_types = index
                                .column_offsets
                                .iter()
                                .filter_map(|offset| table.columns().get(*offset))
                                .map(|column| column.field_type.code())
                                .collect::<Vec<_>>();
                            let mut render = |value: &Datum,
                                              num_columns: usize,
                                              types: &[tidb_datatype::FieldTypeCode]| {
                                tidb_stats::histogram::value_to_string(
                                    value,
                                    num_columns,
                                    Some(types),
                                    Some(&zone),
                                )
                                .map_err(|error| {
                                    DriverError::Exec(tidb_executor::ExecError::internal(
                                        error.to_string(),
                                    ))
                                })
                            };
                            rows.extend(tidb_executor::show_stats::buckets_to_rows(
                                &database,
                                &name,
                                &target.label,
                                &index.name,
                                index.column_offsets.len(),
                                &index_stats.histogram,
                                &column_types,
                                &mut render,
                            )?);
                        }
                    }
                }
            }
            Ok(rows)
        })?;
        Ok(rows)
    }
}

pub(crate) fn analyze_status_rows_of(
    provider: Option<&dyn crate::AnalyzeStatusProvider>,
    resource_group: &str,
    visibility: &crate::infoschema::SchemaVisibility,
    session_zone: &tidb_datatype::SessionTimeZone,
    stats_handle: &tidb_stats_handle::StatsHandle,
    catalog: &tidb_executor::Catalog,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let jobs = match provider {
        Some(provider) => provider
            .load_analyze_status(resource_group)
            .map_err(DriverError::unsupported)?,
        None => Vec::new(),
    };
    let display_time = |mut value: tidb_datatype::Time| -> Result<Datum, DriverError> {
        value
            .convert_time_zone(&chrono::Utc, session_zone)
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        tidb_datatype::Time::new(value.core_time(), tidb_datatype::TimeType::DateTime, 0)
            .map(Datum::Time)
            .map_err(|error| DriverError::unsupported(error.to_string()))
    };
    let mut result_rows = Vec::new();
    for job in jobs {
        // Go asks for `mysql.AllPrivMask`: any effective static table
        // privilege makes the row visible.
        if !visibility.allows(
            &job.table_schema,
            &job.table_name,
            crate::infoschema::PrivMask::Any,
        ) {
            continue;
        }
        let start_time = job
            .start_time
            .map(display_time)
            .transpose()?
            .unwrap_or(Datum::Null);
        let end_time = job
            .end_time
            .map(display_time)
            .transpose()?
            .unwrap_or(Datum::Null);
        let mut remaining = Datum::Null;
        let mut progress = Datum::Null;
        let mut estimated = Datum::Null;
        if job.state == tidb_stats::ANALYZE_RUNNING
            && !job.job_info.starts_with("merge global stats")
        {
            let Some(start) = job.start_time else {
                return Err(DriverError::unsupported("invalid start time"));
            };
            let table_count = (|| -> Result<Option<(i64, i64)>, DriverError> {
                let Some(tidb_executor::TableEntry::Kv(table)) =
                    catalog.table_in(&job.table_schema, &job.table_name)
                else {
                    return Ok(None);
                };
                let physical_id = if job.partition_name.is_empty() {
                    table.table_id
                } else {
                    table
                        .partition()
                        .and_then(|partition| {
                            partition.definitions.iter().find(|definition| {
                                definition.name.go_lower_eq(&job.partition_name)
                            })
                        })
                        .map_or(0, |definition| definition.id)
                };
                Ok(Some((
                    physical_id,
                    catalog
                        .table_statistics_in(stats_handle, physical_id)
                        .map_or(0, |statistics| statistics.row_count),
                )))
            })()?;
            if let Some((physical_id, mut total_rows)) = table_count {
                if (physical_id > 0 && total_rows == 0) || job.processed_rows > total_rows {
                    total_rows = provider.map_or(0, |provider| {
                        provider.approximate_table_count(
                            resource_group,
                            physical_id,
                            &job.table_schema,
                            &job.table_name,
                            &job.partition_name,
                        )
                    });
                }
                let start = start
                    .core_time()
                    .to_datetime(&chrono::Utc)
                    .map_err(|error| DriverError::unsupported(error.to_string()))?;
                let elapsed_seconds = chrono::Utc::now()
                    .signed_duration_since(start)
                    .num_nanoseconds()
                    .map_or(0.0, |nanoseconds| nanoseconds as f64 / 1_000_000_000.0);
                let (remaining_seconds, progress_ratio) =
                    tidb_executor::show_stats::analyze_progress(
                        total_rows,
                        job.processed_rows,
                        elapsed_seconds,
                    );
                remaining = Datum::new_string(
                    tidb_executor::show_stats::format_analyze_remaining_seconds(
                        remaining_seconds,
                    )
                    .into_bytes(),
                );
                progress = Datum::Real(progress_ratio);
                estimated = Datum::Int(total_rows);
            } else {
                // Go logs the table-lookup failure but still projects the
                // helper's named zero return values for these two fields.
                progress = Datum::Real(0.0);
                estimated = Datum::Int(0);
            }
        }
        result_rows.push(vec![
            Datum::new_string(job.table_schema.into_bytes()),
            Datum::new_string(job.table_name.into_bytes()),
            Datum::new_string(job.partition_name.into_bytes()),
            Datum::new_string(job.job_info.into_bytes()),
            Datum::Int(job.processed_rows),
            start_time,
            end_time,
            Datum::new_string(job.state.into_bytes()),
            job.fail_reason
                .map_or(Datum::Null, |value| Datum::new_string(value.into_bytes())),
            Datum::new_string(job.instance.into_bytes()),
            job.process_id.map_or(Datum::Null, Datum::UInt),
            remaining,
            progress,
            estimated,
        ]);
    }
    Ok(result_rows)
}

/// Go `fetchShowStatsMeta` / `fetchShowStatsHealthy`'s filters on one row:
/// `buildShow`'s `ShowBaseExtractor` on the lowercased database name, or the
/// `db_name` / `table_name` sets `PredicatePushDown` gave STATS_META.
fn stats_row_admitted(
    extractor: Option<&tidb_planner::logical::ShowPredicateExtractor>,
    row: &[Datum],
) -> bool {
    let lower = |index: usize| {
        tidb_util::stringutil::go_to_lower(&datum_text(&row[index]).unwrap_or_default())
    };
    match extractor {
        None => true,
        Some(tidb_planner::logical::ShowPredicateExtractor::Base(base)) => {
            let database = lower(0);
            tidb_executor::show::extractor_admits(Some(base), &database, &database)
        }
        Some(tidb_planner::logical::ShowPredicateExtractor::StatsMeta(filters)) => {
            (filters.db.is_empty() || filters.db.contains(&lower(0)))
                && (filters.table.is_empty() || filters.table.contains(&lower(1)))
        }
    }
}
