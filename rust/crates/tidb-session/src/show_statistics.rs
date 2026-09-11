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

impl Session {
    /// Pinned Go `ShowExec.fetchShowColumnStatsUsage`: load the persisted map
    /// once, then walk every schema, table, logical/global table ID, physical
    /// partition ID, and column in infoschema order.
    pub(super) fn column_stats_usage_stmt(
        &mut self,
        filter: Option<&tidb_ast::ShowInspectionFilter>,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match filter {
            None => (None, None),
            Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
        };
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let datetime = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Column_name".to_owned(), varchar()),
                ("Last_used_at".to_owned(), datetime()),
                ("Last_analyzed_at".to_owned(), datetime()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Pinned Go `fetchShowAnalyzeStatus` and
    /// `dataForAnalyzeStatusHelper`: read the thirty newest persisted jobs,
    /// apply table visibility, project UTC timestamps into the session zone,
    /// and calculate progress only for running non-global-merge jobs.
    pub(super) fn analyze_status_stmt(
        &mut self,
        filter: Option<&tidb_ast::ShowInspectionFilter>,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match filter {
            None => (None, None),
            Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
        };
        let jobs = match self.analyze_status.clone() {
            Some(provider) => provider
                .load_analyze_status(&self.active_resource_group)
                .map_err(DriverError::unsupported)?,
            None => Vec::new(),
        };
        let session_zone = self.session_time_zone();
        let display_time = |mut value: tidb_datatype::Time| -> Result<Datum, DriverError> {
            value
                .convert_time_zone(&chrono::Utc, &session_zone)
                .map_err(|error| DriverError::unsupported(error.to_string()))?;
            tidb_datatype::Time::new(value.core_time(), tidb_datatype::TimeType::DateTime, 0)
                .map(Datum::Time)
                .map_err(|error| DriverError::unsupported(error.to_string()))
        };
        let mut result_rows = Vec::new();
        for job in jobs {
            // Go asks for `mysql.AllPrivMask`: any effective static table
            // privilege makes the row visible.
            if !self.has_any_scoped_privilege(
                &job.table_schema,
                &job.table_name,
                privilege::all_privs_mask(),
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
                let table_count = self.with_catalog_mut(|catalog| {
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
                                    definition.name.eq_ignore_ascii_case(&job.partition_name)
                                })
                            })
                            .map_or(0, |definition| definition.id)
                    };
                    Ok(Some((
                        physical_id,
                        catalog
                            .table_statistics(physical_id)
                            .map_or(0, |statistics| statistics.row_count),
                    )))
                })?;
                if let Some((physical_id, mut total_rows)) = table_count {
                    if (physical_id > 0 && total_rows == 0) || job.processed_rows > total_rows {
                        total_rows = self.analyze_status.as_ref().map_or(0, |provider| {
                            provider.approximate_table_count(
                                &self.active_resource_group,
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let datetime = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let double = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Table_schema".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Job_info".to_owned(), varchar()),
                ("Processed_rows".to_owned(), longlong()),
                ("Start_time".to_owned(), datetime()),
                ("End_time".to_owned(), datetime()),
                ("State".to_owned(), varchar()),
                ("Fail_reason".to_owned(), varchar()),
                ("Instance".to_owned(), varchar()),
                ("Process_ID".to_owned(), longlong()),
                ("Remaining_seconds".to_owned(), varchar()),
                ("Progress".to_owned(), double()),
                ("Estimated_total_rows".to_owned(), longlong()),
            ],
            rows: result_rows,
        };
        filter_show_output(output, like_pattern, where_clause)
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
    pub(super) fn stats_meta_stmt(
        &mut self,
        filter: Option<&tidb_ast::ShowInspectionFilter>,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match filter {
            None => (None, None),
            Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
        };
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
                let Some(names) = catalog.table_names(&database) else {
                    continue;
                };
                for name in names {
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
                        let Some(statistics) = catalog.table_statistics(target.physical_id) else {
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
        // Go `buildShowSchema`, planbuilder.go:6031.
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let datetime = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Update_time".to_owned(), datetime()),
                ("Modify_count".to_owned(), longlong()),
                ("Row_count".to_owned(), longlong()),
                ("Last_analyze_time".to_owned(), datetime()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Go `ShowExec.fetchShowStatsLocked`: enumerate every physical table in
    /// the current prune mode, intersect it with the persisted lock rows, and
    /// emit in ascending physical-ID order.
    pub(super) fn stats_locked_stmt(
        &mut self,
        show: &tidb_ast::ShowStatsLockedStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match show.filter.as_ref() {
            None => (None, None),
            Some(tidb_ast::ShowStatsLockedFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowStatsLockedFilter::Where(expr)) => (None, Some(expr)),
        };
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Status".to_owned(), varchar()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Go `ShowExec.fetchShowStatsHealthy`: one health percentage for every
    /// non-pseudo physical statistics object visible under this prune mode.
    pub(super) fn stats_healthy_stmt(
        &mut self,
        filter: Option<&tidb_ast::ShowInspectionFilter>,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match filter {
            None => (None, None),
            Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
        };
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
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
                        let Some(statistics) = catalog.table_statistics(target.physical_id) else {
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Healthy".to_owned(), longlong()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Go `ShowExec.fetchShowStatsHistogram`: expose every initialized
    /// resident column and index statistics object through the ordinary
    /// statistics cache traversal.
    pub(super) fn stats_histograms_stmt(
        &mut self,
        show: &tidb_ast::ShowStatsHistogramsStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match show.filter.as_ref() {
            None => (None, None),
            Some(tidb_ast::ShowStatsHistogramsFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowStatsHistogramsFilter::Where(expr)) => (None, Some(expr)),
        };
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = chrono::Local;
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
                        let Some(statistics) = catalog.table_statistics(target.physical_id) else {
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let double = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let tiny = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Tiny);
        let datetime = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Column_name".to_owned(), varchar()),
                ("Is_index".to_owned(), tiny()),
                ("Update_time".to_owned(), datetime()),
                ("Distinct_count".to_owned(), longlong()),
                ("Null_count".to_owned(), longlong()),
                ("Avg_col_size".to_owned(), double()),
                ("Correlation".to_owned(), double()),
                ("Load_status".to_owned(), varchar()),
                ("Total_mem_usage".to_owned(), longlong()),
                ("Hist_mem_usage".to_owned(), longlong()),
                ("Topn_mem_usage".to_owned(), longlong()),
                ("Cms_mem_usage".to_owned(), longlong()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Go `ShowExec.fetchShowStatsTopN`: render every resident TopN value
    /// through the session-aware statistics value decoder.
    pub(super) fn stats_topn_stmt(
        &mut self,
        show: &tidb_ast::ShowStatsTopNStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match show.filter.as_ref() {
            None => (None, None),
            Some(tidb_ast::ShowStatsTopNFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowStatsTopNFilter::Where(expr)) => (None, Some(expr)),
        };
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
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
                        let Some(statistics) = catalog.table_statistics(target.physical_id) else {
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let tiny = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Tiny);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Column_name".to_owned(), varchar()),
                ("Is_index".to_owned(), tiny()),
                ("Value".to_owned(), varchar()),
                ("Count".to_owned(), longlong()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }

    /// Go `ShowExec.fetchShowStatsBuckets`: expose the resident cumulative
    /// histogram buckets in stable column/index ID order.
    pub(super) fn stats_buckets_stmt(
        &mut self,
        show: &tidb_ast::ShowStatsBucketsStmt,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match show.filter.as_ref() {
            None => (None, None),
            Some(tidb_ast::ShowStatsBucketsFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowStatsBucketsFilter::Where(expr)) => (None, Some(expr)),
        };
        let dynamic_partition_prune = !self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .is_ok_and(|mode| mode.eq_ignore_ascii_case("static"));
        let zone = self.session_time_zone();
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
                        let Some(statistics) = catalog.table_statistics(target.physical_id) else {
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
        let varchar = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let longlong = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let tiny = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Tiny);
        let output = StmtOutput::Rows {
            columns: vec![
                ("Db_name".to_owned(), varchar()),
                ("Table_name".to_owned(), varchar()),
                ("Partition_name".to_owned(), varchar()),
                ("Column_name".to_owned(), varchar()),
                ("Is_index".to_owned(), tiny()),
                ("Bucket_id".to_owned(), longlong()),
                ("Count".to_owned(), longlong()),
                ("Repeats".to_owned(), longlong()),
                ("Lower_Bound".to_owned(), varchar()),
                ("Upper_Bound".to_owned(), varchar()),
                ("Ndv".to_owned(), longlong()),
            ],
            rows,
        };
        filter_show_output(output, like_pattern, where_clause)
    }
}
