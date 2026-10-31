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

use tidb_stats_handle_autoanalyze::analyze_jobs::AnalyzeStatusProvider;
use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use tidb_datatype::Datum;

use tidb_datatype::datum_text::sysvar_datum_text as datum_text;
use crate::DriverError;

use tidb_hack::GoCaseCompare;

/// Go `dataForAnalyzeStatusHelper` over the recorded jobs.
pub fn analyze_status_rows_of(
    provider: Option<&dyn AnalyzeStatusProvider>,
    resource_group: &str,
    visibility: &crate::infoschema_reader::SchemaVisibility,
    session_zone: &tidb_datatype::SessionTimeZone,
    stats_handle: &tidb_stats_handle::StatsHandle,
    catalog: &crate::Catalog,
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
            crate::infoschema_reader::PrivMask::Any,
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
                let Some(crate::TableEntry::Kv(table)) =
                    catalog.table_in(&job.table_schema, &job.table_name)
                else {
                    return Ok(None);
                };
                let physical_id = if job.partition_name.is_empty() {
                    table.table_id()
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
                        .map_or(0, |statistics| statistics.hist_coll.realtime_count),
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
                    crate::show_stats::analyze_progress(
                        total_rows,
                        job.processed_rows,
                        elapsed_seconds,
                    );
                remaining = Datum::new_string(
                    crate::show_stats::format_analyze_remaining_seconds(
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
pub(crate) fn stats_row_admitted(
    extractor: Option<&dyn tidb_planner_core_base::ShowPredicateExtractor>,
    row: &[Datum],
) -> bool {
    let lower = |index: usize| {
        tidb_hack::go_to_lower(&datum_text(&row[index]).unwrap_or_default())
    };
    let Some(extractor) = extractor else {
        return true;
    };
    if let Some(base) = extractor
        .as_any()
        .downcast_ref::<tidb_planner::show_predicate_extractor::ShowBaseExtractor>()
    {
        let database = lower(0);
        return crate::show::extractor_admits(Some(base), &database, &database);
    }
    if let Some(filters) = extractor
        .as_any()
        .downcast_ref::<tidb_planner_core_operator_logicalop::ShowStatsMetaPredicateExtractor>()
    {
        return (filters.db.is_empty() || filters.db.contains(&lower(0)))
            && (filters.table.is_empty() || filters.table.contains(&lower(1)));
    }
    true
}

