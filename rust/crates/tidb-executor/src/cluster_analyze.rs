// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cluster ANALYZE schema planning and statistics construction.
//!
//! The production path in [`sampling`] sends Go-compatible `AnalyzeReq`
//! requests to TiKV and merges regional row collectors before building
//! histograms. Only sampled rows and aggregate sketches cross that boundary.
//! The snapshot-scanning helpers remain available for storage fixtures and
//! independent index analysis; they are not the production table sampler.
//!
//! [`crate::analyze`] owns histogram and TopN construction. This
//! module projects `TableInfo` into its plan and converts its result into
//! the `mysql.stats_*` representation Go's `storage/read.go` statements read.
//! Unsupported schema/value shapes return [`AnalyzeError`] rather than
//! publishing approximate encodings as usable statistics.

pub(crate) mod sampling;
mod virtual_samples;

use std::collections::{BTreeMap, HashSet};

use tidb_datatype::UNSPECIFIED_LENGTH;
use crate::analyze::AnalyzeError as ComputeError;
use crate::analyze::{
    AnalyzePlan, AnalyzedColumn, AnalyzedHistogram, AnalyzedIndex,
};
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;

use tidb_infoschema::cluster_catalog::PagedMetaSnapshot;


use tidb_stats_handle_storage::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use tidb_syssession::mysql_system_tables::SystemTableError;
use tidb_table::origin_default::origin_default;

pub use crate::analyze::{resolve_analyze_options, AnalyzeColumnChoice, AnalyzeOptionOverrides, AnalyzeOptions, AnalyzeStatement, STATS_VERSION_2};

/// Whether this statement is an `ANALYZE TABLE` this node runs, and against
/// which tables.
///
/// [`crate::analyze::lower_analyze`] answers it -- one refusal set for
/// both tiers -- and this only carries the answer in this module's error type.
pub fn lower_analyze(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    Ok(crate::analyze::lower_analyze(
        statement,
        default_schema,
    )?)
}

/// Why one `ANALYZE TABLE` could not run on this tier.
///
/// The computation's own refusals ([`ComputeError`]) are shared with the
/// in-process tier and carried through unchanged; only the read failure is
/// this tier's own.
#[derive(Debug)]
pub enum AnalyzeError {
    /// The table's rows could not be read.
    Read(SystemTableError),
    /// The shared computation refused, or a sampled value did not encode.
    Compute(ComputeError),
}

impl AnalyzeError {
    /// A table shape whose statistics this node does not produce.
    fn unsupported(detail: String) -> Self {
        Self::Compute(ComputeError::Unsupported(detail))
    }
}

impl std::fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Compute(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for AnalyzeError {}

impl From<SystemTableError> for AnalyzeError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<ComputeError> for AnalyzeError {
    fn from(error: ComputeError) -> Self {
        Self::Compute(error)
    }
}

impl From<AnalyzeError> for crate::DriverError {
    fn from(error: AnalyzeError) -> Self {
        match error {
            AnalyzeError::Compute(error) => error.into(),
            AnalyzeError::Read(error) => Self::unsupported(error.to_string()),
        }
    }
}

/// What one `ANALYZE TABLE` produced, plus the receipt of how it got there.
#[derive(Clone, Debug)]
pub struct AnalyzeReport {
    /// The statistics, in the shape `storage/read.go` reads back.
    pub stats: ClusterTableStats,
    /// Rows read.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli rate in force, or `1.0` under `WITH n SAMPLES`.
    pub sample_rate: f64,
}
fn report_from_analyzed(
    analyzed: crate::analyze::AnalyzedTable,
    physical_id: i64,
    version: u64,
) -> AnalyzeReport {
    let stats = ClusterTableStats {
        table_id: physical_id,
        version,
        snapshot: version,
        last_analyze_version: version,
        last_stats_hist_version: version,
        // Go's `SaveAnalyzeResultToStorage` stores
        // `max(curModifyCnt - results.BaseModifyCnt, 0)`: the modifications
        // that arrived *while the analyze ran*, which its sample therefore
        // does not describe. Go has such a window because it reads the rows
        // at one timestamp and saves at a later one, so a `dumpStatsDelta`
        // from another node can land in between and must be preserved.
        //
        // This path has no such window. It reads the rows, reads the previous
        // `stats_meta` and writes the new one all at the single `start_ts` of
        // one transaction, so `curModifyCnt` IS `BaseModifyCnt` and the
        // difference is zero -- not a discarded count. A Go node's delta flush
        // that commits after that `start_ts` writes the same `stats_meta` row
        // and is a plain write conflict at prewrite, so exactly one of the two
        // survives rather than one silently erasing the other.
        modify_count: 0,
        row_count: u64::try_from(analyzed.scanned_rows).unwrap_or_default(),
        columns: analyzed
            .columns
            .into_iter()
            .map(|built| stored_item(built, false))
            .collect(),
        indexes: analyzed
            .indexes
            .into_iter()
            .map(|built| stored_item(built, true))
            .collect(),
    };

    AnalyzeReport {
        stats,
        scanned_rows: analyzed.scanned_rows,
        sampled_rows: analyzed.sampled_rows,
        sample_rate: analyzed.sample_rate,
    }
}

/// Go `IndexInfo.IsColumnarIndex`: a vector, inverted or full-text index,
/// which ANALYZE skips with a warning.
#[must_use]
/// Go's `indexStateAnalyzable`: for an index in state public, we can always
/// analyze it for internal analyze session; for an index in state
/// WriteReorg, we can only analyze it under `EnableDDLAnalyzeExecOpt`.
#[must_use]
pub fn index_state_analyzable(index: &IndexInfo, enable_ddl_analyze_exec_opt: bool) -> bool {
    index.state == SchemaState::PUBLIC
        || (index.state == SchemaState::WRITE_REORGANIZATION && enable_ddl_analyze_exec_opt)
}

pub fn is_columnar_index(index: &IndexInfo) -> bool {
    index.vector_info.is_some() || index.inverted_info.is_some() || index.full_text_info.is_some()
}

/// Go `AnalyzeIndexExec` over one physical table's keys of `index`: the
/// table itself for a special global index, each partition in turn for a
/// multi-valued one. The keys are summarized by the store through
/// `AnalyzeIndexReq` (Go `analyzeIndexPushdown`).
pub(crate) fn analyze_independent_index_with_progress<S>(
    opener: &tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>,
    physical_id: i64,
    index: &IndexInfo,
    statement: &AnalyzeStatement,
    options: &AnalyzeOptions,
    version: u64,
    concurrency: u64,
    timeout: std::time::Duration,
    killer: &tidb_sqlkiller::SqlKiller,
    resource_group: &str,
    progress: &dyn Fn(i64),
) -> Result<AnalyzeReport, AnalyzeError>
where
    S: tidb_store_driver_txn::transaction::StoreClient,
{
    let (built, count) = sampling::collect_index_stats(
        opener,
        physical_id,
        index,
        statement,
        options,
        version,
        concurrency,
        timeout,
        killer,
        resource_group,
        progress,
    )?;
    Ok(AnalyzeReport {
        stats: ClusterTableStats {
            table_id: physical_id,
            version,
            snapshot: version,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count: 0,
            row_count: u64::try_from(count).unwrap_or_default(),
            columns: Vec::new(),
            indexes: vec![ClusterStatsItem {
                id: index.id,
                is_index: true,
                stats_ver: 2,
                flag: 0,
                load_status: tidb_stats::StatsLoadedStatus::full_load(),
                histogram: built.histogram,
                topn: built.topn,
                cms: None,
                fm_sketch: Some(built.fm_sketch),
            }],
        },
        scanned_rows: count,
        sampled_rows: count,
        sample_rate: 1.0,
    })
}

/// One built histogram as `mysql.stats_histograms` holds it.
///
/// Analyze v2 stores no CMSketch, and `flag` is Go's `AnalyzeFlag`, which a
/// full rebuild leaves at zero (it marks a histogram SYNTHESIZED from a
/// default value, which this path never produces).
fn stored_item(built: AnalyzedHistogram, is_index: bool) -> ClusterStatsItem {
    ClusterStatsItem {
        id: built.id,
        is_index,
        stats_ver: built.stats_ver,
        flag: 0,
        load_status: tidb_stats::StatsLoadedStatus::full_load(),
        histogram: built.histogram,
        topn: built.topn,
        cms: None,
        fm_sketch: built.fm_sketch,
    }
}

/// Which columns and indexes an `ANALYZE` of this stored table covers.
///
/// Every refusal here is about a value this tier cannot reproduce from the
/// stored bytes; the shape rules the two tiers share (which TopN is
/// suppressed, which slots exist) are [`AnalyzePlan`]'s own.
fn cluster_analyze_plan(
    table: &TableInfo,
    selected_column_ids: Option<&HashSet<i64>>,
    enable_ddl_analyze_exec_opt: bool,
) -> Result<AnalyzePlan, AnalyzeError> {
    let mut columns = Vec::new();
    let mut by_offset: BTreeMap<i64, usize> = BTreeMap::new();
    // Go `getFullAnalyzeColumnsInfo`: `tbl.TableInfo.Columns`, every
    // column whatever its state -- a reorganizing DDL's hidden changing
    // column among them.
    for column in table.columns.iter_deref() {
        let column = column.read();
        if selected_column_ids.is_some_and(|selected| !selected.contains(&column.id)) {
            continue;
        }
        let qualified = format!("`{}`.`{}`", table.name.original(), column.name.original());
        let collation = AnalyzedColumn::sampling_collation(&column.field_type, &qualified)?;
        // Materialised once, here, so a column whose origin default this
        // node cannot express refuses the whole ANALYZE instead of
        // silently analyzing its pre-DDL rows as NULL.
        let absent_value = origin_default(&column, table.name.original()).map_err(|error| {
            AnalyzeError::unsupported(format!(
                "this node does not analyze `{}`.`{}`: a row written before the column existed \
                 reads as its origin default, which it cannot materialise ({error})",
                table.name.original(),
                column.name.original()
            ))
        })?;
        by_offset.insert(column.offset, columns.len());
        columns.push(AnalyzedColumn {
            id: column.id,
            name: column.name.lowercase().to_owned(),
            field_type: column.field_type.clone(),
            collation,
            absent_value,
        });
    }

    let mut indexes = Vec::new();
    for index in table.indices.iter_deref() {
        let index = index.read();
        if !index_state_analyzable(&index, enable_ddl_analyze_exec_opt) {
            continue;
        }
        let is_special_global = index.global
            && index.columns.iter_deref().any(|index_column| {
                let index_column = index_column.read();
                index_column.length != UNSPECIFIED_LENGTH
                    || table
                        .cols()
                        .get(index_column.offset as usize)
                        .is_some_and(|column| column.read().is_virtual_generated())
            });
        if is_special_global {
            // Pinned Go removes this index from the ordinary column-sampling
            // task and creates one independent ordered index task for it.
            continue;
        }
        // Go `getModifiedIndexesInfoForAnalyze`: a multi-valued index is
        // analyzed as an independent index task over its own keys (one per
        // physical table), and a columnar index is skipped with a warning;
        // neither belongs to the column-sampling task.
        if index.mv_index || is_columnar_index(&index) {
            continue;
        }
        // Selected columns carry the virtual values evaluated after sample
        // decoding. Prefix values are truncated by the shared sample builder.
        let covers_unsampled_part = index.columns.iter_deref().any(|index_column| {
            let index_column = index_column.read();
            !by_offset.contains_key(&index_column.offset)
        });
        if covers_unsampled_part {
            continue;
        }
        let mut column_positions = Vec::with_capacity(index.columns.len());
        let mut prefix_lengths = Vec::with_capacity(index.columns.len());
        for index_column in index.columns.iter_deref() {
            let index_column = index_column.read();
            let position = by_offset
                .get(&index_column.offset)
                .copied()
                .ok_or_else(|| {
                    AnalyzeError::unsupported(format!(
                        "index `{}` covers a column of `{}` that is not analyzable",
                        index.name.original(),
                        table.name.original()
                    ))
                })?;
            column_positions.push(position);
            prefix_lengths.push(index_column.length);
        }
        indexes.push(AnalyzedIndex {
            id: index.id,
            single_column_unique: tidb_stats::index::is_single_col_non_prefix_unique_index(&index),
            column_positions,
            prefix_lengths,
        });
    }

    Ok(AnalyzePlan::new(columns, indexes, table.name.original())?
        .with_unique_columns(table.cols().iter_deref().filter_map(|column| {
            let id = column.read().id;
            tidb_stats::index::unique_by_schema(table, false, id).then_some(id)
        }))
        .with_virtual_columns(table.cols().iter_deref().filter_map(|column| {
            let column = column.read();
            column.is_virtual_generated().then_some(column.id)
        })))
}

#[cfg(test)]
mod tests {
    use super::{cluster_analyze_plan, AnalyzeOptions};
    use tidb_ast::CiString;
    use tidb_datatype::UNSPECIFIED_LENGTH;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_model::column::ColumnInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::table_info::TableInfo;
    use tidb_model::SchemaState;

    fn virtual_sample_table() -> TableInfo {
        let a = ColumnInfo {
            offset: 0,
            ..ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::LongLong))
        };
        let v = ColumnInfo {
            offset: 1,
            hidden: true,
            generated_expr_string: "`a` + 2".to_owned(),
            ..ColumnInfo::new(2, "_V$_ie_0", FieldType::new(FieldTypeCode::LongLong))
        };
        TableInfo {
            id: 42,
            name: tidb_ast::CiString::new("t"),
            columns: vec![a, v].into(),
            indices: vec![IndexInfo {
                id: 7,
                name: tidb_ast::CiString::new("ie"),
                state: SchemaState::PUBLIC,
                columns: vec![IndexColumn {
                    offset: 1,
                    length: UNSPECIFIED_LENGTH,
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        }
    }

    #[test]
    fn cluster_sample_plan_retains_virtual_index_inputs() {
        let table = virtual_sample_table();
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        assert_eq!(plan.columns().len(), 2);
        assert_eq!(plan.indexes().len(), 1);
        assert_eq!(plan.indexes()[0].column_positions, vec![1]);
    }

    #[test]
    fn cluster_virtual_samples_replace_null_placeholders_and_omit_column_histograms() {
        use tidb_stats::row_sample_collector::{
            RowSampleCollector, RowSampleCollectorProto, RowSampleProto, SamplePolicy,
        };
        for all_null in [false, true] {
            let table = virtual_sample_table();
            let plan = cluster_analyze_plan(&table, None, false).unwrap();
            assert_eq!(plan.indexes().len(), 1);
            assert!(super::sampling::index_requires_ndv_scan(
                &table,
                &plan,
                &plan.indexes()[0]
            ));
            let schema = super::sampling::SamplingSchema::new(&table, &plan).unwrap();
            let mut statement =
                super::lower_analyze(&tidb_parser::parse("ANALYZE TABLE t").unwrap(), "test")
                    .unwrap()
                    .unwrap()
                    .remove(0);
            statement.options.sample_rate = Some(1.0);
            let proto = RowSampleCollectorProto {
                count: 4,
                null_counts: vec![if all_null { 4 } else { 0 }, 4],
                total_sizes: vec![if all_null { 0 } else { 4 }, 0],
                fm_sketches: vec![
                    Some(tidb_stats::FmSketchProto {
                        mask: 0,
                        hashset: if all_null { vec![] } else { vec![1, 2, 3] },
                    }),
                    None,
                ],
                samples: [1, 2, 2, 3]
                    .into_iter()
                    .enumerate()
                    .map(|(handle, a)| RowSampleProto {
                        row: [
                            if all_null { Datum::Null } else { Datum::Int(a) },
                            Datum::Null,
                            Datum::Int(handle as i64),
                        ]
                        .iter()
                        .map(|value| tidb_codec::encode_value(std::slice::from_ref(value)).unwrap())
                        .collect(),
                        weight: 0,
                    })
                    .collect(),
            };
            let mut collector = RowSampleCollector::from_proto(
                &proto,
                SamplePolicy::Bernoulli { sample_rate: 1.0 },
                &tidb_memory::Tracker::new(tidb_memory::LABEL_FOR_ANALYZE_MEMORY, -1),
            );
            let mut sketch = tidb_stats::FmSketch::new(1000);
            if !all_null {
                sketch.insert_hashes([3, 4, 5]);
            }
            collector.replace_special_index_stats(
                plan.index_slot(0),
                if all_null { 4 } else { 0 },
                sketch,
            );
            let virtuals =
                super::virtual_samples::VirtualSamples::new(&table, &plan, &statement.eval_context)
                    .unwrap();
            let result = super::sampling::finish_samples(
                &virtuals,
                42,
                &statement,
                Some(4),
                100,
                &plan,
                &schema,
                collector,
            )
            .unwrap();
            assert_eq!(result.stats.columns.len(), 1);
            assert_eq!(result.stats.columns[0].id, 1);
            let index = &result.stats.indexes[0];
            assert_eq!(index.id, 7);
            assert_eq!(index.histogram.ndv, if all_null { 0 } else { 3 });
            assert_eq!(index.histogram.null_count, if all_null { 4 } else { 0 });
            if all_null {
                assert!(index.topn.is_none());
                continue;
            }
            let mut topn = index.topn.clone().unwrap();
            topn.sort();
            for (value, count) in [(3, 1), (4, 2), (5, 1)] {
                assert_eq!(
                    topn.query_bytes(&tidb_codec::encode_key(&[Datum::Int(value)]).unwrap()),
                    Some(count)
                );
            }
        }
    }

    #[test]
    fn selected_virtual_samples_resolve_nested_dependencies_in_sample_order() {
        let mut table = virtual_sample_table();
        let a = table.cols().get(0).unwrap().read().clone();
        let mut v = table.cols().get(1).unwrap().read().clone();
        v.offset = 2;
        let unused = ColumnInfo {
            offset: 1,
            ..ColumnInfo::new(3, "unused", FieldType::new(FieldTypeCode::LongLong))
        };
        let w = ColumnInfo {
            offset: 3,
            generated_expr_string: "`_V$_ie_0` * 2".to_owned(),
            ..ColumnInfo::new(4, "w", FieldType::new(FieldTypeCode::LongLong))
        };
        table.columns = vec![a, unused, v, w].into();
        table
            .indices
            .get(0)
            .unwrap()
            .write()
            .columns
            .get(0)
            .unwrap()
            .write()
            .offset = 3;
        let selected = [1, 2, 4].into_iter().collect();
        let plan = cluster_analyze_plan(&table, Some(&selected), false).unwrap();
        assert_eq!(
            plan.columns()
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>(),
            [1, 2, 4]
        );
        assert_eq!(plan.indexes()[0].column_positions, [2]);
        let context = crate::StmtContext::for_query();
        let virtuals =
            super::virtual_samples::VirtualSamples::new(&table, &plan, &context).unwrap();
        let mut row = vec![Datum::Int(2), Datum::Null, Datum::Null];
        virtuals.materialize(&mut row, &context).unwrap();
        assert_eq!(row, [Datum::Int(2), Datum::Int(4), Datum::Int(8)]);
        let mut run =
            crate::analyze::AnalyzeRun::start(&plan, &AnalyzeOptions::default(), None)
                .unwrap();
        run.push(&row).unwrap();
        assert_eq!(
            run.finish()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>(),
            [1]
        );
    }

    #[test]
    fn virtual_samples_use_the_reading_statement_timezone() {
        let table = virtual_sample_table();
        table.columns.get(1).unwrap().write().generated_expr_string =
            "hour(timestamp '2020-01-01 10:00:00+00:00')".to_owned();
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        for (hours, expected) in [(0, 10), (5, 15), (-8, 2)] {
            let context = crate::StmtContext::for_query().with_time_zone(
                tidb_datatype::SessionTimeZone::Fixed {
                    name: String::new(),
                    offset_secs: hours * 3600,
                },
            );
            let virtuals =
                super::virtual_samples::VirtualSamples::new(&table, &plan, &context).unwrap();
            let mut row = vec![Datum::Int(1), Datum::Null];
            virtuals.materialize(&mut row, &context).unwrap();
            assert_eq!(row[1], Datum::Int(expected));
        }
    }

    #[test]
    fn generated_read_policy_virtual_samples_use_shared_column_conversion() {
        let table = virtual_sample_table();
        table.columns.get(0).unwrap().write().field_type = FieldType::new(FieldTypeCode::Varchar);
        {
            let column = table.columns.get(1).unwrap();
            let mut column = column.write();
            column.field_type = FieldType::new(FieldTypeCode::String).with_flen(4);
            column.generated_expr_string = "a".to_owned();
        }
        let context = crate::StmtContext::for_query();
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        let samples = super::virtual_samples::VirtualSamples::new(&table, &plan, &context).unwrap();
        let mut row = vec![Datum::new_string_datum("a  "), Datum::Null];
        samples.materialize(&mut row, &context).unwrap();
        assert_eq!(row[1], Datum::new_string_datum("a"));
        {
            let column = table.columns.get(1).unwrap();
            let mut column = column.write();
            column.field_type = FieldType::new(FieldTypeCode::LongLong).with_flags(
                tidb_datatype::FieldTypeFlags::UNSIGNED | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
        }
        table.columns.get(0).unwrap().write().field_type = FieldType::new(FieldTypeCode::LongLong);
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        let samples = super::virtual_samples::VirtualSamples::new(&table, &plan, &context).unwrap();
        for input in [Datum::Int(-1), Datum::Null] {
            let mut row = vec![input, Datum::Null];
            samples.materialize(&mut row, &context).unwrap();
            assert_eq!(row[1], Datum::UInt(0));
        }
    }

    #[test]
    fn sampling_request_keeps_common_handle_order_and_index_slots() {
        let table = TableInfo {
            is_common_handle: true,
            columns: vec![
                ColumnInfo::new(7, "a", FieldType::new(FieldTypeCode::LongLong)),
                ColumnInfo {
                    offset: 1,
                    ..ColumnInfo::new(8, "b", FieldType::new(FieldTypeCode::LongLong))
                },
            ]
            .into(),
            indices: vec![IndexInfo {
                id: 1,
                primary: true,
                unique: true,
                state: SchemaState::PUBLIC,
                columns: vec![
                    IndexColumn {
                        offset: 1,
                        length: UNSPECIFIED_LENGTH,
                        ..IndexColumn::default()
                    },
                    IndexColumn {
                        offset: 0,
                        length: UNSPECIFIED_LENGTH,
                        ..IndexColumn::default()
                    },
                ]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        let schema = super::sampling::SamplingSchema::new(&table, &plan).unwrap();
        assert_eq!(schema.handle_positions, vec![1, 0]);
        assert_eq!(schema.statistics_slots(&plan), vec![0, 1, 2]);
        let request = schema
            .request(&plan, &AnalyzeOptions::default(), 0.01, 0, 0)
            .unwrap();
        let columns = request.col_req.unwrap();
        assert_eq!(columns.primary_column_ids, vec![8, 7]);
        assert_eq!(columns.column_groups[0].column_offsets, vec![1, 0]);
        assert_eq!(columns.columns_info.len(), 2);
        assert!(columns
            .columns_info
            .iter()
            .all(|column| column.pk_handle == Some(false)));
    }

    #[test]
    fn sampling_request_carries_hidden_handle_without_persisting_its_stats() {
        let table = TableInfo {
            columns: vec![ColumnInfo::new(
                7,
                "a",
                FieldType::new(FieldTypeCode::LongLong),
            )]
            .into(),
            ..TableInfo::default()
        };
        let plan = cluster_analyze_plan(&table, None, false).unwrap();
        let schema = super::sampling::SamplingSchema::new(&table, &plan).unwrap();
        let request = schema
            .request(&plan, &AnalyzeOptions::default(), 0.01, 0, 0)
            .unwrap();
        let columns = request.col_req.unwrap();
        assert_eq!(columns.columns_info.len(), 2);
        assert_eq!(columns.columns_info[1].column_id, Some(-1));
        assert_eq!(columns.columns_info[1].pk_handle, Some(true));
        assert_eq!(schema.statistics_slots(&plan), vec![0]);
        assert_eq!(columns.sample_rate, Some(0.01));
    }

    #[test]
    fn ordinary_global_index_is_sampled_with_partition_rows_like_go() {
        let column = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::LongLong));
        let table = TableInfo {
            name: CiString::new("t"),
            columns: vec![column].into(),
            indices: vec![IndexInfo {
                id: 2,
                name: CiString::new("idx_a"),
                state: SchemaState::PUBLIC,
                global: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    length: UNSPECIFIED_LENGTH,
                    use_changing_type: false,
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        let plan = cluster_analyze_plan(&table, None, false)
            .expect("an ordinary global index uses the column sampling task");
        assert_eq!(plan.indexes().len(), 1);
        assert_eq!(plan.indexes()[0].id, 2);
    }

    #[test]
    fn special_global_index_is_not_in_the_ordinary_sampling_plan() {
        let column = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::Varchar));
        let table = TableInfo {
            name: CiString::new("t"),
            columns: vec![column].into(),
            indices: vec![IndexInfo {
                id: 2,
                name: CiString::new("idx_a"),
                state: SchemaState::PUBLIC,
                global: true,
                columns: vec![IndexColumn {
                    name: CiString::new("a"),
                    offset: 0,
                    length: 3,
                    use_changing_type: false,
                }]
                .into(),
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        };

        let plan = cluster_analyze_plan(&table, None, false)
            .expect("the independent task owns a special global index");
        assert!(plan.indexes().is_empty());
    }
}
