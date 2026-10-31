// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `DataSource.initStats` (`pkg/planner/core/stats.go:538`) and
//! `statistics.Table.GenerateHistCollFromColumnInfo`: the table profile, its
//! `HistColl` and the scan penalty inputs a source derives its statistics
//! from. The statistics object itself is reached through
//! [`TableStatsSnapshot`], the part of Go's `statistics.Table` this reads.

use std::collections::BTreeSet;

use tidb_planner_core_operator_logicalop::DataSource;
use tidb_stats::table_statistics::{analyze_row_count, TableStatsSnapshot};
use tidb_planner_property::stats_info::{RowSizeColumnStats, RowSizeType};
use tidb_planner_property::stats_info::{HistColl, StatsInfo};



/// Go `initStats`: attaches the table profile (`TableStats`) and its
/// generated `HistColl`, the analyzed index set and the scan penalty's
/// statistics inputs. `predicate_loaded` is the column and index set the
/// statement's predicate-column collection left loaded in full, when it
/// has run.
pub fn init_stats(
    source: &mut DataSource,
    statistics: Option<&dyn TableStatsSnapshot>,
    row_count: f64,
    predicate_loaded: Option<(BTreeSet<i64>, BTreeSet<i64>)>,
) {
    source.analyzed_index_ids = statistics
        .map(|stats| {
            stats
                .index_stats_existence()
                .iter()
                .filter_map(|(id, analyzed)| analyzed.then_some(*id))
                .collect()
        })
        .unwrap_or_default();
    let (loaded_columns, loaded_indexes) = predicate_loaded.unwrap_or_else(|| {
        (
            statistics
                .map(|statistics| {
                    statistics
                        .columns()
                        .iter()
                        .filter(|(id, _)| {
                            statistics
                                .column_load_status()
                                .get(id)
                                .is_some_and(|status| status.is_full_load())
                        })
                        .map(|(id, _)| *id)
                        .collect()
                })
                .unwrap_or_default(),
            statistics
                .map(|statistics| {
                    statistics
                        .indexes()
                        .iter()
                        .filter(|(id, _)| {
                            statistics
                                .index_load_status()
                                .get(id)
                                .is_some_and(|status| status.is_full_load())
                        })
                        .map(|(id, _)| *id)
                        .collect()
                })
                .unwrap_or_default(),
        )
    });
    let ndvs = source
        .columns
        .iter()
        .zip(
            source
                .base
                .base
                .schema()
                .into_iter()
                .flat_map(|schema| &schema.columns),
        )
        .map(|(metadata, column)| {
            // Go `DataSource.initStats` sets every column's NDV from
            // `cardinality.EstimateColumnNDV`: an initialized histogram's
            // NDV scaled from analyze time to the realtime row count, and
            // otherwise `RealtimeCount * distinctFactor` (0.8) -- for a
            // pseudo table too, whose `PseudoTable` count is 10000.
            // `pseudoEqualRate` is an equality SELECTIVITY of the pseudo
            // range estimator, not an NDV.
            let ndv = statistics
                .and_then(|statistics| {
                    tidb_planner_cardinality::ndv::estimate_column_ndv(
                        statistics,
                        metadata.id,
                        &loaded_columns,
                        &loaded_indexes,
                    )
                })
                .unwrap_or(row_count * tidb_planner_cardinality::derive_stats::DISTINCT_FACTOR);
            (column.unique_id, ndv)
        })
        .collect::<Vec<_>>();
    // Go `initStats` always attaches the table's generated HistColl,
    // including for a pseudo table. That presence is semantically visible
    // to cost model v2: base-table rows include DataInDiskByRows' eight
    // bytes per column, while a join/projection-created StatsInfo has a
    // nil HistColl and uses static type width only.
    let row_size_columns = source
        .table_columns
        .iter()
        .filter_map(|column| {
            let loaded = statistics?.columns().get(&column.id)?;
            let field_type = column.ret_type.as_ref()?;
            let is_handle = source
                .handle_cols
                .iter()
                .any(|handle| handle.unique_id == column.unique_id);
            let row_size_col = RowSizeColumnStats::new(
                RowSizeType::from_field_type_code(field_type.code()),
                loaded.histogram.tot_col_size,
                loaded.histogram.null_count,
                loaded.total_row_count(),
                is_handle,
            );
            Some((column.unique_id, row_size_col))
        })
        .collect::<Vec<_>>();
    // The planner's DataSource-statistics rule reads the loaded
    // histograms through `HistColl`; Go's `deriveStats4DataSource` uses
    // the histogram-aware `Selectivity`, not an NDV approximation.
    let histograms = source
        .table_columns
        .iter()
        .filter_map(|column| {
            let loaded = statistics?.columns().get(&column.id)?;
            Some((column.unique_id, std::sync::Arc::new(loaded.clone())))
        })
        .collect::<Vec<_>>();
    let column_load_status = source
        .table_columns
        .iter()
        .map(|column| {
            let status = statistics
                .and_then(|statistics| statistics.column_load_status().get(&column.id))
                .copied()
                .unwrap_or_default();
            (column.unique_id, status)
        })
        .collect::<Vec<_>>();
    // Go `HistColl.Columns`/`Indices` membership: `PseudoTable` holds
    // every public, non-hidden column and every public index; a stored
    // table holds what it analyzed.
    let pseudo_collection = statistics.is_none_or(|statistics| statistics.pseudo());
    // A hypothetical index (negative id here, id 0 in Go) belongs to the
    // session, not to `TableInfo`, so Go's `PseudoTable` and the loaded
    // statistics never hold it: selectivity ignores its columns.
    let index_in_collection = |index: &tidb_infoschema::table_source::SourceIndex| {
        index.id >= 0
            && match statistics {
                Some(statistics) if !pseudo_collection => {
                    statistics.indexes().contains_key(&index.id)
                }
                _ => index.is_public,
            }
    };
    // GenerateHistCollFromColumnInfo builds every index map from retained
    // payloads, including invalid/evicted ones. A schema-only index must
    // not become the first V1 suffix candidate, and a missing mapped
    // column terminates the usable prefix rather than skipping a hole.
    let index_columns = source
        .indexes
        .iter()
        .filter(|index| index_in_collection(index))
        .filter_map(|index| {
            let declared = source.declared_index_columns(index)
                .into_iter().map_while(|column| column).collect::<Vec<_>>();
            // Go resolves the index against `ds.TblCols`, every table
            // column, so an index leading with a pruned or hidden
            // (expression-index) column is still in the collection.
            let mut columns = index.columns.iter()
                .map_while(|key| source.table_columns.get(key.offset))
                .map(|column| column.unique_id)
                .collect::<Vec<_>>();
            // This snapshot is consumed after path preparation. Mirror
            // fillIndexPath's extension of the initial retained-index map,
            // which only ever appends the integer handle, and runs only
            // for the paths in AllPossibleAccessPaths.
            let has_possible_path = source.enumerated_paths.iter().any(|path| {
                matches!(path, tidb_planner_util::path::PossiblePath::Index { index: position }
                    if source.indexes.get(*position).is_some_and(|candidate| candidate.id == index.id))
            });
            if has_possible_path && columns.len() == index.columns.len() {
                columns.extend(source.handle_cols_to_append(index, &declared)
                    .into_iter().map(|(column, _)| column.unique_id));
            }
            (!columns.is_empty()).then_some((index.id, columns))
        })
        .collect::<Vec<_>>();
    let mv_index_columns = source
        .indexes
        .iter()
        .filter(|index| {
            index.is_multi_valued && index_columns.iter().any(|(id, _)| *id == index.id)
        })
        .filter_map(|index| {
            Some((
                index.id,
                crate::access_path::prepare_cols_for_mv_index(source, index)?,
            ))
        })
        .collect::<Vec<_>>();
    let index_histograms = index_columns
        .iter()
        .filter_map(|(id, _)| {
            Some((
                *id,
                std::sync::Arc::new(statistics?.indexes().get(id)?.clone()),
            ))
        })
        .collect::<Vec<_>>();
    let column_index_ids = index_columns
        .iter()
        .map(|(id, columns)| (columns[0], *id))
        .collect::<Vec<_>>();
    let index_row_counts = source
        .indexes
        .iter()
        .filter_map(|index| {
            let statistics = statistics?;
            statistics.indexes().get(&index.id)?;
            source.schema_column_for_index_column(index.columns.first()?)?;
            let mut row_counts =
                tidb_planner_property::stats_info::IndexRowCounts::unscaled(
                    statistics.row_count(),
                    statistics.modify_count(),
                );
            let index_is_fully_loaded = statistics
                .index_load_status()
                .get(&index.id)
                .is_some_and(|status| status.is_full_load());
            if index.is_multi_valued && index_is_fully_loaded {
                let analyzed_row_count = statistics
                    .columns()
                    .iter()
                    .find_map(|(column_id, column)| {
                        statistics
                            .column_load_status()
                            .get(column_id)
                            .is_some_and(|status| status.is_full_load())
                            .then(|| column.total_row_count())
                    })
                    .or_else(|| {
                        statistics
                            .indexes()
                            .iter()
                            .find_map(|(candidate_id, candidate)| {
                                let candidate_is_multi_valued = source
                                    .indexes
                                    .iter()
                                    .find(|source_index| source_index.id == *candidate_id)
                                    .is_some_and(|source_index| source_index.is_multi_valued);
                                (!candidate_is_multi_valued
                                    && statistics
                                        .index_load_status()
                                        .get(candidate_id)
                                        .is_some_and(|status| status.is_full_load()))
                                .then(|| candidate.total_row_count())
                            })
                    })
                    .unwrap_or(-1.0);
                let index_total_count = statistics
                    .indexes()
                    .get(&index.id)
                    .map_or(0.0, tidb_stats::index::Index::total_row_count);
                if analyzed_row_count > 0.0 && index_total_count > 0.0 {
                    let scale = index_total_count / analyzed_row_count;
                    row_counts.index_realtime = (statistics.row_count() as f64 * scale) as i64;
                    row_counts.index_modify = (statistics.modify_count() as f64 * scale) as i64;
                }
            }
            Some((index.id, row_counts))
        })
        .collect::<Vec<_>>();
    // Go `HistColl.Indices`' NDVs feed `getGroupNDVs`, which matches an
    // index's whole column list against a source's asked column groups.
    let index_ndvs = source
        .indexes
        .iter()
        .filter_map(|index| {
            let loaded = statistics?.indexes().get(&index.id)?;
            if loaded.histogram.ndv <= 0 {
                return None;
            }
            let columns = index
                .columns
                .iter()
                .filter_map(|column| {
                    source
                        .schema_column_for_index_column(column)
                        .map(|planned| planned.unique_id)
                })
                .collect::<Vec<_>>();
            (columns.len() == index.columns.len())
                .then(|| (index.id, (columns, loaded.histogram.ndv as f64)))
        })
        .collect::<Vec<_>>();
    let initialized_column_ndvs = source
        .table_columns
        .iter()
        .filter_map(|column| {
            let statistics = statistics?;
            let status = statistics.column_load_status().get(&column.id)?;
            status
                .stats_initialized()
                .then(|| {
                    statistics
                        .columns()
                        .get(&column.id)
                        .map(|column_stats| (column.unique_id, column_stats.histogram.ndv))
                })
                .flatten()
        })
        .collect::<Vec<_>>();
    let initialized_index_ndvs = source
        .indexes
        .iter()
        .filter_map(|index| {
            let statistics = statistics?;
            if !statistics
                .index_load_status()
                .get(&index.id)?
                .stats_initialized()
            {
                return None;
            }
            let ndv = statistics.indexes().get(&index.id)?.histogram.ndv;
            let columns = index
                .columns
                .iter()
                .map(|column| {
                    source
                        .schema_column_for_index_column(column)
                        .map(|column| column.unique_id)
                })
                .collect::<Option<Vec<_>>>()?;
            Some((index.id, (columns, ndv)))
        })
        .collect::<Vec<_>>();
    // Go `HistColl.Columns`' and `Indices`' `Info`, which the
    // selectivity estimators read.
    let column_infos = source
        .columns
        .iter()
        .zip(
            source
                .base
                .base
                .schema()
                .into_iter()
                .flat_map(|schema| &schema.columns),
        )
        .filter(|(metadata, column)| match statistics {
            Some(statistics) if !pseudo_collection => {
                statistics.columns().contains_key(&metadata.id)
            }
            // Go `PseudoTable` walks `TableInfo.Columns`, which holds
            // neither `_tidb_rowid` nor the other extra (negative id) columns.
            _ => !column.is_hidden && metadata.id > 0,
        })
        .map(|(metadata, column)| {
            (
                column.unique_id,
                tidb_planner_property::stats_info::PseudoColumn {
                    lower_name: tidb_hack::go_to_lower(&metadata.name),
                    unique_key_flag: column.ret_type.as_ref().is_some_and(|field_type| {
                        field_type.flags() & tidb_datatype::FieldTypeFlags::UNIQUE_KEY != 0
                    }),
                },
            )
        })
        .collect::<Vec<_>>();
    let index_infos = source
        .indexes
        .iter()
        .filter(|index| index_in_collection(index))
        .map(|index| tidb_planner_property::stats_info::HistCollIndexInfo {
            id: index.id,
            unique: index.unique,
            mv_index: index.is_multi_valued,
            column_lower_names: index
                .columns
                .iter()
                .map(|column| tidb_hack::go_to_lower(&column.name))
                .collect(),
            column_lengths: index.columns.iter().map(|column| column.length).collect(),
        })
        .collect::<Vec<_>>();
    source.table_stats = Some(
        StatsInfo::new(row_count, ndvs)
            .with_hist_coll(
                HistColl::new(
                    statistics.is_none_or(|statistics| statistics.pseudo()),
                    row_count as i64,
                    row_size_columns,
                )
                .with_histograms(histograms)
                .with_column_load_status(column_load_status)
                .with_index_load_status(statistics.into_iter().flat_map(|statistics| {
                    statistics
                        .index_load_status()
                        .iter()
                        .map(|(id, status)| (*id, *status))
                }))
                .with_index_histograms(index_histograms)
                .with_column_index_ids(column_index_ids)
                .with_index_columns(index_columns)
                .with_index_row_counts(index_row_counts)
                .with_index_range_policies(source.indexes.iter().map(|index| {
                    (
                        index.id,
                        tidb_planner_property::stats_info::IndexRangePolicy {
                            has_condition: !index.condition_expr_string.is_empty(),
                            is_multi_value: index.is_multi_valued,
                        },
                    )
                }))
                .with_modify_count(statistics.map_or(0, |statistics| statistics.modify_count()))
                .with_pk_is_handle(source.handle_is_int)
                .with_index_ndvs(index_ndvs)
                .with_initialized_ndvs(initialized_column_ndvs, initialized_index_ndvs)
                .with_column_and_index_infos(column_infos, index_infos)
                .with_mv_index_columns(mv_index_columns),
            )
            .with_stats_version(statistics.map_or(tidb_stats::PSEUDO_VERSION, |statistics| {
                if statistics.pseudo() || statistics.stats_ver() <= 0 {
                    tidb_stats::PSEUDO_VERSION
                } else {
                    statistics.stats_ver() as u64
                }
            })),
    );
    source.table_scan_penalty = tidb_planner_core_operator_logicalop::data_source::TableScanPenaltyInput {
        has_range_info: false,
        // Go's `tidb_opt_prefer_range_scan` default is ON. This executor
        // does not expose a session override yet.
        allow_prefer_range_scan: true,
        pseudo_stats: statistics.is_none_or(|statistics| statistics.pseudo()),
        analyze_row_count: statistics
            .map_or(-1, |statistics| analyze_row_count(statistics) as i64),
        modify_count: statistics.map_or(0, |statistics| statistics.modify_count()),
        has_partition_scan: false,
        has_index_force: false,
        unsigned_int_handle: source.handle_is_int
            && source
                .handle_cols
                .first()
                .and_then(|column| column.ret_type.as_ref())
                .is_some_and(tidb_datatype::FieldType::is_unsigned),
    };
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_charset::Collation;
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;
    use tidb_stats::{Bucket, Histogram, StatsLoadedStatus};

    use super::TableStatsSnapshot;
    use tidb_planner_cardinality::row_count_estimator::EstimatorOptions;
    use tidb_ranger::Range;
    use tidb_planner_cardinality::selectivity::get_row_count_by_index_ranges;
    use tidb_planner_core_operator_logicalop::data_source::DataSourceColumn;
    use tidb_planner_core_operator_logicalop::DataSource;
    use tidb_infoschema::table_source::SourceIndexColumn;
    use tidb_infoschema::table_source::SourceIndex;

    #[derive(Default)]
    struct Snapshot {
        row_count: i64,
        modify_count: i64,
        columns: BTreeMap<i64, tidb_stats::column::Column>,
        indexes: BTreeMap<i64, tidb_stats::index::Index>,
        column_load_status: BTreeMap<i64, StatsLoadedStatus>,
        index_load_status: BTreeMap<i64, StatsLoadedStatus>,
        index_stats_existence: BTreeMap<i64, bool>,
        mv_index_ids: BTreeSet<i64>,
    }

    impl TableStatsSnapshot for Snapshot {
        fn pseudo(&self) -> bool {
            false
        }
        fn row_count(&self) -> i64 {
            self.row_count
        }
        fn modify_count(&self) -> i64 {
            self.modify_count
        }
        fn version(&self) -> u64 {
            1
        }
        fn stats_ver(&self) -> i64 {
            2
        }
        fn columns(&self) -> &BTreeMap<i64, tidb_stats::column::Column> {
            &self.columns
        }
        fn indexes(&self) -> &BTreeMap<i64, tidb_stats::index::Index> {
            &self.indexes
        }
        fn column_load_status(&self) -> &BTreeMap<i64, StatsLoadedStatus> {
            &self.column_load_status
        }
        fn index_load_status(&self) -> &BTreeMap<i64, StatsLoadedStatus> {
            &self.index_load_status
        }
        fn index_stats_existence(&self) -> &BTreeMap<i64, bool> {
            &self.index_stats_existence
        }
        fn mv_index_ids(&self) -> &BTreeSet<i64> {
            &self.mv_index_ids
        }
    }

    /// Go `GetScaledRealtimeAndModifyCnt`: a fully loaded multi-valued
    /// index's entries count array elements, so its histogram scales with the
    /// table's growth since ANALYZE (1000 rows to 1500 turns 5000 entries
    /// into 7500 and a 50-entry point into 75), while an ordinary index
    /// keeps the table's counts.
    #[test]
    fn multi_valued_index_counts_scale_with_table_growth() {
        let a = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let mut a_column = a.clone();
        a_column.id = 1;
        let mut j_column = Column::new(2, FieldType::new(FieldTypeCode::Json));
        j_column.id = 2;
        let mut topn = tidb_stats::cmsketch::TopN::new(1);
        topn.append(&tidb_codec::encode_key(&[Datum::Int(1)]).unwrap(), 50);
        let mut snapshot = Snapshot {
            row_count: 1_500,
            modify_count: 100,
            ..Default::default()
        };
        snapshot.columns.insert(
            1,
            tidb_stats::column::Column {
                histogram: Histogram {
                    id: 1,
                    ndv: 100,
                    last_update_version: 1,
                    buckets: vec![Bucket {
                        count: 1_000,
                        repeat: 1,
                        ndv: 100,
                        lower_bound: Datum::Int(1),
                        upper_bound: Datum::Int(100),
                    }],
                    ..Histogram::default()
                },
                top_n: None,
                cmsketch: None,
                stats_version: 2,
                ..Default::default()
            },
        );
        snapshot.indexes.insert(
            7,
            tidb_stats::index::Index {
                histogram: Histogram {
                    id: 7,
                    ndv: 100,
                    buckets: vec![Bucket {
                        count: 4_950,
                        repeat: 50,
                        ndv: 99,
                        lower_bound: Datum::Bytes(
                            tidb_codec::encode_key(&[Datum::Int(2)]).unwrap(),
                        ),
                        upper_bound: Datum::Bytes(
                            tidb_codec::encode_key(&[Datum::Int(100)]).unwrap(),
                        ),
                    }],
                    ..Histogram::default()
                },
                top_n: Some(topn),
                cmsketch: None,
                stats_version: 2,
                info: Some(tidb_stats::index::IndexInfo {
                    columns: vec![String::new(); 1],
                    unique: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        snapshot
            .column_load_status
            .insert(1, StatsLoadedStatus::full_load());
        snapshot
            .index_load_status
            .insert(7, StatsLoadedStatus::full_load());
        snapshot.index_stats_existence.insert(7, true);

        let estimate = |multi_valued: bool, snapshot: &Snapshot, low: Datum, high: Datum| {
            let mut source = DataSource::new(Default::default(), 1, "t");
            source.columns = vec![
                DataSourceColumn {
                    id: 1,
                    name: "a".to_owned(),
                    ..Default::default()
                },
                DataSourceColumn {
                    id: 2,
                    name: "j".to_owned(),
                    ..Default::default()
                },
            ];
            source.table_columns = vec![a_column.clone(), j_column.clone()];
            source
                .base
                .base
                .set_schema(Some(Schema::new(vec![a_column.clone(), j_column.clone()])));
            source.indexes = vec![SourceIndex {
                id: 7,
                name: "mv_j".to_owned(),
                columns: vec![SourceIndexColumn {
                    name: "j".to_owned(),
                    offset: 1,
                    length: -1,
                }],
                is_public: true,
                is_visible: true,
                is_multi_valued: multi_valued,
                ..Default::default()
            }];
            source.enumerated_paths = vec![tidb_planner_util::path::PossiblePath::Index { index: 0 }];
            super::init_stats(&mut source, Some(snapshot), snapshot.row_count as f64, None);
            let hist = source
                .table_stats
                .as_ref()
                .and_then(|stats| stats.hist_coll())
                .expect("initStats attaches the collection");
            let range = Range {
                collators: vec![Collation::Binary],
                low_val: vec![low],
                high_val: vec![high],
                low_exclude: false,
                high_exclude: false,
            };
            get_row_count_by_index_ranges(
                hist,
                7,
                &[range],
                &[j_column.clone()],
                &EstimatorOptions::default(),
            )
            .unwrap()
            .est
        };
        let point = || (Datum::Int(1), Datum::Int(1));
        let full = || (Datum::Null, Datum::MaxValue);
        // Ordinary indexes keep Go's unscaled-count behavior.
        let (low, high) = point();
        assert_eq!(estimate(false, &snapshot, low, high), 15.0);
        let (low, high) = point();
        assert_eq!(estimate(true, &snapshot, low, high), 75.0);
        let (low, high) = full();
        assert_eq!(estimate(true, &snapshot, low, high), 7_500.0);
        // Not fully loaded: no scaling.
        let mut unloaded = Snapshot {
            row_count: snapshot.row_count,
            modify_count: snapshot.modify_count,
            columns: snapshot.columns.clone(),
            indexes: snapshot.indexes.clone(),
            column_load_status: snapshot.column_load_status.clone(),
            index_stats_existence: snapshot.index_stats_existence.clone(),
            ..Default::default()
        };
        unloaded.index_load_status.clear();
        let (low, high) = point();
        assert_eq!(estimate(true, &unloaded, low, high), 15.0);
        // The MV index itself cannot serve as the analyzed TABLE row count.
        let mut no_columns = Snapshot {
            row_count: snapshot.row_count,
            modify_count: snapshot.modify_count,
            columns: snapshot.columns.clone(),
            indexes: snapshot.indexes.clone(),
            index_load_status: snapshot.index_load_status.clone(),
            index_stats_existence: snapshot.index_stats_existence.clone(),
            ..Default::default()
        };
        no_columns.column_load_status.clear();
        let (low, high) = point();
        assert_eq!(estimate(true, &no_columns, low, high), 15.0);
    }
}
