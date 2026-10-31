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
//! Go's `statistics.Table` ([`Table`]) as `GetStatsTable` answered it.

use std::collections::BTreeSet;

use tidb_planner_core_operator_logicalop::DataSource;
use tidb_stats::Table;
use tidb_planner_property::stats_info::StatsInfo;



/// Go `initStats`: attaches the table profile (`TableStats`) and its
/// generated `HistColl`, the analyzed index set and the scan penalty's
/// statistics inputs. `predicate_loaded` is the column and index set the
/// statement's predicate-column collection left loaded in full, when it
/// has run.
pub fn init_stats(
    source: &mut DataSource,
    table_info: &tidb_model::TableInfo,
    statistics: Option<&Table>,
    row_count: f64,
    predicate_loaded: Option<(BTreeSet<i64>, BTreeSet<i64>)>,
) {
    source.analyzed_index_ids = statistics
        .and_then(|stats| stats.existence_map.as_ref())
        .map(|map| {
            let map = read(map);
            source
                .indexes
                .iter()
                .map(|index| index.read().id)
                .filter(|id| map.has_analyzed(*id, true))
                .collect()
        })
        .unwrap_or_default();
    let (loaded_columns, loaded_indexes) = predicate_loaded.unwrap_or_else(|| {
        (
            statistics
                .map(|statistics| {
                    let mut ids = BTreeSet::new();
                    statistics.hist_coll.for_each_column(|id, column| {
                        if column.is_full_load() {
                            ids.insert(id);
                        }
                        false
                    });
                    ids
                })
                .unwrap_or_default(),
            statistics
                .map(|statistics| {
                    let mut ids = BTreeSet::new();
                    statistics.hist_coll.for_each_index(|id, index| {
                        if index.is_full_load() {
                            ids.insert(id);
                        }
                        false
                    });
                    ids
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
                        metadata.read().id,
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
    let pseudo;
    let table = match statistics {
        Some(statistics) => statistics,
        // Go `GetStatsTable`'s `statistics.PseudoTable(tblInfo, false, true)`.
        None => {
            pseudo = tidb_stats::pseudo_table(table_info, false, true);
            &pseudo
        }
    };
    let mut hist_coll = table.hist_coll.generate_from_column_info(
        table_info,
        &source.table_columns,
        // Go `statistics.PrepareCols4MVIndex`, which the planner injects.
        |_, index_info, _| {
            let index = source.indexes.iter().find(|index| index.read().id == index_info.id)?;
            crate::access_path::prepare_cols_for_mv_index(source, index)
        },
    );
    // Go `fillIndexPath` extends `Idx2ColUniqueIDs` with the integer handle
    // for the paths in `AllPossibleAccessPaths` whose map entry covers every
    // index column; this collection is consumed after path preparation.
    for index in &source.indexes {
        let has_possible_path = source.enumerated_paths.iter().any(|path| {
            matches!(path, tidb_planner_util::path::PossiblePath::Index { index: position }
                if source.indexes.get(*position).is_some_and(|candidate| candidate.read().id == index.read().id))
        });
        let Some(columns) = hist_coll.idx_to_col_unique_ids.get_mut(&index.read().id) else {
            continue;
        };
        if has_possible_path && columns.len() == index.read().columns.len() {
            let declared = source
                .declared_index_columns(index)
                .into_iter()
                .map_while(|column| column)
                .collect::<Vec<_>>();
            columns.extend(
                source
                    .handle_cols_to_append(index, &declared)
                    .into_iter()
                    .map(|(column, _)| column.unique_id),
            );
        }
    }
    source.table_stats = Some(
        StatsInfo::new(row_count, ndvs)
            .with_hist_coll(std::sync::Arc::new(hist_coll))
            .with_stats_version(statistics.map_or(tidb_stats::PSEUDO_VERSION, |statistics| {
                if statistics.hist_coll.pseudo || statistics.hist_coll.stats_version <= 0 {
                    tidb_stats::PSEUDO_VERSION
                } else {
                    statistics.hist_coll.stats_version as u64
                }
            })),
    );
    source.table_scan_penalty = tidb_planner_core_operator_logicalop::data_source::TableScanPenaltyInput {
        has_range_info: false,
        // Go's `tidb_opt_prefer_range_scan` default is ON. This executor
        // does not expose a session override yet.
        allow_prefer_range_scan: true,
        pseudo_stats: statistics.is_none_or(|statistics| statistics.hist_coll.pseudo),
        analyze_row_count: statistics
            .map_or(-1, |statistics| statistics.hist_coll.analyze_row_count() as i64),
        modify_count: statistics.map_or(0, |statistics| statistics.hist_coll.modify_count),
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

fn read<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_charset::Collation;
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;
    use tidb_stats::{Bucket, Histogram, StatsLoadedStatus, Table};
    use tidb_planner_cardinality::row_count_estimator::EstimatorOptions;
    use tidb_ranger::Range;
    use tidb_planner_cardinality::selectivity::get_row_count_by_index_ranges;
    use tidb_planner_core_operator_logicalop::DataSource;

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
        let column_stats = |full: bool| tidb_stats::column::Column {
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
                stats_loaded_status: if full {
                    StatsLoadedStatus::full_load()
                } else {
                    StatsLoadedStatus::default()
                },
                ..Default::default()
            };
        let index_stats = |full: bool, multi_valued: bool| tidb_stats::index::Index {
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
                top_n: Some(topn.clone()),
                cmsketch: None,
                stats_version: 2,
                info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                    columns: (vec![String::new(); 1])
                        .into_iter()
                        .map(|name: String| tidb_model::IndexColumn {
                            name: tidb_ast::CiString::new(name),
                            ..Default::default()
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    mv_index: multi_valued,
                    ..tidb_model::IndexInfo::default()
                })),
                stats_loaded_status: if full {
                    StatsLoadedStatus::full_load()
                } else {
                    StatsLoadedStatus::default()
                },
                ..Default::default()
            };
        // Go `statistics.Table` with RealtimeCount 1500 / ModifyCount 100,
        // column 1 and index 7 analyzed.
        let table = |column_full: bool, index_full: bool, multi_valued: bool| {
            let mut table = Table::default();
            table.version = 1;
            table.hist_coll.realtime_count = 1_500;
            table.hist_coll.modify_count = 100;
            table.hist_coll.stats_version = 2;
            table.hist_coll.set_column(1, column_stats(column_full));
            table
                .hist_coll
                .set_index(7, index_stats(index_full, multi_valued));
            let mut existence = tidb_stats::ColAndIdxExistenceMap::new(1, 1);
            existence.insert_column(1, true);
            existence.insert_index(7, true);
            table.existence_map = Some(std::sync::Arc::new(std::sync::RwLock::new(existence)));
            table
        };

        let estimate = |multi_valued: bool, statistics: &Table, low: Datum, high: Datum| {
            let mut source = DataSource::new(Default::default(), 1, "t");
            source.columns = vec![
                tidb_model::GoShared::new({
                        let info = tidb_model::ColumnInfo {
                            id: 1,
                            name: tidb_ast::CiString::new("a"),
                            state: tidb_model::SchemaState::PUBLIC,
                            ..tidb_model::ColumnInfo::default()
                        };
                        info
                    }),
                tidb_model::GoShared::new({
                        let info = tidb_model::ColumnInfo {
                            id: 2,
                            name: tidb_ast::CiString::new("j"),
                            state: tidb_model::SchemaState::PUBLIC,
                            ..tidb_model::ColumnInfo::default()
                        };
                        info
                    }),
            ];
            source.table_columns = vec![a_column.clone(), j_column.clone()];
            source
                .base
                .base
                .set_schema(Some(Schema::new(vec![a_column.clone(), j_column.clone()])));
            source.indexes = vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
                id: 7,
                name: tidb_ast::CiString::new("mv_j"),
                columns: vec![tidb_model::IndexColumn {
                    name: tidb_ast::CiString::new("j"),
                    offset: 1,
                    length: -1,
                    ..tidb_model::IndexColumn::default()
                }].into(),
                state: tidb_model::SchemaState::PUBLIC,
                mv_index: multi_valued,
                ..tidb_model::IndexInfo::default()
            })];
            source.enumerated_paths = vec![tidb_planner_util::path::PossiblePath::Index { index: 0 }];
            // Go `ds.TableInfo`: columns a and j, and the index on j.
            let table_info = tidb_model::TableInfo {
                columns: vec![
                    tidb_model::ColumnInfo {
                        id: 1,
                        name: tidb_ast::CiString::new("a"),
                        offset: 0,
                        ..tidb_model::ColumnInfo::default()
                    },
                    tidb_model::ColumnInfo {
                        id: 2,
                        name: tidb_ast::CiString::new("j"),
                        offset: 1,
                        ..tidb_model::ColumnInfo::default()
                    },
                ]
                .into(),
                indices: vec![tidb_model::IndexInfo {
                    id: 7,
                    name: tidb_ast::CiString::new("mv_j"),
                    columns: vec![tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new("j"),
                        offset: 1,
                        length: -1,
                        ..tidb_model::IndexColumn::default()
                    }]
                    .into(),
                    mv_index: multi_valued,
                    ..tidb_model::IndexInfo::default()
                }]
                .into(),
                ..tidb_model::TableInfo::default()
            };
            super::init_stats(
                &mut source,
                &table_info,
                Some(statistics),
                statistics.hist_coll.realtime_count as f64,
                None,
            );
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
        assert_eq!(estimate(false, &table(true, true, false), low, high), 15.0);
        let (low, high) = point();
        assert_eq!(estimate(true, &table(true, true, true), low, high), 75.0);
        let (low, high) = full();
        assert_eq!(estimate(true, &table(true, true, true), low, high), 7_500.0);
        // Not fully loaded: no scaling.
        let (low, high) = point();
        assert_eq!(estimate(true, &table(true, false, true), low, high), 15.0);
        // The MV index itself cannot serve as the analyzed TABLE row count.
        let (low, high) = point();
        assert_eq!(estimate(true, &table(false, true, true), low, high), 15.0);
    }
}
