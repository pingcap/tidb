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

//! Go `pkg/planner/core/rule/rule_prune_indexes.go` at the access-path stage
//! used by `CollectPredicateColumnsPoint`.

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::access_path::PossiblePath;

use super::data_source::DataSource;

const DEFAULT_MAX_INDEXES: usize = 10;

#[derive(Clone)]
struct IndexWithScore {
    path: PossiblePath,
    interesting_count: usize,
    consecutive_column_ids: Vec<i64>,
    columns: usize,
    index_id: i64,
}

impl IndexWithScore {
    fn score(&self, total_interesting_columns: usize) -> usize {
        let mut score = self.interesting_count * 10;
        score += self.consecutive_column_ids.len() * 10;
        if self.interesting_count == total_interesting_columns {
            score += 10;
        }
        // Go adds 20 for `IsSingleScan`. At this rule position Rust's
        // `PossiblePath`, like Go's path before `fillIndexPath`, has no
        // `FullIdxCols`, so Go does not calculate or set `IsSingleScan`.
        score
    }
}

fn score_index_path(
    source: &DataSource,
    path: &PossiblePath,
    interesting_ids: &HashSet<i64>,
) -> Option<IndexWithScore> {
    let PossiblePath::Index { index } = path else {
        return None;
    };
    let metadata = source.indexes.get(*index)?;
    if !metadata.condition_expr_string.is_empty()
        && metadata
            .affect_column_offsets
            .iter()
            .any(|offset| !interesting_ids.contains(&source.table_columns[*offset].id))
    {
        return Some(IndexWithScore {
            path: path.clone(),
            interesting_count: 0,
            consecutive_column_ids: Vec::new(),
            columns: 0,
            index_id: metadata.id,
        });
    }
    let interesting_count = metadata
        .columns
        .iter()
        .filter_map(|column| source.table_columns.get(column.offset))
        .filter(|column| interesting_ids.contains(&column.id))
        .count();
    Some(IndexWithScore {
        path: path.clone(),
        interesting_count,
        // Go's fallback branch over `Index.Columns` deliberately cannot
        // derive consecutive IDs because `FullIdxCols` is still nil.
        consecutive_column_ids: Vec::new(),
        columns: 0,
        index_id: metadata.id,
    })
}

fn compare_scored(left: &IndexWithScore, right: &IndexWithScore, total: usize) -> Ordering {
    right
        .score(total)
        .cmp(&left.score(total))
        .then_with(|| {
            right
                .consecutive_column_ids
                .len()
                .cmp(&left.consecutive_column_ids.len())
        })
        // `IsSingleScan` is false for every newborn path here.
        .then_with(|| {
            if left.consecutive_column_ids.len() == 1 {
                left.columns.cmp(&right.columns)
            } else {
                Ordering::Equal
            }
        })
        .then_with(|| left.index_id.cmp(&right.index_id))
}

/// Go `PruneIndexesByWhereAndOrder` at the static pruning position.
#[must_use]
pub fn prune_indexes_by_where_and_order(
    source: &DataSource,
    paths: &[PossiblePath],
    threshold: i32,
) -> Vec<PossiblePath> {
    if paths.len() <= 1 || threshold < 0 {
        return paths.to_vec();
    }

    let total_path_count = paths.len();
    let only_prune_zero_score = threshold == 0 || threshold as usize > total_path_count;
    let interesting_ids = source
        .interesting_columns
        .iter()
        .map(|column| column.id)
        .collect::<HashSet<_>>();
    let mut table_paths = Vec::new();
    let mut multi_value_paths = Vec::new();
    let mut index_merge_paths = Vec::new();
    let mut preferred = Vec::new();
    let prefer_merge =
        !source.index_merge_hints.is_empty() || source.prefer_index_merge_by_fix_control;
    let has_specified_indexes = source
        .index_merge_hints
        .iter()
        .any(|hint| !hint.index_names.is_empty());

    for path in paths {
        match path {
            PossiblePath::Table { .. } | PossiblePath::TiFlashTable => {
                table_paths.push(path.clone())
            }
            PossiblePath::Index { index } => {
                let Some(metadata) = source.indexes.get(*index) else {
                    continue;
                };
                if metadata.is_multi_valued {
                    multi_value_paths.push(path.clone());
                    continue;
                }
                if source.forced_index_ids.contains(&metadata.id) {
                    return paths.to_vec();
                }
                let Some(scored) = score_index_path(source, path, &interesting_ids) else {
                    continue;
                };
                if has_specified_indexes
                    && source
                        .index_merge_hints
                        .iter()
                        .flat_map(|hint| &hint.index_names)
                        .any(|name| metadata.name.eq_ignore_ascii_case(name))
                {
                    index_merge_paths.push(path.clone());
                    continue;
                }
                if prefer_merge && !has_specified_indexes && scored.interesting_count > 0 {
                    preferred.push(scored);
                    continue;
                }
                // `IsSingleScan` is false here, so Go admits this candidate
                // exactly when it covers an interesting column.
                if scored.interesting_count > 0 {
                    preferred.push(scored);
                }
            }
        }
    }

    preferred.retain(|candidate| candidate.score(interesting_ids.len()) > 0);
    preferred.sort_by(|left, right| compare_scored(left, right, interesting_ids.len()));
    let has_preferred = !preferred.is_empty();

    let mut result = table_paths;
    result.extend(multi_value_paths);
    let non_regular_path_count = result.len();
    result.extend(index_merge_paths);
    if only_prune_zero_score {
        result.extend(preferred.into_iter().map(|candidate| candidate.path));
    } else {
        let maximum = (threshold.max(0) as usize).max(DEFAULT_MAX_INDEXES);
        // With nil `FullIdxCols`, Go's phase-two diversity checks retain each
        // positive candidate until the common maximum is reached.
        result.extend(
            preferred
                .into_iter()
                .take(maximum)
                .map(|candidate| candidate.path),
        );
    }

    // Go's two safety checks retain the original list when pruning would
    // leave nothing, or only table/MV paths because no regular index scored.
    if result.is_empty() || (result.len() == non_regular_path_count && !has_preferred) {
        return paths.to_vec();
    }
    result
}

/// Prunes one data source and returns the kept index IDs only when Go records
/// them: after an actual reduction in path count.
pub fn prune_data_source(source: &mut DataSource, threshold: i32) -> Option<HashSet<i64>> {
    if threshold < 0 || source.enumerated_paths.len() <= 1 {
        return None;
    }
    let effective_threshold = if threshold == 0 {
        i32::try_from(source.enumerated_paths.len()).unwrap_or(i32::MAX)
    } else {
        threshold
    };
    let pruned =
        prune_indexes_by_where_and_order(source, &source.enumerated_paths, effective_threshold);
    if pruned.len() >= source.enumerated_paths.len() {
        return None;
    }

    let kept = pruned
        .iter()
        .filter_map(|path| match path {
            PossiblePath::Index { index } => source.indexes.get(*index).map(|index| index.id),
            PossiblePath::Table {
                primary_index: Some(index),
                ..
            } => source.indexes.get(*index).map(|index| index.id),
            PossiblePath::Table { .. } => None,
            PossiblePath::TiFlashTable => None,
        })
        .collect();
    source.enumerated_paths = pruned;
    Some(kept)
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;

    use super::*;
    use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
    }

    fn source() -> DataSource {
        DataSource {
            table_columns: vec![column(1), column(2), column(3)],
            indexes: vec![
                SourceIndex {
                    id: 11,
                    name: "idx_a".to_owned(),
                    columns: vec![SourceIndexColumn {
                        offset: 0,
                        ..SourceIndexColumn::default()
                    }],
                    ..SourceIndex::default()
                },
                SourceIndex {
                    id: 12,
                    name: "idx_b".to_owned(),
                    columns: vec![SourceIndexColumn {
                        offset: 1,
                        ..SourceIndexColumn::default()
                    }],
                    ..SourceIndex::default()
                },
            ],
            enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
                PossiblePath::Index { index: 1 },
            ],
            interesting_columns: vec![column(1)],
            ..DataSource::default()
        }
    }

    #[test]
    fn threshold_zero_removes_only_zero_score_indexes() {
        let mut source = source();
        let kept = prune_data_source(&mut source, 0).expect("one path is pruned");
        assert_eq!(kept, HashSet::from([11]));
        assert_eq!(
            source.enumerated_paths,
            vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
            ]
        );
    }

    #[test]
    fn no_interesting_columns_preserves_go_safety_fallback() {
        let mut source = source();
        source.interesting_columns.clear();
        assert_eq!(prune_data_source(&mut source, 0), None);
        assert_eq!(source.enumerated_paths.len(), 3);
    }

    #[test]
    fn a_forced_path_disables_pruning_for_the_whole_source() {
        let mut source = source();
        source.forced_index_ids.insert(12);
        assert_eq!(
            prune_indexes_by_where_and_order(&source, &source.enumerated_paths, 1),
            source.enumerated_paths
        );
    }

    #[test]
    fn a_named_index_merge_path_survives_without_a_score() {
        let mut source = source();
        source.index_merge_hints = vec![crate::logical::data_source::DataSourceIndexMergeHint {
            index_names: vec!["IDX_B".to_owned()],
            ..Default::default()
        }];
        assert_eq!(
            prune_indexes_by_where_and_order(&source, &source.enumerated_paths, 1),
            vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 1 },
                PossiblePath::Index { index: 0 },
            ]
        );
    }

    #[test]
    fn a_partial_index_missing_an_affected_column_has_zero_score() {
        let mut source = source();
        source.indexes[0].condition_expr_string = "c > 0".to_owned();
        source.indexes[0].affect_column_offsets = vec![2];
        source.interesting_columns = vec![column(1), column(2)];
        assert_eq!(
            prune_indexes_by_where_and_order(&source, &source.enumerated_paths, 0),
            vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 1 },
            ]
        );
    }

    #[test]
    fn positive_threshold_keeps_go_default_maximum() {
        let mut source = source();
        source.indexes = (0..12)
            .map(|offset| SourceIndex {
                id: 100 + offset,
                name: format!("i{offset}"),
                columns: vec![SourceIndexColumn {
                    offset: 0,
                    ..SourceIndexColumn::default()
                }],
                ..SourceIndex::default()
            })
            .collect();
        source.enumerated_paths = std::iter::once(PossiblePath::Table {
            is_int_handle: true,
            primary_index: None,
        })
        .chain((0..12).map(|index| PossiblePath::Index { index }))
        .collect();

        let pruned = prune_indexes_by_where_and_order(&source, &source.enumerated_paths, 1);
        assert_eq!(pruned.len(), 11);
        assert_eq!(
            pruned[1..],
            (0..10)
                .map(|index| PossiblePath::Index { index })
                .collect::<Vec<_>>()
        );
    }
}
