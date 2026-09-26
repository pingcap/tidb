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
    covered_column_ids: Vec<i64>,
    covers_non_discounted: bool,
    is_single_scan: bool,
    columns: usize,
    index_id: i64,
    single_interesting_declared_column: Option<i64>,
}

impl IndexWithScore {
    fn score(&self, total: usize) -> usize {
        self.interesting_count * 10
            + self.consecutive_column_ids.len() * 10
            + usize::from(total > 0 && self.interesting_count == total) * 10
            + usize::from(self.is_single_scan) * 20
    }

    fn droppable(&self) -> bool {
        !self.covers_non_discounted && !self.is_single_scan
    }

    fn covered_key(&self) -> Vec<i64> {
        let mut key = self.covered_column_ids.clone();
        key.sort_unstable();
        key
    }
}

/// Equality/IN-bound leading clustered-key columns are redundant access only
/// when they are the index's entire coverage. They still contribute to scores.
fn discounted_handle_prefix(source: &DataSource, interesting: &HashSet<i64>) -> HashSet<i64> {
    use tidb_expr::expression::Expression;
    let mut bound = HashSet::new();
    for cond in &source.pushed_down_conds {
        let Expression::ScalarFunction(sf) = cond else {
            continue;
        };
        match (sf.func_name.lowercase(), sf.args.as_slice()) {
            ("eq" | "nulleq", [Expression::Column(col), Expression::Constant(_)])
            | ("eq" | "nulleq", [Expression::Constant(_), Expression::Column(col)]) => {
                bound.insert(col.id);
            }
            ("in", [Expression::Column(col), rest @ ..])
                if rest
                    .iter()
                    .all(|arg| matches!(arg, Expression::Constant(_))) =>
            {
                bound.insert(col.id);
            }
            _ => {}
        }
    }
    let mut key = Vec::new();
    if source.pk_is_handle {
        if let Some(col) = source.table_columns.iter().find(|col| {
            col.ret_type
                .as_ref()
                .is_some_and(|ty| ty.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY))
        }) {
            key.push(col.id);
        }
    } else if source.is_common_handle {
        let Some(primary) = source.indexes.iter().find(|index| index.primary) else {
            return HashSet::new();
        };
        for col in &primary.columns {
            let Some(meta) = source.table_columns.get(col.offset) else {
                return HashSet::new();
            };
            if col.length > 0 {
                break;
            }
            key.push(meta.id);
        }
    }
    key.into_iter()
        .take_while(|id| bound.contains(id) && interesting.contains(id))
        .collect()
}

fn score_index_path(
    source: &DataSource,
    path: &PossiblePath,
    interesting_ids: &HashSet<i64>,
    discounted: &HashSet<i64>,
) -> Option<IndexWithScore> {
    let PossiblePath::Index { index } = path else {
        return None;
    };
    let metadata = source.indexes.get(*index)?;
    let state = source.derived_index_paths.get(&metadata.id);
    let declared = state.and_then(|state| state.declared_columns.as_ref());
    let mut score = IndexWithScore {
        path: path.clone(),
        interesting_count: 0,
        consecutive_column_ids: Vec::new(),
        covered_column_ids: Vec::new(),
        covers_non_discounted: false,
        is_single_scan: state
            .and_then(|state| state.is_single_scan)
            .unwrap_or_else(|| {
                declared.is_some()
                    && super::data_source::index_path_is_single_scan(source, metadata, false)
            }),
        columns: declared.map_or(0, Vec::len),
        index_id: metadata.id,
        single_interesting_declared_column: None,
    };
    if !metadata.condition_expr_string.is_empty()
        && metadata.affect_column_offsets.iter().any(|offset| {
            !source
                .table_columns
                .get(*offset)
                .is_some_and(|column| interesting_ids.contains(&column.id))
        })
    {
        return Some(score);
    }
    if let Some(declared) = declared {
        score.single_interesting_declared_column = declared
            .iter()
            .flatten()
            .map(|(col, _)| col.id)
            .find(|id| interesting_ids.contains(id) && !discounted.contains(id));
        let mut ids = declared
            .iter()
            .map(|col| col.as_ref().map_or(-1, |(col, _)| col.id))
            .collect::<Vec<_>>();
        if let Some(columns) = declared.iter().cloned().collect::<Option<Vec<_>>>() {
            ids.extend(
                source
                    .handle_cols_to_append(metadata, &columns)
                    .iter()
                    .map(|(col, _)| col.id),
            );
        }
        for (position, id) in ids.into_iter().enumerate() {
            if id < 0 || !interesting_ids.contains(&id) {
                continue;
            }
            score.interesting_count += 1;
            score.covered_column_ids.push(id);
            score.covers_non_discounted |= !discounted.contains(&id);
            if position == score.consecutive_column_ids.len() {
                score.consecutive_column_ids.push(id);
            }
        }
    } else {
        for col in &metadata.columns {
            let Some(col) = source.table_columns.get(col.offset) else {
                continue;
            };
            if interesting_ids.contains(&col.id) {
                score.interesting_count += 1;
                score.covers_non_discounted |= !discounted.contains(&col.id);
            }
        }
    }
    Some(score)
}

fn compare_scored(left: &IndexWithScore, right: &IndexWithScore, total: usize) -> Ordering {
    right
        .score(total)
        .cmp(&left.score(total))
        .then_with(|| left.droppable().cmp(&right.droppable()))
        .then_with(|| {
            right
                .consecutive_column_ids
                .len()
                .cmp(&left.consecutive_column_ids.len())
        })
        .then_with(|| right.is_single_scan.cmp(&left.is_single_scan))
        .then_with(|| left.columns.cmp(&right.columns))
        .then_with(|| left.index_id.cmp(&right.index_id))
}

/// Go's two phases share the same coverage record: an equal covered set is
/// dominated only by an equal-or-longer usable prefix with the same ordering.
fn select_indexes(
    preferred: Vec<IndexWithScore>,
    maximum: usize,
    only_zero: bool,
) -> Vec<PossiblePath> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let mut coverage = std::collections::HashMap::<Vec<i64>, Vec<Vec<i64>>>::new();
    for candidate in preferred {
        if candidate.droppable() {
            continue;
        }
        if only_zero {
            result.push(candidate.path);
            continue;
        }
        if result.len() == maximum {
            break;
        }
        let has_consecutive = !candidate.consecutive_column_ids.is_empty();
        let key = candidate.covered_key();
        if has_consecutive
            && coverage.get(&key).is_some_and(|prefixes| {
                prefixes
                    .iter()
                    .any(|prefix| prefix.starts_with(&candidate.consecutive_column_ids))
            })
        {
            continue;
        }
        if result.len() >= maximum / 2
            && !has_consecutive
            && candidate.interesting_count == 1
            && candidate
                .single_interesting_declared_column
                .is_some_and(|id| seen.contains(&id))
            && !candidate.is_single_scan
        {
            continue;
        }
        seen.extend(candidate.consecutive_column_ids.iter().copied());
        if has_consecutive {
            coverage
                .entry(key)
                .or_default()
                .push(candidate.consecutive_column_ids);
        }
        result.push(candidate.path);
    }
    result
}

/// Go `PruneIndexesByWhereAndOrder`, including its metadata-only fallback.
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
    let discounted = discounted_handle_prefix(source, &interesting_ids);
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
                let Some(scored) = score_index_path(source, path, &interesting_ids, &discounted)
                else {
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
                if prefer_merge
                    && !has_specified_indexes
                    && (scored.interesting_count > 0 || scored.is_single_scan)
                {
                    preferred.push(scored);
                    continue;
                }
                if scored.interesting_count > 0 || scored.is_single_scan {
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
    let maximum = (threshold.max(0) as usize).max(DEFAULT_MAX_INDEXES);
    result.extend(select_indexes(preferred, maximum, only_prune_zero_score));

    // Go's two safety checks retain the original list when pruning would
    // leave nothing, or only table/MV paths because no regular index scored.
    if result.is_empty()
        || (result.len() == non_regular_path_count && !has_preferred && discounted.is_empty())
    {
        return paths.to_vec();
    }
    result
}

/// Prunes one data source and returns the kept index IDs only when Go records
/// them: after an actual reduction in path count.
pub fn prune_data_source(source: &mut DataSource, threshold: i32) -> Option<HashSet<i64>> {
    prune_data_source_with_options(source, threshold, false)
}

/// Prune using the statement's prefix-index covering policy.
pub fn prune_data_source_with_options(
    source: &mut DataSource,
    threshold: i32,
    prefix_single_scan: bool,
) -> Option<HashSet<i64>> {
    if threshold < 0 || source.enumerated_paths.len() <= 1 {
        return None;
    }
    for index in &source.indexes {
        if source
            .derived_index_paths
            .get(&index.id)
            .is_some_and(|state| state.declared_columns.is_some())
        {
            let single =
                super::data_source::index_path_is_single_scan(source, index, prefix_single_scan);
            source
                .derived_index_paths
                .get_mut(&index.id)
                .unwrap()
                .is_single_scan = Some(single);
        }
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
    source.derived_access_paths = None;
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
    #[test]
    fn initialized_keys_prune_dominated_coverage_before_stats_loading() {
        let mut source = source();
        source.interesting_columns = vec![column(1), column(2)];
        source.indexes[0].columns.push(SourceIndexColumn {
            offset: 1,
            ..Default::default()
        });
        source.indexes[1].columns = source.indexes[0].columns.clone();
        for index in &source.indexes {
            source
                .derived_index_paths
                .entry(index.id)
                .or_default()
                .declared_columns = Some(vec![Some((column(1), -1)), Some((column(2), -1))]);
        }
        assert_eq!(
            prune_indexes_by_where_and_order(&source, &source.enumerated_paths, 1),
            vec![
                source.enumerated_paths[0].clone(),
                PossiblePath::Index { index: 0 }
            ]
        );
    }

    #[test]
    fn effective_key_and_handle_redundancy_share_the_range_layout() {
        use tidb_ast::CiString;
        use tidb_expr::{
            constant::Constant, expression::Expression, scalar_function::ScalarFunction,
            schema::Schema,
        };
        let mut source = source();
        source.pk_is_handle = true;
        source.columns = (1..=3)
            .map(|id| super::super::data_source::DataSourceColumn {
                id,
                name: format!("c{id}"),
                is_primary_key: id == 3,
                ..Default::default()
            })
            .collect();
        source.table_columns[2]
            .ret_type
            .as_mut()
            .unwrap()
            .add_flags(tidb_datatype::FieldTypeFlags::PRI_KEY);
        source
            .base
            .base
            .set_schema(Some(Schema::new(source.table_columns.clone())));
        for index in &mut source.indexes {
            for key in &mut index.columns {
                key.name = format!("c{}", key.offset + 1);
            }
        }
        source.initialize_index_columns();
        source.interesting_columns = vec![column(1), column(3)];
        source.pushed_down_conds = vec![Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column(3)),
                Expression::Constant(Constant::new(
                    tidb_datatype::Datum::new_int(7),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ))];
        let interesting = HashSet::from([1, 3]);
        let discounted = discounted_handle_prefix(&source, &interesting);
        assert_eq!(discounted, HashSet::from([3]));
        let a = score_index_path(
            &source,
            &PossiblePath::Index { index: 0 },
            &interesting,
            &discounted,
        )
        .unwrap();
        assert_eq!(a.consecutive_column_ids, vec![1, 3]);
        assert_eq!(
            a.score(2),
            50,
            "handle prefix still contributes to the score"
        );
        assert!(!a.droppable());
        let b = score_index_path(
            &source,
            &PossiblePath::Index { index: 1 },
            &interesting,
            &discounted,
        )
        .unwrap();
        assert_eq!(b.covered_column_ids, vec![3]);
        assert!(b.droppable(), "table path serves handle-only access");
        source
            .derived_index_paths
            .get_mut(&11)
            .unwrap()
            .declared_columns
            .as_mut()
            .unwrap()[0] = None;
        let missing = score_index_path(
            &source,
            &PossiblePath::Index { index: 0 },
            &interesting,
            &discounted,
        )
        .unwrap();
        assert_eq!(
            missing.interesting_count, 0,
            "unresolved declared column suppresses the suffix"
        );
        source.indexes[0].condition_expr_string = "c1 > 0".to_owned();
        source.indexes[0].affect_column_offsets = vec![99];
        assert_eq!(
            score_index_path(
                &source,
                &PossiblePath::Index { index: 0 },
                &interesting,
                &discounted
            )
            .unwrap()
            .interesting_count,
            0
        );
    }
}
