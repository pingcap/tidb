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

//! Go `pkg/planner/indexadvisor/algorithm.go`: the AutoAdmin index selection
//! of Chaudhuri and Narasayya (VLDB 1997), as the Hyrise evaluation
//! framework implements it.

use std::time::Instant;

use tidb_set::keyset::{KeySet, Keyed};
use crate::model::{Column, Index, IndexSetCost, Query};
use crate::options::AdvisorOption;
use crate::query as qry;
use crate::{advisor_error, Optimizer, Result};

type IndexSet = KeySet<Index>;
type QuerySet = KeySet<Query>;
type ColumnSet = KeySet<Column>;

/// Go `adviseIndexes`.
pub(crate) fn advise_indexes(
    query_set: &QuerySet,
    indexable_columns: &ColumnSet,
    optimizer: &mut dyn Optimizer,
    option: &AdvisorOption,
) -> Result<(IndexSet, IndexSet)> {
    let mut auto_admin = AutoAdmin {
        optimizer,
        option,
        start_at: Instant::now(),
        all_candidates: IndexSet::new(),
    };
    auto_admin.calculate_best_indexes(query_set, indexable_columns)
}

struct AutoAdmin<'a> {
    optimizer: &'a mut dyn Optimizer,
    option: &'a AdvisorOption,
    start_at: Instant,
    all_candidates: IndexSet,
}

impl AutoAdmin<'_> {
    fn record_candidates(&mut self, current: &IndexSet) {
        if self.all_candidates.size() > 10000 {
            return;
        }
        self.all_candidates = IndexSet::union(&[&self.all_candidates, current]);
    }

    fn calculate_best_indexes(
        &mut self,
        query_set: &QuerySet,
        indexable_columns: &ColumnSet,
    ) -> Result<(IndexSet, IndexSet)> {
        if self.option.max_num_indexes == 0 {
            return Ok((IndexSet::new(), IndexSet::new()));
        }

        // Each indexable column as a single-column index.
        let mut potential = IndexSet::new();
        for column in indexable_columns.to_list() {
            potential.add(Index::new(
                &column.schema_name,
                &column.table_name,
                &temp_index_name(std::slice::from_ref(&column)),
                &[column.column_name.as_str()],
            ));
        }
        self.timeout()?;

        let mut current_best = IndexSet::new();
        for current_max_width in 1..=self.option.max_index_width {
            self.record_candidates(&potential);
            let candidates = self.select_index_candidates(query_set, &potential)?;
            let max_indexes = self.option.max_num_indexes;
            current_best = self.enumerate_combinations(query_set, &candidates, max_indexes)?;

            if current_max_width < self.option.max_index_width {
                // Update the potential indexes for the next iteration.
                potential = current_best.clone();
                for index in self
                    .create_multi_column_indexes(indexable_columns, &current_best)
                    .to_list()
                {
                    potential.add(index);
                }
            }
            self.timeout()?;
        }

        current_best = self.heuristic_merge_indexes(current_best, query_set)?;
        current_best = self.heuristic_covered_indexes(current_best, query_set)?;
        current_best = self.filter_indexes(query_set, &mut current_best)?;
        current_best = self.cut_down(current_best, query_set, self.option.max_num_indexes)?;
        self.timeout()?;

        // Try to add more indexes if there are fewer than the maximum.
        let mut limit = 0;
        while limit < 3 && current_best.size() < self.option.max_num_indexes {
            potential = IndexSet::difference(&potential, &current_best);
            let current_cost = evaluate_index_set_cost(query_set, self.optimizer, &current_best)?;
            let (grown, _) = self.enumerate_greedy(
                query_set,
                current_best,
                current_cost,
                potential.clone(),
                self.option.max_num_indexes,
            )?;
            current_best = grown;
            current_best = self.filter_indexes(query_set, &mut current_best)?;
            self.timeout()?;
            limit += 1;
        }

        let all_candidates = std::mem::take(&mut self.all_candidates);
        Ok((current_best, all_candidates))
    }

    /// Go `cutDown`: removes indexes until at most `max_indexes` remain, each
    /// time the one whose absence costs the least.
    fn cut_down(
        &mut self,
        mut candidates: IndexSet,
        query_set: &QuerySet,
        max_indexes: usize,
    ) -> Result<IndexSet> {
        while candidates.size() > max_indexes {
            let mut best_cost = IndexSetCost::default();
            let mut target: Option<Index> = None;
            for (position, index) in candidates.to_list().into_iter().enumerate() {
                candidates.remove(&index);
                let cost = evaluate_index_set_cost(query_set, self.optimizer, &candidates)?;
                candidates.add(index.clone());
                if position == 0 || cost.less(&best_cost) {
                    best_cost = cost;
                    target = Some(index);
                }
            }
            if let Some(target) = target {
                candidates.remove(&target);
            }
        }
        Ok(candidates)
    }

    /// Go `heuristicCoveredIndexes`: an index (b, a) for
    /// `select a from t where b=1` turns an IndexLookup into an IndexScan.
    fn heuristic_covered_indexes(
        &mut self,
        mut candidates: IndexSet,
        query_set: &QuerySet,
    ) -> Result<IndexSet> {
        let mut current_cost = evaluate_index_set_cost(query_set, self.optimizer, &candidates)?;
        for query in query_set.to_list() {
            let Some(select_columns) = qry::collect_select_columns_from_query(&query)? else {
                continue;
            };
            if select_columns.size() == 0 || select_columns.size() > self.option.max_index_width {
                continue;
            }
            let select_list = select_columns.to_list();
            let (schema_name, table_name) = (
                select_list[0].schema_name.clone(),
                select_list[0].table_name.clone(),
            );

            // Generate the cover-index candidates.
            let mut cover_set = IndexSet::new();
            for index in candidates.to_list() {
                if index.schema_name != schema_name || index.table_name != table_name {
                    continue;
                }
                if index.columns.len() + select_columns.size() > self.option.max_index_width {
                    continue;
                }
                let mut columns = index.columns.clone();
                for column in &select_list {
                    if !index
                        .columns
                        .iter()
                        .any(|existing| existing.key() == column.key())
                    {
                        columns.push(column.clone());
                    }
                }
                cover_set.add(Index {
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    index_name: temp_index_name(&columns),
                    columns,
                });
            }

            // Select the best cover-index.
            let mut best_cover: Option<Index> = None;
            let mut best_cover_cost = IndexSetCost::default();
            for (position, cover) in cover_set.to_list().into_iter().enumerate() {
                if candidates.contains(&cover) {
                    continue;
                }
                candidates.add(cover.clone());
                self.record_candidates(&candidates);
                let cost = evaluate_index_set_cost(query_set, self.optimizer, &candidates)?;
                candidates.remove(&cover);
                if position == 0 || cost.less(&best_cover_cost) {
                    best_cover_cost = cost;
                    best_cover = Some(cover);
                }
            }

            // Whether this cover-index brings any benefit.
            if best_cover_cost.less(&current_cost) {
                if let Some(best) = best_cover {
                    candidates.add(best);
                }
                current_cost = best_cover_cost;
            }
        }
        Ok(candidates)
    }

    /// Go `heuristicMergeIndexes`: indexes {(c1), (c2)} for a predicate like
    /// `where c1=1 or c2=2` so that index merge can apply.
    fn heuristic_merge_indexes(
        &mut self,
        mut candidates: IndexSet,
        query_set: &QuerySet,
    ) -> Result<IndexSet> {
        let mut current_cost = evaluate_index_set_cost(query_set, self.optimizer, &candidates)?;
        for query in query_set.to_list() {
            let Some(dnf_columns) = qry::collect_dnf_columns_from_query(&query)? else {
                continue;
            };
            if dnf_columns.size() == 0 {
                continue;
            }
            let order_by_columns = qry::collect_order_by_columns_from_query(&query)?;

            // Create indexes for these DNF columns.
            let mut new_indexes = IndexSet::new();
            for column in dnf_columns.to_list() {
                let index = Index::new(
                    &column.schema_name,
                    &column.table_name,
                    &temp_index_name(std::slice::from_ref(&column)),
                    &[column.column_name.as_str()],
                );
                let contained = candidates
                    .to_list()
                    .iter()
                    .any(|existing| existing.prefix_contains(&index));
                if !contained {
                    new_indexes.add(index);
                }

                // The index of the DNF column plus the order-by columns.
                if order_by_columns.is_empty() {
                    continue;
                }
                let mut columns = vec![column.clone()];
                columns.extend(order_by_columns.iter().cloned());
                columns.truncate(self.option.max_index_width);
                let index = Index::with_columns(&temp_index_name(&columns), &columns);
                let contained = candidates
                    .to_list()
                    .iter()
                    .any(|existing| existing.prefix_contains(&index));
                if !contained {
                    new_indexes.add(index);
                }
            }
            if new_indexes.size() == 0 {
                continue;
            }

            // Whether these new indexes bring benefit for IndexMerge.
            let mut new_candidates = IndexSet::union(&[&candidates, &new_indexes]);
            self.record_candidates(&new_candidates);
            let new_cost = evaluate_index_set_cost(query_set, self.optimizer, &new_candidates)?;
            if new_cost.less(&current_cost) {
                current_cost = new_cost;
                candidates = self.filter_indexes(query_set, &mut new_candidates)?;
            }
        }
        Ok(candidates)
    }

    fn create_multi_column_indexes(
        &mut self,
        indexable_columns: &ColumnSet,
        indexes: &IndexSet,
    ) -> IndexSet {
        let mut multi_column = IndexSet::new();
        for index in indexes.to_list() {
            let Ok(columns) = self
                .optimizer
                .table_columns(&index.schema_name, &index.table_name)
            else {
                continue;
            };
            let table_columns = ColumnSet::from_items(columns);
            let index_columns = ColumnSet::from_items(index.columns.iter().cloned());
            let extra = ColumnSet::difference(
                &ColumnSet::intersection(&table_columns, indexable_columns),
                &index_columns,
            );
            for column in extra.to_list() {
                let mut columns = index.columns.clone();
                columns.push(column);
                multi_column.add(Index {
                    schema_name: index.schema_name.clone(),
                    table_name: index.table_name.clone(),
                    index_name: temp_index_name(&columns),
                    columns,
                });
            }
        }
        multi_column
    }

    /// Go `filterIndexes`: drops the obviously unreasonable ones.
    /// Rule 1: an index that is a prefix of another goes.
    /// Rule 2: an index with no benefit goes.
    /// Rule 3: a candidate that is a prefix of an existing index goes.
    fn filter_indexes(
        &mut self,
        query_set: &QuerySet,
        index_set: &mut IndexSet,
    ) -> Result<IndexSet> {
        let index_list = index_set.to_list();
        let mut filtered = IndexSet::new();
        let original_cost = evaluate_index_set_cost(query_set, self.optimizer, index_set)?;
        for (i, x) in index_list.iter().enumerate() {
            // Rule 1.
            if index_list
                .iter()
                .enumerate()
                .any(|(j, y)| i != j && y.prefix_contains(x))
            {
                continue;
            }

            // Rule 2.
            index_set.remove(x);
            let new_cost = evaluate_index_set_cost(query_set, self.optimizer, index_set)?;
            index_set.add(x.clone());
            if !original_cost.less(&new_cost) {
                continue;
            }

            // Rule 3.
            if self.optimizer.prefix_contain_index(x)? {
                continue;
            }
            filtered.add(x.clone());
        }
        Ok(filtered)
    }

    /// Go `selectIndexCandidates`: the best indexes of each single query.
    fn select_index_candidates(
        &mut self,
        query_set: &QuerySet,
        potential: &IndexSet,
    ) -> Result<IndexSet> {
        let mut candidates = IndexSet::new();
        for query in query_set.to_list() {
            // Each query as a workload of its own.
            let single = QuerySet::from_items([query.clone()]);
            let mut indexes = self.potential_indexes_for_query(&query, potential)?;

            // Keep the 3 best indexes of each single query.
            let best_per_query = 3;
            let mut best_query_indexes = IndexSet::new();
            for _ in 0..best_per_query {
                let best = self.enumerate_combinations(&single, &indexes, 1)?;
                if best.size() == 0 {
                    break;
                }
                for index in best.to_list() {
                    best_query_indexes.add(index);
                }
                for index in best.to_list() {
                    indexes.remove(&index);
                }
                if best_query_indexes.size() > best_per_query {
                    break;
                }
            }
            for index in best_query_indexes.to_list() {
                candidates.add(index);
            }
        }
        Ok(candidates)
    }

    /// Go `enumerateCombinations`: the best indexes of the workload from the
    /// candidates.
    fn enumerate_combinations(
        &mut self,
        query_set: &QuerySet,
        candidates: &IndexSet,
        max_number_indexes: usize,
    ) -> Result<IndexSet> {
        let mut max_indexes_naive = 2;
        if candidates.size() > 50 {
            max_indexes_naive = 1;
        }
        let number_indexes_naive = max_indexes_naive
            .min(candidates.size())
            .min(max_number_indexes);
        let (current, cost) = self.enumerate_naive(query_set, candidates, number_indexes_naive)?;

        let number_indexes = max_number_indexes.min(candidates.size());
        let (indexes, _) =
            self.enumerate_greedy(query_set, current, cost, candidates.clone(), number_indexes)?;
        Ok(indexes)
    }

    /// Go `enumerateGreedy`: the best combination by adding one index at a
    /// time.
    fn enumerate_greedy(
        &mut self,
        query_set: &QuerySet,
        mut current_indexes: IndexSet,
        mut current_cost: IndexSetCost,
        mut candidates: IndexSet,
        number_indexes: usize,
    ) -> Result<(IndexSet, IndexSetCost)> {
        loop {
            if current_indexes.size() >= number_indexes || candidates.size() == 0 {
                return Ok((current_indexes, current_cost));
            }

            // Add each unused index into the current set in turn.
            let mut combinations = Vec::new();
            for index in candidates.to_list() {
                let new_combination =
                    IndexSet::union(&[&current_indexes, &IndexSet::from_items([index])]);
                if new_combination.size() != current_indexes.size() + 1 {
                    // A duplicated index.
                    continue;
                }
                combinations.push(new_combination);
            }
            if combinations.is_empty() {
                return Ok((current_indexes, current_cost));
            }

            // Find the best set.
            let (best_set, best_cost) =
                choose_best_index_set(query_set, self.optimizer, &combinations)?;
            let Some(best_set) = best_set else {
                return Ok((current_indexes, current_cost));
            };
            if best_set.size() == 0 {
                return Ok((current_indexes, current_cost));
            }
            let best_new_index = IndexSet::difference(&best_set, &current_indexes)
                .to_list()
                .remove(0);
            if best_cost.less(&current_cost) {
                current_indexes.add(best_new_index.clone());
                candidates.remove(&best_new_index);
                current_cost = best_cost;
                continue;
            }
            return Ok((current_indexes, current_cost));
        }
    }

    /// Go `enumerateNaive`: every combination of at most
    /// `number_indexes_naive` indexes, and the best of them.
    fn enumerate_naive(
        &mut self,
        query_set: &QuerySet,
        candidates: &IndexSet,
        number_indexes_naive: usize,
    ) -> Result<(IndexSet, IndexSetCost)> {
        let mut combinations = Vec::new();
        for number_of_indexes in 0..=number_indexes_naive {
            combinations.extend(candidates.combinations(number_of_indexes));
        }
        let (lowest, lowest_cost) =
            choose_best_index_set(query_set, self.optimizer, &combinations)?;
        Ok((lowest.unwrap_or_default(), lowest_cost))
    }

    /// Go `potentialIndexesForQuery`: the indexes whose leading column the
    /// query references.
    fn potential_indexes_for_query(
        &mut self,
        query: &Query,
        potential: &IndexSet,
    ) -> Result<IndexSet> {
        let mut indexes = IndexSet::new();
        let mut query_columns: Option<ColumnSet> = None;
        for index in potential.to_list() {
            if query_columns.is_none() {
                query_columns = Some(qry::collect_indexable_columns_from_query(
                    query,
                    self.optimizer,
                )?);
            }
            let columns = query_columns.as_ref().expect("just computed");
            if columns.contains(&index.columns[0]) {
                indexes.add(index);
            }
        }
        Ok(indexes)
    }

    fn timeout(&self) -> Result<()> {
        if self.start_at.elapsed() > self.option.timeout {
            return Err(advisor_error(format!(
                "index advisor timeout after {}",
                tidb_config_configtypes::format_go_duration(
                    i64::try_from(self.option.timeout.as_nanos()).unwrap_or(i64::MAX)
                )
            )));
        }
        Ok(())
    }
}

/// Go `tempIndexName`.
fn temp_index_name(columns: &[Column]) -> String {
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column.column_name.as_str())
        .collect();
    let name = format!("idx_{}", names.join("_"));
    if name.len() <= 64 {
        return name;
    }
    format!("idx_{}", random_uuid())
}

/// A version-4 UUID's text, from the process's random hasher state.
fn random_uuid() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut bytes = [0_u8; 16];
    for half in 0..2 {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u8(half);
        bytes[half as usize * 8..half as usize * 8 + 8]
            .copy_from_slice(&hasher.finish().to_be_bytes());
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Go `chooseBestIndexSet`. `None` when there is no set to choose from.
pub(crate) fn choose_best_index_set(
    query_set: &QuerySet,
    optimizer: &mut dyn Optimizer,
    index_sets: &[IndexSet],
) -> Result<(Option<IndexSet>, IndexSetCost)> {
    let mut best_set = None;
    let mut best_cost = IndexSetCost::default();
    for (position, index_set) in index_sets.iter().enumerate() {
        let cost = evaluate_index_set_cost(query_set, optimizer, index_set)?;
        if position == 0 || cost.less(&best_cost) {
            best_set = Some(index_set.clone());
            best_cost = cost;
        }
    }
    Ok((best_set, best_cost))
}

/// Go `evaluateIndexSetCost`: the workload cost under the given indexes.
pub(crate) fn evaluate_index_set_cost(
    query_set: &QuerySet,
    optimizer: &mut dyn Optimizer,
    index_set: &IndexSet,
) -> Result<IndexSetCost> {
    let queries = query_set.to_list();
    let indexes = index_set.to_list();
    let mut costs = Vec::with_capacity(queries.len());
    for query in &queries {
        costs.push(optimizer.query_plan_cost(&query.text, &indexes)?);
    }

    let mut workload_cost = 0.0_f64;
    for (cost, query) in costs.iter().zip(&queries) {
        workload_cost += cost * query.frequency as f64;
    }
    let mut total_columns = 0;
    let mut keys = Vec::with_capacity(indexes.len());
    for index in &indexes {
        total_columns += index.columns.len();
        keys.push(index.key());
    }
    keys.sort();
    Ok(IndexSetCost {
        total_workload_query_cost: workload_cost,
        total_number_of_index_columns: total_columns,
        index_keys: keys.join(","),
    })
}
