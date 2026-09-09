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

//! Go FDSet.AddFrom and ProjectCols. Projection preserves dependencies
//! through removed intermediates and substitutes surviving equivalents.

use super::{ColSet, FdSet};
use std::collections::HashMap;

impl FdSet {
    /// Combine dependency sets using Go's normalizing insertion rules.
    pub fn add_from(&mut self, other: &Self) {
        for edge in &other.edges {
            self.add_projected_edge(edge.clone());
        }
        self.conditional_edges
            .extend(other.conditional_edges.iter().cloned());
        self.not_null_cols.union_with(&other.not_null_cols);
        for (hash, id) in &other.hash_code_to_unique_id {
            self.hash_code_to_unique_id
                .entry(hash.clone())
                .or_insert(*id);
        }
        self.group_by_cols.union_with(&other.group_by_cols);
        self.has_agg_built = other.has_agg_built;
    }

    /// Go ProjectCols: preserve transitivity, then eliminate hidden columns.
    pub fn project_cols(&mut self, columns: &ColSet) {
        let mut constants = ColSet::default();
        let mut determinants = ColSet::default();
        let mut equivalents = ColSet::default();
        for index in 0..self.edges.len() {
            let edge = &self.edges[index];
            if edge.is_constant() {
                constants = edge.to.clone();
            }
            if !edge.to.subset_of(columns) && !edge.equiv && edge.strict {
                let closure = self.closure_of_strict(&edge.to.union(&edge.from));
                self.edges[index].to = closure.difference(&self.edges[index].from);
            }
            let edge = &self.edges[index];
            if !edge.equiv && !edge.from.subset_of(columns) {
                determinants.union_with(&edge.from.difference(columns));
            }
            if edge.equiv && edge.from.intersects(columns) {
                equivalents.union_with(&edge.from);
            }
        }
        determinants.intersection_with(&equivalents);
        let mut replacements = HashMap::new();
        for column in determinants.sorted_array() {
            let alternatives = self
                .closure_of_equivalence(&ColSet::new([column]))
                .intersection(columns);
            if let Some(alternative) = alternatives.sorted_array().into_iter().next() {
                replacements.insert(column, alternative);
            }
        }
        if !constants.is_empty() {
            self.add_constants(constants.clone());
        }
        let mut retained = Vec::new();
        let mut substituted = Vec::new();
        for mut edge in std::mem::take(&mut self.edges) {
            if !edge.to.subset_of(columns) {
                if edge.equiv {
                    edge.to.intersection_with(columns);
                    edge.from.intersection_with(columns);
                } else if edge.strict {
                    edge.to.intersection_with(columns);
                } else {
                    let removed = edge.to.difference(columns);
                    if removed.subset_of(&constants) || removed.subset_of(&self.not_null_cols) {
                        edge.to.intersection_with(columns);
                    } else {
                        continue;
                    }
                }
                if !edge.is_constant() && edge.remove_columns_to_side(&constants) {
                    continue;
                }
                if !edge.equiv && edge.remove_columns_to_side(&edge.from.clone()) {
                    continue;
                }
            }
            if !edge.from.subset_of(columns) {
                let removed = edge.from.difference(columns);
                let mapped = removed
                    .sorted_array()
                    .into_iter()
                    .map(|column| replacements.get(&column).copied())
                    .collect::<Option<Vec<_>>>();
                if let Some(mapped) = mapped {
                    edge.from = edge.from.union(&ColSet::new(mapped)).difference(&removed);
                    substituted.push(edge);
                }
                continue;
            }
            retained.push(edge);
        }
        self.edges = retained;
        for edge in substituted {
            self.add_projected_edge(edge);
        }
        self.conditional_edges.retain_mut(|conditional| {
            if !conditional.condition.intersects(columns) {
                return true;
            }
            let edge = &mut conditional.edge;
            if edge.is_constant() {
                edge.to.intersection_with(columns);
                return !edge.to.is_empty();
            }
            if edge.equiv {
                edge.from.intersection_with(columns);
                edge.to.intersection_with(columns);
                return !edge.from.is_empty();
            }
            true
        });
    }

    fn add_projected_edge(&mut self, edge: super::FdEdge) {
        if edge.equiv {
            self.add_equivalence_closure(edge.from);
        } else if edge.is_constant() {
            self.add_constants(edge.to);
        } else {
            self.add_functional_dependency(edge.from, edge.to, edge.strict, false);
        }
    }
}
