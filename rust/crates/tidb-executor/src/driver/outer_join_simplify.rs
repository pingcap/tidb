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

//! Outer-to-inner conversion before physical join planning.

use tidb_ast::{Expr, Join, JoinNode, JoinType};

use super::{catalog::split_table_path, Catalog};

#[derive(Clone)]
struct Binding {
    qualifier: String,
    columns: Vec<String>,
    inner: bool,
}

/// Returns a simplified clone when a WHERE/ancestor-ON predicate rejects the
/// NULL-extended side of an outer join.
pub(crate) fn simplify(
    join: &Join,
    where_clause: Option<&Expr>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<Join> {
    let mut simplified = join.clone();
    let mut predicates = Vec::new();
    if let Some(predicate) = where_clause {
        crate::plan_trace::collect_and(predicate, &mut predicates);
    }
    let predicates: Vec<Expr> = predicates.into_iter().cloned().collect();
    simplify_join(&mut simplified, &predicates, catalog, current_db).then_some(simplified)
}

fn simplify_join(
    join: &mut Join,
    predicates: &[Expr],
    catalog: &Catalog,
    current_db: &str,
) -> bool {
    let original_type = join.tp;
    let mut changed = false;

    if matches!(original_type, JoinType::Left | JoinType::Right) {
        let inner = match original_type {
            JoinType::Left => join.right.as_ref(),
            JoinType::Right => Some(&join.left),
            JoinType::Cross => None,
        };
        let outer = match original_type {
            JoinType::Left => Some(&join.left),
            JoinType::Right => join.right.as_ref(),
            JoinType::Cross => None,
        };
        if let (Some(inner), Some(outer)) = (inner, outer) {
            if let (Some(mut inner_bindings), Some(mut outer_bindings)) = (
                bindings(inner, catalog, current_db),
                bindings(outer, catalog, current_db),
            ) {
                for binding in &mut inner_bindings {
                    binding.inner = true;
                }
                outer_bindings.extend(inner_bindings);
                if predicates.iter().any(|predicate| {
                    super::funcdep::null_reject::is_null_rejected_by(predicate, &|path| {
                        resolves_to_inner(path, &outer_bindings)
                    })
                }) {
                    join.tp = JoinType::Cross;
                    changed = true;
                }
            }
        }
    }

    let mut combined = Vec::new();
    if let Some(on) = &join.on {
        let mut conjuncts = Vec::new();
        crate::plan_trace::collect_and(on, &mut conjuncts);
        combined.extend(conjuncts.into_iter().cloned());
    }
    combined.extend_from_slice(predicates);

    match join.tp {
        JoinType::Cross => {
            changed |= simplify_node(&mut join.left, &combined, catalog, current_db);
            if let Some(right) = &mut join.right {
                changed |= simplify_node(right, &combined, catalog, current_db);
            }
        }
        JoinType::Left => {
            changed |= simplify_node(&mut join.left, predicates, catalog, current_db);
            if let Some(right) = &mut join.right {
                changed |= simplify_node(right, &combined, catalog, current_db);
            }
        }
        JoinType::Right => {
            changed |= simplify_node(&mut join.left, &combined, catalog, current_db);
            if let Some(right) = &mut join.right {
                changed |= simplify_node(right, predicates, catalog, current_db);
            }
        }
    }
    changed
}

fn simplify_node(
    node: &mut JoinNode,
    predicates: &[Expr],
    catalog: &Catalog,
    current_db: &str,
) -> bool {
    match node {
        JoinNode::Join(join) => simplify_join(join, predicates, catalog, current_db),
        JoinNode::Table(_) | JoinNode::Derived { .. } => false,
    }
}

fn bindings(node: &JoinNode, catalog: &Catalog, current_db: &str) -> Option<Vec<Binding>> {
    match node {
        JoinNode::Table(table) => {
            let (database, name) = split_table_path(&table.name, current_db).ok()?;
            let columns = catalog
                .get_in(database, name)?
                .column_list()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            Some(vec![Binding {
                qualifier: table.alias.clone().unwrap_or_else(|| name.to_owned()),
                columns,
                inner: false,
            }])
        }
        JoinNode::Derived {
            subquery,
            alias,
            column_names,
            ..
        } => {
            let qualifier = alias.as_ref()?.clone();
            let columns = if column_names.is_empty() {
                super::from::derived_field_names_query(subquery)?
            } else {
                column_names.clone()
            };
            Some(vec![Binding {
                qualifier,
                columns,
                inner: false,
            }])
        }
        JoinNode::Join(join) => {
            let mut result = bindings(&join.left, catalog, current_db)?;
            if let Some(right) = &join.right {
                result.extend(bindings(right, catalog, current_db)?);
            }
            Some(result)
        }
    }
}

fn resolves_to_inner(path: &[String], bindings: &[Binding]) -> bool {
    let Some(column) = path.last() else {
        return false;
    };
    let candidates: Vec<&Binding> = if path.len() >= 2 {
        let qualifier = &path[path.len() - 2];
        bindings
            .iter()
            .filter(|binding| binding.qualifier.eq_ignore_ascii_case(qualifier))
            .collect()
    } else {
        bindings
            .iter()
            .filter(|binding| {
                binding
                    .columns
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(column))
            })
            .collect()
    };
    candidates.len() == 1
        && candidates[0].inner
        && candidates[0]
            .columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column))
}
