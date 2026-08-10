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

//! Native semantic seams for Go `pkg/util/ranger/detacher.go`.
//!
//! The executable index-range rules remain owned by the already-locked
//! `index_range` module. The normalized CNF/DNF traversal remains owned by
//! `tidb_planner::range_detacher`. This module does not duplicate either
//! implementation. It supplies the two condition-list rules that had no
//! native owner and keeps focused integration tests for both seams.
//!
//! This is a file seed. It does not claim completion of Go package
//! `pkg/util/ranger`.

/// Go `removeConditions`: preserve input order and remove every condition
/// found in the removal slice.
#[must_use]
#[allow(dead_code)] // Native seed; the receipt tests are the first caller.
pub(crate) fn remove_conditions<T>(conditions: &[T], conditions_to_remove: &[T]) -> Vec<T>
where
    T: Clone + PartialEq,
{
    conditions
        .iter()
        .filter(|condition| !conditions_to_remove.contains(*condition))
        .cloned()
        .collect()
}

/// Go `AppendConditionsIfNotExist`: append candidates absent from the
/// ORIGINAL condition slice.
///
/// Deliberately, candidates are not deduplicated against one another. Go
/// first builds `shouldAppend` while checking only `conditions`, then appends
/// that whole slice. Thus `[a] + [b, b]` becomes `[a, b, b]`.
#[must_use]
#[allow(dead_code)] // Native seed; the receipt tests are the first caller.
pub(crate) fn append_conditions_if_not_exist<T>(
    conditions: &[T],
    conditions_to_append: &[T],
) -> Vec<T>
where
    T: Clone + PartialEq,
{
    let mut result = conditions.to_vec();
    result.extend(
        conditions_to_append
            .iter()
            .filter(|condition| !conditions.contains(*condition))
            .cloned(),
    );
    result
}

#[cfg(test)]
mod tests {
    use crate::index_range::RangeColumn;
    use crate::plan_trace::range_text;
    use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
    use tidb_planner::range_detacher::{AccessDecision, RangeAtom, RangeAtomKind, RangePredicate};

    fn atom(identity: u32, decision: AccessDecision) -> RangePredicate {
        RangePredicate::atom(RangeAtom::new(
            identity,
            RangeAtomKind::Comparison,
            decision,
        ))
    }

    fn parse_where(expression: &str) -> tidb_ast::Expr {
        let statement = tidb_parser::parse(&format!("SELECT * FROM t WHERE {expression}"))
            .expect("boundary expression parses");
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        select.where_clause.clone().expect("expected WHERE")
    }

    fn rendered_index_range(columns: &[&str], expression: &str) -> String {
        let columns = columns
            .iter()
            .map(|name| {
                RangeColumn::whole((*name).to_owned(), FieldType::new(FieldTypeCode::LongLong))
            })
            .collect::<Vec<_>>();
        let predicate = parse_where(expression);
        let detached = crate::index_range::detach_cond_and_build_range_for_index(
            &columns,
            &predicate,
            &SessionTimeZone::utc(),
        )
        .expect("predicate has an index range");
        detached
            .ranges
            .iter()
            .map(range_text)
            .collect::<Vec<_>>()
            .join(", ")
    }

    #[test]
    fn cnf_detachment_preserves_access_filter_and_order() {
        let access = atom(1, AccessDecision::access());
        let approximate = atom(2, AccessDecision::access_and_reserve());
        let filter = atom(3, AccessDecision::filter());
        let result = tidb_planner::range_detacher::detach_cnf_predicates(&[
            access.clone(),
            approximate.clone(),
            filter.clone(),
        ]);
        assert_eq!(
            result.access_conditions(),
            &[access.clone(), approximate.clone()]
        );
        assert_eq!(result.filter_conditions(), &[approximate, filter]);

        let nested_or = RangePredicate::or([access, atom(4, AccessDecision::filter())]);
        let result =
            tidb_planner::range_detacher::detach_cnf_predicates(std::slice::from_ref(&nested_or));
        assert!(result.access_conditions().is_empty());
        assert_eq!(result.filter_conditions(), &[nested_or]);
    }

    #[test]
    fn dnf_detachment_rejects_filter_only_branch() {
        let access = atom(1, AccessDecision::access());
        let filter = atom(2, AccessDecision::filter());
        let result = tidb_planner::range_detacher::detach_dnf_predicates(&[
            RangePredicate::and([filter]),
            access,
        ]);
        assert!(result.access_conditions().is_empty());
        assert!(result.has_residual());
    }

    #[test]
    fn dnf_detachment_rebuilds_and_tracks_residual() {
        let exact = atom(1, AccessDecision::access());
        let approximate = atom(2, AccessDecision::access_and_reserve());
        let result = tidb_planner::range_detacher::detach_dnf_predicates(&[
            RangePredicate::and([exact.clone(), approximate.clone()]),
            exact.clone(),
        ]);
        assert_eq!(
            result.access_conditions(),
            &[RangePredicate::and([exact.clone(), approximate]), exact]
        );
        assert!(result.has_residual());

        let empty = tidb_planner::range_detacher::detach_dnf_predicates(&[RangePredicate::and([])]);
        assert!(empty.access_conditions().is_empty());
        assert!(empty.has_residual());
    }

    #[test]
    fn condition_list_helpers_match_go_existing_slice_semantics() {
        assert_eq!(
            crate::ranger_detacher::remove_conditions(&[1, 2, 1, 3], &[1, 4]),
            vec![2, 3]
        );
        assert_eq!(
            crate::ranger_detacher::remove_conditions::<i32>(&[], &[1]),
            Vec::<i32>::new()
        );

        assert_eq!(
            crate::ranger_detacher::append_conditions_if_not_exist(&[1, 2], &[2, 3, 3],),
            vec![1, 2, 3, 3]
        );
        assert_eq!(
            crate::ranger_detacher::append_conditions_if_not_exist(&[1, 1], &[1]),
            vec![1, 1]
        );
    }

    #[test]
    fn executor_index_detacher_keeps_dnf_and_prefix_boundaries() {
        assert_eq!(rendered_index_range(&["a"], "a = 1 OR a = 2"), "[1,2]");
        assert_eq!(
            rendered_index_range(&["a", "b"], "a = 1 AND b > 2"),
            "(1 2,1 +inf]"
        );
    }

    #[test]
    fn executor_column_detacher_intersects_and_retains_residual() {
        let column = RangeColumn::whole("a".to_owned(), FieldType::new(FieldTypeCode::LongLong));
        let low = parse_where("a >= 3");
        let high = parse_where("a <= 7");
        let residual = parse_where("b = 9");
        let detached = crate::index_range::detach_conds_for_column(
            &column,
            &[&low, &high, &residual],
            &SessionTimeZone::utc(),
        );
        assert_eq!(detached.access_count, 2);
        assert_eq!(detached.residual, vec![&residual]);
        assert_eq!(
            detached.ranges.iter().map(range_text).collect::<Vec<_>>(),
            vec!["[3,7]"]
        );
    }
}
