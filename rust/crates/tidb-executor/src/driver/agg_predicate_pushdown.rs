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

use std::any::Any;

use tidb_ast::{BinaryOp, Expr, SelectField, SelectStmt, Visitable, Visitor};

/// Returns the access-path view of a grouped SELECT when its HAVING predicate
/// can also be evaluated on source rows.
///
/// TiDB's `LogicalAggregation.PredicatePushDown` sends a scalar predicate
/// below aggregation when all of its columns are grouping columns. The HAVING
/// selection remains above aggregation as well; this clone changes only what
/// the source may use for pruning and range construction.
pub(super) fn for_access(select: &SelectStmt) -> Option<SelectStmt> {
    if select.rollup || select.group_by.is_empty() {
        return None;
    }
    let having = select.having.as_ref()?;
    if having.has_aggregate_flag() || having.has_window_flag() {
        return None;
    }
    let groups: Vec<Vec<String>> = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            Expr::Column(path) => Some(path.clone()),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let aliases: Vec<String> = select
        .fields
        .fields()
        .iter()
        .filter_map(|field| match field {
            SelectField::Expr {
                alias: Some(alias), ..
            } => Some(alias.clone()),
            _ => None,
        })
        .collect();
    let mut examined = having.clone();
    let mut eligibility = Eligibility {
        groups: &groups,
        aliases: &aliases,
        columns: 0,
        valid: true,
    };
    examined.accept(&mut eligibility);
    if !eligibility.valid || eligibility.columns == 0 {
        return None;
    }

    let mut planned = select.clone();
    planned.where_clause = Some(match planned.where_clause.take() {
        Some(where_clause) => Expr::Binary(
            BinaryOp::LogicAnd,
            Box::new(where_clause),
            Box::new(having.clone()),
        ),
        None => having.clone(),
    });
    Some(planned)
}

struct Eligibility<'a> {
    groups: &'a [Vec<String>],
    aliases: &'a [String],
    columns: usize,
    valid: bool,
}

impl Visitor for Eligibility<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        let Some(expr) = node.downcast_mut::<Expr>() else {
            return false;
        };
        match expr {
            Expr::Column(path) => {
                self.columns += 1;
                let shadowed = path.len() == 1
                    && self
                        .aliases
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(&path[0]));
                if shadowed
                    || !self
                        .groups
                        .iter()
                        .any(|group| same_source_column(path, group))
                {
                    self.valid = false;
                }
                true
            }
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::CompareSubquery { .. } => {
                self.valid = false;
                true
            }
            _ => false,
        }
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

fn same_source_column(left: &[String], right: &[String]) -> bool {
    let exact = left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right));
    exact
        || (left.len() == 1
            && right
                .last()
                .is_some_and(|name| name.eq_ignore_ascii_case(&left[0])))
        || (right.len() == 1
            && left
                .last()
                .is_some_and(|name| name.eq_ignore_ascii_case(&right[0])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::{QueryStmt, Stmt};
    use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode, SessionTimeZone};

    fn select(sql: &str) -> Box<SelectStmt> {
        let Stmt::Query(query) = tidb_parser::parse(sql).expect("valid SELECT") else {
            panic!("expected query");
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected SELECT");
        };
        select
    }

    #[test]
    fn grouped_key_having_is_added_only_to_the_access_view() {
        let select = select("SELECT t.a FROM t WHERE t.a > 1 GROUP BY t.a HAVING t.a < 4");
        let planned = for_access(&select).expect("the grouped key can be pushed");
        assert_ne!(planned.where_clause, select.where_clause);
        assert_eq!(planned.having, select.having);
        assert!(matches!(
            planned.where_clause,
            Some(Expr::Binary(BinaryOp::LogicAnd, _, _))
        ));
    }

    #[test]
    fn aggregate_non_group_alias_subquery_and_rollup_having_stay_above() {
        for sql in [
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
            "SELECT a FROM t GROUP BY a HAVING b > 1",
            "SELECT b AS a FROM t GROUP BY a HAVING a > 1",
            "SELECT a FROM t GROUP BY a HAVING a > (SELECT MIN(a) FROM t)",
            "SELECT a FROM t GROUP BY a WITH ROLLUP HAVING a > 1",
        ] {
            assert!(for_access(&select(sql)).is_none(), "{sql}");
        }
    }

    #[test]
    fn two_group_key_disjunctions_intersect_to_the_one_source_key() {
        let select = select(
            "SELECT t.col_95 FROM t \
             WHERE t.col_95 BETWEEN 'Dyw=*7nigCMh' AND 'Im0*7sZ' \
                OR t.col_95 IN ('58y-j)84-&Y*', 'WNe(rS5uwmvIvFnHw', \
                                'j9FsMawX5uBro%$p', 'C(#EQm@J') \
             GROUP BY t.col_95 \
             HAVING t.col_95 BETWEEN '%^2' AND '38ABfC-' \
                OR t.col_95 BETWEEN 'eKCAE$d2x_hxscj' AND 'zcw35^ATEEp1md=L'",
        );
        let planned = for_access(&select).expect("HAVING uses only the grouped key");
        let column = crate::index_range::RangeColumn::whole(
            "col_95".to_owned(),
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4Bin),
        );
        let ranges = crate::index_range::detach_cond_and_build_range_for_index(
            &[column],
            planned.where_clause.as_ref().expect("combined predicate"),
            &SessionTimeZone::utc(),
        )
        .expect("the grouped key constrains the clustered index");
        assert_eq!(ranges.ranges.len(), 1, "{:?}", ranges.ranges);
        // The ranger converts index-range endpoints to SORT KEYS (Go
        // `convertToSortKey`); a _bin collation's weight string is the value's
        // own bytes.
        assert_eq!(
            ranges.ranges[0].low,
            vec![Datum::Bytes(b"j9FsMawX5uBro%$p".to_vec())]
        );
        assert_eq!(ranges.ranges[0].high, ranges.ranges[0].low);
    }
}
