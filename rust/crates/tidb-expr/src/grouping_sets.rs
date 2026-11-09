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

//! Go `pkg/expression/grouping_sets.go`: the grouping sets an `Expand`
//! replicates its input for (a rollup's, or a 3-stage multi-distinct
//! aggregate's).
//!
//! `GroupingSet.ToPB` / `GroupingSets.ToPB` sit beside
//! `ExpressionsToPBList`, which this port keeps in `physicalop`'s `to_pb.rs`.

use std::collections::{BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};

use tidb_intset::FastIntSet;
use tidb_datatype::FieldTypeFlags;
use tidb_size::{SIZE_OF_INTERFACE, SIZE_OF_POINTER, SIZE_OF_SLICE};

use crate::column::Column;
use crate::explain::{physical_expression_text_with_columns, ExpressionTextStyle};
use crate::expression::Expression;
use crate::schema::Schema;
use crate::simple_expr::extract_columns;

/// Go `GroupingSets`: the grouping sets definition.
#[derive(Clone, Debug, Default)]
pub struct GroupingSets(pub Vec<GroupingSet>);

/// Go `GroupingSet`: one grouping set definition.
#[derive(Clone, Debug, Default)]
pub struct GroupingSet(pub Vec<GroupingExprs>);

/// Go `GroupingExprs`: one grouping-expressions inside a grouping set.
#[derive(Clone, Debug, Default)]
pub struct GroupingExprs(pub Vec<Expression>);

macro_rules! go_slice {
    ($outer:ident, $inner:ty) => {
        impl Deref for $outer {
            type Target = Vec<$inner>;
            fn deref(&self) -> &Vec<$inner> {
                &self.0
            }
        }
        impl DerefMut for $outer {
            fn deref_mut(&mut self) -> &mut Vec<$inner> {
                &mut self.0
            }
        }
    };
}
go_slice!(GroupingSets, GroupingSet);
go_slice!(GroupingSet, GroupingExprs);
go_slice!(GroupingExprs, Expression);

/// Go's `one.(*Column)`: every grouping expression is a column here, and Go
/// panics on the type assertion otherwise.
fn grouping_column(expr: &Expression) -> &Column {
    match expr {
        Expression::Column(column) => column,
        other => panic!("interface conversion: expression.Expression is {other:?}, not *expression.Column"),
    }
}

/// Go `NewGroupingSets`: one grouping set per expression.
#[must_use]
pub fn new_grouping_sets(group_cols: &[Expression]) -> GroupingSets {
    GroupingSets(group_cols.iter().map(|g_col| GroupingSet(vec![GroupingExprs(vec![g_col.clone()])])).collect())
}

impl GroupingSets {
    /// Go `Merge`: explores the internal grouping expressions and tries to
    /// find the minimum grouping sets (prefix merging): `(a,b,c)`, `(a,b)`
    /// and `(a)` merge into one set `[[a],[a,b],[a,b,c]]`.
    #[must_use]
    pub fn merge(&self) -> GroupingSets {
        let mut new_grouping_sets = GroupingSets(Vec::with_capacity(self.len()));
        for one_grouping_set in self.iter() {
            for one_grouping_expr in one_grouping_set.iter() {
                if new_grouping_sets.is_empty() {
                    // means there is nothing in new grouping sets, adding current one anyway.
                    new_grouping_sets.push(new_grouping_set(one_grouping_expr.clone()));
                    continue;
                }
                new_grouping_sets = new_grouping_sets.merge_one(one_grouping_expr.clone());
            }
        }
        new_grouping_sets
    }

    /// Go `MergeOne`: merges one grouping expressions into the grouping sets,
    /// at the position where every previous one is a subset of the latter
    /// one, or as a new grouping set.
    #[must_use]
    pub fn merge_one(mut self, target_one: GroupingExprs) -> GroupingSets {
        for i in 0..self.len() {
            let one_new_grouping_set = &self[i];
            // for every group set, try to find its position to insert if possible, otherwise create a new grouping set.
            for j in (0..one_new_grouping_set.len()).rev() {
                let cur = &one_new_grouping_set[j];
                if target_one.sub_set_of(cur) {
                    if j == 0 {
                        // the right pos should be the head (-1)
                        self[i].insert(0, target_one);
                        return self;
                    }
                    // do the left shift to find the right insert pos.
                    continue;
                }
                if j == one_new_grouping_set.len() - 1 {
                    // which means the targetOne itself is the super set of current right-most grouping set.
                    if cur.sub_set_of(&target_one) {
                        // the right pos should be the len(oneNewGroupingSet)
                        self[i].push(target_one);
                        return self;
                    }
                    // which means the targetOne can't fit itself in this grouping set, continue next grouping set.
                    break;
                }
                // successfully fit in, current j is the right pos to insert.
                self[i].insert(j + 1, target_one);
                return self;
            }
        }
        // here means we couldn't find even one GroupingSet to fill the targetOne, creating a new one.
        self.push(new_grouping_set(target_one));
        self
    }

    /// Go `TargetOne`: the grouping set a normal aggregate reads, the first
    /// whose null-filled columns are not among its arguments' columns; `-1`
    /// when there is none.
    #[must_use]
    pub fn target_one(&self, normal_agg_args: &[Expression]) -> i32 {
        let column_in_normal_agg_args: Vec<Column> = normal_agg_args.iter().flat_map(extract_columns).collect();
        if column_in_normal_agg_args.is_empty() {
            // no column in normal agg. eg: count(1), specify the default grouping set ID 0+1.
            return 0;
        }
        // for other normal agg args like: count(a), count(a+b), count(not(a is null)) and so on.
        let normal_agg_args_id_set = FastIntSet::new(column_in_normal_agg_args.iter().map(|one| one.unique_id));

        // identify the suitable grouping set for normal agg.
        let all_grouping_col_ids = self.all_sets_col_ids();
        for (idx, grouping_set) in self.iter().enumerate() {
            // diffCols are those columns being filled with null in the group row data of current grouping set.
            let diff_cols = all_grouping_col_ids.difference(&grouping_set.all_col_ids());
            if diff_cols.intersects(&normal_agg_args_id_set) {
                // normal agg's target arg columns are being filled with null value in this grouping set, continue next grouping set check.
                continue;
            }
            return idx as i32;
        }
        -1
    }

    /// Go `NeedCloneColumn`: whether a column is in two grouping sets, so it
    /// must be copied when expanding the data source.
    #[must_use]
    pub fn need_clone_column(&self) -> bool {
        let set_ids: Vec<FastIntSet> = self.iter().map(GroupingSet::all_col_ids).collect();
        for (idx, one_set_ids) in set_ids.iter().enumerate() {
            for other_set_ids in &set_ids[idx + 1..] {
                if one_set_ids.intersects(other_set_ids) {
                    return true;
                }
            }
        }
        false
    }

    /// Go `IsEmpty`.
    #[must_use]
    pub fn is_empty_sets(&self) -> bool {
        self.iter().all(GroupingSet::is_empty_set)
    }

    /// Go `AllSetsColIDs`: the union of every set's column ids.
    #[must_use]
    pub fn all_sets_col_ids(&self) -> FastIntSet {
        let mut res = FastIntSet::default();
        for grouping_set in self.iter() {
            res.union_with(&grouping_set.all_col_ids());
        }
        res
    }

    /// Go `StringWithCtx`.
    #[must_use]
    pub fn string_with_ctx(&self, ctx: &dyn crate::Columns) -> String {
        format!("[{}]", self.iter().map(|gs| gs.string_with_ctx(ctx)).collect::<Vec<_>>().join(","))
    }

    /// Go `DistinctSize`: [`Self::distinct_size_with_threshold`] at 64.
    #[must_use]
    pub fn distinct_size(&self) -> (usize, Option<Vec<u64>>, Option<HashMap<i64, BTreeSet<u64>>>) {
        self.distinct_size_with_threshold(64)
    }

    /// Go `DistinctSizeWithThreshold`: the number of distinct grouping sets;
    /// over `n` of them, also each set's incrementally allocated grouping id
    /// and, per column, the ids of the sets it is in.
    #[must_use]
    pub fn distinct_size_with_threshold(
        &self,
        n: usize,
    ) -> (usize, Option<Vec<u64>>, Option<HashMap<i64, BTreeSet<u64>>>) {
        let mut distinct_grouping_ids_pos: Vec<usize> = Vec::with_capacity(self.len());
        let mut origin_grouping_ids_slice: Vec<FastIntSet> = Vec::with_capacity(self.len());

        for one_grouping_set in self.iter() {
            let cur_ids = one_grouping_set.all_col_ids();
            let duplicate =
                distinct_grouping_ids_pos.iter().any(|offset| origin_grouping_ids_slice[*offset].equals(&cur_ids));
            if !duplicate {
                distinct_grouping_ids_pos.push(origin_grouping_ids_slice.len());
            }
            origin_grouping_ids_slice.push(cur_ids);
        }
        if distinct_grouping_ids_pos.len() > n {
            let mut gids = vec![0_u64; origin_grouping_ids_slice.len()];
            for (gid, offset) in distinct_grouping_ids_pos.iter().enumerate() {
                let one_distinct_set_ids = &origin_grouping_ids_slice[*offset];
                for (idx, one_origin_set_ids) in origin_grouping_ids_slice.iter().enumerate() {
                    if one_distinct_set_ids.equals(one_origin_set_ids) {
                        gids[idx] = gid as u64;
                    }
                }
            }

            let all_ids = self.all_sets_col_ids();
            let mut id2_gids = HashMap::with_capacity(all_ids.len());
            all_ids.for_each(|i| {
                let collection_map: BTreeSet<u64> = origin_grouping_ids_slice
                    .iter()
                    .enumerate()
                    .filter(|(_, one_origin_set_ids)| one_origin_set_ids.has(i))
                    .map(|(idx, _)| gids[idx])
                    .collect();
                id2_gids.insert(i, collection_map);
            });
            return (distinct_grouping_ids_pos.len(), Some(gids), Some(id2_gids));
        }
        (distinct_grouping_ids_pos.len(), None, None)
    }
}

impl GroupingSet {
    /// Go `IsEmpty`.
    #[must_use]
    pub fn is_empty_set(&self) -> bool {
        self.iter().all(|g| g.is_empty())
    }

    /// Go `AllColIDs`: every grouping column's unique id (each grouping
    /// expression is a single column).
    #[must_use]
    pub fn all_col_ids(&self) -> FastIntSet {
        let mut res = FastIntSet::default();
        for grouping_exprs in self.iter() {
            for one in grouping_exprs.iter() {
                res.insert(grouping_column(one).unique_id);
            }
        }
        res
    }

    /// Go `ExtractCols`: the grouping set's columns appended to `cols`.
    pub fn extract_cols(&self, cols: &mut Vec<Column>) {
        for grouping_exprs in self.iter() {
            for one in grouping_exprs.iter() {
                cols.push(grouping_column(one).clone());
            }
        }
    }

    /// Go `StringWithCtx`.
    #[must_use]
    pub fn string_with_ctx(&self, ctx: &dyn crate::Columns) -> String {
        format!("{{{}}}", self.iter().map(|one| one.string_with_ctx(ctx)).collect::<Vec<_>>().join(","))
    }

    /// Go `MemoryUsage`.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        SIZE_OF_SLICE + self.capacity() as i64 * SIZE_OF_POINTER + self.iter().map(GroupingExprs::memory_usage).sum::<i64>()
    }
}

/// Go `newGroupingSet`.
fn new_grouping_set(one_grouping_expr: GroupingExprs) -> GroupingSet {
    GroupingSet(vec![one_grouping_expr])
}

impl GroupingExprs {
    /// Go `SubSetOf`: whether these columns are a subset of `other`'s.
    #[must_use]
    pub fn sub_set_of(&self, other: &GroupingExprs) -> bool {
        self.id_set().subset_of(&other.id_set())
    }

    /// Go `IDSet`.
    #[must_use]
    pub fn id_set(&self) -> FastIntSet {
        FastIntSet::new(self.iter().map(|one| grouping_column(one).unique_id))
    }

    /// Go `StringWithCtx`.
    #[must_use]
    pub fn string_with_ctx(&self, ctx: &dyn crate::Columns) -> String {
        format!(
            "<{}>",
            self.iter()
                .map(|one| {
                    physical_expression_text_with_columns(ctx, one, &[], ExpressionTextStyle::StringWithCtx)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    /// Go `MemoryUsage`.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        SIZE_OF_SLICE + self.capacity() as i64 * SIZE_OF_INTERFACE + self.iter().map(Expression::memory_usage).sum::<i64>()
    }
}

/// Go `RollupGroupingSets`: `<a,b,c>` becomes `{}, {a}, {a,b}, {a,b,c}`,
/// every prefix of the rollup expressions.
#[must_use]
pub fn rollup_grouping_sets(rollup_exprs: &[Expression]) -> GroupingSets {
    GroupingSets(
        (0..=rollup_exprs.len())
            .map(|len| new_grouping_set(GroupingExprs(rollup_exprs[..len].to_vec())))
            .collect(),
    )
}

/// Go `AdjustNullabilityFromGroupingSets`: a grouping column missing from
/// some grouping set is filled with null there, so it loses `NotNull`.
pub fn adjust_nullability_from_grouping_sets(gss: &GroupingSets, schema: &mut Schema) {
    let grouping_ids = gss.all_sets_col_ids();
    let grouping_ids_slice: Vec<FastIntSet> = gss.iter().map(GroupingSet::all_col_ids).collect();
    for col in &mut schema.columns {
        if !grouping_ids.has(col.unique_id) {
            continue;
        }
        if grouping_ids_slice.iter().any(|one_set_ids| !one_set_ids.has(col.unique_id)) {
            if let Some(ret_type) = col.ret_type.as_mut() {
                ret_type.del_flag(FieldTypeFlags::NOT_NULL);
            }
        }
    }
}

/// Go `DeduplicateGbyExpression`: the distinct group-by expressions (by
/// `CanonicalHashCode`), plus for each original item the position of its
/// representative.
#[must_use]
pub fn deduplicate_gby_expression(exprs: &[Expression]) -> (Vec<Expression>, Vec<usize>) {
    let mut distinct_exprs = Vec::with_capacity(exprs.len());
    let mut distinct_map: HashMap<Vec<u8>, usize> = HashMap::with_capacity(exprs.len());
    let mut pos_slice = Vec::with_capacity(exprs.len());
    for one in exprs {
        let key = one.canonical_hash_code();
        let pos = *distinct_map.entry(key).or_insert_with(|| {
            distinct_exprs.push(one.clone());
            distinct_exprs.len() - 1
        });
        pos_slice.push(pos);
    }
    (distinct_exprs, pos_slice)
}

/// Go `RestoreGbyExpression`: the group-by list rebuilt over the projected
/// columns.
#[must_use]
pub fn restore_gby_expression(exprs: &[Column], idxes: &[usize]) -> Vec<Expression> {
    idxes.iter().map(|pos| Expression::Column(exprs[*pos].clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn col(unique_id: i64) -> Expression {
        let mut column = Column::default();
        column.unique_id = unique_id;
        Expression::Column(column)
    }

    fn plus(left: Expression, right: Expression) -> Expression {
        Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![left, right],
        ))
    }

    fn longlong(value: i64) -> Expression {
        Expression::Constant(crate::constant::Constant::new(
            tidb_datatype::Datum::Int(value),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn sets(groups: &[&[i64]]) -> GroupingSets {
        GroupingSets(
            groups
                .iter()
                .map(|ids| GroupingSet(vec![GroupingExprs(ids.iter().map(|id| col(*id)).collect())]))
                .collect(),
        )
    }

    /// Go `TestGroupSetsTargetOne`.
    #[test]
    fn test_group_sets_target_one() {
        let (a, b, c, d) = (col(1), col(2), col(3), col(4));
        let new_grouping_sets = sets(&[&[1, 2], &[3]]);
        assert_eq!(new_grouping_sets.target_one(&[d]), 0); // <a,b> and <c> both ok
        assert_eq!(new_grouping_sets.target_one(&[c.clone()]), 1); // only <c> is ok
        assert_eq!(new_grouping_sets.target_one(&[b.clone()]), 0); // only <a,b> is ok
        assert_eq!(new_grouping_sets.target_one(&[a]), 0); // only <a,b> is ok
        assert_eq!(new_grouping_sets.target_one(&[b, c]), -1); // no valid one can be found.
    }

    /// Go `TestGroupSetsTargetOneCompoundArgs`.
    #[test]
    fn test_group_sets_target_one_compound_args() {
        let (a, c, d) = (col(1), col(3), col(4));
        let new_grouping_sets = sets(&[&[1, 2], &[3]]);
        // mock normal agg count(1)
        assert_eq!(new_grouping_sets.target_one(&[longlong(1)]), 0); // default
        // mock normal agg count(d+1)
        assert_eq!(new_grouping_sets.target_one(&[plus(d.clone(), longlong(1))]), 0); // default
        // mock normal agg count(d+c)
        assert_eq!(new_grouping_sets.target_one(&[plus(d.clone(), c.clone())]), 1); // only {c} can supply d and c
        // mock normal agg count(d+a)
        assert_eq!(new_grouping_sets.target_one(&[plus(d.clone(), a.clone())]), 0); // only {a,b} can supply d and a
        // mock normal agg count(d+a+c)
        assert_eq!(new_grouping_sets.target_one(&[plus(d, plus(a, c))]), -1);
    }

    /// Go `TestGroupingSetsMergeOneUnitTest`.
    #[test]
    fn test_grouping_sets_merge_one_unit_test() {
        let (a, b, c, d) = (col(1), col(2), col(3), col(4));
        // test case about the right most fitness.
        let new_grouping_sets = sets(&[&[1]]).merge_one(GroupingExprs(vec![a.clone(), b.clone()]));
        assert_eq!(new_grouping_sets.len(), 1);
        assert_eq!(new_grouping_sets[0].len(), 2);
        assert_eq!(new_grouping_sets[0][0].len(), 1); // {a}
        assert_eq!(new_grouping_sets[0][1].len(), 2); // {a,b}

        let new_grouping_sets = sets(&[&[1]])
            .merge_one(GroupingExprs(vec![d.clone(), c, b.clone(), a.clone()]))
            .merge_one(GroupingExprs(vec![d, b.clone(), a.clone()]))
            .merge_one(GroupingExprs(vec![b, a]));
        assert_eq!(new_grouping_sets.len(), 1);
        assert_eq!(new_grouping_sets[0].len(), 4);
        assert_eq!(new_grouping_sets[0][0].len(), 1); // {a}
        assert_eq!(new_grouping_sets[0][1].len(), 2); // {b,a}
        assert_eq!(new_grouping_sets[0][2].len(), 3); // {d,b,a}
        assert_eq!(new_grouping_sets[0][3].len(), 4); // {d,c,b,a}
    }

    /// Go `TestRollupGroupingSets`.
    #[test]
    fn test_rollup_grouping_sets() {
        let not_null_col = |unique_id| {
            let mut tp = FieldType::new(FieldTypeCode::Long);
            tp.set_flag(FieldTypeFlags::NOT_NULL);
            Column::new(unique_id, tp)
        };
        let (a, b, c, d) = (not_null_col(1), not_null_col(2), not_null_col(3), not_null_col(4));
        let rollup_exprs: Vec<Expression> =
            [&a, &b, &c].into_iter().map(|one| Expression::Column(one.clone())).collect();
        let rollup_grouping_sets = rollup_grouping_sets(&rollup_exprs);
        assert_eq!(rollup_grouping_sets.len(), 4);
        for (i, set) in rollup_grouping_sets.iter().enumerate() {
            assert_eq!(set.len(), 1);
            assert_eq!(set[0].len(), i);
        }

        let mut expand_schema = Schema::new(vec![a, b, c, d]);
        let mut expand_schema2 = expand_schema.clone();
        // remove the first grouping set {}
        // so the {a,b,c},{a,b},{a}, a is every grouping set, and it should be changed as nullable.
        let rollup_grouping_sets2 = GroupingSets(rollup_grouping_sets[1..].to_vec());
        adjust_nullability_from_grouping_sets(&rollup_grouping_sets2, &mut expand_schema2);
        let not_null = |schema: &Schema, i: usize| {
            schema.columns[i].ret_type.as_ref().unwrap().has_flag(FieldTypeFlags::NOT_NULL)
        };
        assert!(not_null(&expand_schema2, 0));
        assert!(!not_null(&expand_schema2, 1));
        assert!(!not_null(&expand_schema2, 2));
        assert!(not_null(&expand_schema2, 3));

        adjust_nullability_from_grouping_sets(&rollup_grouping_sets, &mut expand_schema);
        assert!(!not_null(&expand_schema, 0));
        assert!(!not_null(&expand_schema, 1));
        assert!(!not_null(&expand_schema, 2));
        assert!(not_null(&expand_schema, 3));
    }

    /// Go `TestGroupingSetsMergeUnitTest`.
    #[test]
    fn test_grouping_sets_merge_unit_test() {
        // [[c,d,a,b], [b], [a,b]]
        let new_grouping_sets = sets(&[&[3, 4, 1, 2], &[2], &[1, 2]]).merge();
        // [[b], [a,b], [c,d,a,b]]
        assert_eq!(new_grouping_sets.len(), 1);
        // only one grouping set with 3 grouping expressions.
        assert_eq!(new_grouping_sets[0].len(), 3);
        assert_eq!(new_grouping_sets[0][0].len(), 1); // [b]
        assert_eq!(new_grouping_sets[0][1].len(), 2); // [a,b]
        assert_eq!(new_grouping_sets[0][2].len(), 4); // [c,d,a,b]]

        //         [[a],[b,a],[c,b,a],]
        //         [[d],]
        let new_grouping_sets = sets(&[&[3, 2, 1], &[1], &[2, 1], &[4]]).merge();
        assert_eq!(new_grouping_sets.len(), 2);
        assert_eq!(new_grouping_sets[0][0].len(), 1); // [a]
        assert_eq!(new_grouping_sets[0][1].len(), 2); // [b,a]
        assert_eq!(new_grouping_sets[0][2].len(), 3); // [c,b,a]
        assert_eq!(new_grouping_sets[1][0].len(), 1); // [d]
    }

    /// Go `TestDistinctGroupingSets`.
    #[test]
    fn test_distinct_grouping_sets() {
        let ctx = crate::NoColumns;
        let text = |expr: &Expression| {
            physical_expression_text_with_columns(&ctx, expr, &[], ExpressionTextStyle::StringWithCtx).unwrap()
        };
        let (a, b, c, d) = (col(1), col(2), col(3), col(4));
        // case1: non-duplicated case.
        let raw_rollup_exprs = vec![a.clone(), b.clone(), c.clone(), d];
        let (deduplicate_exprs, pos) = deduplicate_gby_expression(&raw_rollup_exprs);
        // nothing to deduplicate.
        assert_eq!(raw_rollup_exprs.len(), deduplicate_exprs.len());
        assert_eq!(pos, [0, 1, 2, 3]);

        // rollup grouping sets.
        let gss = rollup_grouping_sets(&deduplicate_exprs);
        assert_eq!(gss.len(), 5); // len = N + 1 (N = len(raw rollup expression))
        let ids: Vec<String> = gss.iter().map(|gs| gs.all_col_ids().to_string()).collect();
        assert_eq!(ids, ["()", "(1)", "(1,2)", "(1-3)", "(1-4)"]);
        assert_eq!(gss.distinct_size().0, 5);

        // case2: duplicated case.
        let raw_rollup_exprs = vec![a, b.clone(), b, c];
        let (deduplicate_exprs, pos) = deduplicate_gby_expression(&raw_rollup_exprs);
        assert_eq!(deduplicate_exprs.iter().map(text).collect::<Vec<_>>(), ["Column#1", "Column#2", "Column#3"]);
        let deduplicate_columns: Vec<Column> =
            deduplicate_exprs.iter().map(|one| grouping_column(one).clone()).collect();
        assert_eq!(pos, [0, 1, 1, 2]);

        // restoreGbyExpression (some complicated expression will be projected as column before enter Expand)
        let restore_gby_expressions = restore_gby_expression(&deduplicate_columns, &pos);
        assert_eq!(
            restore_gby_expressions.iter().map(text).collect::<Vec<_>>(),
            ["Column#1", "Column#2", "Column#2", "Column#3"]
        );

        // rollup grouping sets (build grouping sets on the restored gby expression).
        let gss = rollup_grouping_sets(&restore_gby_expressions);
        assert_eq!(gss.len(), 5);
        let ids: Vec<String> = gss.iter().map(|gs| gs.all_col_ids().to_string()).collect();
        assert_eq!(ids, ["()", "(1)", "(1,2)", "(1,2)", "(1-3)"]);

        // after gss is built on all the simple columns, get the distinct size if possible.
        assert_eq!(gss.distinct_size().0, 4);

        // case3: when grouping sets number bigger than 64, we will allocate gid incrementally rather bit map.
        let (size, gids, id2_gids) = gss.distinct_size_with_threshold(3);
        assert_eq!(size, 4);
        let gids = gids.unwrap();
        assert_eq!(gids.len(), gss.len());
        assert_eq!(gids, [0, 1, 2, 2, 3]);

        // for every col id, mapping them to a slice of gid.
        let id2_gids = id2_gids.unwrap();
        assert_eq!(id2_gids.len(), 3);
        // when grouping(a), col-a is needed when grouping id = 1,2,3.
        assert_eq!(id2_gids[&1].len(), 3);
        assert!([1, 2, 3].iter().all(|gid| id2_gids[&1].contains(gid)));
        // when grouping(b), col-b is needed when grouping id = 2,3.
        assert_eq!(id2_gids[&2].len(), 2);
        assert!([2, 3].iter().all(|gid| id2_gids[&2].contains(gid)));
        // when grouping(c), col-c is needed when grouping id = 3.
        assert_eq!(id2_gids[&3].len(), 1);
        assert!(id2_gids[&3].contains(&3));
    }
}
