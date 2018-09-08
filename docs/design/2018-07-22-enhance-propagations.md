# Proposal: Enhance constraint propagation in TiDB logical plan

- Author(s):     [@bb7133](https://github.com/bb7133), [@zz-jason](https://github.com/zz-jason) 
- Last updated:  2018-07-22
- Discussion at: https://docs.google.com/document/d/1G3wVBaiza9GI5q9nwHLCB2I7r1HUbg6RpTNFpQWTFhQ/edit#

## Abstract

This proposal tries to illustrate some rules that can be added to current constraint/filter propgation optimizations in TiDB logcial plan.

## Background

For now, most of the constraint propagation work in TiDB is done by `propagateConstantSolver`, it does:

1. Find `column = constant` expression and substitue the constant for column, as well as try to fold the substituted constant expression if possible, for example:

 Given `a = b and a = 2 and b = 3`, it becomes `2 = 3` after substitution and lead to a final `false` constant.

2. Find `column A = column B` expression(which happens in `join` statements mostly) and propagate expressions like `column op constant`(as well as `constant op column`) based on the equliaty relation, the supported operators are:

 * `ast.LT('<')`
 * `ast.GT('>')`
 * `ast.LE('<=')`
 * `ast.GE('>=')`
 * `ast.NE('!=')`

The `propagateConstantSolver` makes more detailed/explicit filters/constraints, which can be used within other optimization rule. For example, in predicate-pushdown optimization, it generates more predicates that can be pushed closer to the data source(TiKV), and thus reduce the amount of data in the whole data path.

We can further do the optimization by introduce more rules and infer/propagate more constraints from the existings ones, which helps us building better logical plan.

## Proposal

Here are proposed rules we can consider:

1. Infer more filters/constraints from column equality relation

 We should be able to infer set of data constraints based on column equality, that is, any constraint on `a` now applies to `b` as long as:

 * The constraint is deterministic
 * The constraint doesn’t have any side effect

 For example, 

 `t1.a = t2.a and t1.a < 5` => `t2.a < 5`

 `t1.a = t2.a and t1.a in (12, 13) and t2.a in (14, 15)` => `t1.a in (14, 15) and t2.a in (12, 13)`

 `t1.a = t2.a and cast(t1.a, varchar(20) rlike 'abc'` => `cast(t1.a, varchar(20)) rlike 'abc'`
  
 Equality propagation should also be included:

 `t1.a = t2.a and cast(t1.a, varchar(20) = 5` and` => `cast(t2.a, varchar(20)) = 5`

 But following predicates cannot be propagated:

 `t1.a = t2.a and t1.a < random()` -- the expression is non-deterministic

 `t1.a = t2.a and t1.a < sleep()` -- the expression has side effect

2. Infer NotNULL filters from comparison operator

 We can infer `NotNULL` constraints from a comparison operator that doesn’t accept NULL, then we can know that related columns cannot be NULL and add `NotNull` filter to them:

 `t1.a = t2.a` => `not(isnull(a)) and not(isnull(b))`

 `t1.a != t2.a` => `not(isnull(a)) and not(isnull(b))`

 `a < 5` => `not(isnull(a))`

 NOTE: Those columns should not have `NotNULL` constraint attribute, or the inferred filters are unnecessary.

3. Fold the constraints based their semantics to avoid redundancy 

 After the propagations we may produce some duplicates predicates, and they can be combined. We should analyze all of the conditions and try to make the predicates clean:

 `a = b and b = a` -> `a = b`

 `a < 3 and 3 > a` -> `a < 3`

 `a < 5 and a > 5` -> `False`

 `a < 10 and a <= 5` => `a <= 5`

 `isnull(a) and not(isnull(a))` => `False`

 `a < 3 or a >= 3` => `True`

 `a in (1, 2) and a in (3, 5)` => `False`

 `a in (1, 2) or a in (3, 5)` => `a in (1, 2, 3, 5)`

4. Filter propagation for outer join

 When doing an equality outer join, we can propagate predicates on outer table in `where` condition to the inner table in `on` condition.
 For example:

 `select * from t1 left join t2 on t1.a=t2.a where t1.a in (12, 13);`

```
TiDB(localhost:4000) > desc select * from t1 left join t2 on t1.a=t2.a where t1.a in (12, 13);
+-------------------------+----------+------+-------------------------------------------------------------------------+
| id                      | count    | task | operator info                                                           |
+-------------------------+----------+------+-------------------------------------------------------------------------+
| HashLeftJoin_7          | 25.00    | root | left outer join, inner:TableReader_12, equal:[eq(test.t1.a, test.t2.a)] |
| ├─TableReader_10        | 20.00    | root | data:Selection_9                                                        |
| │ └─Selection_9         | 20.00    | cop  | in(test.t1.a, 12, 13)                                                   |
| │   └─TableScan_8       | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo             |
| └─TableReader_12        | 10000.00 | root | data:TableScan_11                                                       |
|   └─TableScan_11        | 10000.00 | cop  | table:t2, range:[-inf,+inf], keep order:false, stats:pseudo             |
+-------------------------+----------+------+-------------------------------------------------------------------------+
6 rows in set (0.00 sec)
```

 NOTE: in this case, `t1.a in (12, 13)` works on the result of the outer join and we have pushed it down to the outer table.

 But we can further push this filter down to the inner table, since only the the records satisfy `t2.a in (12, 13)` could make join predicate `t1.a = t2.a` be positive in the join operator. So we can optimize this query to:

 `select * from t1 left join t2 on t1.a=t2.a and t2.a in (12, 13) where t1.a in (12, 13);`

And the join predicate `t2.a in (12, 13)` can be pushed down:

```
TiDB(localhost:4000) > desc select * from t1 left join t2 on t1.a=t2.a and t2.a in (12, 13) where t1.a in (12, 13);
+-------------------------+-------+------+-------------------------------------------------------------------------+
| id                      | count | task | operator info                                                           |
+-------------------------+-------+------+-------------------------------------------------------------------------+
| HashLeftJoin_7          | 0.00  | root | left outer join, inner:TableReader_13, equal:[eq(test.t1.a, test.t2.a)] |
| ├─TableReader_10        | 0.00  | root | data:Selection_9                                                        |
| │ └─Selection_9         | 0.00  | cop  | in(test.t1.a, 12, 13)                                                   |
| │   └─TableScan_8       | 2.00  | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo             |
| └─TableReader_13        | 0.00  | root | data:Selection_12                                                       |
|   └─Selection_12        | 0.00  | cop  | in(test.t2.a, 12, 13)                                                   |
|     └─TableScan_11      | 2.00  | cop  | table:t2, range:[-inf,+inf], keep order:false, stats:pseudo             |
+-------------------------+-------+------+-------------------------------------------------------------------------+
7 rows in set (0.00 sec)
```

## Rationale

Constraint propagation is commonly used as logcial plan optimization in traditional databases, for example, [this doc](https://dev.mysql.com/doc/internals/en/optimizer-constant-propagation.html) explained some details of constant propagtions in MySQL. It is is also widely adopted in distributed analytical engines, like Apache Hive, Apache SparkSQL, Apache Impala, et al, those engines usually query on huge amount of data.

### Advantages:

 Constraint propagation brings more detailed, explicit constraints to each data source involved in a query. With those constraints we can filter data as early as possible, and thus reduce disc/network IO and computational overheads during the execution of a query. In TiDB, most propagated filters can be pushed down to the storage level(TiKV) as a coprocessor task, and lead to the following benefits:

 * Apply the filters at each TiKV instance, which make the calculation distributed.

 * When loading data, skip some table partitions if its data range doesn't pass the filter

 * For the columnar storage format to be supported in the future, we may apply some filters directly when accessing the raw storage.

 * Reduce the data transfered from TiKV to TiDB

### Disadvantages:

 Constraint propagation may brings unnecessary filters and leads to unnecessary overheads during a query, this is mostly due to the fact that logical optimization doesn't take data statistics into account, for example:

 For a query `select * from t0, t1 on t0.a = t1.a where t1.a < 5`, we get a propagation `t0.a < 5`, but if all `t0.a` is greater than 5, applying the filter brings unnecessary overheads.

Considering the trade-off, most of the time we gain benefits from constraint propagation and still treat it useful.

## Compatibility

All rules mentioned in this proposal are logical plan optimization, they should not change the semantic of a query, and thus dont't lead to any compatibility issue

## Implementation

Here are rough ideas about possible implementations:

 * For proposal #1, we can extend current `propagateConstantSolver` to support wider types of operators from column equiality.

 * For proposal #2, `propagateConstantSolver` is also a applicable way to add `NotNULL` filter(`not(isnull())`), but should examine if the column doesn't have `NotNULL` constraint.

 * For proposal #3, the [ranger](https://github.com/pingcap/tidb/blob/6fb1a637fbfd41b2004048d8cb995de6442085e2/util/ranger/ranger.go#L14) may be useful to help us collecting and folding comparison constraints

 * For proposal #4, current rule [PredicatePushDown](https://github.com/pingcap/tidb/blob/b3d4ed79b978efadf2974f78db8eeb711509e545/plan/rule_predicate_push_down.go#L1) may be enhanced to archive it

## Open issues (if applicable)

Related issues:

 https://github.com/pingcap/tidb/issues/7098 - the very issue that inspires this proposal

Related PRs:

 https://github.com/pingcap/tidb/pull/7276 - Related to proposal #1 

 https://github.com/pingcap/tidb/pull/7643 - Related to proposal #1 and #3

