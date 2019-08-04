# Proposal: Enhance constraint propagation in TiDB logical plan

- Author(s):     [@bb7133](https://github.com/bb7133), [@zz-jason](https://github.com/zz-jason) 
- Last updated:  2018-07-22
- Discussion at: https://docs.google.com/document/d/1G3wVBaiza9GI5q9nwHLCB2I7r1HUbg6RpTNFpQWTFhQ/edit#

## Abstract

This proposal tries to illustrate some rules that can be added to current constraint/filter propgation optimizations in TiDB logcial plan.

## Background

Currently, most of the constraint propagation work in TiDB is done by `propagateConstantSolver`, which does:

1. Find the `column = constant` expression and substitute the constant for the column, as well as try to fold the substituted constant expression if possible, for example:

 Given `a = b and a = 2 and b = 3`, it becomes `2 = 3` after substitution and leads to a final `false` constant.

2. Find the `column A = column B` expression (which happens in `join` statements mostly) and propagate expressions like `column op constant` (as well as `constant op column`) based on the equality relation. The supported operators are:

 * `ast.LT('<')`
 * `ast.GT('>')`
 * `ast.LE('<=')`
 * `ast.GE('>=')`
 * `ast.NE('!=')`

The `propagateConstantSolver` makes more detailed/explicit filters/constraints, which can be used within other optimization rules. For example, in the predicate-pushdown optimization, it generates more predicates that can be pushed closer to the data source (TiKV), and thus reduces the amount of data in the whole data path.

We can further do the optimization by introducing more rules and inferring/propagating more constraints from the existing ones, which helps us build a better logical plan.

## Proposal

Here are proposed rules we can consider:

1. Infer more filters/constraints from column equality relation

 We should be able to infer a set of data constraints based on column equality, that is, any constraint on `a` now applies to `b` as long as:

 * The constraint is deterministic
 * The constraint doesn’t have any side effect
 * The constraint doesn't include `isnull()` expression: `isnull()` constraint cannot be propagated through equality
 * The constraint doesn't include `cast()`/`convert()` expression: type cast may break the equality relation

 For example, 

| original expressions                       | propagated filters               |
| --------------------                       | ------------------               |
| a = b and a < 5                            | b < 5                            |
| a = b and a in (12, 13) and b in (14, 15)  | a in (14, 15) and b in (12, 13)  |
| a = b and cast(a, varchar(20)) rlike 'abc' | cast(b, varchar(20)) rlike 'abc' |
  
 Equality propagation should also be included:

| original expressions | propagated filters |
| -------------------- | ------------------ |
| a = b and abs(a) = 5 | abs(b) = 5         |

 But following predicates cannot be propagated:

| unpropagateable expressions            | reason                                    |
| ---------------------------            | ------                                    |
| a = b and a < random()                 | the expression is non-deterministic       |
| a = b and a < sleep()                  | the expression has side effect            |
| a = b and isnull(a)                    | isnull() cannot be propagated             |
| a = b and cast(a as char(10)) = '+0.0' | type cast expression cannot be propagated |

2. Infer `NotNULL` filters from null-rejected scalar expression

 We can infer `NotNULL` constraints from a scalar expression that doesn’t accept NULL, then we can know that involved columns cannot be NULL and add `NotNull` filter to them:

| original expressions | inferred filters                  |
| -------------------- | ----------------                  |
| a = b                | not(isnull(a)) and not(isnull(b)) |
| a != b               | not(isnull(a)) and not(isnull(b)) |
| a < 5                | not(isnull(a))                    |
| abs(a) < 3           | not(isnull(a))                    |

 NOTE: Those columns should not have `NotNULL` constraint attribute, or the inferred filters are unnecessary.

3. Fold the constraints based on their semantics to avoid redundancy

 After the propagations we may produce some duplicate predicates, and they can be combined. We should analyze all of the conditions and try to make the predicates clean:

| original expressions         | combined expression |
| --------------------         | ------------------  |
| a = b and b = a              | a = b               |
| a < 3 and 3 > a              | a < 3               |
| a < 5                        | not(isnull(a))      |
| a < 5 and a > 5              | false               |
| a < 10 and a <= 5            | a <= 5              |
| isnull(a) and not(isnull(a)) | false               |
| a < 3 or a >= 3              | true                |
| a in (1, 2) and a in (3, 5)  | false               |
| a in (1, 2) or a in (3, 5)   | a in (1, 2, 3, 5)   |

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

 But we can further push this filter down to the inner table, since only the records satisfying `t2.a in (12, 13)` can make join predicate `t1.a = t2.a` positive in the join operator. So we can optimize this query to:

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

Constraint propagation is commonly used as logical plan optimization in traditional databases. For example, [this doc](https://dev.mysql.com/doc/internals/en/optimizer-constant-propagation.html) explains some details of constant propagations in MySQL. It is also widely adopted in distributed analytical engines, like Apache Hive, Apache SparkSQL, Apache Impala, et al. Those engines usually query on a huge amount of data.

### Advantages:

 Constraint propagation brings more detailed, explicit constraints to each data source involved in a query. With those constraints, we can filter data as early as possible, and thus reduce disk/network I/O and computational overheads during the execution of a query. In TiDB, most propagated filters can be pushed down to the storage level (TiKV), as a Coprocessor task, and leads to the following benefits:

 * Apply the filters at each storage segment (Region), which make the calculation distributed.

 * When loading data, skip some partitions of a table if the partitioning expression doesn't pass the filter.

 * For the columnar storage format to be supported in the future, we may apply some filters directly when accessing the raw storage.

 * Reduce the data transferred from TiKV to TiDB.

### Disadvantages:

 Constraint propagation may bring unnecessary filters and lead to unnecessary overheads during a query. This is mostly due to the fact that logical optimization doesn't take data statistics into account, for example:

 For a query `select * from t0, t1 on t0.a = t1.a where t1.a < 5`, we get a propagation `t0.a < 5`, but if all `t0.a` is greater than 5, applying the filter brings unnecessary overheads.

Considering the trade-off, we still gain a lot of benefits from constraint propagation in most of cases; hence it still can be treated as useful.

## Compatibility

All rules mentioned in this proposal are logical plan optimization, which do not change the semantics of a query, and thus this proposal will not lead to any compatibility issue

## Implementation

Here are rough ideas about possible implementations:

 * For proposal #1, we can extend the current `propagateConstantSolver` to support wider types of operators from column equality.

 * For proposal #2, `propagateConstantSolver` is also an applicable way to add `NotNULL` filter(`not(isnull())`), but should examine whether the column has the `NotNULL` constraint already.

 * For proposal #3, the [ranger](https://github.com/pingcap/tidb/blob/6fb1a637fbfd41b2004048d8cb995de6442085e2/util/ranger/ranger.go#L14) may be useful to help us collect and fold comparison constraints

 * For proposal #4, current rule [PredicatePushDown](https://github.com/pingcap/tidb/blob/b3d4ed79b978efadf2974f78db8eeb711509e545/plan/rule_predicate_push_down.go#L1) may be enhanced to achieve it

## Open issues (if applicable)

Related issues:

 https://github.com/pingcap/tidb/issues/7098 - the very issue that inspires this proposal

Related PRs:

 https://github.com/pingcap/tidb/pull/7276 - Related to proposal #1 

 https://github.com/pingcap/tidb/pull/7643 - Related to proposal #1 and #3

