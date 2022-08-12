# Functional Dependency

- Author(s): [AilinKid](https://github.com/AilinKid) (Lingxiang Tai) [winoros](https://github.com/winoros) (Yiding Cui)
- Last updated: 2022-03-16
- Motivation Issue: https://github.com/pingcap/tidb/issues/29766
- Related Document: https://pingcap.feishu.cn/docs/doccndyrRWfcGALhjcyVhOrxWYd

## Table of Contents

* [Introduction](#introduction)
* [Motivation And Background](#motivation-And-background)
* [Detail Design](#detail-design)
    * [Implementation Overview](#implementation-overview)
        * [How to store functional dependency](#how-to-store-functional-dependency)
        * [How to maintain functional dependency](#how-to-maintain-functional-dependency)
        * [How to integrate functional dependency with TiDB arch](#how-to-integrate-functional-dependency-with-tidb-arch)
    * [Operator Overview](#operator-overview)
        * [Logical Datasource](#logical-datasource)
        * [Logical Selection](#logical-selection)
        * [Logical Projection](#logical-projection)
        * [Logical Aggregation](#logical-aggregation)
        * [Logical Join](#logical-join)
* [Investigation](#investigation)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Functional Dependency (abbreviated as FD below) is a traditional relation concept with a history as long as database itself. In relational database theory, a functional dependency is a constraint between two sets of attributes in a relation from a database. In other words, a functional dependency is a constraint between two attributes in a relation.

Given a relation R and sets of attributes X,Y ∈ R, X is said to functionally determine Y (written as X → Y) if and only if each X value in R is associated with precisely one Y value in R; R is then said to satisfy the functional dependency X → Y. In other words, a dependency FD: X → Y means that the values of Y are determined by the values of X. Two tuples sharing the same values of X will necessarily have the same values of Y.

## Motivation And Background

Since we have already known the dependencies between sets of attributes (columns) after the FD has been built, we can make use of this relationship to do optimization in many scenarios. Eg: in only-full-group-by mode, for non-aggregated columns in the select list, as long as it is functionally dependent on the group-by clause columns, then we can consider it legal. Otherwise, extracting the constant, equivalence, dependency relationship between columns everytime we use it is tricky and costly.

After trials and errors of only-full-group-by check, TiDB ultimately seeks to use functional dependency to solve its only-full-group-by check problem completely. Besides, functional dependency can also be applied to many other scenarios to reduce complexity, for example, distinct elimination with/without order by, selectivity for the correlated column and so on.

## Detail Design

### Implementation Overview

* How to store functional dependency
* How to maintain functional dependency
* How to integrate functional dependency with TiDB arch

#### How to store functional dependency

Generally speaking, functional dependencies essentially store the relationship between columns. According to the paper [Exploiting functional dependence in query optimization](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf), this relationship can be constant, equivalence, strict and lax. Except constant is a unary, all others can be regarded as binary relationships. Therefore, we can use graph theory to store functional dependencies, maintaining edge sets between point-set and point-set, and attaching equivalence, strict and lax attributes as a label on each edge. For unary constant property, we can still save it as an edge, and since constant has no starting point-set, let's regard it as a headless edge.

* equivalence: {a} == {b}
* strict FD:   {a} -> {b}
* lax FD:      {a} ~> {b}
* constant:    { } -> {b}

For computation convenience, we can optimize equivalence as {a,b} == {a,b}, so we don't have to search from both directions when confirming whether a point is in the equivalence set.

#### How to maintain functional dependency

For the open-sourced MySQL, the construction and maintenance of FD are still relatively heavy. It uses sub-queries as logical units and uses nested methods to recursively advance FD. For each logical unit, the source for where FD is derived is relatively simple, basically from three elements there: `GROUP BY` clause, `WHERE` clause and the datasource (join/table). However, according to the paper [CS-2000-11.thesis.pdf](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf), TiDB chooses to build FDs based on logical operators as an alternative, without the boundary restrictions of logical sub-queries.

Example:

```sql
create table t1(a int, c int)
create table t2(a int, b int)
explain select t1.a, count(t2.b), t1.c from t1 join t2 on t2.a=t1.a where t1.c=1 group by t2.a;
```

Result:

```sql
mysql> explain  select t1.a, count(t2.b), t1.c  from t1 join t2 on t2.a=t1.a where t1.c=1 group by t2.a;
+----------------------------------+----------+-----------+---------------+----------------------------------------------------------------------------------------------------------------------------------+
| id                               | estRows  | task      | access object | operator info                                                                                                                    |
+----------------------------------+----------+-----------+---------------+----------------------------------------------------------------------------------------------------------------------------------+
| Projection_9                     | 12.49    | root      |               | test.t1.a, Column#7, test.t1.c                                                                                                   |
| └─HashAgg_10                     | 12.49    | root      |               | group by:test.t2.a, funcs:count(test.t2.b)->Column#7, funcs:firstrow(test.t1.a)->test.t1.a, funcs:firstrow(test.t1.c)->test.t1.c |
|   └─HashJoin_12                  | 12.49    | root      |               | inner join, equal:[eq(test.t1.a, test.t2.a)]                                                                                     |
|     ├─TableReader_15(Build)      | 9.99     | root      |               | data:Selection_14                                                                                                                |
|     │ └─Selection_14             | 9.99     | cop[tikv] |               | eq(test.t1.c, 1), not(isnull(test.t1.a))                                                                                         |
|     │   └─TableFullScan_13       | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo                                                                                                   |
|     └─TableReader_18(Probe)      | 9990.00  | root      |               | data:Selection_17                                                                                                                |
|       └─Selection_17             | 9990.00  | cop[tikv] |               | not(isnull(test.t2.a))                                                                                                           |
|         └─TableFullScan_16       | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo                                                                                                   |
+----------------------------------+----------+-----------+---------------+----------------------------------------------------------------------------------------------------------------------------------+
9 rows in set (0.00 sec)
```

Analysis:

When building the FD through these logical operators, we call `ExtractFD()` recursively from the root node of the logical plan tree. When the call down to the leaf node (actually source table for both join side), for the inner side, the FD from the datasource of base table of t2 are nil because they have no basic key constraint. While for outer side, since there is a filter on the datasource of base table t1, we got FD as {} -> {t1.c}. Because t1.c can only be constant 1 behind the filter. Up to next level, the join condition t1=t2 will build the equivalence for point-set of {t1.a, t2.a} == {t1.a, t2.a}. Finally, when arriving the root aggregate node, we will build strict FD from group-by point-set {t2.a} to select aggregate function {count(t2.b)} reasonably. Consequently, we get FD as {} -> {t1.c} & {t1.a, t2.a} == {t1.a, t2.a} & {t2.a} -> {count(t2.b)}.

Application:

For the only-full-group-by check of building phase, after getting the FD as illustrated in the last subchapter, we can easily identify from the select list that t1.a is equivalent to the group-by column t2.a, count(t2.b) is strictly functional dependent on group-by column t2.a and t.c is definitely constant after the filter is passed. So it can pass only-full-group-by check here. For more sophisticated cases with recursive sub-queries, maintenance work of this kind of FD architecture will be automatical and reasonable because we're just pulling the request from the top node of the entire logical plan tree, and all things happen naturally.

#### How to integrate functional dependency with TiDB arch

Where to put this stuff in and when should the FD collection work be done? In FD logic, the relationship between columns is not limited to base col, but also between expressions, even sub-queries. We need to be able to identify sameness between cols, expressions and even better to have a unique id to tag them. When joining different tables, there are maybe some columns with the same name from different sources, we should still utilize this unified unique id to identify and distinguish them. Obviously, the expression tree after ast rewrite has this convenient functionality, and plus we need to utilize FD functionality to check validation when building a logical plan, so eventually we can only perform FD collection while building the logical plan stage.

As illustrated above, tidb chooses the collect FD in units of logical operators. So for the first step, integrating FD collection logic for some meaningful logic operators is necessary, such as: datasource, join, apply, selection, projection and so on. For other meaningless logical operator like sort, it will not make any difference to FD collected from its child operator, so just output what it got from its child. The most important things in FD construction chain are from various join keys, join conditions, join types, selection predicates and  group-by clause. Let's break it down and take it one by one in next chapter.

### Operator Overview

* Logical Datasource
* Logical Selection
* Logical Projection
* Logical Aggregation
* Logical Join

#### Logical Datasource

The logical datasource is the place where the most basic FD is generated. The natural FD is generated from the primary key and candidate key (unique) of the table, corresponding to strict FD and lax FD respectively. Of course, the not null attribute of the column is also a very important role. Because all rows related to null value can be rejected by that not null attribute, so that the related lax FD can become strict FD yet.

#### Logical Selection

As you can see from the sql example above, logical selection from where/having clause can be used to filter data. The selected data will have the properties constrained by this predicate. Most common FDs constructed from selection are constant, not null and equivalence, corresponding to t.a=1, t.a=1 and t.a = t.b respectively. The reason why t.a=1 can generate both constant and not null relationship is that in the expression calculation, if t.a is null, then null=1 will be calculated as null, and when the result of the predicate in where is null, this row will be rejected. So after the data is pulled from this filter, t.a mustn't be null and must be 1 only. For most operators, null value rejection characteristic is with them, which means that if a parameter on the any side of an operator is null, then the final result is null (just like what t.a=1 is shown here).

#### Logical Projection

Logical projection can be described simply because it does not generate FD itself, but truncate some useless FD sets according to the projected columns. For example, providing that the FD graph has {a} -> {b} & {b} -> {c}, when only the {a,c} columns are selected in the projection, we need to maintain the closure and transitive properties of this functional dependency graph, generating the remaining {a} -> {c} functional dependencies for the rest FD graph when column b is projected out.

#### Logical Aggregation

Logical aggregation mainly builds related FDs around the columns or expressions in the group by clause. It should be noted that if there is no group by clause but agg functions, all rows should be implicitly regarded as one group. Columns or expressions from group by clause naturally functional determine these aggregate expressions. But for others, they're waited to be judged, that's what and where only-full-group-by check does.

#### Logical Join

Logical join must be the most difficult part to infer with, because the presence of join key values, the variability of join conditions, and the difference of join types will all lead to the diversity of FD inference. For inner join, the final data comes first from cartesian product of inner and outer join side, and then pull the result through the filter generated from the inner join predicate (same as what selection does in where clause). Since reducing row data does not affect the presence of FD in that join side, data after the cartesian product can retain all FDs of operators at both sides of the join. But when data pulled through filters, it is necessary to add the FD generated by the inner join conditions.

For outer join, we need to consider more, because outer join will choose the connection method based on the position of the join conditions and the presence of the join key.

* only inner side condition           --- apply inner filter after the cartesian product is made, which means no supplied-null row will be appended on the inner side if any row is preserved; or all rows are supplied-null if all rows are filtered out.
* has outer condition or join key     --- apply all of this filter as matching judgement on the process of cartesian product, which means supplied-null row will be appended on the inner side when matching is failed.

Although both cases will keep all rows from the outer side, but how to derive FDs from both sides depends on where the conditions come from, what the columns used by the conditions are, and what the attributes the conditions can provide. We can treat the former one as an inner join, but for the latter one, we can only keep the FD from the outer table of join, and inner side's FD and filter FD has many complicated rules to infer with, we are not going to expand all of this here.

## References

- [MySQL source code](https://github.com/mysql/mysql-server/blob/8.0/sql/aggregate_check.h)
- [Exploiting functional dependence in query optimization](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf).
- [functional dependency reduce](https://www.inf.usi.ch/faculty/soule/teaching/2014-spring/cover.pdf).
- [Canonical Cover of Functional Dependencies in DBMS](https://www.geeksforgeeks.org/canonical-cover-of-functional-dependencies-in-dbms/)

## Unresolved Questions

- None
