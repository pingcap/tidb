# Functional Dependency

- Author(s): [AilinKid](https://github.com/AilinKid) (Lingxiang Tai) [Winoros](https://github.com/Winoros) Yiding Cui
- Last updated: 2022-03-16
- Motivation Issue: https://github.com/pingcap/tidb/issues/29766
- Related Document: https://pingcap.feishu.cn/docs/doccndyrRWfcGALhjcyVhOrxWYd

## Table of Contents

* [Introduction](#introduction)
* [Motivation And Background](#motivation-And-background)
* [Detail Design](#detail-design)
    * [Implementation Overview](#implementation-overview)
      * [How to store function dependency](#how-to-store-function-dependency)
      * [How to maintain function dependency](#how-to-maintain-function-dependency)
      * [How to integrate function dependency with TiDB arch](#how-to-integrate-function-dependency-with-tidb-arch)
    * [Operator Overview](#operator-overview)
      * [Logical Datasource](#logical-datasource)
      * [Logical Selection](#logical-selection)
      * [Logical Projection](#logical-projection)
      * [Logical Aggregation](#logical-aggregation)
      * [Logical Join](#logical-join)
* [Investigation](#investigation)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Functional Dependency is traditional relation concept with a history as long as database itself. In relational database theory, a functional dependency is a constraint between two sets of attributes in a relation from a database. In other words, a functional dependency is a constraint between two attributes in a relation.

Given a relation R and sets of attributes  X,Y ∈ R, X is said to functionally determine Y (written X → Y) if and only if each X value in R is associated with precisely one Y value in R; R is then said to satisfy the functional dependency X → Y. In other words, a dependency FD: X → Y means that the values of Y are determined by the values of X. Two tuples sharing the same values of X will necessarily have the same values of Y. 

## Motivation And Background

Since we already know the dependencies between columns after the FD has been built, we can take use of this relationship to do some optimization in many scenarios, eg: in only-full-group-by mode, for non-aggregated columns in the select list, as long as it is a functionally dependent column of the group-by clause column, then we can consider it legal. Otherwise, to extract the constant/equivalence/dependency relationship between columns everytime we use it is tricky and costly.  

After trials and errors of only-full-group-by check, TiDB ultimately seek to functional dependency to solve it's check completely. Besides, functional dependency can also be applied to many others scenarios to reduce complexity, for example, distinct elimination with/without order by, selectivity for the correlated column and so on.

## Detail Design

### Implementation Overview

* How to store function dependency
* How to maintain function dependency
* How to integrate function dependency with TiDB arch

#### How to store function dependency

Generally speaking, functional dependencies store the relationship between columns. According to paper [CS-2000-11.thesis.pdf](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf), this relationship can be constant, equivalence, strict and lax. Except constant is a unary relationship, all other relationships can be regarded as binary relationships. Therefore, we can try to use graph theory to store functional dependencies, maintain edge sets between point-set and point-set, and identify different equivalence, strict and lax attributes on the edge sets. For unary constant property, we can still save it as an edge, since constant has no start point-set, we can think of it as a headless edge.

* equivalence: {a} == {b}
* strict FD:   {a} -> {b}
* lax Fd:      {a} ~> {b}
* constant:    { } -> {b}

for computation convenience, we can optimize equivalence as {a,b} == {a,b}, so we don't need to search for the both direction when encountering an equivalence edge.

#### How to maintain function dependency

For open source mysql, the construction and maintenance of FD is still relatively heavy. It uses sub-queries as logical units and uses nested methods to recursively advance FD. For each logical unit, the source of FD is relatively simple, and the basic consideration is for three elements where, group-by and datasource(join/table). According to the paper [CS-2000-11.thesis.pdf](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf), TiDB chooses to build FDs based on logical operators, without the boundary restrictions of logical sub-queries. 

example: 

```sql
create table t1(a int, c int)
create table t2(a int, b int)
select t1.a from t1 join t2 on t2.a=t1.a group by t2.a where c=1;
```

result:

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

analysis: 
When building the FD through these logical operators, we are calling the function of ExtractFD recursively from the root node of the logical plan tree. When the call down to the leaf node, for the inner side, the FD from the datasource of base table of t2 are nil because they have no basic key constraint, while for outer side, since there is a filter on the datasource of base table t1, we got FD as {} -> {t1.c} because t1.c can only be constant 1 behind the filter. Up to next level, the join condition t1=t2 will build the equivalence for point-set of {t1.a, t2.a} == {t1.a, t2.a}. Finally, when arriving the root aggregate node, we will build strict FD from group-by point-set {t2.a} to select aggregate function {count(t2.b)}. Consequently, we get FD as {} -> {t1.c} & {t1.a, t2.a} == {t1.a, t2.a} & {t2.a} -> {count(t2.b)}.

application:
For the only-full-group-check of building phase, after we got the FD as illustrated in the last subchapter, we can easily identify the select list that t1.a are equivalent to the group-by column t2.a, count(t2.b) are strictly functional dependent on group-by column t2.a and t.c are definitely constant after the filter is done. So only full group check is valid here. For more sophisticated cases with recursive sub-queries, maintenance work of this kind of FD will be automatically and reasonable when we're just pulling the request from the top node of the entire logical plan tree, all things are naturally.

#### How to integrate function dependency with TiDB arch

Where to put this stuff in? When should the FD collection work be done? In FD logic, the relationship between columns is not limited to base col, but also between expressions, even sub-queries, we need to be able to identify two cols, expressions are equivalent, and even better to have a unique id that identifies them. When joining different tables, even if there are maybe some columns with the same name, we can still utilize this unified unique id to identify and distinguish them. Obviously, the expression tree after ast rewrite has this convenient functionality, and because we need to utilize FD functionality to check validation when building a logical plan, so eventually we can only perform FD collection while building the logical plan stage.

As illustrated above, tidb choose the collect FD in units of logical operators. So for the first step, we need to integrate FD collection logic for some meaningful logic operators, such as: datasource, join, apply, selection, projection and so on. For other meaningless logical operator like sort, it will not make any difference to FD collected from its child operator, so just output what it got from its child. The most important things in the FD construction chain are from various join keys, join conditions, join types, selection predicates and  group-by clause. Let's break it down and take it one by one in next chapter.

### Operator Overview

* Logical Datasource
* Logical Selection
* Logical Projection
* Logical Aggregation
* Logical Join

#### Logical Datasource

The logical datasource is the place where the most basic FD is generated. The natural FD is generated from the primary key and candidate key (unique) of the table, corresponding to strict FD and lax FD respectively. Of course, the not null attribute of the column is also a very important role. Because all rows related to null value can be rejected by that not null attribute, so that the related lax FD can become strict FD yet.

#### Logical Selection

As you can see from the sql example above, logical selection from where/have clause can be used as a filter to filter data, the selected data will have the properties constrained by this predicate. The most common FD constructed from selection are constant, not null and equivalence, corresponding to t.a=1, t.a=1 and t.a = t.b respectively. The reason why ta=1 can generate both constant and not null is that in the expression calculation, if t.a is null, then null=1 will be calculated as null, and when the result of the predicate in where is null, this row will be rejected. So after the data is passed from this filter, t.a mustn't be null and must be 1 only. For most operators, there is a null value rejection feature with them, which means that if a parameter on the any side of an operator is null, then the final result is null (just like what t.a=1 is shown here).

#### Logical Projection

Logical projection can be described simply because it does not generate FD itself, but truncate some useless FD sets according to the projected columns. For example, providing that the FD graph has {a} -> {b} & {b} -> {c}, when only the {a,c} columns are selected in the projection, we need to maintain the closure and transitive properties of this functional dependency graph, generating the remaining {a} -> {c} functional dependencies for the rest when column b is projected out.

#### Logical Aggregation

Logical aggregation mainly builds related FDs around the columns or expressions in the group by clause. It should be noted that if there is no group by clause, but there is agg function, all rows should be implicitly regarded as one group. Columns or expressions from group by clause naturally functional determine these aggregate expressions, but for others, it's waited to be judged, that's what and where only-full-group-by check does.

#### Logical Join

Logical join must be the most difficult part to infer with, because the presence or absence of join key values, the variability of join conditions, and the difference of join types will all lead to the diversity of FD inference. For inner join, the final data comes first from cartesian product of inner and outer join side, and then pull the result through the filter generated from the inner join predicate (same as what selection does in where clause). Since reducing row data does not affect the presence or absence of FD in that join side, data after the cartesian product can retain all FDs of operators at both sides of the join. But when data pulled through filters, it is necessary to add the FD generated by the inner join condition.

For outer join, we need to consider more, because outer join will choose the connection method based on the position of the join condition and the presence or absence of the join key.

* only inner condition                --- apply inner filter after the cartesian product is made, which means no supplied-null row will be appended on the inner side.
* has outer condition or join key     --- apply all of this filter as matching judgement on the process of cartesian product, which means supplied-null row will be appended on the inner side when matching is failed.

both cases will keep all rows from the outer side, but the time to apply the filter depends on where the conditions come from, which leading whether append the null rows on the inner side. So what's the big deal between this two? the former one can be seen as a inner join, but the later can only keep the FD from the outer side join, inner side's FD and filter FD has many complicated rules to infer with, we are not going to both cases will keep all rows from the outer side, but the time to apply the filter depends on where the conditions come from, which leading whether append the null rows on the inner side. So what's the big deal between this two? the former one can be seen as a inner join, but the later can only keep the FD from the outer side join, inner side's FD and filter FD has many complicated rules to infer with, we are not going to expand all of this here.

## Investigation

- [mysql source code](https://github.com/mysql/mysql-server/blob/8.0/sql/aggregate_check.h)
- [CS-2000-11.thesis.pdf](https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf).
- [functional dependency reduce](https://www.inf.usi.ch/faculty/soule/teaching/2014-spring/cover.pdf).
- [Canonical Cover of Functional Dependencies in DBMS](https://www.geeksforgeeks.org/canonical-cover-of-functional-dependencies-in-dbms/)

## Unresolved Questions

- None
