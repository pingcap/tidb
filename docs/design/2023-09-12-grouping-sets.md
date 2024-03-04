# Proposal: Grouping Sets

- Author(s): [AilinKid](http://github.com/ailinkid), [windtalker](https://github.com/windtalker), [xzhangxian1008](http://github.com/xzhangxian1008), ...
- Tracking Issue: https://github.com/pingcap/tidb/issues/42631

## Introduction

Grouping Sets is internal implementation mechanism for supporting Multi-Distinct-Aggregate MPP Optimization and Rollup/Cube syntax. Both scenarios need the underlying data to be grouped with different grouping layout, consequently feeding different aggregation function accordingly or feeding all the same aggregation functions continuously , while one replica of source data can rarely do that, that's why grouping sets is introduced and additional data replication is necessary even under our trade-off.

## Motivation or Background

**Multi-Distinct-Aggregate MPP Optimization**

```sql
MySQL [test]> explain select count(distinct a), count(distinct b) from t;
+------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------+
| id                                 | estRows  | task         | access object | operator info                                                                      |
+------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------+
| TableReader_32                     | 1.00     | root         |               | MppVersion: 2, data:ExchangeSender_31                                              |
| └─ExchangeSender_31                | 1.00     | mpp[tiflash] |               | ExchangeType: PassThrough                                                          |
|   └─Projection_27                  | 1.00     | mpp[tiflash] |               | Column#4, Column#5                                                                 |
|     └─HashAgg_28                   | 1.00     | mpp[tiflash] |               | funcs:count(distinct test.t.a)->Column#4, funcs:count(distinct test.t.b)->Column#5 |
|       └─ExchangeReceiver_30        | 1.00     | mpp[tiflash] |               |                                                                                    |
|         └─ExchangeSender_29        | 1.00     | mpp[tiflash] |               | ExchangeType: PassThrough, Compression: FAST                                       |
|           └─HashAgg_26             | 1.00     | mpp[tiflash] |               | group by:test.t.a, test.t.b,                                                       |
|             └─TableFullScan_14     | 10000.00 | mpp[tiflash] | table:t       | keep order:false, stats:pseudo                                                     |
+------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------+
8 rows in set (0.000 sec)
```
As shown above, the source data will be pass through to single node, and all the intensive aggregation computation resides that node (especially on operator `HashAgg_28`). Once the number of rows on it is large, the single point bottleneck will appear to be obvious.

**Rollup Syntax**
```sql
MySQL [test]> explain SELECT a, b, sum(1) FROM t GROUP BY a, b With Rollup;
+--------------------------------------+----------+--------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id                                   | estRows  | task         | access object | operator info                                                                                                                                                                   |
+--------------------------------------+----------+--------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TableReader_43                       | 8000.00  | root         |               | MppVersion: 2, data:ExchangeSender_42                                                                                                                                           |
| └─ExchangeSender_42                  | 8000.00  | mpp[tiflash] |               | ExchangeType: PassThrough                                                                                                                                                       |
|   └─Projection_7                     | 8000.00  | mpp[tiflash] |               | Column#4, Column#5, Column#7                                                                                                                                                    |
|     └─Projection_37                  | 8000.00  | mpp[tiflash] |               | Column#7, Column#4, Column#5                                                                                                                                                    |
|       └─HashAgg_35                   | 8000.00  | mpp[tiflash] |               | group by:Column#4, Column#5, gid, funcs:sum(1)->Column#7, funcs:firstrow(Column#4)->Column#4, funcs:firstrow(Column#5)->Column#5, stream_count: 10                              |
|         └─ExchangeReceiver_21        | 10000.00 | mpp[tiflash] |               | stream_count: 10                                                                                                                                                                |
|           └─ExchangeSender_20        | 10000.00 | mpp[tiflash] |               | ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#4, collate: binary], [name: Column#5, collate: binary], [name: gid, collate: binary], stream_count: 10 |
|             └─Expand_19              | 10000.00 | mpp[tiflash] |               | level-projection:[<nil>->Column#4, <nil>->Column#5, 0->gid],[Column#4, <nil>->Column#5, 1->gid],[Column#4, Column#5, 3->gid]; schema: [Column#4,Column#5,gid]                   |
|               └─TableFullScan_16     | 10000.00 | mpp[tiflash] | table:t       | keep order:false, stats:pseudo                                                                                                                                                  |
+--------------------------------------+----------+--------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
9 rows in set (0.018 sec)
```
Most modern databases with AP functionality always include multi-dimensional aggregate analysis. Rollup/Cute are two common syntax forms of them. MySQL also provides rollup syntax support after version 8.0, along with supporting implementation of the grouping function. Expand operator can supply underlying data replication, data being grouped with different dimensional grouping layout, and consequently outputting different dimensional aggregation results.

Behind the realization of both phenomena, the essence is the application of grouping sets. We are going to discover and analyze the necessary requirements for data replication from different scenarios, replicate the underlying data via building Expand operator consuming grouping sets collected, and feedback the grouping function and grouping column related expressions in the upper layer by rewriting.

## Detailed Design

### Backend Implementation

**Grouping Sets Brief**

Grouping Sets is a union set of grouping layout requirements, generally speaking, normal group-by clause without rollup/cute suffix can produce only one grouping set. eg: group-by(a,b), and it's enough that the underlying data should only be grouped by {a,b}.

For a more advanced syntax, like dimensional group-by --- rollup(a,b), it requires the several grouping layout: {{a,b}, {a},{}}, which means the underlying data should be grouped multi-time according to different group layout, among each of them, output current aggregation result, then regroup the data again. Note: all dimensional aggregated result are dispatched to client as a union.

Expect for explicit syntax specification of grouping sets like rollup/cube, some implicit cases like: `select count(distinct a), count(distinct b) from t` also requires the underlying data to be grouped by a and b respectively. Rather than feeding all the aggregation function like rollup statement, here different grouped data only feed one specified aggregation function. In this case, two grouping sets {{a}, {b}} is generated, the underlying data replica grouped by a should feed `count(a)`, the other one should feed `count(b)`. You may have noticed that the `distinct` keyword is missing, that's because de-duplication work have been done by group action.

**Expand Operator Brief**

Intuitively, Grouping sets should be the direct input accepted by the Expand operator. Expand operator's data expansion logic is based on the analysis of grouping sets, coping each row several times and modifying special columns of each row accordingly.

Currently, grouping sets analysis work is done by TiDB optimizer and expand operator execution logic can be seen as a leveled-projection composition. The N-th row expansion logic is analyzed and organized as a series of projection expressions, consequently being composed as a leveled-projection as a whole.

Take the case above as an example, the operation info of expand operator show some details above the level-projection: `level-projection:[<nil>->Column#4, <nil>->Column#5, 0->gid],[Column#4, <nil>->Column#5, 1->gid],[Column#4, Column#5, 3->gid]`, each projection out of 3 can be seen as an independent projection respectively. Every original row should be projected 3 times when the data stream being pulled through expand operator.

**Conclusion**

In a words, grouping sets are the basic important logic concept through the entire optimization phase. After the physical optimization phase, the data replication logic are analyzed and organized as level-projection, which make the execution logic more general and clean.

### Frontend Application Access

**Rollup**

```sql
MySQL [test]> explain SELECT a, b, grouping(a), sum(a) FROM t GROUP BY a, b With Rollup;
+------------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id                                       | estRows  | task         | access object | operator info                                                                                                                                                                                        |
+------------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TableReader_43                           | 8000.00  | root         |               | MppVersion: 2, data:ExchangeSender_42                                                                                                                                                                |
| └─ExchangeSender_42                      | 8000.00  | mpp[tiflash] |               | ExchangeType: PassThrough                                                                                                                                                                            |
|   └─Projection_7                         | 8000.00  | mpp[tiflash] |               | Column#4, Column#5, grouping(gid)->Column#8, Column#7                                                                                                                                                |
|     └─Projection_37                      | 8000.00  | mpp[tiflash] |               | Column#7, Column#4, Column#5, gid                                                                                                                                                                    |
|       └─HashAgg_35                       | 8000.00  | mpp[tiflash] |               | group by:Column#26, Column#27, Column#28, funcs:sum(Column#22)->Column#7, funcs:firstrow(Column#23)->Column#4, funcs:firstrow(Column#24)->Column#5, funcs:firstrow(Column#25)->gid, stream_count: 10 |
|         └─Projection_44                  | 10000.00 | mpp[tiflash] |               | cast(test.t.a, decimal(10,0) BINARY)->Column#22, Column#4->Column#23, Column#5->Column#24, gid->Column#25, Column#4->Column#26, Column#5->Column#27, gid->Column#28, stream_count: 10                |
|           └─ExchangeReceiver_21          | 10000.00 | mpp[tiflash] |               | stream_count: 10                                                                                                                                                                                     |
|             └─ExchangeSender_20          | 10000.00 | mpp[tiflash] |               | ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#4, collate: binary], [name: Column#5, collate: binary], [name: gid, collate: binary], stream_count: 10                      |
|               └─Expand_19                | 10000.00 | mpp[tiflash] |               | level-projection:[test.t.a, <nil>->Column#4, <nil>->Column#5, 0->gid],[test.t.a, Column#4, <nil>->Column#5, 1->gid],[test.t.a, Column#4, Column#5, 3->gid]; schema: [test.t.a,Column#4,Column#5,gid] |
|                 └─Projection_15          | 10000.00 | mpp[tiflash] |               | test.t.a, test.t.a->Column#4, test.t.b->Column#5                                                                                                                                                     |
|                   └─TableFullScan_16     | 10000.00 | mpp[tiflash] | table:t       | keep order:false, stats:pseudo                                                                                                                                                                       |
+------------------------------------------+----------+--------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
11 rows in set (0.028 sec)
```
Rollup syntax indicates the data replication is inevitable, so a logical expand operator is built under aggregation in advance, while it's level-projection expression generation is postponed into the last logical optimization rule because we should wait column pruning logic is all set about.

To keep companies with rollup syntax, grouping-function/grouping-expression-ref analysis work is introduced in upper expression rewriter, because grouping-expression may be filled with null value, it may be no longer the same as what it was after expand operation. That's means name resolution logic of grouping expression above expand operation should be redirected to new generated column which should be respected and distinguished from original expression format.

For example:
- the normal column-ref `a` in the select list should be name-resolved to grouping expression a', distinguished from base column a from original source table t.
- the column-ref `a` in the sum aggregation function should be name-resolved to original base column a from source table t.
- the column-ref `a` in the grouping function should be name-resolved to grouping expression a', distinguished from base column a from original source table t.

Except for base column-ref, expression-ref will be more normal in producing environment. like: 'select a+b, grouping(a+b) from t group by a+b, c with rollup', here `a+b` in select list should also be name-resolved to the grouping expression `a+b` rather a scalar plus function with base column a and b as its parameters. Except for select list, having-clause/order-by clause should have this kind of special name resolution handle logic as well.

```sql
MySQL [test]>  SELECT count(1),grouping(a),grouping(b) FROM t GROUP BY a, b With Rollup;
+----------+-------------+-------------+
| count(1) | grouping(a) | grouping(b) |
+----------+-------------+-------------+
|        1 |           0 |           0 |
|        1 |           0 |           1 |
|        1 |           1 |           1 |
+----------+-------------+-------------+
3 rows in set (0.027 sec)
```

Grouping function is used to indicate what kind of grouping dimension is, from which your aggregation result is aggregated from in current output line. eg: grouping(a) in the above case, there is only one row (1,1) in the table t, since rollup require the underlying data to be grouped 3 time as {a,b},{a},{}, so the final aggregation results will also be 3 lines.

Let's go through these 3 lines:

- for the first line, grouping(a)=0 and grouping(b)=0, that means the aggregation result count(1)=1 is aggregated from grouping layout {a,b} in which a,b is neither grouped.
- for the second line, grouping(a)=0 and grouping(b)=1, that means the aggregation result count(1)=1 is aggregated from grouping layout {a} in which a is not grouped while b does.
- for the third line, grouping(a)=1 and grouping(b)=1, that means the aggregation result count(1)=1 is aggregated from grouping layout {} in which a and b is both grouped.

With the fine-grained help from grouping function, we can easily identify what the aggregation dimension current output result is from. In internal implementation of grouping function, we should be able to analyze out the relationship between parameter column/expression and grouping expressions, and convert this kind of relationship into computation logic with GID, finally embedding them into grouping function as metadata.

**Multi Distinct Aggregate**

```sql
MySQL [test]> explain select count(distinct a), count(distinct b) from t;
+--------------------------------------------+---------+--------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id                                         | estRows | task         | access object | operator info                                                                                                                                                                        |
+--------------------------------------------+---------+--------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TableReader_35                             | 1.00    | root         |               | MppVersion: 2, data:ExchangeSender_34                                                                                                                                                |
| └─ExchangeSender_34                        | 1.00    | mpp[tiflash] |               | ExchangeType: PassThrough                                                                                                                                                            |
|   └─Projection_25                          | 1.00    | mpp[tiflash] |               | Column#4, Column#5                                                                                                                                                                   |
|     └─HashAgg_26                           | 1.00    | mpp[tiflash] |               | funcs:sum(Column#10)->Column#4, funcs:sum(Column#11)->Column#5                                                                                                                       |
|       └─ExchangeReceiver_33                | 1.00    | mpp[tiflash] |               |                                                                                                                                                                                      |
|         └─ExchangeSender_32                | 1.00    | mpp[tiflash] |               | ExchangeType: PassThrough, Compression: FAST                                                                                                                                         |
|           └─HashAgg_28                     | 1.00    | mpp[tiflash] |               | funcs:count(distinct test.t.a)->Column#10, funcs:count(distinct test.t.b)->Column#11, stream_count: 10                                                                               |
|             └─ExchangeReceiver_31          | 2.00    | mpp[tiflash] |               | stream_count: 10                                                                                                                                                                     |
|               └─ExchangeSender_30          | 2.00    | mpp[tiflash] |               | ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary], [name: test.t.b, collate: binary], [name: Column#9, collate: binary], stream_count: 10 |
|                 └─HashAgg_24               | 2.00    | mpp[tiflash] |               | group by:gid(Column#9), test.t.a, test.t.b,                                                                                                                                               |
|                   └─Expand_27              | 2.00    | mpp[tiflash] |               | level-projection:[test.t.a, <nil>->test.t.b],[<nil>->test.t.a, test.t.b]; schema: [test.t.a,test.t.b,gid]                                                                                                                    |
|                     └─TableFullScan_12     | 1.00    | mpp[tiflash] | table:t       | keep order:false, stats:pseudo                                                                                                                                                       |
+--------------------------------------------+---------+--------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
12 rows in set (0.021 sec)
```
Multi Distinct Aggregation is not strong dependent on grouping sets and expand operator (data replication), we can still fallback to single node execution under MPP mode, or even do it by ourselves in TiDB side to avoid underlying data replication, it's cost based.

From the explanation result above, we could find that, aggregation has been pull as long as 3 stages.

- `HashAgg_24` is used to group underlying data with a unified group expressions (a,b), while actually for a specified data replica from level-projection expressions: like [a, null], group-by(a,b) is functioned as group(a), which is exactly what `count(distint a)` wants.
- `HashAgg_28` is used to firstly count(distinct x) partial result after data shuffled by [gid, a, b]
- `HashAgg_26` is used to secondly collect all partial result as final result after distributing intensive computation task out and data are pass through to single node again.

We noticed that hash-agg has been extended as 3 stages to collect result, while for smaller set of data, distributing intensive computation task out is not that necessary because of shuffle/pass-through network consumption and hash-table mem/latency impact. Naturally and intuitively 2/3 stages should be adaptive in the execution layer, for now we could hardly do that.

**Conclusion**

Currently, Rollup and MDA are two basic upper application of grouping sets relatively. Smooth workflow need us first to derive basic grouping sets out from advanced rollup syntax or multi distinct aggregation, then utilizing the backend logic of grouping sets and converting the data replica logic implied by those grouping sets into Expand's level-projection.

## Unresolved Questions

- Currently, `Rollup` syntax is only supported under MPP engine, which means tiflash nodes are necessary.
- Currently, MDA optimization is only supported under MPP engine, which means tiflash nodes are necessary.
- Currently, MDA optimization is control by an internal variable `tidb_opt_enable_three_stage_multi_distinct_agg`, the 2/3 stages shift couldn't be self-adaptive in execution layer.
