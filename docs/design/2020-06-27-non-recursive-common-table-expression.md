
# Proposal: Non-Recursive Common Table Expression

- Author(s):     [Pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-06-29
- Discussion at: https://github.com/pingcap/tidb/issues/17472

## Abstract

This proposal proposes to support the __Non-recursive Common Table Expression (CTE)__.

## Background

From MySQL User Document, [13.2.15 WITH (Common Table Expressions)](https://dev.mysql.com/doc/refman/8.0/en/with.html):
> A common table expression (CTE) is a named temporary result set that exists within the scope of a single statement and that can be referred to later within that statement, possibly multiple times. The following discussion describes how to write statements that use CTEs.

E.g.
```sql
WITH
  cte1 AS (SELECT a, b FROM table1),
  cte2 AS (SELECT c, d FROM table2)
SELECT b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c;
```

## Proposal

We design two strategies for CTE: __Inline__ & __Materialization__:

#### Inline
The CTE is expanded on where it's referenced:
```sql
WITH cte(k, v) AS (SELECT i, j FROM t1)
SELECT v FROM cte WHERE k > 10
UNION DISTINCT
SELECT v FROM cte WHERE k < 100
```
After "inline":
```sql
SELECT v FROM (SELECT i AS k, j AS v FROM t1) WHERE k > 10
UNION DISTINCT
SELECT v FROM (SELECT i AS k, j AS v FROM t1) WHERE k < 100
```

We will add some rules to rewrite the plan trees, similar to `subquery optimization`[2].

#### Materialization
A CTE is executed first, and the result is stored temporarily during the statement execution.

And on each point the CTE is referenced, the tuples of CTE is read from the temporary storage.

To improve performance, the temporay result is stored in memory first, then spill to disk if out of memory quota, similar to disk-based executors[3].

Besides, [4] introduces a `Producer-Consumer` model. We can utilize this model to make writing and reading of CTE parallel.

![cte01](imgs/cte01.png)

#### Optimization
For `Inline` strategy, most existed rules for subquery can be reused.

For `Materialization` strategy, disjunction of all predicates on top can be push down to CTE [4].

Finally, the costs should be estimated, to determine which strategy is better for each CTE.

## Rationale

Mainstream database systems utilize `Inline` and `Materialization` to implement CTE, including MySQL[1], MariaDB[2] and Greenplum[4].

## Implementation

1. Stage 1: `Inline` should be not difficult. A rewrite rule is enough, similar to rewriting subquery[2].
2. Stage 2: Implement `Materialization`, by storing chunks temporarily in memory or disk.
3. Stage 3: `Predicate Push-down` for `Materialization`, and cost estimate for both strategies.


## Open issues
[TiDB#17472: support common table expression](https://github.com/pingcap/tidb/issues/17472)

## References
1. [MySQL 8.0 Reference Manual, 8.2.2.4 Optimizing Derived Tables, View References, and Common Table Expressions with Merging or Materialization](https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html)
2. [Subquery Optimization in TiDB](https://pingcap.com/blog/2016-12-07-Subquery-Optimization-in-TiDB/)
3. [Consider using a disk-based hash table for hash join avoiding OOM](https://github.com/pingcap/tidb/issues/11607)
4. [Optimization of Common Table Expressions in MPP Database Systems](http://www.vldb.org/pvldb/vol8/p1704-elhelw.pdf)
5. [Implementing Common Table Expressions for MariaDB](https://seim-conf.org/media/materials/2017/proceedings/SEIM-2017_Full_Papers.pdf#page=13)

