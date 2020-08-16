
# Proposal: Recursive Common Table Expression

- Author(s):     [Pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-08-16
- Discussion at: https://github.com/pingcap/tidb/issues/17472

## Abstract

This proposal proposes to support the __Recursive Common Table Expression (CTE)__.

## Background

From MySQL User Document, [13.2.15 WITH (Common Table Expressions)](https://dev.mysql.com/doc/refman/8.0/en/with.html):
> A common table expression (CTE) is a named temporary result set that exists within the scope of a single statement and that can be referred to later within that statement, possibly multiple times. The following discussion describes how to write statements that use CTEs.

E.g.
```sql
WITH RECURSIVE cte (n) AS
(
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM cte WHERE n < 5
)
SELECT * FROM cte;
```

Result:
```sql
+------+
| n    |
+------+
|    1 |
|    2 |
|    3 |
|    4 |
|    5 |
+------+
```

## Proposal

### Seed part & Recursive part
Recursive CTE has two parts, seed part and recursive part, and seperated by `UNION ALL` or `UNION [DISTINCT]`.

Seed part contains no reference of CTE, and produces initial rows. Recursive part contains reference of CTE, and produces additional rows iteratively. When no more rows are produced, the iterations ends.

### End of Itertions
When no more rows are produced, the iterations end.

Besides [1]:
* `cte_max_recursion_depth` system variable limits the number of recursion levels.
* [`max_execution_time`](https://docs.pingcap.com/tidb/dev/system-variables#max_execution_time) system variable limits execution time.
* [`MAX_EXECUTION_TIME(N)`](https://docs.pingcap.com/tidb/dev/optimizer-hints#max_execution_timen) optimizer hint limits execution time too.

### Linear Recursion
Recursive CTE is a "linear recursion"[1][6][7][8]. As a result, each iteration of the recursive part operates only on the rows produced by the previous iteration[1].
In order to meet the requirement of "linear recursion", some syntax constraints apply within recursive part[1]:

* Must not contain these contructs: `aggregate functions`, `window functions`, `GROUP BY`, `ORDER BY`, `DISTINCT`
* References CTE only and only in `FROM` clause. If used in `JOIN`, CTE must not be the right side of a `LEFT` join.

### Multiple Recursive CTEs
A CTE can refer to CTEs itself or defined earlier in the same `WITH` clause, but not those defined later. So mutually-recursive CTEs are not supported.

CTEs depending on other CTE(s) begins to execute after the dependencies are all met. Independent CTEs can execute parallelly.

## Rationale
MariaDB[6] build a dependency matrix in preparatory stage to detect recursive CTEs, check seed parts, and determine dependency between CTEs.

At the same time, MariaDB[6] utilize two temporary tables to save results:
* Records for final result.
* Records for latest iteration.


## Implementation

* Stage 1: Build dependency graph, to detect recursive CTEs, dependency relations, and check there are enough seed parts.
* Stage 2: Compute all dependent non-recursive CTEs.
* Stage 3: Traverse dependency graph in width first manner. For each recursive CTE:
    - 3.1 Create an empty `chunk.RowContainer`, with schema of seed part, and a additional hidden field `_iteration_id`, represents the tuple is produced by which iteration.
    - 3.2 Compute seed part, and save in `chunk.RowContainer`, with `_iteration_id = 0`.
    - 3.3 



## Open issues
* [TiDB#6824: support recursive common table expression (CTE)](https://github.com/pingcap/tidb/issues/6824)
* [TiDB#17472: support common table expression](https://github.com/pingcap/tidb/issues/17472)

## References
1. [MySQL 8.0 Reference Manual, 13.2.15 WITH (Common Table Expressions)](https://dev.mysql.com/doc/refman/8.0/en/with.html)
2. [MySQL 8.0 Reference Manual, 8.2.2.4 Optimizing Derived Tables, View References, and Common Table Expressions with Merging or Materialization](https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html)
3. [Subquery Optimization in TiDB](https://pingcap.com/blog/2016-12-07-Subquery-Optimization-in-TiDB/)
4. [Consider using a disk-based hash table for hash join avoiding OOM](https://github.com/pingcap/tidb/issues/11607)
5. [Optimization of Common Table Expressions in MPP Database Systems](http://www.vldb.org/pvldb/vol8/p1704-elhelw.pdf)
6. [Implementing Common Table Expressions for MariaDB](https://seim-conf.org/media/materials/2017/proceedings/SEIM-2017_Full_Papers.pdf#page=13)
7. [ANSI/ISO/IEC International Standard (IS) Database Language SQL — Part 2: Foundation (SQL/Foundation)](http://web.cecs.pdx.edu/~len/sql1999.pdf)
8. [Types of Recursions](https://www.geeksforgeeks.org/types-of-recursions/)
