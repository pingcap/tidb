
# Proposal: Recursive Common Table Expression

- Author(s):     [Pingyu](https://github.com/pingyu) (Ping Yu)
- Last updated:  2020-08-18
- Discussion at: https://github.com/pingcap/tidb/issues/17472


## Abstract

This proposal proposes to support the __Recursive Common Table Expression (Recursive CTE)__.


## Background

From MySQL User Document, [13.2.15 WITH (Common Table Expressions)](https://dev.mysql.com/doc/refman/8.0/en/with.html):
> A common table expression (CTE) is a named temporary result set that exists within the scope of a single statement and that can be referred to later within that statement, possibly multiple times.

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
Recursive CTE has two parts, seed part and recursive part, separated by `UNION ALL` or `UNION [DISTINCT]`.

Seed part contains no reference of CTE(s) or reference of non-recursive CTE(s), and produces initial rows.

Recursive part contains reference of itself, and produces additional rows iteratively. When no more rows are produced, the iterations ends. Mutual-recursive is not supported.

### End of Iterations
When no more rows are produced, the iterations end. Besides[1]:
* `cte_max_recursion_depth` system variable limits the number of recursion levels.
* [`max_execution_time`](https://docs.pingcap.com/tidb/dev/system-variables#max_execution_time) system variable limits execution time.
* [`MAX_EXECUTION_TIME(N)`](https://docs.pingcap.com/tidb/dev/optimizer-hints#max_execution_timen) optimizer hint limits execution time too.

### Linear Recursion
Recursive CTE is a "linear recursion"[1][5][6]. As a result, each iteration of the recursive part computes only on the rows produced by previous iteration.
In order to meet the requirement of "linear recursion", some syntax constraints apply within recursive part[1]:

* Must not contain these constructs: `aggregate functions`, `window functions`, `GROUP BY`, `ORDER BY`, `DISTINCT`
* References CTE only and only in `FROM` clause. If used in `JOIN`, CTE must not be the right side of a `LEFT` join.

### Multiple Recursive CTEs
A CTE can refer to CTEs itself or defined earlier in the same `WITH` clause, but not those defined later. So mutually-recursive CTEs are not supported.

CTEs depending on other CTE(s) begins to execute after the dependencies are all met. Independent CTEs can execute parallel.

## Rationale
MariaDB[4] build a dependency matrix in preparatory stage to detect recursive CTEs, check seed parts, and determine dependency between CTEs.

At the same time, MariaDB[4] utilize two temporary tables to save results:
* Records for final result.
* Records for latest iteration.


## Implementation

### Temporary Table
Temporary table is implemented by `chunk.RowContainer`, which stores tuples in memory, or spill to disk if exceeds memory quota.

### Computing Flow
* Stage 1: Build dependency graph, to detect recursive CTEs, dependency relations, and check whether there are enough seed parts.
* Stage 2: Compute all non-recursive CTEs in graph.
* Stage 3: Traverse dependency graph in width first manner. For each recursive CTE:
    - 3.1 Create an empty temporary table `t`, with schema of seed part, and a additional hidden field `_iteration_id`, represents the tuple is produced by which iteration. 
    - 3.2 Compute seed parts, and save in `t`, with `_iteration_id = 0`.
    - 3.3 `set @@iteration_id = 0`
    - 3.4 `++@@iteration_id`
    - 3.5 Let `t_last := select * from t where _iteration_id = @@iteration_id-1`
    - 3.6 `If (t_last is empty or @@cte_max_recursion_depth>@@iteration_id or @@max_execution_time>MaxExecutionTime)`: break
    - 3.7 Compute recursive part on `t_last`, and append to `t` with `_iteration_id = @@iteration_id`. If recursive part is union with distinct, compute `t1 := select min(_iteration_id), ALL_OTHER_FIELDS from t group by ALL_OTHER_FIELDS`; `t := t1`
    - 3.8 Goto `3.4`



## Open issues
* [TiDB#6824: support recursive common table expression (CTE)](https://github.com/pingcap/tidb/issues/6824)
* [TiDB#17472: support common table expression](https://github.com/pingcap/tidb/issues/17472)

## References
1. [MySQL 8.0 Reference Manual, 13.2.15 WITH (Common Table Expressions)](https://dev.mysql.com/doc/refman/8.0/en/with.html)
2. [MySQL 8.0 Reference Manual, 8.2.2.4 Optimizing Derived Tables, View References, and Common Table Expressions with Merging or Materialization](https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html)
3. [Consider using a disk-based hash table for hash join avoiding OOM](https://github.com/pingcap/tidb/issues/11607)
4. [Implementing Common Table Expressions for MariaDB](https://seim-conf.org/media/materials/2017/proceedings/SEIM-2017_Full_Papers.pdf#page=13)
5. [ANSI/ISO/IEC International Standard (IS) Database Language SQL — Part 2: Foundation (SQL/Foundation)](http://web.cecs.pdx.edu/~len/sql1999.pdf)
6. [Types of Recursions](https://www.geeksforgeeks.org/types-of-recursions/)
