# Proposal: Improve rule based index selection

- Author(s): [xuyifangreeneyes](https://github.com/xuyifangreeneyes)
- Discussion PR: https://github.com/pingcap/tidb/pull/27223
- Tracking Issue: https://github.com/pingcap/tidb/issues/26020

## Abstract

This proposal aims to improve rule based index selection including heuristics and skyline pruning, which helps avoid choosing the wrong index due to outdated statistics.

## Background

Currently, the choice of access path mainly depends on statistics. The wrong index may be chosen due to outdated or inaccurate statistics. In order to reduce the possibility of the situation, the optimizer introduces some rule based index selection mechanisms, including heuristics and skyline pruning. We try to improve rule based index selection to enhance robustness of index selection.

## Proposal

The improvement of rule based index selection consists of always-good heuristics, skyline pruning and maybe-good heuristics.

### Always-Good Heuristics

The rules of always-good heuristics are listed as following.

1. If a unique index is covered by conditions and single scan is enough, we directly choose it.
2. If a unique index is covered by conditions but it needs double scan(index scan + table scan), we push it into an array `uniqueIdxsWithDoubleScan`. After collecting all the unique indices satisfying the condition into `uniqueIdxsWithDoubleScan`, we select the one that has the smallest number of rows to read as `uniqueBest`.
3. If an index `idx1` only needs single scan, and there is a unique index `idx2` belonging to `uniqueIdxsWithDoubleScan` whose access condition is subset of `idx1`'s, we can infer that `idx1` has limited rows to read. In this way, we refine `idx2` to obtain `idx1` so we push it into an array `singleScanIdxs`. After collecting all the indices satisfying the condition into `singleScanIdxs`, we select the one that has the smallest number of rows to read as `refinedBest`.
4. If `uniqueBest` exists while `refinedBest` doesn't, we choose `uniqueBest`. If both `uniqueBest` and `refinedBest` exist, we choose the one with the smaller number of rows to read.

In the above rules, an index being covered by conditions means for each index column there is an equal condition.

The following examples show how those rules work. The primary key `a` in the first `SELECT` statement matches rule 1, so it is chosen directly. For the second `SELECT` statement, `uniqueBest` is `idx_b` and we refine `idx_b` to obtain `idx_b_c` which is `refinedBest`. We choose `idx_b_c` finally since it has the smaller number of rows to read.

```sql
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, UNIQUE INDEX idx_b(b), UNIQUE INDEX idx_b_c(b, c));
SELECT * FROM t WHERE a = 2 OR a = 5;
SELECT b, c FROM t WHERE b = 5 AND c > 10;
```

### Skyline Pruning

TiDB has [skyline pruning](https://github.com/pingcap/tidb/blob/master/docs/design/2019-01-25-skyline-pruning.md) to prune the index which is strictly worse than another index. When comparing two indices, it considers three dimensions: single scan or double scan, the set of columns in the access condition, matching of the physical property. There are two improvements we can make for the first dimension and the third dimension respectively.

1. If both indices need double scan, the current implementation regards the two indices equal on the first dimension. However, we can check the set of columns in `AccessPath.IndexFilters` to compare the number of table-read rows. For example, though `idx_b` and `idx_b_c` have the same access condition `b > 5` and both need double scan, `idx_b_c` has the filter condition `c > 5` but `idx_b` doesn't. Hence `idx_b_c` is better than `idx_b`.
```sql
CREATE TABLE t(a INT, b INT, c INT, INDEX idx_b(b), INDEX idx_b_c(b, c));
SELECT * FROM t WHERE b > 5 and c > 5;
```
2. The current implementation doesn't correctly identify whether the index matches the physical property in some cases and we can fix them. For example, `idx_a_b_c` matches the required order `ORDER BY a, c` since `b = 4` is constant.
```
CREATE TABLE t(a INT, b INT, c INT, d INT, INDEX idx_a_b_c(a, b, c))
SELECT * FROM t WHERE b = 4 ORDER BY a, c;
```

### Maybe-Good Heuristics

```sql
CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX idx_b(b));
SELECT * FROM t WHERE b = 2 ORDER BY a LIMIT 10;
```
In the above example, the optimizer may choose `TableFullScan` which takes much more time than  `IndexRangeScan`. The cause is that it is hard to tell which is better between `TableFullScan` matching the required order and fetching the limited number of rows and `IndexRangeScan`. For the situation, TiDB has a switch `tidb_opt_prefer_range_scan`. When the switch turns on, the optimizer always prefers range scan over full scan. There are three improvements we can do on the switch.

1. The switch is session-level, which is not enough for use in real scenes. Hence we make the switch both session-level and global-level.
2. For the unsigned handle of the table, the full range is `[0, +inf]` but the current implementation doesn't regard it as full range, which needs fixing.

## Rationale

Rules such as heuristics and skyline pruning can prevent the optimizer from choosing some obviously wrong index, which are implemented in many other databases.

## Compatibility

It does not affect the compatibility.

## Implementation

Always-good heuristics have nothing to do with the physical property, so it will be implemented in `(*DataSource).DeriveStats` after filling content for each `(*DataSource).possibleAccessPaths`. Skyline pruning and prefer-range-scan are implemented in `(*DataSource).findBestTask` and we just need to make the improvements in place.
