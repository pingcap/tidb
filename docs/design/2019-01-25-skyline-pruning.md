# Proposal: Support Skyline Pruning

- Author(s):     [Haibin Xie](https://github.com/lamxTyler)
- Last updated:  2019-01-25
- Discussion at:

## Abstract

This proposal introduces some heuristics and a general framework for pruning the access paths. With the help of it, the optimizer can avoid some wrong choices on access paths.

## Background

Currently, the choice of access path strongly depends on the statistics. We may choose the wrong index due to outdated statistics. However, many of the wrong choices can be eliminated by simple heuristics, for example, if the primary key or unique indices can be fully matched, we can choose it without the referring to the statistics.

## Proposal

The most important factors to choose the access paths are the number of rows that need to be scanned, whether or not it matches the physical property and does it require a double scan. Among these three factors, only the scan row count depends on statistics. So how can we compare the scan row count without statistics? Let's take a look at an example:

```sql
create table t(a int, b int, c int, index idx1(b, a), index idx2(a));
select * from t where a = 1 and b = 1;
```

From the query and schema, we can know that the access condition of  `idx1` could strictly cover `idx2`, therefore the number of rows scanned by `idx1` will be no more than `idx2`,  so `idx1` will be better than `idx2`  in this case.

So how can we combine these factors to prune the access paths? Consider two access paths `x` and `y`, if `x` is not worse than `y` at all factors, and there exists one factor that `x` is better than `y`, then we can prune `y` before referring to the statistics, because `x` works better than `y` at all circumstances. This is also called skyline pruning.

## Rationale

The skyline pruning is also implemented in other databases, including MySQL and OceanBase.  Without it, we may suffer from choosing the wrong access path in some simple cases.

## Compatibility

It does not affect the compatibility.

## Implementation

Since we need to decide whether the access path matches the physical property, we need to do the skyline pruning when finding the best task for the data source. And usually there won't be too many indices, a naive nested-loops algorithm will suffice. The comparison of any two access paths has been explained in the previous `Proposal` section.

## Open issues (if applicable)

## References

- [The Skyline Operator](https://www.cse.ust.hk/~dimitris/PAPERS/SIGMOD03-Skyline.pdf)
