# Proposal: Maintain statistics in `Plan`

- Author:     [Yiding CUI](https://github.com/winoros)
- Last updated:  2018-09-04

## Abstract

This proposal proposes to maintain the histogram in `Plan`’s statistics information.


## Background

Currently, TiDB only uses statistics when deciding which physical scan method a table should use. And TiDB only stores simple statistics in the plan structure. But when deciding the join order and considering some other optimization rules, we need more detailed statistics.

So we need to maintain the statistics in the plan structure to get sufficient statistics information to do optimizations.

## Proposal

The new `statsInfo` of `plan` should be something like the following structure:

```
type statsInfo struct {
	// This two attributes are already used in current statsInfo.
	count float64
	// The number of distinct value of each column.
	ndv []float64

	// HistColl collects histograms of columns.
	histColl statistics.HistColl
	// rangesOfXXX This won’t be maintained in the first version.
	rangesOfColumn map[int][]*ranger.Range 
	rangesOfIndex map[int][]*ranger.Range
	// max/minValues is a simplified version of rangesOfXXX. Since maintaining rangesOfXXXX consumes a lot of time and memory, we can first check whether this can meet our needs.
	maxValues map[int][]types.Datum
	minValues map[int][]types.Datum
}
```

This struct will be maintained when we call `deriveStats`.

Currently we don't change the histogram itself during planning. Because it will consume a lot of time and memory space. I’ll try to maintain ranges slice or the max/min value to improve the accuracy of row count estimation instead.

Before we maintain `rangesOfXXX` or `max and min values`:

We maintain the histogram in `Projection`, `Selection`, `Join`, `Aggregation`, `Sort`, `Limit` and `DataSource` operators.

For `Sort`, we can just copy children's `statsInfo` without doing any change.

For `Limit`, we can just copy children's `statsInfo` or ignore the histogram information. As you know, its execution logic is based on randomization. It's hard to maintain the statistics information after `Limit`. But we may use the information before it to do some estimation in some scenarios.

For `Projection`, change the reflection of the map we maintained.

For `Aggregation`, use the histogram to estimate the NDV of group-by items. If one index cannot cover the group-by item, we’ll multiply the NDV of each group-by column. If the output of `Aggregation` includes group-by columns, we’ll maintain the histogram of them for future use.

For `Join`, there’re joins as follows:

- Inner join: use histograms to do the row count estimation with the join key condition. Since it won’t have one side filter, we only need to consider the composite filters after considering the join key. We can simply multiply `selectionFactor` if there are other composite filters in our first version of implementation. Since `Selectivity` cannot calculate selectivity of an expression containing multiple columns.

- One side outer join: It depends on the join keys’ NDV. And we can just use histograms to estimate it if there’re non-join-key filters.

- Semi join: It’s something similar to inner join. But no data expanding occurs. When we maintain the range information, we can get a nearly accurate answer of its row count.

- Anti semi join: Same with semi join.

For `Selection`, just use it to calculate the selectivity. 

For `DataSource`, if it’s a non-partitioned table, we just maintain the map. If it’s a partitioned table, we now only store the statistics of each partition. So we need to merge them. We need a cache or something else to ensure that we won’t merge them each time we need it, which will consume tooooo much time and memory space.

For other plan operators or `DataSource` which do not contain any histogram, we just use the original way to maintain `statsInfo`. We won’t maintain histograms for them. Taking `Union` for an example, if we maintain histograms for it, we need to merge the histograms, which is really an expensive operation. But we can consider maintaining this if a hint is given.

When we maintain `rangesOfXXX` or `max and min values`:

Most things are the same with above. But there will be something more to do.

For `Selection` and `Join`, we need to cut off the things which are not in ranges when doing estimation, and update the ranges information after the estimation. Also the NDV of column can be estimated more accurately.

For `Aggregation`, we only need to cut off the things which are not in ranges when doing estimation. There is no need to update the ranges information.

For `TopN`, we now have the ability to maintain histograms of the order-by items.


## Rationale

### How other systems solve the same issue?

I’ve looked into Spark. They did nearly the same thing with what I said. They only maintain the max and min values, rather than the `ranges` information. And they don’t have the index, so they only maintain the column’s max/min value which make problem much easier to solve.

As for Orca and Calcite, I haven’t discovered where they maintain this information. But there’s something about statistics in Orca’s paper. According to the paper, I think they construct new histograms during planning and cache it to avoid building too many times.

### What is the disadvantage of this design?

This may have side effects on OLTP performance. But I’ll try to reduce it.
 
For now, only join reorder and the position `after logical optimize before physical optimize` will trigger this mechanism and it’s controlled by `tidb_optimizer_selectivity_level` which is disabled by default. This may not have much more side effects on simple point queries.

And the `expectedCount` we used in physical plan is something same with `Limit`. So the row count modification during physical plan won’t be affected.

After we switch to the cascade-like planner, the rule that needs cost to make decision is still a small set of all. And the existence of `Group` can also help us. If we lazily construct the `statsInfo`, this may not be the bottleneck.

### What is the impact of not doing this?

Many cases reported by our customer already prove that we need more accurate statistics to choose a better join order and a proper join algorithm. Only maintaining a number about row count and a slice about NDV is not enough for making that decision.

If we don’t do this, we cannot fix some cases except we use hints.

## Compatibility

There’s no compatibility issue.

## Implementation

First, maintain the histogram in `DataSource`. In this step, there will be some changes in the `statistics` package to make it work. It may take a little long time to do this. [PR#7385](https://github.com/pingcap/tidb/pull/7385)

Then, all changes will be things inside `statsInfo`, which won’t affect other parts. It may not take too much time to implement this part. Like [PR#7595](https://github.com/pingcap/tidb/pull/7595)

The future work will depend on the benchmark result of the join reorder algorithm.
