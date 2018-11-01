# Proposal: Join Reorder Design v1

- Author:     [Yiding CUI](https://github.com/winoros)
- Last updated:  2018-10-20

## Abstract

This proposal proposes to improve TiDB’s join reorder algorithm when join size is not large.


## Background

Currently, TiDB’s join reorder algorithm is introduced before we have CBO. This algorithm is very simple and doesn’t work well in many scenarios.

## Proposal

I’ve explored the join reorder algorithms of Orca, Calcite, Spark, MySQL and Postgres. Using DP when the number of nodes are small is an operable strategy.

I implemented two DP algorithms. You can find them [here](https://github.com/winoros/DP-CCP/tree/master). DP-CPP is introduced in [this paper](https://dl.acm.org/citation.cfm?id=1164207).

My experiment result shows that DP-CCP only has a better performance than DP-SUB when the input is list and circle. It cannot beat the DP-SUB when input is star and clique. This is different with the experiment result posted in the paper. The main reason is that maintaining histograms of join online is very costly. If we use 1μs to get the statistics of one join, the most costly part of star graph will be statistics.

So I suggest using DP-SUB as the first version of join reorder since the performance of DP-CCP is not good enough.

The core DP-SUB algorithm is like the following code, a complete implementation is [here](https://github.com/winoros/DP-CCP/blob/dp-sub/dpsub.cpp):

```
func dp(int nodeCnt) {
	for i := 0; i < nodeCnt; i++ {
		bestPlan[1 << i] = planByNodeID[i]
		bestCost[1 << i] = 0
	}
	// Iterate the sub from small to big.
	// This can make sure that node set S1 is visited earlier than S2 if S1 belongs to S2.
	for state := 1; state < (1 << nodeCnt); state++ {
		if bits.OnesCount(i) == 1 || (nodeCnt not connected) {
			continue // It’s a single node or not connected, skip it;
		}
		// This loop can enumerate all its subset.
		for sub := (state - 1) & state； sub > 0; sub = (sub - 1) & state {
			// Inner join’s left and right have no difference, so we only consider it once.
			if sub > remain {
				continue
			}
			// That the dp result is nil means the subset is not connected.
			if bestPlan[sub] == nil || bestPlan[state ^ sub] == nil {
				continue
			}
			join := buildJoin(bestPlan[sub], bestPlan[state^sub], edges between them)
			if bestCost[state] > join.Cost {
				bestPlan[state] = join
				bestCost[state] = join.Cost
			}
		}
	}
}
```

Here the set of node is shown as an integer. If the i-th bit of its binary representation is 1, the i-th node will be in this set.

## Rationale

### How other systems solve the same issue?

DP-SUB is the most commonlly used solution in open source databases.

### What is the disadvantage of this design?

DP-SUB is hard to prune invalid branches. 

And it’s a little hard to enlarge its search space to involve the outer join efficiently.

In a word, DP-SUB cannot be extended easily. The next version of our join reorder will be very likely a new one instead of enhancing DP-SUB.

## Compatibility

There’s no compatibility issue.

## Implementation

First, extract the join nodes. If the number of nodes is less than x, we go into DP-SUB.

For DP-SUB:

1. Enlarge its edges by constant propagation. i.e. If we have `t1.a=t2.a and  t2.a=t3.a`, we should get `t1.a=t3.a`.
2. Number join node from 0 to n. Construct a hash map `pair<int, int> to join key filters`.
3. Do the core dp algo.
4. Get the final result.
5. Note that we can ignore the `OtherCondition` in TiDB since we cannot calculate the selectivity of it. It won’t influence the join reorder’s estimation.

[PR#8025](https://github.com/pingcap/tidb/pull/8025) contains step 3 and 4. Step 2 is done when extract the join group. Step 1 and 5 will be involved in the future pull request.


## Future

- DP-CPP has the ability to prune branches and has an enhanced version involving outer join, semi join and aggregation. You can find it [here](https://pdfs.semanticscholar.org/b24a/e7a6a57c083e441d7c96cb6d71472c6e0c9b.pdf).

- There’s also one way called [transformation based join reorder](http://www.vldb.org/pvldb/vol7/p1243-shanbhag.pdf). The benefit of it is that it can be easily involved in Cascade-like optimizer, and can be easily integrated with other rules. Branch pruning will be done by planner not this reorder algorithm itself.


