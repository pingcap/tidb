# Priority Queue for Auto Analyze

- Author(s): [Rustin Liu](http://github.com/hi-rustin)
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of Contents

- [Priority Queue for Auto Analyze](#priority-queue-for-auto-analyze)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
  - [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
    - [Scenario Tests](#scenario-tests)
    - [Compatibility Tests](#compatibility-tests)
    - [Benchmark Tests](#benchmark-tests)
  - [Impacts \& Risks](#impacts--risks)
  - [Investigation \& Alternatives](#investigation--alternatives)
  - [Unresolved Questions](#unresolved-questions)

## Introduction

Auto analyze is a background job that automatically collects statistics for tables. This design proposes a priority queue for auto analyze to improve the efficiency of auto analyze.

## Motivation or Background

We have received numerous complaints in the past regarding the auto-analysis tasks. If users have many tables, we must assist them in analyzing the tables in the background to ensure up-to-date statistics are available for generating the best plan in the optimizer.
There are some complaints from users:

1. The automatic analysis retry mechanism has flaws.
   1. If one table analysis keeps failing, it will halt the entire auto-analysis process.
2. Automatic analysis may encounter starvation issues.
   1. The analysis of small tables is being delayed due to the analysis of some large tables.
3. Due to the random selection algorithm, some tables cannot be analyzed for a long time.

So we need to design a priority queue to solve these problems.

## Detailed Design

Before we design the priority queue, we need to know the current auto analyze process.

1. During the TiDB bootstrap process, we initiate the load and update of the stats worker. [session.go?L3470:15](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/session/session.go?L3470:15)
2. We spawn a dedicated worker to perform automated analysis tasks. [domain.go?L2457:19](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/domain/domain.go?L2457:19&popover=pinned)
3. For every stats lease, we will trigger an auto-analysis check: [domain.go?L2469:17](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/domain/domain.go?L2469:17&popover=pinned)
   > Please note that we only run it on the TiDB owner instance.
4. Check if we are currently within the allowed timeframe to perform the auto-analysis. [autoanalyze.go?L149:15](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L149:15&popover=pinned)
5. To prevent the process from getting stuck on the same table's analysis failing, randomize the order of databases and tables. [autoanalyze.go?L155:5](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L155:5&popover=pinned)
6. Try to execute the auto-analysis SQL. [autoanalyze.go?L243:6](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L243:6&popover=pinned)
   1. **If the statistics are pseudo, it means they have not been loaded yet, so there is no need to analyze them.** [autoanalyze.go?L247:5](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L247:5&popover=pinned)
   2. **If the table is too small, it may not be worth analyzing. We should focus on larger tables.** [autoanalyze.go?L247:5](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L247:5&popover=pinned)
   3. **If the table has never been analyzed, we need to analyze it.** [autoanalyze.go?L287:6](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L287:6&popover=pinned)
   4. **If the table reaches the autoAnalyzeRatio, we need to analyze it.** [autoanalyze.go?L287:6](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L287:6&popover=pinned)
      > We use `modify_count/count` to calculate the ratio. [autoanalyze.go?L301:40](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L301:40&popover=pinned)
   5. **If the indexes have no statistics, we need to analyze them anyway.** [autoanalyze.go?L262:15](https://sourcegraph.com/github.com/hi-rustin/tidb@d4618d4a5f91ee3703336fd1ba328c2e477652e5/-/blob/pkg/statistics/handle/autoanalyze/autoanalyze.go?L262:15&popover=pinned)

The above process is the current auto analyze process. We can see that the current auto analyze process is a random selection algorithm. We need to design a priority queue to solve the above problems.

Since we perform analysis tasks synchronously on a single point for each table, we need to carry out weighted sorting on the tables that need analysis.

   1. We still use tidb_auto_analyze_ratio to determine if the table needs to be analyzed. The default value is 0.5. It means that auto-analyze is triggered when greater than 50% of the rows in a table have been modified.
   2. If a table is added to the auto-analysis queue, a weight must be assigned to it to determine the execution order.

Weight table:
| Name                 | Meaning                                                                 | Weight                                                                                                                                                                                                                                                                                                                                                            |
|----------------------|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Percentage of Change | The percentage of change since the last analysis.                       | 50% <= x < 70%: 1 <br/> 70% <= x < 80%: 1.3 <br/> x >= 80%: 1.5  <br/>                                                                                                                                                                                                                                                                                            |
| Last Failed Interval | The interval between now and the last failed analysis ends time.        | Here we cannot easily determine what interval is long and what interval is short. Therefore, we can decide by querying the interval between each automatic analysis in history. <br/> interval < average automatic analysis interval: 0 <br/> interval >=  average automatic analysis interval: 0.5 <br/> interval >=  2 * average automatic analysis interval: 1 |
| Special Event        | For example, the table has a new index but it hasn't been analyzed yet. | HasNewIndexWithoutStats: 1.5                                                                                                                                                                                                                                                                                                                                      |

The calculation formula is: `priority_score = (change_percentage[size] * last_failed_time_weight[interval] * special_event[event])`

## Test Design

A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.

### Functional Tests

It's used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.

### Scenario Tests

It's used to ensure this feature works as expected in some common scenarios.

### Compatibility Tests

A checklist to test compatibility:

- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility

### Benchmark Tests

The following two parts need to be measured:

- The performance of this feature under different parameters
- The performance influence on the online workload

## Impacts & Risks

Describe the potential impacts & risks of the design on overall performance, security, k8s, and other aspects. List all the risks or unknowns by far.

Please describe impacts and risks in two sections: Impacts could be positive or negative, and intentional. Risks are usually negative, unintentional, and may or may not happen. E.g., for performance, we might expect a new feature to improve latency by 10% (expected impact), there is a risk that latency in scenarios X and Y could degrade by 50%.

## Investigation & Alternatives

How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?

## Unresolved Questions

What parts of the design are still to be determined?
