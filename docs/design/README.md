# TiDB Improvement Proposals

This page describes a proposed TiDB Improvement Proposal process for proposing a major change to TiDB projects.

## What is the purpose of improvement proposals?

The purpose of improvement proposals is to have a central place to collect and document planned major enhancements to TiDB projects. While [GitHub Issue](https://github.com/pingcap/tidb/issues) is still the tool to track tasks, bugs, and progress, the proposals give an accessible high-level overview of the result of design discussions and proposals.

## Who should initiate the proposal?

Anyone can initiate a proposal but you shouldn't do it unless you have an intention of getting the work done to implement it (otherwise it is silly).

## Process

Here is the process for making a proposal:

1. [Create an issue](https://github.com/pingcap/tidb/issues/new/choose) to track the proposal progress, basically track subtasks.
2. Create a pull request with a new proposal filed under this directory named as `YYYY-MM-DD-descriptive-title-of-your-proposal.md`, with content follows the [template](./TEMPLATE.md).
3. Request review to reviewers/committers and respond to change requests. The proposal is considered accepted with 2+ approvals from committers and no change requests from reviewers/committers. Please ask a committer to help merge the proposal on accepted.
4. **_Please_** update this page and the index below, to reflect the current stage of the proposal. This acts as the permanent record indicating the result of the proposal (Accepted, Completed or Discarded).

## Status

### Accepted

| Proposal                                                                                    | Target Release |
| ------------------------------------------------------------------------------------------- | -------------- |
| [A new command to restore dropped table](./2018-08-10-restore-dropped-table.md)             |                |
| [Support SQL Plan Management](./2018-12-11-sql-plan-management.md)                          |                |
| [A new storage row format for efficient decoding](./2018-07-19-row-format.md)               |                |
| [Enhance constraint propagation in TiDB logical plan](./2018-07-22-enhance-propagations.md) |                |
| [A SQL Planner based on the Volcano/Cascades model](./2018-08-29-new-planner.md)            |                |
| [Implement Radix Hash Join](./2018-09-21-radix-hashjoin.md)                                 |                |
| [Maintaining histograms in plan](./2018-09-04-histograms-in-plan.md)                        |                |
| [Support a Global Column Pool](./2018-10-22-the-column-pool.md)                             |                |
| [Join Reorder Design v1](./2018-10-20-join-reorder-dp-v1.md)                                |                |
| [Support Window Functions](./2018-10-31-window-functions.md)                                |                |
| [Access a table using multiple indexes](./2019-04-11-indexmerge.md)                         |                |
| [Collations in TiDB](./2020-01-24-collations.md)                                            |                |

### Completed

| Proposal                                                                                                 | First Release |
| -------------------------------------------------------------------------------------------------------- | ------------- |
| [A new aggregate function execution framework](./2018-07-01-refactor-aggregate-framework.md)             |               |
| [TiDB DDL architecture](./2018-10-08-online-DDL.md)                                                      |               |
| [Infer the System Timezone of a TiDB cluster via TZ environment variable](./2018-09-10-adding-tz-env.md) |               |
| [Table Partition](./2018-10-19-table-partition.md)                                                       |               |
| [Implement View Feature](./2018-10-24-view-support.md)                                                   |               |
| [Support restoring SQL text from an AST tree](./2018-11-29-ast-to-sql-text.md)                           |               |
| [Support Plugin](./2018-12-10-plugin-framework.md)                                                       |               |
| [Support Skyline Pruning](./2019-01-25-skyline-pruning.md)                                               |               |
| [Support Index Merge](./2019-04-11-indexmerge.md)                                                        |               |
| [Support Automatically Index Recommendation](./2019-11-05-index-advisor.md)                              |               |

### Discarded

This section consists of proposal that fails to complete or rejected but worth recording.

| Proposal | Discarded Reason |
| -------- | ---------------- |
|          |                  |
