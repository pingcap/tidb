# TiDB Design Documents

## Why We Need Design Documents

The design document provides a high-level description of the architecture and important details of what we want to do. It is the most powerful tool to ensure that our work is correctly performed.

Writing a design document can promote us to think deliberately and gather knowledge from others to get our job done better. An excellent design document is closely related to the success of our task.

## Proprosal Process

1. Before starting to write a design document, please [create a new issue](https://github.com/pingcap/tidb/issues/new/choose) for tracing the process of your design.
2. Create a new [Google Doc](https://docs.google.com/document/u/0/) to write the design document. Please refer to this [proposal template](./TEMPLATE.md).
3. If the design document is ready to be reviewed, please put the shared link with the `can comment` permission in the issue you've created, and one of our engineers will follow up this issue and keep it updated.
4. If needed, we may organize a communications seminar through [Google Hangouts](https://hangouts.google.com/) to discuss your design with you and the interested friends from the community.
5. When your design is finalized, please submit a pull request (PR) to add your new file under this directory, and put the link of your PR in the issue you've created. 
6. Once your PR has been merged, please close the old issue.
7. Start the implementation referring to the proposal, and create a new issue to trace the process.

## Proposal Status

### Proposed

- [Proposal: A new command to restore dropped table](./2018-08-10-restore-dropped-table.md)
- [Proposal: Support SQL Plan Management](./2018-12-11-sql-plan-management.md)

### In Progress

- [Proposal: A new storage row format for efficient decoding](./2018-07-19-row-format.md)
- [Proposal: Enhance constraint propagation in TiDB logical plan](./2018-07-22-enhance-propagations.md)
- [Proposal: A SQL Planner based on the Volcano/Cascades model](./2018-08-29-new-planner.md)
- [Proposal: Implement Radix Hash Join](./2018-09-21-radix-hashjoin.md)
- [Proposal: Maintaining histograms in plan](./2018-09-04-histograms-in-plan.md)
- [Proposal: Support a Global Column Pool](./2018-10-22-the-column-pool.md)
- [Proposal: Join Reorder Design v1](./2018-10-20-join-reorder-dp-v1.md)
- [Proposal: Support Window Functions](./2018-10-31-window-functions.md)
- [Proposal: Access a table using multiple indexes](./2019-04-11-indexmerge.md)

### Completed

- [Proposal: A new aggregate function execution framework](./2018-07-01-refactor-aggregate-framework.md)
- [Proposal: TiDB DDL architecture](./2018-10-08-online-DDL.md)
- [Proposal: Infer the System Timezone of a TiDB cluster via TZ environment variable](./2018-09-10-adding-tz-env.md)
- [Proposal: Table Partition](./2018-10-19-table-partition.md)
- [Proposal: Implement View Feature](./2018-10-24-view-support.md)
- [Proposal: Support restoring SQL text from an AST tree](./2018-11-29-ast-to-sql-text.md)
- [Proposal: Support Plugin](./2018-12-10-plugin-framework.md)
- [Proposal: Support Skyline Pruning](./2019-01-25-skyline-pruning.md)
- [Proposal: Support Index Merge](./2019-04-11-indexmerge.md)
- [Proposal: Support Automatically Index Recommendation](./2019-11-05-index-advisor.md)
