# TiDB Design Documents

- Author(s): [fengou1](http://github.com/fengou1)
- Discussion PR: https://github.com/pingcap/tidb/issues/28763
- Tracking Issue: https://github.com/pingcap/tidb/pull/27036

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
  * [Compatibility Tests](#compatibility-tests)
  * [Benchmark Tests](#benchmark-tests)

## Introduction

The proposal of this design is to speed up the BR restore.

## Motivation or Background

The cluster has 6 TiB data, 30k tables, and 11 TiKVs. When I use BR to backup and restore the cluster, the speed is particularly slow. After investigation, BR can only create 2 tables per second, the entire restore speed takes nearly 4h, and the execution time spent creating tables is close to 4h. It can be seen that the execution speed of ddl is the bottleneck in this scenario.

## Detailed Design

Backup and Restore for massive tables is extremely slow, the bottleneck of creating the table is to waiting for the schema version change. Each table creates ddl cause a schema version change. 60000 tables have almost 60000 times schema change, it is very cost of restoring massive table

Currently, BR uses an internal interface named CreateTableWithInfo to create table, which creating the table and wait the schema changing one-by-one, omitting the sync of the ddl job between BR and leader, the procedure of creating one table would be like this:
```go
for _, t := range tables {
  RunInTxn(func(txn) {
  m := meta.New(txn)
  schemaVesrion := m.CreateTable(t)
  m.UpdateSchema(schemaVersion)
 })
```

the new design will introduce a new batch create table api `BatchCreateTableWithInfo`
```go
for _, info := range tableInfo {
  job, err := d.createTableWithInfoJob(ctx, dbName, info, onExist, true)
  if err != nil {
  return errors.Trace(err)
  }

  // if jobs.Type == model.ActionCreateTables, it is initialized
  // if not, initialize jobs by job.XXXX
  if jobs.Type != model.ActionCreateTables {
    jobs.Type = model.ActionCreateTables
    jobs.SchemaID = job.SchemaID
    jobs.SchemaName = job.SchemaName
  }

  // append table job args
  info, ok := job.Args[0].(*model.TableInfo)
  if !ok {
    return errors.Trace(fmt.Errorf("except table info"))
  }
    args = append(args, info)
  }

  jobs.Args = append(jobs.Args, args)

  err = d.doDDLJob(ctx, jobs)

  for j := range args {
  if err = d.createTableWithInfoPost(ctx, args[j], jobs.SchemaID); err != nil {
    return errors.Trace(d.callHookOnChanged(err))
  }
 }
```

For ddl work, introduce a new job type `ActionCreateTables`
```go
 case model.ActionCreateTables:
 tableInfos := []*model.TableInfo{}
 err = job.DecodeArgs(&tableInfos)

 diff.AffectedOpts = make([]*model.AffectedOption, len(tableInfos))
 for i := range tableInfos {
  diff.AffectedOpts[i] = &model.AffectedOption{
  SchemaID: job.SchemaID,
  OldSchemaID: job.SchemaID,
  TableID: tableInfos[i].ID,
  OldTableID: tableInfos[i].ID,
  }
 }
```

for each job `ActionCreateTables`, only one schema change, so one schema version for one batch of creating table, it greatly improves the performance of creating tables.


The feature auto-enabled with batch size 128 to batch create the table. Users can disable the feature by specifying `--ddl-batch-size=0`. The max batch size depends on the txn message size, currently, it configures 8MB by default. 

## Test Design
UT: see PRs https://github.com/pingcap/tidb/pull/28763, https://github.com/pingcap/tics/pull/4201, https://github.com/pingcap/tidb/pull/29380

Integration test also covered among CDC, BR, TiDB, binlog, TiFlash etc.


### Compatibility Tests

- Compatibility with binlog, please refer to https://github.com/pingcap/tidb-binlog/pull/1114.
- Compatibility with CDC, a regression test made for cdc work with br test. since CDC has a whitelist for unrecognizing ddl job and pulling data from tikv directly so that we did not find regression issues.
- Compatibility with TiFlash https://github.com/pingcap/tics/pull/4201.
- Upgrade compatibility: BR + tidb without interface BatchCreateTableWithInfo, the restore falls back to old legacy way that creates table one by one.
- Downgrade compatibility: old BR + new tidb with BatchCreateTableWithInfo, the restore using legacy way to create table.

### Benchmark Tests
- 61259 tables restore takes 4 minute 50 seconds, 200+ tables/per seconds with following configuration:
TiDB x 1: 16 CPU, 32 GB
PD x 1: 16 CPU, 32 GB
TiKV x 3: 16 CPU, 32 GB