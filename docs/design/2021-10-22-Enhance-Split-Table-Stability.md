# Proposal: Enhance Split Table Stability and Usability

- Author(s): [hzh](https://github.com/hzh0425)
- Discussion PR: https://github.com/tikv/pd/issues/4095
- Tracking Issue: https://github.com/pingcap/tidb/issues/29034

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Follow-up work](#Follow-up work)

## Introduction

Currently, the split table(regions) in tidb is constructed by following 2 steps:

1. Notify tikv to split regions
2. Notify pd to scatter regions

But this will let the time cost for the whole splitting table become unstable and rather time-consuming.

This proposal aims to split and scatter regions in pd directly

## Motivation or Background

Split table region : split a region into multi regions by split-keys

Scatter table regions: regions are still in the original stores after split. Scatter is to load balance regions and evenly distribute regions to other stores

However, before this proposal, TIDB and some tools were inconsistent when calling PD API -- split and scatter regions.

Our purpose is to add a new  PD API -- SplitAndScatterRegions(), so that TIDB and other tools can easily call this API uniformly

## Detailed Design

### kvproto: pdpb.proto

add a new interface SplitAndScatterRegions in pdpb.proto

```
rpc SplitAndScatterRegions(SplitAndScatterRegionsRequest) returns (SplitAndScatterRegionsResponse) {}
```

### PD: Grpc_Service, client.go

Implement SplitAndScatterRegions in grpc_service and client

grpc_service can directly call the original implementation splitRegions and scatterregions to achieve simultaneous split and scatter

client can

### TIDB

1.add new interface SplitAndScatterRegions in kv/kv.go/SplittableStore

```
SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, tableID *int64) (regionID []uint64, err error)
```

this interface will be implement by client-go

2.use new api SplitAndScatterRegions in executor/split.go

### client-go

Implement SplitAndScatterRegions in tikv/split_regions.go





## Test Design

I will test the stability and performance by controlling variables under different parameter conditions

For example:

- Split a empty table into 32 regions / 400 regions ....
- Split a table that contains data into 32 regions / 400 regions ....
- Split a table which dose not contain primary key into 32 regions / 400 regions ....
- ...............

I will record the time spent in these cases



## Follow-up work

Enhance split region statement























