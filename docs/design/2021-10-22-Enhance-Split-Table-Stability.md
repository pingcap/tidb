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
1. Notify tikv to split regions.
2. Notify pd to scatter regions.

However, because of the following reasons, the time cost for the whole splitting table become unstable and rather time-consuming:
1. Split and scatter tasks are asynchronous, which will cause many split and scatter tasks at times.
2. At the same time will cause multiple conflicts on the same region, then we need more time to retry.
3. It is very likely to cause a split from a scattering region, resulting in many 4 replicas regions, which will lead to the time-consuming of split table be unstable.

More importantly, there exists different ways of tidb and other tools to split and scatter region which makes it hard to be managed. 

So, in order to make the time cost of the whole process more stable and unify the upstream, this proposal aims to let pd handle split and scatter region work directly.

## Motivation or Background

`Split table region` means split a region into multi regions by split-keys.

`Scatter table regions`: means distribute new regions to other stores.

Our purpose is to add a new PD API -- SplitAndScatterRegions(), so that TIDB and other tools can easily call this API uniformly and make the time cost of the whole process more stable.

## Detailed Design

As you can see from above, the goal is to add an API in PD and change upstream like TIDB and other tools to use this API.

This can be achieved through the following steps:

1. Add an API -- SplitAndScatterRegions() in PD.

2. As for the implementation of this API, We can directly use SplitRegions() and ScatterRegions() provided in PD.

3. Call this API when TIDB or other tools need to  Split and Scatter regions.

4. Wait scatter done if needed.

## Test Design

I will test the stability and performance by controlling variables under different parameter conditions.

For example:

- Split a empty table into 32 regions / 400 regions ....
- Split a table that contains data into 32 regions / 400 regions ....
- Split a table which dose not contain primary key into 32 regions / 400 regions ....
- ...............

I will record the time spent in these cases.

## Follow-up work

Enhance split region statement.
