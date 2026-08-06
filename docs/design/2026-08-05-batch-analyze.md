# Batch Requests for the `ANALYZE` Statement

- Author(s): [0xPoe](https://github.com/0xPoe)
- Discussion PR: https://github.com/pingcap/tidb/pull/70355
- Tracking Issue:

## Table of Contents

* [Introduction](#introduction)
* [Motivation](#motivation)
* [Detailed Design](#detailed-design)
    * [Current Implementation](#current-implementation)
    * [Batched Analyze Requests](#batched-analyze-requests)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)
* [Future Possibility](#future-possibility)
* [FAQ](#faq)

## Introduction

This document proposes reusing the existing batch-request mechanism in the coprocessor framework to pre-merge the statistics collected from TiKV, so that `ANALYZE` can reduce network traffic and memory usage on TiDB.

## Motivation

Large tables can contain thousands of regions. In the current statistics-collection process, TiDB sends a coprocessor task to each region, collects the partial statistics, and merges them locally.

The partial statistics collected from TiKV include row samples, a row count, null counts, total sizes, and FM sketches. Samples are simply raw row data, which makes them the most expensive part to transfer over the network and process on TiDB. There is little we can do about that, however, since TiDB needs the raw rows to build the final statistics. FMSketch is a different story: it is also large, but unlike samples, it can be merged before it ever leaves TiKV.

A single FMSketch, once serialized to Protobuf, takes at most 160 KiB. For a table with 2 billion rows spread across, say, 20,000 regions, transferring all of the sketches would cost up to 20,000 × 160 KiB ≈ 3 GiB of network traffic.

Since FMSketch is a mergeable data structure, nothing prevents us from merging sketches on TiKV before sending them back. This document therefore proposes reusing the existing batch-request mechanism in the coprocessor framework to merge sketches locally and return far fewer partial statistics to TiDB, saving both network traffic and memory on TiDB.

## Detailed Design

### Current Implementation

### Batched Analyze Requests

## Test Design

### Functional Tests

### Scenario Tests

### Compatibility Tests

### Benchmark Tests

## Impacts & Risks



## Investigation & Alternatives



## Unresolved Questions



## Future Possibility



## FAQ

-
