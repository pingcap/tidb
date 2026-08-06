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

Before diving into the new design, let's take a look at the current batch-request implementation.

Currently, store-batched coprocessor requests are used for handle-based table reads. They combine eligible small Region tasks targeting the same TiKV store into a single unary coprocessor RPC.

The [workflow](https://editor.plantuml.com/uml/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40) is as follows:

![](https://img.plantuml.biz/plantuml/png/lPPDJiCm48NtESM85GY50tI14EL721PLMzW0Ysaoj5OTZsKxG6_Fn0qaaK3Gj7HMJYlvytjlnWcS-O0kb8M6Vwm4WWgQO5WwHoR09B2Zz1n3jg0SXcmTP-GzExZI_BO5PY-LW1NBLAOiYfQ3gReuXnkJq_iTy_BU7W3wvhcqkyIqhHfg9Lv6sdgv8ypjGmTpQNBBgWPzFkm6CoRCOSIiuzxLef-43cOlbNG2Ja_h10OmAMU52X1mfYbz8MbmMVkbXzbAvCuLcyqmTR8jmhLZGVe2jMwsZdRwQghwgMamdmcB538vi24e3RfLfoV6e-6JEInGcNW4E5vFT1pegVpm-7pqBUQhVGHKnIsGwWOevMeW5AjgX-AUIWosSWGvbrvjfYPsKjeHCwPGEXFbEFAb3c39zKZa9pKDQOJP4kS4qHrXMJQU04t-PBecZUl_7f__YcbrGKkFXD5m-bRJ0W9fzVO0BaYhL_5A_4fhlzOE-bwOlLG2XeCahUQB0FoonRNr2sQw8CZgbrf1sGADyeMa8XqxYsyt3y6XaN1SEDa2KurtphARw1AGcpTjZ2lDcFlUkxSmrS17u_wp4ZWJJlt1yG40)

1. During the table-fetch phase of an `IndexLookUp` query, TiDB divides the handle reads into Region tasks and groups eligible small tasks that target the same TiKV store.

2. TiDB sends each group as a single unary coprocessor RPC, placing one task in the main request and the rest in `StoreBatchTask` entries.

3. TiKV schedules each Region task independently in its read pool and collects the results.

4. TiKV returns the main response together with the corresponding `StoreBatchTaskResponse` entries. TiDB then unpacks them into per-Region results. TiKV never merges the payloads.

The same mechanism can batch `ANALYZE` requests for multiple Regions on the same TiKV store. However, it must be extended to merge the individual statistics payloads on TiKV and return the combined payload in the main response.

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
