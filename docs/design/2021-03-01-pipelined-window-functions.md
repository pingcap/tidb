# Proposal: Pipeline Window Function Execution

- Author(s): [ichn-hu](https://github.com/ichn-hu)
- Discussion at: https://github.com/pingcap/tidb/pull/23028
- Tracking issue: https://github.com/pingcap/tidb/pull/23022

## Note

* Row number is often shortened to RN, and RNF for RN function
* Window function is often shortened to WF

## Abstract

This document proposes to support executing window functions in a pipelined manner.

## Background

The current WF implementation materialized a whole partition before processing it, and if a partition is too large, it will cause TiDB OOM. One particular example is seen in [issue/18444](https://github.com/pingcap/tidb/issues/18444) where the whole table is processed as a single partition in order to get a row number for the paging scenario, while the alternative solution using user variable could significantly decrease the memory usage.

As the cause is clear, we aim to pipeline the calculation of some of the window function, which means the window function executor will return data as soon as possible before the whole partition is consumed. After this design is implemented, the evaluation of RN WF will not cause the whole partition to be materialized, instead, it can be processed in a pipelined manner in the whole executor pipeline, that’s why we call it pipelining.

### Review of current implementation

The current window function implementation is like this (with a focus on processing RN):

1. Data is sorted by partition key and order by key when feeding to window function.
2. vecGroupChecker is used to split data by the partition key.
3. Data is accumulated in groupRows until a whole partition is read from child executor.
4. Then e.consumeGroupRows will be called, which in turn uses windowProcessor to process the rows.
5. There are current 3 processor types that implement the windowProcessor interface:
   1. aggWindowProcessor, dealt with partition without frame constraint, i.e. the function will be called upon the whole partition, e.g. sum over whole partition, then every row gets the same result on the window function, it is indeed confusing that RN is implemented on aggWindowProcessor, latter we’ll show that it is more natural to be expressed in rowFrameWindowProcessor.
   2. rowFrameWindowProcessor, dealt with partition with ROWS frame constraint, i.e. a fixed length bounding window sliding over rows, each step produced a new value given the rows within the window. Note the window can have unbounded preceding and following.
   3. rangeFrameWindowProcessor, with RANGES frame constraint, i.e. the window is defined by value range, so it can vary (a lot) from row to row.
6. For RN, it only uses `aggWindowProcessor`, as [the MySQL document](https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html) pointed out.

> Standard SQL specifies that window functions that operate on the entire partition should have no frame clause. MySQL permits a frame clause for such functions but ignores it. These functions use the entire partition even if a frame is specified:  

* CUME_DIST()
* DENSE_RANK()
* LAG()
* LEAD()
* NTILE()
* PERCENT_RANK()
* RANK()
* ROW_NUMBER()

7. In aggWindowProcessor, three functions are implemented:
   1. consumeGroupRows: call agg function’s UpdatePartialResult on all rows within a partition
   2. appendResult2Chunk: call agg function’s AppendFinalResult2Chunk and write result to the result chunk, this function is called repetitively until every row is processed in a partition
   3. resetPartialResult: call agg function’s ResetPartialResult
8. Accordingly, the RN agg function does nothing on UpdatePartialResult, increases the RN counter and append to result on AppendFinalResult2Chunk and resets the counter on ResetPartialResult

## Proposal

After carefully examining the source code, we provide the following solution, which is based on unifying windowProcessor, and then pipeline it, so that RN function as well as many other WF currently using sliding windows can be pipelined.

### Unify windowProcessor

* For rowFrameWindowProcessor and rangeFrameWindowProcessor does nothing in consumeGroupRows  
  * And they will call Slide if the WF has implemented slide (i.e. Slide, AppendFinalResult2Chunk), or it will recalculate the result on the whole frame using the traditional aggFunc calculation strategy (i.e. UpdatePartialResult and then AppendFinalResult2Chunk and ResetPartialResult for each row)
* The Slide implementation is by nature pipelinable.  
  * **The two sides of the sliding window only moves monotonically**.
  * However, the current implementation requires the number of rows in the whole partition to be known (or it can’t be pipelined) if the end is unbounded.
* For aggWindowProcessor:
  * RN can definitely be pipelined, and it can be implemented in a sliding way (the window is the current row itself)
  * Aggregation over the whole partition can’t be pipelined, and it can only be processed after the whole partition is ready.

However, we could see it as the sliding window is the whole partition for each row

### How to unify?

We need to modify the executor build to support this:

* For row number: the sliding window is of length 1, it slides with current row, **i.e. is a rowFrame start at currentRow and end at currentRow**
* For other agg functions on the whole partition: the sliding window is the whole partition, invariant for each row, **i.e. is a rowFrame start at unbounded preceding and end at unbounded following**

### Pipelining

* assume the total number of rows in a partition is N, which we do not know in advance since the data is pipelined  
  * UpdatePartialResult: append partial rows to the aggregation function, needs to append N rows eventually
  * Slide (perhaps this function is better implemented on windowProcessor): must be called before calling AppendFinalResult2Chunk, it returns success or fail if the current rows consumed by UpdatePartialResult is not enough to slide for next row, it will also return an estimated number of rows so that it can be called (useful for unbounded following, we can use -1 to denote that it needs the whole partition, 0 means success immediately, and n if we could know the number of rows needed (for rowFrame) or 1 for rangeFrame since we need to examine row by row
  * AppendFinalResult2Chunk: append result for one row, can be called N times, and can only be called after a successful slide
  * FinishUpdate: called upon the whole partition has been appended, this is to notify the SlidingWindowAggFunc that the whole partition is consumed, so that for those function that needs the whole partition, it is now time to return success on slide
* We want the movement of the sliding window to be the driver for data fetching on the children executor, so the dataflow logic needs to be modified  
  * Next() will call windowProcessor’s Slide function to drive it
  * Slide function will fetch data from child, and use vecGroupCheck to split it
  * Then the data is processed at maximum effort using  
    * UpdatePartialResult
    * Slide or do nothing if it is not SlidingWindowAggFunc
    * If Slide returns success or the whole partition is processed  
      * Obtain the result using AppendFinalResult2Chunk
  * Result is then feed back to Next, and returned once the chunk is full  
    * There could be a background goroutine pulling data

## Rationale

This feature will decrease memory consumption for executing window function.

## Compatibility

Pipelining won't cause any compatibility issue.

## Implementation

All implemented by [PR23022](https://github.com/pingcap/tidb/pull/23022).

* [x] Create PipelinedWindowExec based on current implementation and modify the windowProcessor interface.
* [x] Change data flow, make Next() pulling data from windowProcessor, and windowProcessor calls fetchChild and process data at maximum effort.
* [x] Modify Slide semantic and add FinishUpdate function on SlidingWindowAggFunc interface, and modify correspondingly on each window function.
* [x] Done pipelining for SlidingWindowAggFunc, add test to make sure it is correct.
* [x] Modify RN to be SlidingWindowAggFunc, and add planner support.
* [x] Add test for RN.
* [x] Benchmark, make sure it has constant memory consumption and no execution time regression.
