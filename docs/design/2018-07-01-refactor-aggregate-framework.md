# Proposal: A new aggregate function execution framework

- Author(s):     [@zz-jason](https://github.com/zz-jason)
- Last updated:  2018-07-01
- Discussion at: https://github.com/pingcap/tidb/pull/6852

## Abstract

This proposal proposes a new execution framework for the aggregate functions, to improve the execution performance of aggregate functions.

## Background

In release-2.0, the framework of aggregate functions is located in the “expression/aggregation” package. In this framework, all the aggregate functions implement the `Aggregation` interface. It uses the `AggEvaluateContext` to store partial result for all the aggregate functions with all kind of possible argument types. The `DistinctChecker` in the `AggEvaluateContext` uses a `[]byte` as the key of `map`, which is used to de-duplicate the values in the same group. During the execution, the `Update` interface is called to update the partial result for every input record. It enumerates every possible state during the execution of every aggregate function, which introduces a lot of CPU branch predictions.

It’s easy to implement a new aggregate function under this framework. But it has some disadvantages as well:

- `Update` is called for every input record. The per record function call could bring a huge overhead, especially when the execution involves tens of thousands of input records.
- `Update` is called for every possible aggregate state, which also introduces a lot of CPU branch predictions. For example, function `AVG` behaves different when the state is `Partial1` and `Final`, it has to make a `switch` statement to handle all the possible states.
- `GetResult` returns a `types.Datum` as the final result of a group. Currently, TiDB uses `Chunk` to store the records during the query execution. The returned `Datum` has to be converted to `Chunk` outside the aggregate framework, which introduces a lot of data conversions and object allocations.
- `AggEvaluateContext` is used to store every possible partial result and maps, which consumes more memory than what is actually needed. For example, the `COUNT` only need a `int64` field to store the row count it concerns.
- `distinctChecker` is used to de-duplicate values, and uses `[]byte` as key. The encoding and decoding operations on the input values also bring a lot of CPU overhead, which can be avoided by directly using the input value as the key of that `map`.

## Proposal

In this PR: https://github.com/pingcap/tidb/pull/6852, a new framework was proposed. The new framework locates in the “executor/aggfuncs” package.

In the new execution framework, every aggregate function implements the `AggFunc` interface. It uses the `PartialResult`, which is actually a `unsafe.Pointer`, as the type of the partial result for every aggregate function. `unsafe.Pointer` allows the partial result to be any kind of data object.

The `AggFunc` interface contains the following functions:
- `AllocPartialResult` allocates a specific data structure to store the partial result, initializes it, converts it to `PartialResult` and returns it back. Aggregate operator implementation, for example, stream aggregate operator, should hold this allocated `PartialResult` for further operations involving the partial result such as `ResetPartialResult`, `UpdatePartialResult`, etc.
- `ResetPartialResult` resets the partial result to the original state for a specific aggregate function. It converts the input `PartialResult` to the specific data structure which stores the partial result and then resets every field to the proper original state.
- `UpdatePartialResult` updates the specific partial result for an aggregate function using the input rows which all belong to the same data group. It converts the `PartialResult` to the specific data structure which stores the partial result and then iterates on the input rows and updates that partial result according to the functionality and the state of the aggregate function.
- `AppendFinalResult2Chunk` finalizes the partial result and appends the final result directly to the input `Chunk`. Like other operations, it converts the input `PartialResult` to the specific data structure firstly, calculates the final result and appends that final result to the `Chunk` provided.
- `MergePartialResult` evaluates the final result using the input `PartialResults`. Suppose the input `PartialResults` names are `dst` and `src` respectively. It converts `dst` and `src` to the same data structure firstly, merges the partial results and store the result in `dst`. 

The new framework uses the `Build()` function to build an executable aggregate function. Its input parameters are:
- `aggFuncDesc`: the aggregate function representation used by the planner layer.
- `ordinal`: the ordinal of the aggregate functions. This is also the ordinal of the output column in the `Chunk` of the corresponding aggregate operator.

The `Build()` function builds an executable aggregate function for a specific input argument type, aggregate state, etc. The more specific, the better.

## Rationale

Advantages:
- The partial result under the new framework can be any type. Aggregate functions can allocate the memory according to the exact need without any wasting. The OOM risk can also be reduced when it is used in the hash aggregate operator.
- That the partial result can be any type also means the aggregate functions can use a map with the specific input type as keys. For example, use `map[types.MyDecimal]` to de-duplicate the input decimal values. In this way, the overhead on the encoding and decoding operations on the old framework can be reduced.
- `UpdatePartialResult` is called with a batch of input records. The overhead caused by the per record function call on the framework can be saved. Since all the execution operators are using `Chunk` to store the input rows, in which the data belonging to the same column is stored consecutively in the memory, the aggregate functions should be executed one by one, take full utilization of the CPU caches, reduce the cache misses, and improve the execution performance.
- For every state and every kind of input type, a specific aggregate function should be implemented to handle it. This means the CPU branch predictions on the aggregate state and input value types can reduce during the execution of `UpdatePartialResult`, utilize the CPU pipelines, and improve the execution speed.
- `AppendFinalResult2Chunk` directly finalizes the partial result to the chunk, without converting it to `Datum` and then converting the `Datum` back into `Chunk`. This saves a lot of object allocation, reduces the overhead of golang’s gc worker, and avoids the unnecessary value convertings between `Datum` and `Chunk`.

Disadvantages:
- An aggregate function need to be implemented for every possible state and input type. This could introduces a lot of development work. And it takes more coding work to add a new aggregate function.

## Compatibility

For now, the new framework is only supported in the stream aggregate operator. If the `Build()` function returns a `nil` pointer, it rolls back to the old framework.

So this new framework can be tested during the development, and all the results should be the same with the old framework.

## Implementation

- Implement the new framework, including the `AggFunc` interface and the `Build()` function. [@zz-jason](https://github.com/zz-jason)
- Implement the `AVG` function under the new framework. [@zz-jason](https://github.com/zz-jason)
- Implement other aggregate functions under the new framework. [@XuHuaiyu](https://github.com/XuHuaiyu)
- Add more integration tests using the random query generator. [@XuHuaiyu](https://github.com/XuHuaiyu)
- Add unit tests for the new framework. [@XuHuaiyu](https://github.com/XuHuaiyu)
- Infer input parameter type for new aggregate functions when extracting a `Project` operator under the `Aggregate` operator. [@XuHuaiyu](https://github.com/XuHuaiyu)
- Use the new framework in the hash aggregate operator. [@XuHuaiyu](https://github.com/XuHuaiyu)

## Open issues (if applicable)

