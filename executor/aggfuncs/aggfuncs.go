// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// All the AggFunc implementations are listed here for navigation.
var (
	// All the AggFunc implementations for "COUNT" are listed here.
	_ AggFunc = (*countPartial)(nil)
	_ AggFunc = (*countOriginal4Int)(nil)
	_ AggFunc = (*countOriginal4Real)(nil)
	_ AggFunc = (*countOriginal4Decimal)(nil)
	_ AggFunc = (*countOriginal4Time)(nil)
	_ AggFunc = (*countOriginal4Duration)(nil)
	_ AggFunc = (*countOriginal4JSON)(nil)
	_ AggFunc = (*countOriginal4String)(nil)
	_ AggFunc = (*countOriginalWithDistinct4Int)(nil)
	_ AggFunc = (*countOriginalWithDistinct4Real)(nil)
	_ AggFunc = (*countOriginalWithDistinct4Decimal)(nil)
	_ AggFunc = (*countOriginalWithDistinct4Duration)(nil)
	_ AggFunc = (*countOriginalWithDistinct4String)(nil)
	_ AggFunc = (*countOriginalWithDistinct)(nil)

	// All the AggFunc implementations for "APPROX_COUNT_DISTINCT" are listed here.
	_ AggFunc = (*approxCountDistinctOriginal)(nil)
	_ AggFunc = (*approxCountDistinctPartial1)(nil)
	_ AggFunc = (*approxCountDistinctPartial2)(nil)
	_ AggFunc = (*approxCountDistinctFinal)(nil)

	// All the AggFunc implementations for "FIRSTROW" are listed here.
	_ AggFunc = (*firstRow4Decimal)(nil)
	_ AggFunc = (*firstRow4Int)(nil)
	_ AggFunc = (*firstRow4Time)(nil)
	_ AggFunc = (*firstRow4String)(nil)
	_ AggFunc = (*firstRow4Duration)(nil)
	_ AggFunc = (*firstRow4Float32)(nil)
	_ AggFunc = (*firstRow4Float64)(nil)
	_ AggFunc = (*firstRow4JSON)(nil)
	_ AggFunc = (*firstRow4Enum)(nil)
	_ AggFunc = (*firstRow4Set)(nil)

	// All the AggFunc implementations for "MAX"/"MIN" are listed here.
	_ AggFunc = (*maxMin4Int)(nil)
	_ AggFunc = (*maxMin4Uint)(nil)
	_ AggFunc = (*maxMin4Float32)(nil)
	_ AggFunc = (*maxMin4Float64)(nil)
	_ AggFunc = (*maxMin4Decimal)(nil)
	_ AggFunc = (*maxMin4String)(nil)
	_ AggFunc = (*maxMin4Duration)(nil)
	_ AggFunc = (*maxMin4JSON)(nil)

	// All the AggFunc implementations for "AVG" are listed here.
	_ AggFunc = (*avgOriginal4Decimal)(nil)
	_ AggFunc = (*avgOriginal4DistinctDecimal)(nil)
	_ AggFunc = (*avgPartial4Decimal)(nil)

	_ AggFunc = (*avgOriginal4Float64)(nil)
	_ AggFunc = (*avgPartial4Float64)(nil)
	_ AggFunc = (*avgOriginal4DistinctFloat64)(nil)

	// All the AggFunc implementations for "SUM" are listed here.
	_ AggFunc = (*sum4DistinctFloat64)(nil)
	_ AggFunc = (*sum4DistinctDecimal)(nil)
	_ AggFunc = (*sum4Decimal)(nil)
	_ AggFunc = (*sum4Float64)(nil)

	// All the AggFunc implementations for "GROUP_CONCAT" are listed here.
	_ AggFunc = (*groupConcatDistinct)(nil)
	_ AggFunc = (*groupConcat)(nil)

	// All the AggFunc implementations for "BIT_OR" are listed here.
	_ AggFunc = (*bitOrUint64)(nil)

	// All the AggFunc implementations for "BIT_XOR" are listed here.
	_ AggFunc = (*bitXorUint64)(nil)

	// All the AggFunc implementations for "BIT_AND" are listed here.
	_ AggFunc = (*bitAndUint64)(nil)

	// All the AggFunc implementations for "JSON_OBJECTAGG" are listed here
	_ AggFunc = (*jsonObjectAgg)(nil)
)

const (
	// DefInt64Size is the size of int64
	DefInt64Size = int64(unsafe.Sizeof(int64(0)))
	// DefFloat64Size is the size of float64
	DefFloat64Size = int64(unsafe.Sizeof(float64(0)))
	// DefTimeSize is the size of time
	DefTimeSize = int64(16)

	// DefPartialResult4AvgDecimalSize is the size of partialResult4AvgDecimal
	DefPartialResult4AvgDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDecimal{}))
	// DefPartialResult4AvgDistinctDecimalSize is the size of partialResult4AvgDistinctDecimal
	DefPartialResult4AvgDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDistinctDecimal{}))
	// DefPartialResult4AvgFloat64Size is the size of partialResult4AvgFloat64
	DefPartialResult4AvgFloat64Size = int64(unsafe.Sizeof(partialResult4AvgFloat64{}))
	// DefPartialResult4AvgDistinctFloat64Size is the size of partialResult4AvgDistinctFloat64
	DefPartialResult4AvgDistinctFloat64Size = int64(unsafe.Sizeof(partialResult4AvgDistinctFloat64{}))

	// DefPartialResult4CountDistinctIntSize is the size of partialResult4CountDistinctInt
	DefPartialResult4CountDistinctIntSize = int64(unsafe.Sizeof(partialResult4CountDistinctInt{}))
	// DefPartialResult4CountDistinctRealSize is the size of partialResult4CountDistinctReal
	DefPartialResult4CountDistinctRealSize = int64(unsafe.Sizeof(partialResult4CountDistinctReal{}))
	// DefPartialResult4CountDistinctDecimalSize is the size of partialResult4CountDistinctDecimal
	DefPartialResult4CountDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4CountDistinctDecimal{}))
	// DefPartialResult4CountDistinctDurationSize is the size of partialResult4CountDistinctDuration
	DefPartialResult4CountDistinctDurationSize = int64(unsafe.Sizeof(partialResult4CountDistinctDuration{}))
	// DefPartialResult4CountDistinctStringSize is the size of partialResult4CountDistinctString
	DefPartialResult4CountDistinctStringSize = int64(unsafe.Sizeof(partialResult4CountDistinctString{}))
	// DefPartialResult4CountWithDistinctSize is the size of partialResult4CountWithDistinct
	DefPartialResult4CountWithDistinctSize = int64(unsafe.Sizeof(partialResult4CountWithDistinct{}))
	// DefPartialResult4ApproxCountDistinctSize is the size of partialResult4ApproxCountDistinct
	DefPartialResult4ApproxCountDistinctSize = int64(unsafe.Sizeof(partialResult4ApproxCountDistinct{}))
)

// PartialResult represents data structure to store the partial result for the
// aggregate functions. Here we use unsafe.Pointer to allow the partial result
// to be any type.
type PartialResult unsafe.Pointer

// AggFunc is the interface to evaluate the aggregate functions.
type AggFunc interface {
	// AllocPartialResult allocates a specific data structure to store the
	// partial result, initializes it, and converts it to PartialResult to
	// return back. The second returned value is the memDelta used to trace
	// memory usage. Aggregate operator implementation, no matter it's a hash
	// or stream, should hold this allocated PartialResult for the further
	// operations like: "ResetPartialResult", "UpdatePartialResult".
	AllocPartialResult() (pr PartialResult, memDelta int64)

	// ResetPartialResult resets the partial result to the original state for a
	// specific aggregate function. It converts the input PartialResult to the
	// specific data structure which stores the partial result and then reset
	// every field to the proper original state.
	ResetPartialResult(pr PartialResult)

	// UpdatePartialResult updates the specific partial result for an aggregate
	// function using the input rows which all belonging to the same data group.
	// It converts the PartialResult to the specific data structure which stores
	// the partial result and then iterates on the input rows and update that
	// partial result according to the functionality and the state of the
	// aggregate function. The returned value is the memDelta used to trace memory
	// usage.
	UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error)

	// MergePartialResult will be called in the final phase when parallelly
	// executing. It converts the PartialResult `src`, `dst` to the same specific
	// data structure which stores the partial results, and then evaluate the
	// final result using the partial results as input values. The returned value
	// is the memDelta used to trace memory usage.
	MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error)

	// AppendFinalResult2Chunk finalizes the partial result and append the
	// final result to the input chunk. Like other operations, it converts the
	// input PartialResult to the specific data structure which stores the
	// partial result and then calculates the final result and append that
	// final result to the chunk provided.
	AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error
}

type baseAggFunc struct {
	// args stores the input arguments for an aggregate function, we should
	// call arg.EvalXXX to get the actual input data for this function.
	args []expression.Expression

	// ordinal stores the ordinal of the columns in the output chunk, which is
	// used to append the final result of this function.
	ordinal int
}

func (*baseAggFunc) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	return 0, nil
}

// SlidingWindowAggFunc is the interface to evaluate the aggregate functions using sliding window.
type SlidingWindowAggFunc interface {
	// Slide evaluates the aggregate functions using a sliding window. The input
	// lastStart and lastEnd are the interval of the former sliding window,
	// shiftStart, shiftEnd mean the sliding window offset. Note that the input
	// PartialResult stores the intermediate result which will be used in the next
	// sliding window, ensure call ResetPartialResult after a frame are evaluated
	// completely.
	Slide(sctx sessionctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error
}
