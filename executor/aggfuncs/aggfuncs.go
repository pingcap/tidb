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
	// All the AggFunc implementations for "SUM" are listed here.
	// All the AggFunc implementations for "AVG" are listed here.
	_ AggFunc = (*avgOriginal4Decimal)(nil)
	_ AggFunc = (*avgPartial4Decimal)(nil)

	_ AggFunc = (*avgOriginal4Float64)(nil)
	_ AggFunc = (*avgPartial4Float64)(nil)

	_ AggFunc = (*sumAggFunc4Float64)(nil)
	_ AggFunc = (*sumAggFunc4Decimal)(nil)

	// All the AggFunc implementations for "FIRSTROW" are listed here.
	// All the AggFunc implementations for "MAX" are listed here.
	// All the AggFunc implementations for "MIN" are listed here.
	// All the AggFunc implementations for "GROUP_CONCAT" are listed here.
	// All the AggFunc implementations for "BIT_OR" are listed here.
	_ AggFunc = (*bitOrUint64)(nil)

// All the AggFunc implementations for "BIT_XOR" are listed here.
// All the AggFunc implementations for "BIT_AND" are listed here.
)

// PartialResult represents data structure to store the partial result for the
// aggregate functions. Here we use unsafe.Pointer to allow the partial result
// to be any type.
type PartialResult unsafe.Pointer

// AggFunc is the interface to evaluate the aggregate functions.
type AggFunc interface {
	// AllocPartialResult allocates a specific data structure to store the
	// partial result, initializes it, and converts it to PartialResult to
	// return back. Aggregate operator implementation, no matter it's a hash
	// or stream, should hold this allocated PartialResult for the further
	// operations like: "ResetPartialResult", "UpdatePartialResult".
	AllocPartialResult() PartialResult

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
	// aggregate function.
	UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error

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
