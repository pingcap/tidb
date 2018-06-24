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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// All the AggFunc implementations are listed here for navigation.
var (
// All the AggFunc implementations for "COUNT" are listed here.
// All the AggFunc implementations for "SUM" are listed here.
// All the AggFunc implementations for "AVG" are listed here.
// All the AggFunc implementations for "FIRSTROW" are listed here.
// All the AggFunc implementations for "MAX" are listed here.
// All the AggFunc implementations for "MIN" are listed here.
// All the AggFunc implementations for "GROUP_CONCAT" are listed here.
// All the AggFunc implementations for "BIT_OR" are listed here.
// All the AggFunc implementations for "BIT_XOR" are listed here.
// All the AggFunc implementations for "BIT_AND" are listed here.
)

// AggFunc is the interface to evaluate the aggregate functions.
type AggFunc interface {
	// AllocPartialResult allocates a specific data structure to store the
	// partial result, initializes it, and converts it to a bype slice to return
	// back. Aggregate operator implementations, no mater whether it's a hash or
	// stream implementation, should hold this byte slice for further operations
	// like: "ResetPartialResult", "UpdatePartialResult".
	AllocPartialResult() []byte

	// ResetPartialResult resets the partial result to the original state for a
	// specific aggregate function. It converts the input byte slice to the
	// specific data structure which stores the partial result and then reset
	// every field to the proper original state.
	ResetPartialResult(partialBytes []byte)

	// UpdatePartialResult updates the specific partial result for an aggregate
	// function using the input rows which all belonging to the same data group.
	// It converts the input byte slice to the specific data structure which
	// stores the partial result and then iterates on the input rows and update
	// that partial result according to the functionality and the state of the
	// aggregate function.
	UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, partialBytes []byte) error

	// AppendFinalResult2Chunk finalizes the partial result and append the
	// final result to the input chunk. Like other operations, it converts the
	// input byte slice to the specific data structure which stores the partial
	// result and then calculates the final result and append that final result
	// to the chunk provided.
	AppendFinalResult2Chunk(sctx sessionctx.Context, partialBytes []byte, chk *chunk.Chunk) error
}

type baseAggFunc struct {
	input  []expression.Expression
	output []int
}
