// Copyright 2019 PingCAP, Inc.
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

package windowfuncs

import (
	"unsafe"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// PartialResult represents data structure to store the partial result for the
// aggregate functions. Here we use unsafe.Pointer to allow the partial result
// to be any type.
type PartialResult unsafe.Pointer

// WindowFunc is the interface for processing window functions.
type WindowFunc interface {
	// ProcessOneChunk processes one chunk and write results to chunk.
	ProcessOneChunk(sctx sessionctx.Context, rows []chunk.Row, pr PartialResult, dest *chunk.Chunk, remainedRows int64) ([]chunk.Row, int64, error)
	// ExhaustResult exhausts result to chunk.
	ExhaustResult(sctx sessionctx.Context, rows []chunk.Row, pr PartialResult, dest *chunk.Chunk, remainedRows int64) ([]chunk.Row, int64, error)
	// AllocPartialResult allocates a specific data structure to store the partial result.
	AllocPartialResult() PartialResult
	// ResetPartialResult resets the partial result.
	ResetPartialResult(pr PartialResult)
}

type baseWindowFunc struct {
	// args stores the input arguments for an aggregate function, we should
	// call arg.EvalXXX to get the actual input data for this function.
	args []expression.Expression

	// ordinal stores the ordinal of the columns in the output chunk, which is
	// used to append the final result of this function.
	ordinal int
}
