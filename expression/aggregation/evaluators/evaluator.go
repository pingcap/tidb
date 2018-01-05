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

package evaluators

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type AggEvaluator interface {
	AllocPartialResult() []byte
	ResetPartialResult(partialResult []byte)
	UpdatePartialResult(ctx context.Context, input types.Row, partialResult []byte) error
	AppendPartialResult2Chunk(ctx context.Context, partialResult []byte, chk *chunk.Chunk) error
	AppendFinalResult2Chunk(ctx context.Context, partialResult []byte, chk *chunk.Chunk) error
}

type baseAggEvaluator struct {
	inputIdx  []int
	outputIdx []int
}

var (
	_ AggEvaluator = (*aggEvaluator4MapAvgInt)(nil)
	_ AggEvaluator = (*aggEvaluator4MapAvgReal)(nil)
	_ AggEvaluator = (*aggEvaluator4MapAvgDecimal)(nil)
	_ AggEvaluator = (*aggEvaluator4DistinctMapAvgReal)(nil)
	_ AggEvaluator = (*aggEvaluator4DistinctMapAvgDecimal)(nil)
	_ AggEvaluator = (*aggEvaluator4ReduceAvgReal)(nil)
	_ AggEvaluator = (*aggEvaluator4ReduceAvgDecimal)(nil)
)
