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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ evaluator = (*columnEvaluator)(nil)
	_ evaluator = (*funcEvaluator)(nil)
)

type evaluator interface {
	run(ctx context.Context, input, output *chunk.Chunk) error
}

type columnEvaluator struct {
	inputIdx2OutputIdxes map[int][]int
}

func (e *columnEvaluator) add(colExpr *Column, outputIdx int) {
	inputIdx := colExpr.Index
	e.inputIdx2OutputIdxes[inputIdx] = append(e.inputIdx2OutputIdxes[inputIdx], outputIdx)
}

func (e *columnEvaluator) run(ctx context.Context, input, output *chunk.Chunk) error {
	for inputIdx, outputIdxes := range e.inputIdx2OutputIdxes {
		output.SwapColumn(outputIdxes[0], input, inputIdx)
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

type funcEvaluator struct {
	outputIdxes []int
	scalaFunc   []Expression
}

func (e *funcEvaluator) run(ctx context.Context, input, output *chunk.Chunk) error {
	for i := range e.outputIdxes {
		err := evalOneColumn(ctx.GetSessionVars().StmtCtx, e.scalaFunc[i], input, output, e.outputIdxes[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type EvaluatorSuit struct {
	evaluator4Func *funcEvaluator
	evaluator4Col  *columnEvaluator
}

func NewEvaluatorSuit(exprs []Expression) *EvaluatorSuit {
	e := &EvaluatorSuit{
		evaluator4Func: &funcEvaluator{
			outputIdxes: make([]int, 0, len(exprs)),
			scalaFunc:   make([]Expression, 0, len(exprs)),
		},
		evaluator4Col: &columnEvaluator{
			inputIdx2OutputIdxes: make(map[int][]int),
		},
	}

	for i, expr := range exprs {
		switch x := expr.(type) {
		case *Column:
			e.evaluator4Col.add(x, i)
		default:
			e.evaluator4Func.outputIdxes = append(e.evaluator4Func.outputIdxes, i)
			e.evaluator4Func.scalaFunc = append(e.evaluator4Func.scalaFunc, x)
		}
	}

	return e
}

func (e *EvaluatorSuit) Run(ctx context.Context, input, output *chunk.Chunk) error {
	if err := e.evaluator4Func.run(ctx, input, output); err != nil {
		return errors.Trace(err)
	}
	if err := e.evaluator4Col.run(ctx, input, output); err != nil {
		return errors.Trace(err)
	}
	return nil
}
