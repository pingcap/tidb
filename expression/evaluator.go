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

type columnEvaluator struct {
	inputIdx2OutputIdxes map[int][]int
}

func (e *columnEvaluator) add(colExpr *Column, outputIdx int) {
	inputIdx := colExpr.Index
	e.inputIdx2OutputIdxes[inputIdx] = append(e.inputIdx2OutputIdxes[inputIdx], outputIdx)
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//	     since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx context.Context, input, output *chunk.Chunk) error {
	for inputIdx, outputIdxes := range e.inputIdx2OutputIdxes {
		output.SwapColumn(outputIdxes[0], input, inputIdx)
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

type otherEvaluator struct {
	outputIdxes  []int
	scalaFunc    []Expression
	vectorizable bool
}

func (e *otherEvaluator) run(ctx context.Context, input, output *chunk.Chunk) error {
	sc := ctx.GetSessionVars().StmtCtx
	if e.vectorizable {
		for i := range e.outputIdxes {
			err := evalOneColumn(sc, e.scalaFunc[i], input, output, e.outputIdxes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	for row := input.Begin(); row != input.End(); row = row.Next() {
		for i := range e.outputIdxes {
			err := evalOneCell(sc, e.scalaFunc[i], row, output, e.outputIdxes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// EvaluatorSuit is responsible for the evaluation of a list of expressions.
// It seperates them to "column" and "other" expressions and evaluates "other"
// expressions before "column" expressions.
type EvaluatorSuit struct {
	evaluator4Col   *columnEvaluator
	evaluator4Other *otherEvaluator
}

// NewEvaluatorSuit creates an EvaluatorSuit to evaluate all the exprs.
func NewEvaluatorSuit(exprs []Expression) *EvaluatorSuit {
	e := &EvaluatorSuit{
		evaluator4Col: &columnEvaluator{
			inputIdx2OutputIdxes: make(map[int][]int),
		},
		evaluator4Other: &otherEvaluator{
			outputIdxes:  make([]int, 0, len(exprs)),
			scalaFunc:    make([]Expression, 0, len(exprs)),
			vectorizable: Vectorizable(exprs),
		},
	}

	for i, expr := range exprs {
		switch x := expr.(type) {
		case *Column:
			e.evaluator4Col.add(x, i)
		default:
			e.evaluator4Other.outputIdxes = append(e.evaluator4Other.outputIdxes, i)
			e.evaluator4Other.scalaFunc = append(e.evaluator4Other.scalaFunc, x)
		}
	}

	return e
}

// Run evaluates all the expressions hold by this EvaluatorSuit.
func (e *EvaluatorSuit) Run(ctx context.Context, input, output *chunk.Chunk) error {
	// NOTE: "evaluator4Other" must be evaluated before "evaluator4Col".
	if err := e.evaluator4Other.run(ctx, input, output); err != nil {
		return errors.Trace(err)
	}
	if err := e.evaluator4Col.run(ctx, input, output); err != nil {
		return errors.Trace(err)
	}
	return nil
}
