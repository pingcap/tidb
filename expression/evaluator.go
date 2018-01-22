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
	inputIdxToOutputIdxes map[int][]int
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//	     since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx context.Context, input, output *chunk.Chunk) {
	for inputIdx, outputIdxes := range e.inputIdxToOutputIdxes {
		output.SwapColumn(outputIdxes[0], input, inputIdx)
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
}

type defaultEvaluator struct {
	outputIdxes  []int
	exprs        []Expression
	vectorizable bool
}

func (e *defaultEvaluator) run(ctx context.Context, input, output *chunk.Chunk) error {
	iter := chunk.NewIterator4Chunk(input)
	if e.vectorizable {
		for i := range e.outputIdxes {
			err := evalOneColumn(ctx, e.exprs[i], iter, output, e.outputIdxes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		for i := range e.outputIdxes {
			err := evalOneCell(ctx, e.exprs[i], row, output, e.outputIdxes[i])
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
	*columnEvaluator  // Evaluator for column expressions.
	*defaultEvaluator // Evaluator for other expressions.
}

// NewEvaluatorSuit creates an EvaluatorSuit to evaluate all the exprs.
func NewEvaluatorSuit(exprs []Expression) *EvaluatorSuit {
	e := &EvaluatorSuit{}

	for i, expr := range exprs {
		switch x := expr.(type) {
		case *Column:
			if e.columnEvaluator == nil {
				e.columnEvaluator = &columnEvaluator{inputIdxToOutputIdxes: make(map[int][]int)}
			}
			inputIdx, outputIdx := x.Index, i
			e.columnEvaluator.inputIdxToOutputIdxes[inputIdx] = append(e.columnEvaluator.inputIdxToOutputIdxes[inputIdx], outputIdx)
		default:
			if e.defaultEvaluator == nil {
				e.defaultEvaluator = &defaultEvaluator{
					outputIdxes: make([]int, 0, len(exprs)),
					exprs:       make([]Expression, 0, len(exprs)),
				}
			}
			e.defaultEvaluator.exprs = append(e.defaultEvaluator.exprs, x)
			e.defaultEvaluator.outputIdxes = append(e.defaultEvaluator.outputIdxes, i)
		}
	}

	if e.defaultEvaluator != nil {
		e.defaultEvaluator.vectorizable = Vectorizable(e.defaultEvaluator.exprs)
	}
	return e
}

// Run evaluates all the expressions hold by this EvaluatorSuit.
// NOTE: "defaultEvaluator" must be evaluated before "columnEvaluator".
func (e *EvaluatorSuit) Run(ctx context.Context, input, output *chunk.Chunk) error {
	if e.defaultEvaluator != nil {
		err := e.defaultEvaluator.run(ctx, input, output)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.columnEvaluator != nil {
		e.columnEvaluator.run(ctx, input, output)
	}
	return nil
}
