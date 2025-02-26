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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type defaultEvaluator struct {
	outputIdxes  []int
	exprs        []Expression
	vectorizable bool
}

func (e *defaultEvaluator) run(ctx EvalContext, vecEnabled bool, input, output *chunk.Chunk) error {
	iter := chunk.NewIterator4Chunk(input)
	if e.vectorizable {
		for i := range e.outputIdxes {
			if vecEnabled && e.exprs[i].Vectorized() {
				if err := evalOneVec(ctx, e.exprs[i], input, output, e.outputIdxes[i]); err != nil {
					return err
				}
				continue
			}

			err := evalOneColumn(ctx, e.exprs[i], iter, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
		return nil
	}

	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		for i := range e.outputIdxes {
			err := evalOneCell(ctx, e.exprs[i], row, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// RequiredOptionalEvalProps exposes all optional evaluation properties that this evaluator requires.
func (e *defaultEvaluator) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	props := exprctx.OptionalEvalPropKeySet(0)
	for _, expr := range e.exprs {
		props = props | GetOptionalEvalPropsForExpr(expr)
	}

	return props
}

// GetOptionalEvalPropsForExpr gets all optional evaluation properties that this expression requires.
func GetOptionalEvalPropsForExpr(expr Expression) exprctx.OptionalEvalPropKeySet {
	switch e := expr.(type) {
	case *ScalarFunction:
		props := e.Function.RequiredOptionalEvalProps()
		for _, arg := range e.GetArgs() {
			props = props | GetOptionalEvalPropsForExpr(arg)
		}

		return props
	default:
		return 0
	}
}

// EvaluatorSuite is responsible for the evaluation of a list of expressions.
// It separates them to "column" and "other" expressions and evaluates "other"
// expressions before "column" expressions.
type EvaluatorSuite struct {
	ColumnSwapHelper  *chunk.ColumnSwapHelper // Evaluator for column expressions.
	*defaultEvaluator                         // Evaluator for other expressions.
}

// NewEvaluatorSuite creates an EvaluatorSuite to evaluate all the exprs.
// avoidColumnEvaluator can be removed after column pool is supported.
func NewEvaluatorSuite(exprs []Expression, avoidColumnEvaluator bool) *EvaluatorSuite {
	e := &EvaluatorSuite{}

	for i := 0; i < len(exprs); i++ {
		if col, isCol := exprs[i].(*Column); isCol && !avoidColumnEvaluator {
			if e.ColumnSwapHelper == nil {
				e.ColumnSwapHelper = &chunk.ColumnSwapHelper{InputIdxToOutputIdxes: make(map[int][]int)}
			}
			inputIdx, outputIdx := col.Index, i
			e.ColumnSwapHelper.InputIdxToOutputIdxes[inputIdx] = append(e.ColumnSwapHelper.InputIdxToOutputIdxes[inputIdx], outputIdx)
			continue
		}
		if e.defaultEvaluator == nil {
			e.defaultEvaluator = &defaultEvaluator{
				outputIdxes: make([]int, 0, len(exprs)),
				exprs:       make([]Expression, 0, len(exprs)),
			}
		}
		e.defaultEvaluator.exprs = append(e.defaultEvaluator.exprs, exprs[i])
		e.defaultEvaluator.outputIdxes = append(e.defaultEvaluator.outputIdxes, i)
	}

	if e.defaultEvaluator != nil {
		e.defaultEvaluator.vectorizable = Vectorizable(e.defaultEvaluator.exprs)
	}
	return e
}

// NewEvaluatorSuiteTopN creates an EvaluatorSuite for Pusheddown TopN.
func NewEvaluatorSuiteTopN(exprs []Expression, inputoffsets []uint32, handleIdxs []int) *EvaluatorSuite {
	e := &EvaluatorSuite{}

	if len(handleIdxs) > 0 {
		if e.ColumnSwapHelper == nil {
			e.ColumnSwapHelper = &chunk.ColumnSwapHelper{InputIdxToOutputIdxes: make(map[int][]int)}
		}
	}
	// Map column index to its position in inputoffsets
	inputIdxMap := make(map[int]int)
	for idx, offset := range inputoffsets {
		inputIdxMap[int(offset)] = idx
	}

	for i := 0; i < len(exprs); i++ {
		if col, isCol := exprs[i].(*Column); isCol {
			if e.ColumnSwapHelper == nil {
				e.ColumnSwapHelper = &chunk.ColumnSwapHelper{InputIdxToOutputIdxes: make(map[int][]int)}
			}
			e.ColumnSwapHelper.InputIdxToOutputIdxes[col.Index] = append(e.ColumnSwapHelper.InputIdxToOutputIdxes[col.Index], i)
			continue
		}
		if e.defaultEvaluator == nil {
			e.defaultEvaluator = &defaultEvaluator{
				outputIdxes: make([]int, 0, len(exprs)),
				exprs:       make([]Expression, 0, len(exprs)),
			}
		}
		e.defaultEvaluator.exprs = append(e.defaultEvaluator.exprs, exprs[i])
		e.defaultEvaluator.outputIdxes = append(e.defaultEvaluator.outputIdxes, i)
	}

	// Ensure the handle columns are included in the output
	handlesInExprs := make(map[int]struct{})
	for i := 0; i < len(exprs); i++ {
		if col, isCol := exprs[i].(*Column); isCol {
			for _, handleIdx := range handleIdxs {
				if inputIdxMap[handleIdx] == col.Index {
					handlesInExprs[handleIdx] = struct{}{}
				}
			}
		}
	}

	appendIdx := 0
	for _, handleIdx := range handleIdxs {
		if _, exists := handlesInExprs[handleIdx]; !exists {
			if inputIdx, exists := inputIdxMap[handleIdx]; exists {
				if e.ColumnSwapHelper.InputIdxToOutputIdxes[inputIdx] == nil {
					e.ColumnSwapHelper.InputIdxToOutputIdxes[inputIdx] = []int{len(exprs) + appendIdx}
					appendIdx++
				}
			}
		}
	}

	if e.defaultEvaluator != nil {
		e.defaultEvaluator.vectorizable = Vectorizable(e.defaultEvaluator.exprs)
	}
	return e
}

// Vectorizable checks whether this EvaluatorSuite can use vectorizd execution mode.
func (e *EvaluatorSuite) Vectorizable() bool {
	return e.defaultEvaluator == nil || e.defaultEvaluator.vectorizable
}

// Run evaluates all the expressions hold by this EvaluatorSuite.
// NOTE: "defaultEvaluator" must be evaluated before "columnEvaluator".
func (e *EvaluatorSuite) Run(ctx EvalContext, vecEnabled bool, input, output *chunk.Chunk) error {
	if e.defaultEvaluator != nil {
		err := e.defaultEvaluator.run(ctx, vecEnabled, input, output)
		if err != nil {
			return err
		}
	}

	// NOTE: It should be called after all the other expressions are evaluated
	//	since it will change the content of the input Chunk.
	if e.ColumnSwapHelper != nil {
		return e.ColumnSwapHelper.SwapColumns(input, output)
	}
	return nil
}

// RequiredOptionalEvalProps exposes all optional evaluation properties that this evaluator requires.
func (e *EvaluatorSuite) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	if e.defaultEvaluator != nil {
		return e.defaultEvaluator.RequiredOptionalEvalProps()
	}

	return 0
}
