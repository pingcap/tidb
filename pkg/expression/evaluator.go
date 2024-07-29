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
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disjointset"
)

type columnEvaluator struct {
	inputIdxToOutputIdxes map[int][]int
	// mergedInputIdxToOutputIdxes is only determined in runtime when saw the input chunk.
	mergedInputIdxToOutputIdxes map[int][]int
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//
//	since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx EvalContext, input, output *chunk.Chunk) error {
	// mergedInputIdxToOutputIdxes only can be determined in runtime when we saw the input chunk structure.
	if e.mergedInputIdxToOutputIdxes == nil {
		e.mergeInputIdxToOutputIdxes(input, e.inputIdxToOutputIdxes)
	}
	for inputIdx, outputIdxes := range e.mergedInputIdxToOutputIdxes {
		if err := output.SwapColumn(outputIdxes[0], input, inputIdx); err != nil {
			return err
		}
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

// mergeInputIdxToOutputIdxes try to merge two separate inputIdxToOutputIdxes item together when we find
// there is some column-ref inside the input chunk.
// imaging a case:
//
// scan:                       a(addr: ???)
//
//	_________________________//            \\
//
// proj1:          a1(addr:0xe)              a2(addr:0xe)
//
//	_________________//  \\                      //  \\
//
// proj2:   a3(addr:0xe) a4(addr:0xe)   a3(addr:0xe) a4(addr:0xe)
//
// when we done with proj1 we can output a chunk with two column2 inside[a1, a2], since this two
// is derived from single column a from scan, they are already made as column ref with same addr
// in projection1. say the addr is 0xe for both a1 and a2 here.
//
// when we start the projection2, we have two separate <inputIdx,[]outputIdxes> items here, like
// <0, [0,1]> means project input chunk's 0th column twice, put them in 0th and 1st of output chunk.
// <1, [2,3]> means project input chunk's 1st column twice, put them in 2nd and 3rd of output chunk.
//
// since we do have swap column logic inside projection for each <inputIdx,[]outputIdxes>, after the
// <0, [0,1]> is applied, the input chunk a1 and a2's address will be swapped as a fake column. like
//
// proj1:          a1(addr:invalid)             a2(addr:invalid)
//
// ___________________//  \\                      //  \\
//
// proj2:   a3(addr:0xe) a4(addr:0xe)   a3(addr:???) a4(addr:???)
//
// then when we start the projection for second <1, [2,3]>, the targeted column a2 addr has already been
// swapped as an invalid one. so swapping between a2 <-> a3 and a4 is not safe anymore.
//
// keypoint: we should identify the original column ref from input chunk, and merge current inputIdxToOutputIdxes
// as soon as possible. since input chunk's 0 and 1 is column referred. the final inputIdxToOutputIdxes should
// be like: <0, [0,1,3,4]>.
func (e *columnEvaluator) mergeInputIdxToOutputIdxes(input *chunk.Chunk, inputIdxToOutputIdxes map[int][]int) {
	// step1: we should detect the self column-ref inside input chunk.
	// we use the generic set rather than single int set to leverage the value mapping.
	// the original column ref inside may not be too much, give size 4 here rather input.NumCols().
	originalDJSet := disjointset.NewSet[int](4)
	flag := make([]bool, input.NumCols())
	// we can only detect the self column-ref inside input chunk by address equal.
	for i := 0; i < input.NumCols(); i++ {
		if flag[i] {
			continue
		}
		for j := i; j < input.NumCols(); j++ {
			if input.Column(i) == input.Column(j) {
				// mark referred column to avoid successive detection.
				flag[j] = true
				// make j union ref i.
				originalDJSet.Union(i, j)
			}
		}
	}
	// step2: link items inside inputIdxToOutputIdxes with originalDisJoint set.
	// since originDJSet covers offset 0 to input.NumCols(), it may overlap with the output indexes.
	newInputIdxToOutputIdxes := make(map[int][]int, len(inputIdxToOutputIdxes))
	for inputIdx := range inputIdxToOutputIdxes {
		// if originIdx is in originalDJSet, find the root.
		originalRootIdx := originalDJSet.FindRootForV(inputIdx)
		// initialize the map item if not exist.
		if _, ok := newInputIdxToOutputIdxes[originalRootIdx]; !ok {
			newInputIdxToOutputIdxes[originalRootIdx] = []int{}
		}
		// eg: assuming A and B are in the same set of originalDJSet, and root is A.
		// if inputIdxToOutputIdxes[A]=[0,1], A is in the same set of B
		// and inputIdxToOutputIdxes[B]=[2,3], and we will get
		// newInputIdxToOutputIdxes[A] = [0,1,2,3]
		newInputIdxToOutputIdxes[originalRootIdx] = append(newInputIdxToOutputIdxes[originalRootIdx], inputIdxToOutputIdxes[inputIdx]...)
	}
	e.mergedInputIdxToOutputIdxes = newInputIdxToOutputIdxes
}

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
func (e *defaultEvaluator) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	props := context.OptionalEvalPropKeySet(0)
	for _, expr := range e.exprs {
		props = props | getOptionalEvalPropsForExpr(expr)
	}

	return props
}

func getOptionalEvalPropsForExpr(expr Expression) context.OptionalEvalPropKeySet {
	switch e := expr.(type) {
	case *ScalarFunction:
		props := e.Function.RequiredOptionalEvalProps()
		for _, arg := range e.GetArgs() {
			props = props | getOptionalEvalPropsForExpr(arg)
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
	*columnEvaluator  // Evaluator for column expressions.
	*defaultEvaluator // Evaluator for other expressions.
}

// NewEvaluatorSuite creates an EvaluatorSuite to evaluate all the exprs.
// avoidColumnEvaluator can be removed after column pool is supported.
func NewEvaluatorSuite(exprs []Expression, avoidColumnEvaluator bool) *EvaluatorSuite {
	e := &EvaluatorSuite{}

	for i := 0; i < len(exprs); i++ {
		if col, isCol := exprs[i].(*Column); isCol && !avoidColumnEvaluator {
			if e.columnEvaluator == nil {
				e.columnEvaluator = &columnEvaluator{inputIdxToOutputIdxes: make(map[int][]int)}
			}
			inputIdx, outputIdx := col.Index, i
			e.columnEvaluator.inputIdxToOutputIdxes[inputIdx] = append(e.columnEvaluator.inputIdxToOutputIdxes[inputIdx], outputIdx)
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

	if e.columnEvaluator != nil {
		return e.columnEvaluator.run(ctx, input, output)
	}
	return nil
}
