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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disjointset"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type columnEvaluator struct {
	inputIdxToOutputIdxes map[int][]int
	// mergedInputIdxToOutputIdxes is only determined in runtime when saw the input chunk.
	mergedInputIdxToOutputIdxes atomic.Pointer[map[int][]int]
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//
//	since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx EvalContext, input, output *chunk.Chunk) error {
	// mergedInputIdxToOutputIdxes only can be determined in runtime when we saw the input chunk structure.
	if e.mergedInputIdxToOutputIdxes.Load() == nil {
		e.mergeInputIdxToOutputIdxes(input, e.inputIdxToOutputIdxes)
	}
	for inputIdx, outputIdxes := range *e.mergedInputIdxToOutputIdxes.Load() {
		if err := output.SwapColumn(outputIdxes[0], input, inputIdx); err != nil {
			return err
		}
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

// mergeInputIdxToOutputIdxes merges separate inputIdxToOutputIdxes entries when column references
// are detected within the input chunk. This process ensures consistent handling of columns derived
// from the same original source.
//
// Consider the following scenario:
//
// Initial scan operation produces a column 'a':
//
// scan:                       a (addr: ???)
//
// This column 'a' is used in the first projection (proj1) to create two columns a1 and a2, both referencing 'a':
//
//	                      proj1
//	                     /     \
//	                    /       \
//	                   /         \
//	     a1 (addr: 0xe)           a2 (addr: 0xe)
//	     /                         \
//	    /                           \
//	   /                             \
//	  proj2                          proj2
//	  /     \                       /     \
//	 /       \                     /       \
//	a3        a4                  a5        a6
//
// (addr: 0xe) (addr: 0xe)      (addr: 0xe) (addr: 0xe)
//
// Here, a1 and a2 share the same address (0xe), indicating they reference the same data from the original 'a'.
//
// When moving to the second projection (proj2), the system tries to project these columns further:
// - The first set (left side) consists of a3 and a4, derived from a1, both retaining the address (0xe).
// - The second set (right side) consists of a5 and a6, derived from a2, also starting with address (0xe).
//
// When proj1 is complete, the output chunk contains two columns [a1, a2], both derived from the single column 'a' from the scan.
// Since both a1 and a2 are column references with the same address (0xe), they are treated as referencing the same data.
//
// In proj2, two separate <inputIdx, []outputIdxes> items are created:
// - <0, [0,1]>: This means the 0th input column (a1) is projected twice, into the 0th and 1st columns of the output chunk.
// - <1, [2,3]>: This means the 1st input column (a2) is projected twice, into the 2nd and 3rd columns of the output chunk.
//
// Due to the column swapping logic in each projection, after applying the <0, [0,1]> projection,
// the addresses for a1 and a2 may become swapped or invalid:
//
// proj1:          a1 (addr: invalid)             a2 (addr: invalid)
//
// This can lead to issues in proj2, where further operations on these columns may be unsafe:
//
// proj2:   a3 (addr: 0xe) a4 (addr: 0xe)   a5 (addr: ???) a6 (addr: ???)
//
// Therefore, it's crucial to identify and merge the original column references early, ensuring
// the final inputIdxToOutputIdxes mapping accurately reflects the shared origins of the data.
// For instance, <0, [0,1,2,3]> indicates that the 0th input column (original 'a') is referenced
// by all four output columns in the final output.
//
// mergeInputIdxToOutputIdxes merges inputIdxToOutputIdxes based on detected column references.
// This ensures that columns with the same reference are correctly handled in the output chunk.
func (e *columnEvaluator) mergeInputIdxToOutputIdxes(input *chunk.Chunk, inputIdxToOutputIdxes map[int][]int) {
	originalDJSet := disjointset.NewSet[int](4)
	flag := make([]bool, input.NumCols())
	// Detect self column-references inside the input chunk by comparing column addresses
	for i := 0; i < input.NumCols(); i++ {
		if flag[i] {
			continue
		}
		for j := i + 1; j < input.NumCols(); j++ {
			if input.Column(i) == input.Column(j) {
				flag[j] = true
				originalDJSet.Union(i, j)
			}
		}
	}
	// Merge inputIdxToOutputIdxes based on the detected column references.
	newInputIdxToOutputIdxes := make(map[int][]int, len(inputIdxToOutputIdxes))
	for inputIdx := range inputIdxToOutputIdxes {
		// Root idx is internal offset, not the right column index.
		originalRootIdx := originalDJSet.FindRoot(inputIdx)
		originalVal, ok := originalDJSet.FindVal(originalRootIdx)
		intest.Assert(ok)
		mergedOutputIdxes := newInputIdxToOutputIdxes[originalVal]
		mergedOutputIdxes = append(mergedOutputIdxes, inputIdxToOutputIdxes[inputIdx]...)
		newInputIdxToOutputIdxes[originalVal] = mergedOutputIdxes
	}
	// Update the merged inputIdxToOutputIdxes automatically.
	// Once failed, it means other worker has done this job at meantime.
	e.mergedInputIdxToOutputIdxes.CompareAndSwap(nil, &newInputIdxToOutputIdxes)
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

// RequiredOptionalEvalProps exposes all optional evaluation properties that this evaluator requires.
func (e *EvaluatorSuite) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	if e.defaultEvaluator != nil {
		return e.defaultEvaluator.RequiredOptionalEvalProps()
	}

	return 0
}
