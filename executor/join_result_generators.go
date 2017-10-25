// Copyright 2017 PingCAP, Inc.
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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ joinResultGenerator = &mergeJoinResultGenerator4SemiJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4AntiSemiJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4LeftOuterSemiJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4AntiLeftOuterSemiJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4LeftOuterJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4RightOuterJoin{}
	_ joinResultGenerator = &mergeJoinResultGenerator4InnerJoin{}
)

// joinResultGenerator is used to generate join results according the join type, see every implementor for detailed information.
type joinResultGenerator interface {
	// emitMatchedInners should be called when key in outer row is equal to key in every inner row.
	emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error)
	// emitUnMatchedOuter should be called when key in outer row is equal to key in inner row.
	emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row
	// emitUnMatchedOuter should be called when outer row is not matched to any inner row.
	emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row
}

func newMergeJoinResultGenerator(ctx context.Context, joinType plan.JoinType, defaultInner Row, filter []expression.Expression) joinResultGenerator {
	switch joinType {
	case plan.SemiJoin:
		return &mergeJoinResultGenerator4SemiJoin{}
	case plan.AntiSemiJoin:
		return &mergeJoinResultGenerator4AntiSemiJoin{}
	case plan.LeftOuterSemiJoin:
		return &mergeJoinResultGenerator4LeftOuterSemiJoin{}
	case plan.AntiLeftOuterSemiJoin:
		return &mergeJoinResultGenerator4AntiLeftOuterSemiJoin{}
	case plan.LeftOuterJoin:
		return &mergeJoinResultGenerator4LeftOuterJoin{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.RightOuterJoin:
		return &mergeJoinResultGenerator4RightOuterJoin{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.InnerJoin:
		return &mergeJoinResultGenerator4InnerJoin{
			ctx:    ctx,
			filter: filter,
		}
	}
	panic("unsupported join type in func newMergeJoinResultGenerator()")
}

type mergeJoinResultGenerator4SemiJoin struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4SemiJoin) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return append(resultBuffer, outer), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4SemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4SemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

type mergeJoinResultGenerator4AntiSemiJoin struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiSemiJoin) emitMatchedInners(_ Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiSemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiSemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, outer)
	}
	return resultBuffer
}

type mergeJoinResultGenerator4LeftOuterSemiJoin struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterSemiJoin) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	joinedRow := append(outer, types.NewDatum(true))
	return append(resultBuffer, joinedRow), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterSemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(false))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterSemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(false)))
	}
	return resultBuffer
}

type mergeJoinResultGenerator4AntiLeftOuterSemiJoin struct{}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiLeftOuterSemiJoin) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	joinedRow := append(outer, types.NewDatum(false))
	return append(resultBuffer, joinedRow), nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiLeftOuterSemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(true))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4AntiLeftOuterSemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(true)))
	}
	return resultBuffer
}

type mergeJoinResultGenerator4LeftOuterJoin struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4LeftOuterJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
	}
	return resultBuffer
}

type mergeJoinResultGenerator4RightOuterJoin struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4RightOuterJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(inner, outer)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4RightOuterJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4RightOuterJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
	}
	return resultBuffer
}

type mergeJoinResultGenerator4InnerJoin struct {
	ctx    context.Context
	filter []expression.Expression
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4InnerJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		if outputer.filter != nil {
			matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
			if err != nil {
				return resultBuffer, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		resultBuffer = append(resultBuffer, joinedRow)
	}
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4InnerJoin) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *mergeJoinResultGenerator4InnerJoin) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}
