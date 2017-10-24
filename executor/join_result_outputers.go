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
	_ joinResultOutputer = &mergeJoinOutputer4SemiJoin{}
	_ joinResultOutputer = &mergeJoinOutputer4AntiSemiJoin{}
	_ joinResultOutputer = &mergeJoinOutputer4LeftOuterSemiJoin{}
	_ joinResultOutputer = &mergeJoinOutputer4LeftOuterJoin{}
	_ joinResultOutputer = &mergeJoinOutputer4RightOuterJoin{}
	_ joinResultOutputer = &mergeJoinOutputer4InnerJoin{}
)

type joinResultOutputer interface {
	emitMatchedInners(outer Row, inner []Row, resultBuffer []Row) ([]Row, error)
	emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row
	emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row
}

func newMergeJoinOutputer(ctx context.Context, joinType plan.JoinType, isAntiMode bool, defaultInner Row, filter []expression.Expression) joinResultOutputer {
	switch joinType {
	case plan.SemiJoin:
		if isAntiMode {
			return &mergeJoinOutputer4AntiSemiJoin{}
		}
		return &mergeJoinOutputer4SemiJoin{}
	case plan.LeftOuterSemiJoin:
		return &mergeJoinOutputer4LeftOuterSemiJoin{isAntiMode}
	case plan.LeftOuterJoin:
		return &mergeJoinOutputer4LeftOuterJoin{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.RightOuterJoin:
		return &mergeJoinOutputer4RightOuterJoin{
			ctx:          ctx,
			defaultInner: defaultInner,
			filter:       filter,
		}
	case plan.InnerJoin:
		return &mergeJoinOutputer4InnerJoin{
			ctx:    ctx,
			filter: filter,
		}
	}
	panic("unsupported join type in func newMergeJoinOutputer()")
}

type mergeJoinOutputer4SemiJoin struct{}

// emitMatchedInners implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4SemiJoin) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return append(resultBuffer, outer), nil
}

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4SemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4SemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

type mergeJoinOutputer4AntiSemiJoin struct{}

// emitMatchedInners implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4AntiSemiJoin) emitMatchedInners(_ Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	return resultBuffer, nil
}

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4AntiSemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4AntiSemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, outer)
	}
	return resultBuffer
}

type mergeJoinOutputer4LeftOuterSemiJoin struct {
	isAntiMode bool
}

// emitMatchedInners implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4LeftOuterSemiJoin) emitMatchedInners(outer Row, _ []Row, resultBuffer []Row) ([]Row, error) {
	joinedRow := append(outer, types.NewDatum(!outputer.isAntiMode))
	return append(resultBuffer, joinedRow), nil
}

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4LeftOuterSemiJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(outputer.isAntiMode))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4LeftOuterSemiJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(outputer.isAntiMode)))
	}
	return resultBuffer
}

type mergeJoinOutputer4LeftOuterJoin struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

func (outputer *mergeJoinOutputer4LeftOuterJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
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

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4LeftOuterJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4LeftOuterJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
	}
	return resultBuffer
}

type mergeJoinOutputer4RightOuterJoin struct {
	ctx          context.Context
	defaultInner Row
	filter       []expression.Expression
}

func (outputer *mergeJoinOutputer4RightOuterJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
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

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4RightOuterJoin) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4RightOuterJoin) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
	}
	return resultBuffer
}

type mergeJoinOutputer4InnerJoin struct {
	ctx    context.Context
	filter []expression.Expression
}

func (outputer *mergeJoinOutputer4InnerJoin) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
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

// emitUnMatchedOuter implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4InnerJoin) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultOutputer interface.
func (outputer *mergeJoinOutputer4InnerJoin) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}
