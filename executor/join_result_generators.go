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
	"github.com/pingcap/tidb/types"
)

var (
	_ joinResultGenerator = &semiJoinResultGenerator{}
	_ joinResultGenerator = &antiSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &antiLeftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterJoinResultGenerator{}
	_ joinResultGenerator = &rightOuterJoinResultGenerator{}
	_ joinResultGenerator = &innerJoinResultGenerator{}
)

// joinResultGenerator is used to generate join results according the join type, see every implementor for detailed information.
type joinResultGenerator interface {
	// emitMatchedInners should be called when key in outer row is equal to key in every inner row.
	// Reutrn true if outer row can be joined with any input inner row.
	emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error)
	// emitUnMatchedOuter should be called when outer row is not matched to any inner row.
	emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row
	// emitUnMatchedOuters should be called when outer row is not matched to any inner row.
	emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row
}

func newJoinResultGenerator(ctx context.Context, joinType plan.JoinType, outerIsRight bool, defaultInner Row, filter []expression.Expression) joinResultGenerator {
	baseGenerator := baseJoinResultGenerator{ctx, filter, defaultInner, outerIsRight}
	switch joinType {
	case plan.SemiJoin:
		return &semiJoinResultGenerator{baseGenerator}
	case plan.AntiSemiJoin:
		return &antiSemiJoinResultGenerator{baseGenerator}
	case plan.LeftOuterSemiJoin:
		return &leftOuterSemiJoinResultGenerator{baseGenerator}
	case plan.AntiLeftOuterSemiJoin:
		return &antiLeftOuterSemiJoinResultGenerator{baseGenerator}
	case plan.LeftOuterJoin:
		return &leftOuterJoinResultGenerator{baseGenerator}
	case plan.RightOuterJoin:
		return &rightOuterJoinResultGenerator{baseGenerator}
	case plan.InnerJoin:
		return &innerJoinResultGenerator{baseGenerator}
	}
	panic("unsupported join type in func newJoinResultGenerator()")
}

type baseJoinResultGenerator struct {
	ctx          context.Context
	filter       []expression.Expression
	defaultInner Row
	outerIsRight bool
}

type semiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if outputer.filter == nil {
		return append(resultBuffer, outer), true, nil
	}

	for _, inner := range inners {
		var joinedRow Row
		if outputer.outerIsRight {
			joinedRow = makeJoinRow(inner, outer)
		} else {
			joinedRow = makeJoinRow(outer, inner)
		}

		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			return append(resultBuffer, outer), true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) (_ []Row, matched bool, err error) {
	if outputer.filter == nil {
		return resultBuffer, true, nil
	}

	for _, inner := range inners {
		var joinedRow Row
		if outputer.outerIsRight {
			joinedRow = makeJoinRow(inner, outer)
		} else {
			joinedRow = makeJoinRow(outer, inner)
		}

		matched, err = expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			return resultBuffer, true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, outer)
	}
	return resultBuffer
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if outputer.filter == nil {
		joinedRow := append(outer, types.NewDatum(true))
		return append(resultBuffer, joinedRow), true, nil
	}

	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			joinedRow = joinedRow[:len(outer)]
			joinedRow = append(outer, types.NewDatum(true))
			return append(resultBuffer, joinedRow), true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(false))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(false)))
	}
	return resultBuffer
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if outputer.filter == nil {
		joinedRow := append(outer, types.NewDatum(false))
		return append(resultBuffer, joinedRow), true, nil
	}

	for _, inner := range inners {
		joinedRow := makeJoinRow(outer, inner)
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			joinedRow = joinedRow[:len(outer)]
			joinedRow = append(outer, types.NewDatum(false))
			return append(resultBuffer, joinedRow), true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	outer = append(outer, types.NewDatum(true))
	return append(resultBuffer, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, append(outer, types.NewDatum(true)))
	}
	return resultBuffer
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	joinedRows := make([]Row, 0, len(inners))
	for _, inner := range inners {
		joinedRows = append(joinedRows, makeJoinRow(outer, inner))
	}

	if outputer.filter == nil {
		return append(resultBuffer, joinedRows...), len(joinedRows) > 0, nil
	}

	originLen := len(resultBuffer)
	for _, joinedRow := range joinedRows {
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			resultBuffer = append(resultBuffer, joinedRow)
		}
	}
	return resultBuffer, len(resultBuffer) > originLen, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
	}
	return resultBuffer
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	joinedRows := make([]Row, 0, len(inners))
	for _, inner := range inners {
		joinedRows = append(joinedRows, makeJoinRow(inner, outer))
	}

	if outputer.filter == nil {
		return append(resultBuffer, joinedRows...), len(joinedRows) > 0, nil
	}

	originLen := len(resultBuffer)
	for _, joinedRow := range joinedRows {
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			resultBuffer = append(resultBuffer, joinedRow)
		}
	}
	return resultBuffer, len(resultBuffer) > originLen, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
	}
	return resultBuffer
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	joinedRows := make([]Row, 0, len(inners))
	if outputer.outerIsRight {
		for _, inner := range inners {
			joinedRows = append(joinedRows, makeJoinRow(inner, outer))
		}
	} else {
		for _, inner := range inners {
			joinedRows = append(joinedRows, makeJoinRow(outer, inner))
		}
	}

	if outputer.filter == nil {
		return append(resultBuffer, joinedRows...), len(joinedRows) > 0, nil
	}

	originLen := len(resultBuffer)
	for _, joinedRow := range joinedRows {
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			resultBuffer = append(resultBuffer, joinedRow)
		}
	}
	return resultBuffer, len(resultBuffer) > originLen, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}
