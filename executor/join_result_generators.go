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

// makeJoinRowToBuffer concatenates "lhs" and "rhs" to "buffer" and return that buffer.
// With the help of this function, we can make all of the joined rows to a consecutive
// memory buffer and explore the best cache performance.
func (outputer *baseJoinResultGenerator) makeJoinRowToBuffer(buffer []types.Datum, lhs, rhs Row) []types.Datum {
	buffer = append(buffer, lhs...)
	buffer = append(buffer, rhs...)
	return buffer
}

// growResultBufferIfNecessary grows resultBuffer if necessary.
func (outputer *baseJoinResultGenerator) growResultBufferIfNecessary(resultBuffer []Row, numToAppend int) []Row {
	length := len(resultBuffer)
	if cap(resultBuffer)-length >= numToAppend {
		return resultBuffer
	}
	newResultBuffer := make([]Row, length, length+numToAppend)
	copy(newResultBuffer, resultBuffer)
	return newResultBuffer
}

// filterResult filters resultBuffer according to filter.
func (outputer *baseJoinResultGenerator) filterResult(resultBuffer []Row, originLen int) ([]Row, bool, error) {
	if outputer.filter == nil {
		return resultBuffer, len(resultBuffer) > originLen, nil
	}

	curLen := originLen
	for _, joinedRow := range resultBuffer[originLen:] {
		matched, err := expression.EvalBool(outputer.filter, joinedRow, outputer.ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			resultBuffer[curLen] = joinedRow
			curLen++
		}
	}
	return resultBuffer[:curLen], curLen > originLen, nil
}

type semiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	if outputer.filter == nil {
		return append(resultBuffer, outer), true, nil
	}

	buffer := make(Row, 0, len(inners[0])+len(outer))
	for _, inner := range inners {
		if outputer.outerIsRight {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], inner, outer)
		} else {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		}

		matched, err := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
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
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	if outputer.filter == nil {
		return resultBuffer, true, nil
	}

	buffer := make(Row, 0, len(outer)+len(inners[0]))
	for _, inner := range inners {
		if outputer.outerIsRight {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], inner, outer)
		} else {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		}

		matched, err = expression.EvalBool(outputer.filter, buffer, outputer.ctx)
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
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
	if outputer.filter == nil {
		joinedRow := outputer.makeJoinRowToBuffer(buffer[:0], outer, Row{types.NewIntDatum(1)})
		return append(resultBuffer, joinedRow), true, nil
	}

	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		matched, err := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			buffer = append(buffer[:len(outer)], types.NewDatum(true))
			return append(resultBuffer, buffer), true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(0)})
	return append(resultBuffer, joinedRow)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	if len(outers) == 0 {
		return resultBuffer
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(outers))
	buffer := make(Row, 0, (len(outers[0])+1)*len(outers))
	inner := Row{types.NewIntDatum(0)}
	for _, outer := range outers {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, inner)
		resultBuffer = append(resultBuffer, buffer)
	}
	return resultBuffer
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
	if outputer.filter == nil {
		joinedRow := outputer.makeJoinRowToBuffer(buffer[:0], outer, Row{types.NewIntDatum(0)})
		return append(resultBuffer, joinedRow), true, nil
	}

	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		matched, err := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
		if err != nil {
			return resultBuffer, false, errors.Trace(err)
		}
		if matched {
			buffer = append(buffer[:len(outer)], types.NewDatum(false))
			return append(resultBuffer, buffer), true, nil
		}
	}
	return resultBuffer, false, nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(1)})
	return append(resultBuffer, joinedRow)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	if len(outers) == 0 {
		return resultBuffer
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(outers))
	buffer := make(Row, 0, (len(outers[0])+1)*len(outers))
	inner := Row{types.NewIntDatum(1)}
	for _, outer := range outers {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, inner)
		resultBuffer = append(resultBuffer, buffer)
	}
	return resultBuffer
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(inners))
	originLen := len(resultBuffer)
	buffer := make([]types.Datum, 0, len(inners)*(len(outer)+len(inners[0])))
	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, inner)
		resultBuffer = append(resultBuffer, buffer)
	}
	return outputer.filterResult(resultBuffer, originLen)
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	if len(outers) == 0 {
		return resultBuffer
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(outers))
	buffer := make([]types.Datum, 0, len(outers)*(len(outers[0])+len(outputer.defaultInner)))
	for _, outer := range outers {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, outputer.defaultInner)
		resultBuffer = append(resultBuffer, buffer)
	}
	return resultBuffer
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(inners))
	originLen := len(resultBuffer)
	buffer := make([]types.Datum, 0, len(inners)*(len(outer)+len(inners[0])))
	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], inner, outer)
		resultBuffer = append(resultBuffer, buffer)
	}
	return outputer.filterResult(resultBuffer, originLen)
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	if len(outers) == 0 {
		return resultBuffer
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(outers))
	buffer := make([]types.Datum, 0, len(outers)*(len(outers[0])+len(outputer.defaultInner)))
	for _, outer := range outers {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outputer.defaultInner, outer)
		resultBuffer = append(resultBuffer, buffer)
	}
	return resultBuffer
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, bool, error) {
	if len(inners) == 0 {
		return resultBuffer, false, nil
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(inners))
	originLen := len(resultBuffer)
	buffer := make([]types.Datum, 0, (len(outer)+len(inners[0]))*len(inners))
	if outputer.outerIsRight {
		for _, inner := range inners {
			buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], inner, outer)
			resultBuffer = append(resultBuffer, buffer)
		}
	} else {
		for _, inner := range inners {
			buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, inner)
			resultBuffer = append(resultBuffer, buffer)
		}
	}
	return outputer.filterResult(resultBuffer, originLen)
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}
