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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
	// emit tries to join an outer row with a batch of inner rows.
	// When len(inners) == 0, it means that the outer row can not be joined with any inner row:
	//     1. SemiJoin:	unmatched outer row is ignored.
	//     2. AntiSemiJoin:  unmatched outer row is appended to the result buffer.
	//     3. LeftOuterSemiJoin: unmatched outer row is appended with 0 and appended to the result buffer.
	//     4. AntiLeftOuterSemiJoin: unmatched outer row is appended with 1 and appended to the result buffer.
	//     5. LeftOuterJoin: unmatched outer row is joined with a row of NULLs and appended to the result buffer.
	//     6. RightOuterJoin: unmatched outer row is joined with a row of NULLs and appended to the result buffer.
	//     7. InnerJoin: unmatched outer row is ignored.
	// When len(inner) != 0 but all the joined rows are filtered, this means that the outer row is unmatched and the above action is tacked as well.
	// Otherwise, the outer row is matched and some joined rows is appended to the result buffer.
	emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error)

	// emitToChunk takes the same operation as emit, but the joined rows is appended to a Chunk instead of a result buffer.
	emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error
}

func newJoinResultGenerator(ctx context.Context, joinType plan.JoinType,
	outerIsRight bool, defaultInner Row, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) joinResultGenerator {
	base := baseJoinResultGenerator{
		ctx:          ctx,
		filter:       filter,
		defaultInner: defaultInner,
		outerIsRight: outerIsRight,
	}
	colTypes := make([]*types.FieldType, 0, len(lhsColTypes)+len(rhsColTypes))
	colTypes = append(colTypes, lhsColTypes...)
	colTypes = append(colTypes, rhsColTypes...)
	base.chk = chunk.NewChunk(colTypes)
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	if joinType == plan.LeftOuterJoin || joinType == plan.RightOuterJoin {
		innerColTypes := lhsColTypes
		if !outerIsRight {
			innerColTypes = rhsColTypes
		}
		base.initDefaultChunkInner(innerColTypes)
	}
	switch joinType {
	case plan.SemiJoin:
		return &semiJoinResultGenerator{base}
	case plan.AntiSemiJoin:
		return &antiSemiJoinResultGenerator{base}
	case plan.LeftOuterSemiJoin:
		return &leftOuterSemiJoinResultGenerator{base}
	case plan.AntiLeftOuterSemiJoin:
		return &antiLeftOuterSemiJoinResultGenerator{base}
	case plan.LeftOuterJoin:
		return &leftOuterJoinResultGenerator{base}
	case plan.RightOuterJoin:
		return &rightOuterJoinResultGenerator{base}
	case plan.InnerJoin:
		return &innerJoinResultGenerator{base}
	}
	panic("unsupported join type in func newJoinResultGenerator()")
}

type baseJoinResultGenerator struct {
	ctx               context.Context
	filter            []expression.Expression
	defaultChunkInner chunk.Row
	outerIsRight      bool
	chk               *chunk.Chunk
	selected          []bool
	defaultInner      Row
}

func (outputer *baseJoinResultGenerator) initDefaultChunkInner(innerTypes []*types.FieldType) {
	shadowChunk := chunk.NewChunk(innerTypes)
	for i, colType := range innerTypes {
		if outputer.defaultInner[i].IsNull() {
			shadowChunk.AppendNull(i)
			continue
		}
		switch colType.Tp {
		case mysql.TypeNull:
			shadowChunk.AppendNull(i)
		case mysql.TypeFloat:
			shadowChunk.AppendFloat32(i, outputer.defaultInner[i].GetFloat32())
		case mysql.TypeDouble:
			shadowChunk.AppendFloat64(i, outputer.defaultInner[i].GetFloat64())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			shadowChunk.AppendInt64(i, outputer.defaultInner[i].GetInt64())
		case mysql.TypeDuration:
			shadowChunk.AppendDuration(i, outputer.defaultInner[i].GetMysqlDuration())
		case mysql.TypeNewDecimal:
			shadowChunk.AppendMyDecimal(i, outputer.defaultInner[i].GetMysqlDecimal())
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			shadowChunk.AppendTime(i, outputer.defaultInner[i].GetMysqlTime())
		case mysql.TypeJSON:
			shadowChunk.AppendJSON(i, outputer.defaultInner[i].GetMysqlJSON())
		case mysql.TypeBit:
			shadowChunk.AppendBytes(i, outputer.defaultInner[i].GetMysqlBit())
		case mysql.TypeEnum:
			shadowChunk.AppendEnum(i, outputer.defaultInner[i].GetMysqlEnum())
		case mysql.TypeSet:
			shadowChunk.AppendSet(i, outputer.defaultInner[i].GetMysqlSet())
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			shadowChunk.AppendBytes(i, outputer.defaultInner[i].GetBytes())
		default:
			shadowChunk.AppendNull(i)
		}
	}
	outputer.defaultChunkInner = shadowChunk.Begin()
}

// makeJoinRowToBuffer concatenates "lhs" and "rhs" to "buffer" and return that buffer.
// With the help of this function, we can make all of the joined rows to a consecutive
// memory buffer and explore the best cache performance.
func (outputer *baseJoinResultGenerator) makeJoinRowToBuffer(buffer []types.Datum, lhs, rhs Row) []types.Datum {
	buffer = append(buffer, lhs...)
	buffer = append(buffer, rhs...)
	return buffer
}

func (outputer *baseJoinResultGenerator) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	chk.AppendRow(0, lhs)
	chk.AppendRow(lhs.Len(), rhs)
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

func (outputer *baseJoinResultGenerator) filterChunk(input, output *chunk.Chunk) (matched bool, err error) {
	outputer.selected, err = expression.VectorizedFilter(outputer.ctx, outputer.filter, input, outputer.selected)
	if err != nil {
		return false, errors.Trace(err)
	}
	for i := 0; i < len(outputer.selected); i++ {
		if !outputer.selected[i] {
			continue
		}
		matched = true
		output.AppendRow(0, input.GetRow(i))
	}
	return matched, nil
}

type semiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return resultBuffer, nil
	}
	// outer row can be joined with an inner row.
	if outputer.filter == nil {
		return append(resultBuffer, outer), nil
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
			return resultBuffer, errors.Trace(err)
		}
		if matched {
			// outer row can be joined with an inner row.
			return append(resultBuffer, outer), nil
		}
	}
	// outer row can not be joined with any inner row.
	return resultBuffer, nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		return nil
	}
	if outputer.filter == nil {
		chk.AppendRow(0, outer)
		return nil
	}

	for _, inner := range inners {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}
		selected, err := expression.EvalBool(outputer.filter, outputer.chk.Begin(), outputer.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if selected {
			chk.AppendRow(0, outer)
			return nil
		}
	}
	return nil
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) (_ []Row, err error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return append(resultBuffer, outer), nil
	}
	// outer row can be joined with an inner row.
	if outputer.filter == nil {
		return resultBuffer, nil
	}

	buffer := make(Row, 0, len(outer)+len(inners[0]))
	for _, inner := range inners {
		if outputer.outerIsRight {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], inner, outer)
		} else {
			buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		}

		matched, err1 := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if matched {
			// outer row can be joined with an inner row.
			return resultBuffer, nil
		}
	}
	// outer row can not be joined with any inner row.
	return append(resultBuffer, outer), nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		chk.AppendRow(0, outer)
		return nil
	}
	if outputer.filter == nil {
		return nil
	}

	for _, inner := range inners {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}

		matched, err := expression.EvalBool(outputer.filter, outputer.chk.Begin(), outputer.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			return nil
		}
	}
	chk.AppendRow(0, outer)
	return nil
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
	// outer row can be joined with an inner row.
	if outputer.filter == nil {
		joinedRow := outputer.makeJoinRowToBuffer(buffer[:0], outer, Row{types.NewIntDatum(1)})
		return append(resultBuffer, joinedRow), nil
	}

	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		matched, err := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
		if err != nil {
			return resultBuffer, errors.Trace(err)
		}
		if matched {
			// outer row can be joined with an inner row.
			buffer = append(buffer[:len(outer)], types.NewDatum(true))
			return append(resultBuffer, buffer), nil
		}
	}
	// outer row can not be joined with any inner row.
	return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		chk.AppendRow(0, outer)
		chk.AppendInt64(outer.Len(), 0)
		return nil
	}
	if outputer.filter == nil {
		chk.AppendRow(0, outer)
		chk.AppendInt64(outer.Len(), 1)
		return nil
	}

	for _, inner := range inners {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		matched, err := expression.EvalBool(outputer.filter, outputer.chk.Begin(), outputer.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			chk.AppendRow(0, outer)
			chk.AppendInt64(outer.Len(), 1)
			return nil
		}
	}
	chk.AppendRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(0)})
	return append(resultBuffer, joinedRow)
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
	// outer row can be joined with an inner row.
	if outputer.filter == nil {
		joinedRow := outputer.makeJoinRowToBuffer(buffer[:0], outer, Row{types.NewIntDatum(0)})
		return append(resultBuffer, joinedRow), nil
	}

	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[:0], outer, inner)
		matched, err := expression.EvalBool(outputer.filter, buffer, outputer.ctx)
		if err != nil {
			return resultBuffer, errors.Trace(err)
		}
		if matched {
			// outer row can be joined with an inner row.
			buffer = append(buffer[:len(outer)], types.NewDatum(false))
			return append(resultBuffer, buffer), nil
		}
	}
	// outer row can not be joined with any inner row.
	return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		chk.AppendRow(0, outer)
		chk.AppendInt64(outer.Len(), 1)
		return nil
	}

	// outer row can be joined with an inner row.
	if outputer.filter == nil {
		chk.AppendRow(0, outer)
		chk.AppendInt64(outer.Len(), 0)
		return nil
	}

	for _, inner := range inners {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		matched, err := expression.EvalBool(outputer.filter, outputer.chk.Begin(), outputer.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// outer row can be joined with an inner row.
		if matched {
			chk.AppendRow(0, outer)
			chk.AppendInt64(outer.Len(), 0)
			return nil
		}
	}

	// outer row can not be joined with any inner row.
	chk.AppendRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(1)})
	return append(resultBuffer, joinedRow)
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner)), nil
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(inners))
	originLen := len(resultBuffer)
	buffer := make([]types.Datum, 0, len(inners)*(len(outer)+len(inners[0])))
	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], outer, inner)
		resultBuffer = append(resultBuffer, buffer)
	}
	var matched bool
	var err error
	resultBuffer, matched, err = outputer.filterResult(resultBuffer, originLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !matched {
		// outer row can not be joined with any inner row.
		return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner)), nil
	}
	return resultBuffer, nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		chk.AppendRow(0, outer)
		chk.AppendRow(outer.Len(), outputer.defaultChunkInner)
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if outputer.filter == nil {
		chkForJoin = chk
	}
	for _, inner := range inners {
		outputer.makeJoinRowToChunk(chkForJoin, outer, inner)
	}
	if outputer.filter == nil {
		return nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filterChunk(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	if !matched {
		// outer row can not be joined with any inner row.
		chk.AppendRow(0, outer)
		chk.AppendRow(outer.Len(), outputer.defaultChunkInner)
	}
	return nil
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer)), nil
	}
	resultBuffer = outputer.growResultBufferIfNecessary(resultBuffer, len(inners))
	originLen := len(resultBuffer)
	buffer := make([]types.Datum, 0, len(inners)*(len(outer)+len(inners[0])))
	for _, inner := range inners {
		buffer = outputer.makeJoinRowToBuffer(buffer[len(buffer):], inner, outer)
		resultBuffer = append(resultBuffer, buffer)
	}
	var matched bool
	var err error
	resultBuffer, matched, err = outputer.filterResult(resultBuffer, originLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// outer row can not be joined with any inner row.
	if !matched {
		return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer)), nil
	}
	return resultBuffer, nil
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		chk.AppendRow(0, outputer.defaultChunkInner)
		chk.AppendRow(outputer.defaultChunkInner.Len(), outer)
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if outputer.filter == nil {
		chkForJoin = chk
	}
	for _, inner := range inners {
		outputer.makeJoinRowToChunk(chkForJoin, inner, outer)
	}
	if outputer.filter == nil {
		return nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filterChunk(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	// outer row can not be joined with any inner row.
	if !matched {
		chk.AppendRow(0, outputer.defaultChunkInner)
		chk.AppendRow(outputer.defaultChunkInner.Len(), outer)
	}
	return nil
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emit(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	// outer row can not be joined with any inner row.
	if len(inners) == 0 {
		return resultBuffer, nil
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
	var err error
	resultBuffer, _, err = outputer.filterResult(resultBuffer, originLen)
	return resultBuffer, errors.Trace(err)
}

// emitToChunk implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if outputer.filter == nil {
		chkForJoin = chk
	}
	if outputer.outerIsRight {
		for _, inner := range inners {
			outputer.makeJoinRowToChunk(chkForJoin, inner, outer)
		}
	} else {
		for _, inner := range inners {
			outputer.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if outputer.filter == nil {
		return nil
	}

	// reach here, chkForJoin is outputer.chk
	_, err := outputer.filterChunk(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
