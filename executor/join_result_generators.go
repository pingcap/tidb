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
	// outerIdx returns the child index of outer table.
	outerIdx() int
	// initDefaultChunkInner converts default inner rows stored in a Datum slice to a chunk.Row.
	initDefaultChunkInner(innerTypes []*types.FieldType)

	// emitMatchedInners should be called when key in outer row is equal to key in every inner row.
	// Reutrn true if outer row can be joined with any input inner row.
	emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error)
	emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error

	// emitUnMatchedOuter should be called when outer row is not matched to any inner row.
	emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row
	emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk)

	// emitUnMatchedOuters should be called when outer row is not matched to any inner row.
	emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row
	emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk)
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

func (outputer *baseJoinResultGenerator) outerIdx() int {
	if outputer.outerIsRight {
		return 1
	}
	return 0
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
	outputer.selected = outputer.selected[:0]
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

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return resultBuffer, nil
	}
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
			return append(resultBuffer, outer), nil
		}
	}
	return resultBuffer, nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
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

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) (_ []Row, err error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
	}
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
			return resultBuffer, nil
		}
	}
	return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
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
	outputer.emitUnMatchedOuterToChunk(outer, chk)
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, outer)
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(0, outer)
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOuters(outers []Row, resultBuffer []Row) []Row {
	for _, outer := range outers {
		resultBuffer = append(resultBuffer, outer)
	}
	return resultBuffer
}

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
	for i := range outers {
		chk.AppendRow(0, outers[i])
	}
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
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
			buffer = append(buffer[:len(outer)], types.NewDatum(true))
			return append(resultBuffer, buffer), nil
		}
	}
	return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
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
	outputer.emitUnMatchedOuterToChunk(outer, chk)
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(0)})
	return append(resultBuffer, joinedRow)
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
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

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
	for i := range outers {
		chk.AppendRow(0, outers[i])
		chk.AppendInt64(outers[i].Len(), 0)
	}
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
	}
	buffer := make(Row, 0, len(outer)+len(inners[0]))
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
			buffer = append(buffer[:len(outer)], types.NewDatum(false))
			return append(resultBuffer, buffer), nil
		}
	}
	return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
		return nil
	}
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
		if matched {
			chk.AppendRow(0, outer)
			chk.AppendInt64(outer.Len(), 0)
			return nil
		}
	}
	outputer.emitUnMatchedOuterToChunk(outer, chk)
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	buffer := make(Row, 0, len(outer)+1)
	joinedRow := outputer.makeJoinRowToBuffer(buffer, outer, Row{types.NewIntDatum(1)})
	return append(resultBuffer, joinedRow)
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
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

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
	for i := range outers {
		chk.AppendRow(0, outers[i])
		chk.AppendInt64(outers[i].Len(), 1)
	}
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
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
		resultBuffer = outputer.emitUnMatchedOuter(outer, resultBuffer)
	}
	return resultBuffer, nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
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
		outputer.emitUnMatchedOuterToChunk(outer, chk)
	}
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outer, outputer.defaultInner))
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(0, outer)
	chk.AppendRow(outer.Len(), outputer.defaultChunkInner)
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

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
	for i := range outers {
		outputer.makeJoinRowToChunk(chk, outers[i], outputer.defaultChunkInner)
	}
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
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
	if !matched {
		resultBuffer = outputer.emitUnMatchedOuter(outer, resultBuffer)
	}
	return resultBuffer, nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
	if len(inners) == 0 {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
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
	if outputer.filterChunk == nil {
		return nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filterChunk(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	if !matched {
		outputer.emitUnMatchedOuterToChunk(outer, chk)
	}
	return nil
}

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuter(outer Row, resultBuffer []Row) []Row {
	return append(resultBuffer, makeJoinRow(outputer.defaultInner, outer))
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(0, outputer.defaultChunkInner)
	chk.AppendRow(outputer.defaultChunkInner.Len(), outer)
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

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
	for i := range outers {
		outputer.makeJoinRowToChunk(chk, outputer.defaultChunkInner, outers[i])
	}
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emitMatchedInners implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitMatchedInners(outer Row, inners []Row, resultBuffer []Row) ([]Row, error) {
	if len(inners) == 0 {
		return outputer.emitUnMatchedOuter(outer, resultBuffer), nil
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
	var matched bool
	var err error
	resultBuffer, matched, err = outputer.filterResult(resultBuffer, originLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !matched {
		resultBuffer = outputer.emitUnMatchedOuter(outer, resultBuffer)
	}
	return resultBuffer, nil
}

// emitMatchedInnersToChunk implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitMatchedInnersToChunk(outer chunk.Row, inners []chunk.Row, chk *chunk.Chunk) error {
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

// emitUnMatchedOuter implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuter(_ Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOuterToChunk implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuterToChunk(outer chunk.Row, chk *chunk.Chunk) {
}

// emitUnMatchedOuters implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOuters(_ []Row, resultBuffer []Row) []Row {
	return resultBuffer
}

// emitUnMatchedOutersToChunk implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emitUnMatchedOutersToChunk(outers []chunk.Row, chk *chunk.Chunk) {
}
