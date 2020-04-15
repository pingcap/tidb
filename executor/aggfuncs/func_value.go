// Copyright 2019 PingCAP, Inc.
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

package aggfuncs

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/types/json"
	"github.com/pingcap/tidb/v4/util/chunk"
)

// valueEvaluator is used to evaluate values for `first_value`, `last_value`, `nth_value`,
// `lead` and `lag`.
type valueEvaluator interface {
	// evaluateRow evaluates the expression using row and stores the result inside.
	evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error
	// appendResult appends the result to chunk.
	appendResult(chk *chunk.Chunk, colIdx int)
}

type value4Int struct {
	val    int64
	isNull bool
}

func (v *value4Int) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalInt(ctx, row)
	return err
}

func (v *value4Int) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendInt64(colIdx, v.val)
	}
}

type value4Float32 struct {
	val    float32
	isNull bool
}

func (v *value4Float32) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	var val float64
	val, v.isNull, err = expr.EvalReal(ctx, row)
	v.val = float32(val)
	return err
}

func (v *value4Float32) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendFloat32(colIdx, v.val)
	}
}

type value4Decimal struct {
	val    *types.MyDecimal
	isNull bool
}

func (v *value4Decimal) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalDecimal(ctx, row)
	return err
}

func (v *value4Decimal) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendMyDecimal(colIdx, v.val)
	}
}

type value4Float64 struct {
	val    float64
	isNull bool
}

func (v *value4Float64) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalReal(ctx, row)
	return err
}

func (v *value4Float64) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendFloat64(colIdx, v.val)
	}
}

type value4String struct {
	val    string
	isNull bool
}

func (v *value4String) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalString(ctx, row)
	return err
}

func (v *value4String) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendString(colIdx, v.val)
	}
}

type value4Time struct {
	val    types.Time
	isNull bool
}

func (v *value4Time) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalTime(ctx, row)
	return err
}

func (v *value4Time) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendTime(colIdx, v.val)
	}
}

type value4Duration struct {
	val    types.Duration
	isNull bool
}

func (v *value4Duration) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalDuration(ctx, row)
	return err
}

func (v *value4Duration) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendDuration(colIdx, v.val)
	}
}

type value4JSON struct {
	val    json.BinaryJSON
	isNull bool
}

func (v *value4JSON) evaluateRow(ctx sessionctx.Context, expr expression.Expression, row chunk.Row) error {
	var err error
	v.val, v.isNull, err = expr.EvalJSON(ctx, row)
	v.val = v.val.Copy() // deep copy to avoid content change.
	return err
}

func (v *value4JSON) appendResult(chk *chunk.Chunk, colIdx int) {
	if v.isNull {
		chk.AppendNull(colIdx)
	} else {
		chk.AppendJSON(colIdx, v.val)
	}
}

func buildValueEvaluator(tp *types.FieldType) valueEvaluator {
	evalType := tp.EvalType()
	if tp.Tp == mysql.TypeBit {
		evalType = types.ETString
	}
	switch evalType {
	case types.ETInt:
		return &value4Int{}
	case types.ETReal:
		switch tp.Tp {
		case mysql.TypeFloat:
			return &value4Float32{}
		case mysql.TypeDouble:
			return &value4Float64{}
		}
	case types.ETDecimal:
		return &value4Decimal{}
	case types.ETDatetime, types.ETTimestamp:
		return &value4Time{}
	case types.ETDuration:
		return &value4Duration{}
	case types.ETString:
		return &value4String{}
	case types.ETJson:
		return &value4JSON{}
	}
	return nil
}

type firstValue struct {
	baseAggFunc

	tp *types.FieldType
}

type partialResult4FirstValue struct {
	gotFirstValue bool
	evaluator     valueEvaluator
}

func (v *firstValue) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4FirstValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *firstValue) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstValue)(pr)
	p.gotFirstValue = false
}

func (v *firstValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4FirstValue)(pr)
	if p.gotFirstValue {
		return nil
	}
	if len(rowsInGroup) > 0 {
		p.gotFirstValue = true
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[0])
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *firstValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstValue)(pr)
	if !p.gotFirstValue {
		chk.AppendNull(v.ordinal)
	} else {
		p.evaluator.appendResult(chk, v.ordinal)
	}
	return nil
}

type lastValue struct {
	baseAggFunc

	tp *types.FieldType
}

type partialResult4LastValue struct {
	gotLastValue bool
	evaluator    valueEvaluator
}

func (v *lastValue) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4LastValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *lastValue) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4LastValue)(pr)
	p.gotLastValue = false
}

func (v *lastValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4LastValue)(pr)
	if len(rowsInGroup) > 0 {
		p.gotLastValue = true
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[len(rowsInGroup)-1])
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *lastValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4LastValue)(pr)
	if !p.gotLastValue {
		chk.AppendNull(v.ordinal)
	} else {
		p.evaluator.appendResult(chk, v.ordinal)
	}
	return nil
}

type nthValue struct {
	baseAggFunc

	tp  *types.FieldType
	nth uint64
}

type partialResult4NthValue struct {
	seenRows  uint64
	evaluator valueEvaluator
}

func (v *nthValue) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4NthValue{evaluator: buildValueEvaluator(v.tp)})
}

func (v *nthValue) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4NthValue)(pr)
	p.seenRows = 0
}

func (v *nthValue) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	if v.nth == 0 {
		return nil
	}
	p := (*partialResult4NthValue)(pr)
	numRows := uint64(len(rowsInGroup))
	if v.nth > p.seenRows && v.nth-p.seenRows <= numRows {
		err := p.evaluator.evaluateRow(sctx, v.args[0], rowsInGroup[v.nth-p.seenRows-1])
		if err != nil {
			return err
		}
	}
	p.seenRows += numRows
	return nil
}

func (v *nthValue) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4NthValue)(pr)
	if v.nth == 0 || p.seenRows < v.nth {
		chk.AppendNull(v.ordinal)
	} else {
		p.evaluator.appendResult(chk, v.ordinal)
	}
	return nil
}
