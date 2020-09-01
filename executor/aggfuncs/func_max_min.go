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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
)

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
}

type partialResult4Time struct {
	val    types.Time
	isNull bool
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
}

type partialResult4MaxMinString struct {
	val    string
	isNull bool
}

type partialResult4MaxMinJSON struct {
	val    json.BinaryJSON
	isNull bool
}

type partialResult4MaxMinEnum struct {
	val    types.Enum
	isNull bool
}

type partialResult4MaxMinSet struct {
	val    types.Set
	isNull bool
}

type baseMaxMinAggFunc struct {
	baseAggFunc

	isMax bool
}

type maxMin4Int struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Int) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Int) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinInt)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return nil
}

func (e *maxMin4Int) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Uint) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Uint) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinUint)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Uint) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		if e.isMax && uintVal > p.val || !e.isMax && uintVal < p.val {
			p.val = uintVal
		}
	}
	return nil
}

func (e *maxMin4Uint) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float32) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		f := float32(input)
		if p.isNull {
			p.val = f
			p.isNull = false
			continue
		}
		if e.isMax && f > p.val || !e.isMax && f < p.val {
			p.val = f
		}
	}
	return nil
}

func (e *maxMin4Float32) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Float64) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return nil
}

func (e *maxMin4Float64) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Decimal) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		cmp := input.Compare(&p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = *input
		}
	}
	return nil
}

func (e *maxMin4Decimal) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (e *maxMin4String) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
}

func (e *maxMin4String) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinString)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *maxMin4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinString)(pr)
	tp := e.args[0].GetType()
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			// The string returned by `EvalString` may be referenced to an underlying buffer,
			// for example ‘Chunk’, which could be reset and reused multiply times.
			// We have to deep copy that string to avoid some potential risks
			// when the content of that underlying buffer changed.
			p.val = stringutil.Copy(input)
			p.isNull = false
			continue
		}
		cmp := types.CompareString(input, p.val, tp.Collate)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = stringutil.Copy(input)
		}
	}
	return nil
}

func (e *maxMin4String) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	tp := e.args[0].GetType()
	cmp := types.CompareString(p1.val, p2.val, tp.Collate)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Time) AllocPartialResult() PartialResult {
	p := new(partialResult4Time)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Time)(pr)
	p.isNull = true
}

func (e *maxMin4Time) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Time)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Time)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return nil
}

func (e *maxMin4Time) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Time)(src), (*partialResult4Time)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Duration) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
}

func (e *maxMin4Duration) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDuration)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return nil
}

func (e *maxMin4Duration) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return nil
}

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (e *maxMin4JSON) AllocPartialResult() PartialResult {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p)
}

func (e *maxMin4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinJSON)(pr)
	p.isNull = true
}

func (e *maxMin4JSON) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinJSON)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *maxMin4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Copy()
			p.isNull = false
			continue
		}
		cmp := json.CompareBinary(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = input.Copy()
		}
	}
	return nil
}

func (e *maxMin4JSON) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		return nil
	}
	if p2.isNull {
		*p2 = *p1
		return nil
	}
	cmp := json.CompareBinary(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return nil
}

type maxMin4Enum struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinEnum)
	p.isNull = true
	return PartialResult(p), 0
}

func (e *maxMin4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinEnum)(pr)
	p.isNull = true
}

func (e *maxMin4Enum) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinEnum)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Enum) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinEnum)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(row)
		if err != nil {
			return 0, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlEnum().Copy()
			p.isNull = false
			continue
		}
		en := d.GetMysqlEnum()
		if e.isMax && en.Value > p.val.Value || !e.isMax && en.Value < p.val.Value {
			p.val = en.Copy()
		}
	}
	return 0, nil
}

func (e *maxMin4Enum) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinEnum)(src), (*partialResult4MaxMinEnum)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val.Value > p2.val.Value || !e.isMax && p1.val.Value < p2.val.Value {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

type maxMin4Set struct {
	baseMaxMinAggFunc
}

func (e *maxMin4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinSet)
	p.isNull = true
	return PartialResult(p), 0
}

func (e *maxMin4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinSet)(pr)
	p.isNull = true
}

func (e *maxMin4Set) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinSet)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Set) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinSet)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(row)
		if err != nil {
			return 0, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlSet().Copy()
			p.isNull = false
			continue
		}
		s := d.GetMysqlSet()
		if e.isMax && s.Value > p.val.Value || !e.isMax && s.Value < p.val.Value {
			p.val = s.Copy()
		}
	}
	return 0, nil
}

func (e *maxMin4Set) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinSet)(src), (*partialResult4MaxMinSet)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val.Value > p2.val.Value || !e.isMax && p1.val.Value < p2.val.Value {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}
