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

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

const (
	// DefPartialResult4FirstRowIntSize is the size of partialResult4FirstRowInt
	DefPartialResult4FirstRowIntSize = int64(unsafe.Sizeof(partialResult4FirstRowInt{}))
	// DefPartialResult4FirstRowFloat32Size is the size of partialResult4FirstRowFloat32
	DefPartialResult4FirstRowFloat32Size = int64(unsafe.Sizeof(partialResult4FirstRowFloat32{}))
	// DefPartialResult4FirstRowFloat64Size is the size of partialResult4FirstRowFloat64
	DefPartialResult4FirstRowFloat64Size = int64(unsafe.Sizeof(partialResult4FirstRowFloat64{}))
	// DefPartialResult4FirstRowStringSize is the size of partialResult4FirstRowString
	DefPartialResult4FirstRowStringSize = int64(unsafe.Sizeof(partialResult4FirstRowString{}))
	// DefPartialResult4FirstRowTimeSize is the size of partialResult4FirstRowTime
	DefPartialResult4FirstRowTimeSize = int64(unsafe.Sizeof(partialResult4FirstRowTime{}))
	// DefPartialResult4FirstRowDurationSize is the size of partialResult4FirstRowDuration
	DefPartialResult4FirstRowDurationSize = int64(unsafe.Sizeof(partialResult4FirstRowDuration{}))
	// DefPartialResult4FirstRowJSONSize is the size of partialResult4FirstRowJSON
	DefPartialResult4FirstRowJSONSize = int64(unsafe.Sizeof(partialResult4FirstRowJSON{}))
	// DefPartialResult4FirstRowVectorFloat32Size is the size of partialResult4FirstRowVectorFloat32
	DefPartialResult4FirstRowVectorFloat32Size = int64(unsafe.Sizeof(partialResult4FirstRowVectorFloat32{}))
	// DefPartialResult4FirstRowDecimalSize is the size of partialResult4FirstRowDecimal
	DefPartialResult4FirstRowDecimalSize = int64(unsafe.Sizeof(partialResult4FirstRowDecimal{}))
	// DefPartialResult4FirstRowEnumSize is the size of partialResult4FirstRowEnum
	DefPartialResult4FirstRowEnumSize = int64(unsafe.Sizeof(partialResult4FirstRowEnum{}))
	// DefPartialResult4FirstRowSetSize is the size of partialResult4FirstRowSet
	DefPartialResult4FirstRowSetSize = int64(unsafe.Sizeof(partialResult4FirstRowSet{}))
)

// nolint:structcheck
type basePartialResult4FirstRow struct {
	// isNull indicates whether the first row is null.
	isNull bool
	// gotFirstRow indicates whether the first row has been got,
	// if so, we would avoid evaluating the values of the remained rows.
	gotFirstRow bool
}

type partialResult4FirstRowInt struct {
	basePartialResult4FirstRow

	val int64
}

type partialResult4FirstRowFloat32 struct {
	basePartialResult4FirstRow

	val float32
}

type partialResult4FirstRowDecimal struct {
	basePartialResult4FirstRow

	val types.MyDecimal
}

type partialResult4FirstRowFloat64 struct {
	basePartialResult4FirstRow

	val float64
}

type partialResult4FirstRowString struct {
	basePartialResult4FirstRow

	val string
}

type partialResult4FirstRowTime struct {
	basePartialResult4FirstRow

	val types.Time
}

type partialResult4FirstRowDuration struct {
	basePartialResult4FirstRow

	val types.Duration
}

type partialResult4FirstRowJSON struct {
	basePartialResult4FirstRow

	val types.BinaryJSON
}

type partialResult4FirstRowVectorFloat32 struct {
	basePartialResult4FirstRow

	val types.VectorFloat32
}

type partialResult4FirstRowEnum struct {
	basePartialResult4FirstRow

	val types.Enum
}

type partialResult4FirstRowSet struct {
	basePartialResult4FirstRow

	val types.Set
}

type firstRow4Int struct {
	baseAggFunc
}

func (*firstRow4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowInt)), DefPartialResult4FirstRowIntSize
}

func (*firstRow4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowInt)(pr)
	p.val, p.isNull, p.gotFirstRow = 0, false, false
}

func (e *firstRow4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowInt)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalInt(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstRow4Int) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowInt)(src), (*partialResult4FirstRowInt)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Int) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowInt)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Int) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowInt)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowInt(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Int) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Int) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowInt)(pr)
	success := helper.deserializePartialResult4FirstRowInt(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4Float32 struct {
	baseAggFunc
}

func (*firstRow4Float32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowFloat32)), DefPartialResult4FirstRowFloat32Size
}

func (*firstRow4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowFloat32)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Float32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowFloat32)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalReal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, float32(input)
	}
	return memDelta, nil
}

func (*firstRow4Float32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowFloat32)(src), (*partialResult4FirstRowFloat32)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Float32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowFloat32)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Float32) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowFloat32)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowFloat32(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Float32) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Float32) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowFloat32)(pr)
	success := helper.deserializePartialResult4FirstRowFloat32(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4Float64 struct {
	baseAggFunc
}

func (*firstRow4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowFloat64)), DefPartialResult4FirstRowFloat64Size
}

func (*firstRow4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowFloat64)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowFloat64)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalReal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstRow4Float64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowFloat64)(src), (*partialResult4FirstRowFloat64)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowFloat64)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Float64) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowFloat64)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowFloat64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Float64) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Float64) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowFloat64)(pr)
	success := helper.deserializePartialResult4FirstRowFloat64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4String struct {
	baseAggFunc
}

func (*firstRow4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowString)), DefPartialResult4FirstRowStringSize
}

func (*firstRow4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowString)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowString)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalString(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, stringutil.Copy(input)
		memDelta += int64(len(input))
	}
	return memDelta, nil
}

func (*firstRow4String) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowString)(src), (*partialResult4FirstRowString)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4String) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowString)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *firstRow4String) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowString)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowString(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4String) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4String) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowString)(pr)
	success := helper.deserializePartialResult4FirstRowString(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4Time struct {
	baseAggFunc
}

func (*firstRow4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowTime)), DefPartialResult4FirstRowTimeSize
}

func (*firstRow4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowTime)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Time) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowTime)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalTime(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstRow4Time) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowTime)(src), (*partialResult4FirstRowTime)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Time) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowTime)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Time) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Time) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Time) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowTime)(pr)
	success := helper.deserializePartialResult4FirstRowTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
