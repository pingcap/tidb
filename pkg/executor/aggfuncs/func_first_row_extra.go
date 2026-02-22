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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type firstRow4Duration struct {
	baseAggFunc
}

func (*firstRow4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowDuration)), DefPartialResult4FirstRowDurationSize
}

func (*firstRow4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowDuration)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowDuration)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalDuration(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstRow4Duration) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowDuration)(src), (*partialResult4FirstRowDuration)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Duration) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowDuration)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Duration) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowDuration)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowDuration(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Duration) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Duration) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowDuration)(pr)
	success := helper.deserializePartialResult4FirstRowDuration(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4JSON struct {
	baseAggFunc
}

func (*firstRow4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowJSON)), DefPartialResult4FirstRowJSONSize
}

func (*firstRow4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowJSON)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4JSON) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowJSON)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalJSON(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input.Copy()
		memDelta += int64(len(input.Value))
	}
	return memDelta, nil
}
func (*firstRow4JSON) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowJSON)(src), (*partialResult4FirstRowJSON)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4JSON) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowJSON)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *firstRow4JSON) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowJSON)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowJSON(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4JSON) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4JSON) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowJSON)(pr)
	success := helper.deserializePartialResult4FirstRowJSON(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4VectorFloat32 struct {
	baseAggFunc
}

func (*firstRow4VectorFloat32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowVectorFloat32)), DefPartialResult4FirstRowVectorFloat32Size
}

func (*firstRow4VectorFloat32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowVectorFloat32)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4VectorFloat32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowVectorFloat32)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalVectorFloat32(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, isNull, input.Clone()
		memDelta += int64(input.EstimatedMemUsage())
	}
	return memDelta, nil
}
func (*firstRow4VectorFloat32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowVectorFloat32)(src), (*partialResult4FirstRowVectorFloat32)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4VectorFloat32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowVectorFloat32)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendVectorFloat32(e.ordinal, p.val)
	return nil
}

type firstRow4Decimal struct {
	baseAggFunc
}

func (*firstRow4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowDecimal)), DefPartialResult4FirstRowDecimalSize
}

func (*firstRow4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowDecimal)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowDecimal)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull = true, isNull
		if input != nil {
			p.val = *input
		}
	}
	return memDelta, nil
}

func (e *firstRow4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowDecimal)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	if e.retTp == nil {
		return errors.New("e.retTp of first_row should not be nil")
	}
	frac := e.retTp.GetDecimal()
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err := p.val.Round(&p.val, frac, types.ModeHalfUp)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (*firstRow4Decimal) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowDecimal)(src), (*partialResult4FirstRowDecimal)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Decimal) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowDecimal)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowDecimal(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Decimal) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Decimal) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowDecimal)(pr)
	success := helper.deserializePartialResult4FirstRowDecimal(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4Enum struct {
	baseAggFunc
}

func (*firstRow4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowEnum)), DefPartialResult4FirstRowEnumSize
}

func (*firstRow4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowEnum)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Enum) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowEnum)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		d, err := e.args[0].Eval(ctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, d.IsNull(), d.GetMysqlEnum().Copy()
		memDelta += int64(len(p.val.Name))
	}
	return memDelta, nil
}

func (*firstRow4Enum) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowEnum)(src), (*partialResult4FirstRowEnum)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Enum) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowEnum)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Enum) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowEnum)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowEnum(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Enum) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Enum) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowEnum)(pr)
	success := helper.deserializePartialResult4FirstRowEnum(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type firstRow4Set struct {
	baseAggFunc
}

func (*firstRow4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstRowSet)), DefPartialResult4FirstRowSetSize
}

func (*firstRow4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstRowSet)(pr)
	p.isNull, p.gotFirstRow = false, false
}

func (e *firstRow4Set) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstRowSet)(pr)
	if p.gotFirstRow {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		d, err := e.args[0].Eval(ctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstRow, p.isNull, p.val = true, d.IsNull(), d.GetMysqlSet().Copy()
		memDelta += int64(len(p.val.Name))
	}
	return memDelta, nil
}

func (*firstRow4Set) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstRowSet)(src), (*partialResult4FirstRowSet)(dst)
	if !p2.gotFirstRow {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstRow4Set) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstRowSet)(pr)
	if p.isNull || !p.gotFirstRow {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}

func (e *firstRow4Set) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4FirstRowSet)(partialResult)
	resBuf := spillHelper.serializePartialResult4FirstRowSet(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *firstRow4Set) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *firstRow4Set) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4FirstRowSet)(pr)
	success := helper.deserializePartialResult4FirstRowSet(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
