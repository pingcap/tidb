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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)
type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (*maxMin4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinJSONSize
}

func (*maxMin4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinJSON)(pr)
	p.isNull = true
}

func (e *maxMin4JSON) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinJSON)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *maxMin4JSON) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Copy()
			memDelta += int64(len(input.Value))
			p.isNull = false
			continue
		}
		cmp := types.CompareBinaryJSON(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			oldMem := len(p.val.Value)
			newMem := len(input.Value)
			memDelta += int64(newMem - oldMem)
			p.val = input.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4JSON) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := types.CompareBinaryJSON(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return 0, nil
}

func (e *maxMin4JSON) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinJSON)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinJSON(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4JSON) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4JSON) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinJSON)(pr)
	success := helper.deserializePartialResult4MaxMinJSON(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4VectorFloat32 struct {
	baseMaxMinAggFunc
}

func (*maxMin4VectorFloat32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinVectorFloat32)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinVectorFloat32Size
}

func (*maxMin4VectorFloat32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	p.isNull = true
}

func (e *maxMin4VectorFloat32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendVectorFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4VectorFloat32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalVectorFloat32(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Clone()
			memDelta += int64(input.EstimatedMemUsage())
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			oldMem := p.val.EstimatedMemUsage()
			newMem := input.EstimatedMemUsage()
			memDelta += int64(newMem - oldMem)
			p.val = input.Clone()
		}
	}
	return memDelta, nil
}

func (e *maxMin4VectorFloat32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinVectorFloat32)(src), (*partialResult4MaxMinVectorFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return 0, nil
}

type maxMin4Enum struct {
	baseMaxMinAggFunc
}

func (*maxMin4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinEnum)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinEnumSize
}

func (*maxMin4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinEnum)(pr)
	p.isNull = true
}

func (e *maxMin4Enum) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinEnum)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Enum) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinEnum)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlEnum().Copy()
			memDelta += int64(len(d.GetMysqlEnum().Name))
			p.isNull = false
			continue
		}
		en := d.GetMysqlEnum()
		if e.isMax && e.collator.Compare(en.Name, p.val.Name) > 0 || !e.isMax && e.collator.Compare(en.Name, p.val.Name) < 0 {
			oldMem := len(p.val.Name)
			newMem := len(en.Name)
			memDelta += int64(newMem - oldMem)
			p.val = en.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Enum) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinEnum)(src), (*partialResult4MaxMinEnum)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) > 0 || !e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Enum) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinEnum)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinEnum(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Enum) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Enum) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinEnum)(pr)
	success := helper.deserializePartialResult4MaxMinEnum(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Set struct {
	baseMaxMinAggFunc
}

func (*maxMin4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinSet)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinSetSize
}

func (*maxMin4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinSet)(pr)
	p.isNull = true
}

func (e *maxMin4Set) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinSet)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Set) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinSet)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlSet().Copy()
			memDelta += int64(len(d.GetMysqlSet().Name))
			p.isNull = false
			continue
		}
		s := d.GetMysqlSet()
		if e.isMax && e.collator.Compare(s.Name, p.val.Name) > 0 || !e.isMax && e.collator.Compare(s.Name, p.val.Name) < 0 {
			oldMem := len(p.val.Name)
			newMem := len(s.Name)
			memDelta += int64(newMem - oldMem)
			p.val = s.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Set) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinSet)(src), (*partialResult4MaxMinSet)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) > 0 || !e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Set) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinSet)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinSet(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Set) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Set) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinSet)(pr)
	success := helper.deserializePartialResult4MaxMinSet(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
