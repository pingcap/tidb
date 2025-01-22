// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// DefPartialResult4JsonArrayagg is the size of partialResult4JsonArrayagg
	DefPartialResult4JsonArrayagg = int64(unsafe.Sizeof(partialResult4JsonArrayagg{}))
)

type jsonArrayagg struct {
	baseAggFunc
}

type partialResult4JsonArrayagg struct {
	entries []any
}

func (*jsonArrayagg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4JsonArrayagg{}
	p.entries = make([]any, 0)
	return PartialResult(&p), DefPartialResult4JsonArrayagg + DefSliceSize
}

func (*jsonArrayagg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonArrayagg)(pr)
	p.entries = p.entries[:0]
}

func (e *jsonArrayagg) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonArrayagg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	json, err := types.CreateBinaryJSONWithCheck(p.entries)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendJSON(e.ordinal, json)
	return nil
}

func (e *jsonArrayagg) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4JsonArrayagg)(pr)
	for _, row := range rowsInGroup {
		item, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return 0, errors.Trace(err)
		}

		realItem, err := getRealJSONValue(item, e.args[0].GetType(ctx))
		if err != nil {
			return 0, errors.Trace(err)
		}

		switch x := realItem.(type) {
		case nil, bool, int64, uint64, float64, string, types.BinaryJSON, types.Opaque, types.Time, types.Duration:
			p.entries = append(p.entries, realItem)
			memDelta += getValMemDelta(realItem)
		default:
			return 0, types.ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)
		}
	}
	return memDelta, nil
}

func (*jsonArrayagg) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4JsonArrayagg)(src), (*partialResult4JsonArrayagg)(dst)
	p2.entries = append(p2.entries, p1.entries...)
	return 0, nil
}

func (e *jsonArrayagg) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4JsonArrayagg)(partialResult)
	resBuf := spillHelper.serializePartialResult4JsonArrayagg(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *jsonArrayagg) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *jsonArrayagg) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4JsonArrayagg)(pr)
	success := helper.deserializePartialResult4JsonArrayagg(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
