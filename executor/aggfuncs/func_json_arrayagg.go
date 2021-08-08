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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs

import (
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	// DefPartialResult4JsonArrayagg is the size of partialResult4JsonArrayagg
	DefPartialResult4JsonArrayagg = int64(unsafe.Sizeof(partialResult4JsonArrayagg{}))
)

type jsonArrayagg struct {
	baseAggFunc
}

type partialResult4JsonArrayagg struct {
	entries []interface{}
}

func (e *jsonArrayagg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4JsonArrayagg{}
	p.entries = make([]interface{}, 0)
	return PartialResult(&p), DefPartialResult4JsonArrayagg + DefSliceSize
}

func (e *jsonArrayagg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonArrayagg)(pr)
	p.entries = p.entries[:0]
}

func (e *jsonArrayagg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonArrayagg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	// appendBinary does not support some type such as uint8、types.time，so convert is needed here
	for idx, val := range p.entries {
		switch x := val.(type) {
		case *types.MyDecimal:
			float64Val, err := x.ToFloat64()
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[idx] = float64Val
		case []uint8, types.Time, types.Duration:
			strVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[idx] = strVal
		}
	}

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *jsonArrayagg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4JsonArrayagg)(pr)
	for _, row := range rowsInGroup {
		item, err := e.args[0].Eval(row)
		if err != nil {
			return 0, errors.Trace(err)
		}

		realItem := item.Clone().GetValue()
		switch x := realItem.(type) {
		case nil, bool, int64, uint64, float64, string, json.BinaryJSON, *types.MyDecimal, []uint8, types.Time, types.Duration:
			p.entries = append(p.entries, realItem)
			memDelta += getValMemDelta(realItem)
		default:
			return 0, json.ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)
		}
	}
	return memDelta, nil
}

func (e *jsonArrayagg) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4JsonArrayagg)(src), (*partialResult4JsonArrayagg)(dst)
	p2.entries = append(p2.entries, p1.entries...)
	return 0, nil
}
