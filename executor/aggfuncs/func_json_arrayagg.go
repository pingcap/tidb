// Copyright 2021 PingCAP, Inc.
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
	// DefPartialResult4JsonArrayAgg is the size of partialResult4JsonArrayAgg
	DefPartialResult4JsonArrayAgg = int64(unsafe.Sizeof(partialResult4JsonArrayAgg{}))
)

type jsonArrayAgg struct {
	baseAggFunc
}

type partialResult4JsonArrayAgg struct {
	entries []interface{}
}

func (e *jsonArrayAgg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4JsonArrayAgg{}
	p.entries = make([]interface{}, 0)
	return PartialResult(&p), DefPartialResult4JsonArrayAgg
}

func (e *jsonArrayAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonArrayAgg)(pr)
	p.entries = make([]interface{}, 0)
}

func (e *jsonArrayAgg) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonArrayAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	for i, val := range p.entries {
		switch x := val.(type) {
		case *types.MyDecimal:
			float64Val, err := x.ToFloat64()
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[i] = float64Val
		case []uint8, types.Time, types.Duration:
			strVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[i] = strVal
		}
	}

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *jsonArrayAgg) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4JsonArrayAgg)(pr)
	for _, row := range rowsInGroup {
		value, err := e.args[0].Eval(row)
		if err != nil {
			return 0, errors.Trace(err)
		}

		realVal := value.Clone().GetValue()
		switch x := realVal.(type) {
		case nil, bool, int64, uint64, float64, string, json.BinaryJSON, *types.MyDecimal, []uint8, types.Time, types.Duration:
			memDelta += getValMemDelta(realVal)
			p.entries = append(p.entries, realVal)
		default:
			return 0, json.ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)
		}
	}

	return memDelta, nil
}

func (e *jsonArrayAgg) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4JsonArrayAgg)(src), (*partialResult4JsonArrayAgg)(dst)
	for _, v := range p1.entries {
		memDelta += getValMemDelta(v)
		p2.entries = append(p2.entries, v)
	}
	return 0, nil
}
