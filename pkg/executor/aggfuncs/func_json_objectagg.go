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
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
)

const (
	// DefPartialResult4JsonObjectAgg is the size of partialResult4JsonObject
	DefPartialResult4JsonObjectAgg = int64(unsafe.Sizeof(partialResult4JsonObjectAgg{}))
)

type jsonObjectAgg struct {
	baseAggFunc
}

type partialResult4JsonObjectAgg struct {
	entries map[string]any
	bInMap  int // indicate there are 2^bInMap buckets in entries.
}

func (*jsonObjectAgg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4JsonObjectAgg{}
	p.entries = make(map[string]any)
	p.bInMap = 0
	return PartialResult(&p), DefPartialResult4JsonObjectAgg + (1<<p.bInMap)*hack.DefBucketMemoryUsageForMapStringToAny
}

func (*jsonObjectAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonObjectAgg)(pr)
	p.entries = make(map[string]any)
	p.bInMap = 0
}

func (e *jsonObjectAgg) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	bj, err := types.CreateBinaryJSONWithCheck(p.entries)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendJSON(e.ordinal, bj)
	return nil
}

func (e *jsonObjectAgg) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4JsonObjectAgg)(pr)
	for _, row := range rowsInGroup {
		key, keyIsNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return 0, errors.Trace(err)
		}

		if keyIsNull {
			return 0, types.ErrJSONDocumentNULLKey
		}

		if e.args[0].GetType(sctx).GetCharset() == charset.CharsetBin {
			return 0, types.ErrInvalidJSONCharset.GenWithStackByArgs(e.args[0].GetType(sctx).GetCharset())
		}

		key = strings.Clone(key)
		value, err := e.args[1].Eval(sctx, row)
		if err != nil {
			return 0, errors.Trace(err)
		}

		realVal, err := getRealJSONValue(value, e.args[1].GetType(sctx))
		if err != nil {
			return 0, errors.Trace(err)
		}

		switch x := realVal.(type) {
		case nil, bool, int64, uint64, float64, string, types.BinaryJSON, types.Opaque, types.Time, types.Duration:
			if _, ok := p.entries[key]; !ok {
				memDelta += int64(len(key)) + getValMemDelta(realVal)
				if len(p.entries)+1 > (1<<p.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
					memDelta += (1 << p.bInMap) * hack.DefBucketMemoryUsageForMapStringToAny
					p.bInMap++
				}
			}
			p.entries[key] = realVal

		default:
			return 0, types.ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)
		}
	}
	return memDelta, nil
}

func (e *jsonObjectAgg) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4JsonObjectAgg)(partialResult)
	resBuf := spillHelper.serializePartialResult4JsonObjectAgg(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *jsonObjectAgg) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *jsonObjectAgg) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4JsonObjectAgg)(pr)
	success, deserializeMemDelta := helper.deserializePartialResult4JsonObjectAgg(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta + deserializeMemDelta
}

func getRealJSONValue(value types.Datum, ft *types.FieldType) (any, error) {
	realVal := value.Clone().GetValue()
	switch value.Kind() {
	case types.KindBinaryLiteral, types.KindMysqlBit, types.KindBytes:
		buf := value.GetBytes()
		realVal = types.Opaque{
			TypeCode: ft.GetType(),
			Buf:      buf,
		}
	case types.KindString:
		if ft.GetCharset() == charset.CharsetBin {
			buf := value.GetBytes()
			resultBuf := buf
			if ft.GetType() == mysql.TypeString {
				// the tailing zero should also be in the opaque json
				resultBuf = make([]byte, ft.GetFlen())
				copy(resultBuf, buf)
			}
			realVal = types.Opaque{
				TypeCode: ft.GetType(),
				Buf:      resultBuf,
			}
		}
	}

	// appendBinary does not support some type such as uint8、types.time，so convert is needed here
	switch x := realVal.(type) {
	case float32:
		realVal = float64(x)
	case *types.MyDecimal:
		float64Val, err := x.ToFloat64()
		if err != nil {
			return nil, errors.Trace(err)
		}
		realVal = float64Val
	case []uint8:
		strVal, err := types.ToString(x)
		if err != nil {
			return nil, errors.Trace(err)
		}
		realVal = strVal
	}

	return realVal, nil
}

func getValMemDelta(val any) (memDelta int64) {
	memDelta = DefInterfaceSize
	switch v := val.(type) {
	case bool:
		memDelta += DefBoolSize
	case int64:
		memDelta += DefInt64Size
	case uint64:
		memDelta += DefUint64Size
	case float64:
		memDelta += DefFloat64Size
	case string:
		memDelta += int64(len(v))
	case types.BinaryJSON:
		// +1 for the memory usage of the JSONTypeCode of json
		memDelta += int64(len(v.Value) + 1)
	case types.Opaque:
		// +1 for the memory usage of the JSONTypeCode of opaque value
		memDelta += int64(len(v.Buf) + 1)
	case *types.MyDecimal:
		memDelta += DefMyDecimalSize
	case []uint8:
		memDelta += int64(len(v))
	case types.Time:
		memDelta += DefTimeSize
	case types.Duration:
		memDelta += DefDurationSize
	}
	return memDelta
}

func (*jsonObjectAgg) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4JsonObjectAgg)(src), (*partialResult4JsonObjectAgg)(dst)
	// When the result of this function is normalized, values having duplicate keys are discarded,
	// and only the last value encountered is used with that key in the returned object
	for k, v := range p1.entries {
		p2.entries[k] = v
		memDelta += int64(len(k)) + getValMemDelta(v)
		if len(p2.entries)+1 > (1<<p2.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			memDelta += (1 << p2.bInMap) * hack.DefBucketMemoryUsageForMapStringToAny
			p2.bInMap++
		}
	}
	return memDelta, nil
}
