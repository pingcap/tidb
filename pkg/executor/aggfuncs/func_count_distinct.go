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
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

const (
	// DefPartialResult4CountDistinctIntSize is the size of partialResult4CountDistinctInt
	DefPartialResult4CountDistinctIntSize = int64(unsafe.Sizeof(partialResult4CountDistinctInt{}))
	// DefPartialResult4CountDistinctRealSize is the size of partialResult4CountDistinctReal
	DefPartialResult4CountDistinctRealSize = int64(unsafe.Sizeof(partialResult4CountDistinctReal{}))
	// DefPartialResult4CountDistinctDecimalSize is the size of partialResult4CountDistinctDecimal
	DefPartialResult4CountDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4CountDistinctDecimal{}))
	// DefPartialResult4CountDistinctDurationSize is the size of partialResult4CountDistinctDuration
	DefPartialResult4CountDistinctDurationSize = int64(unsafe.Sizeof(partialResult4CountDistinctDuration{}))
	// DefPartialResult4CountDistinctStringSize is the size of partialResult4CountDistinctString
	DefPartialResult4CountDistinctStringSize = int64(unsafe.Sizeof(partialResult4CountDistinctString{}))
	// DefPartialResult4CountWithDistinctSize is the size of partialResult4CountWithDistinct
	DefPartialResult4CountWithDistinctSize = int64(unsafe.Sizeof(partialResult4CountWithDistinct{}))
	// DefPartialResult4ApproxCountDistinctSize is the size of partialResult4ApproxCountDistinct
	DefPartialResult4ApproxCountDistinctSize = int64(unsafe.Sizeof(partialResult4ApproxCountDistinct{}))
)

type partialResult4CountDistinctInt struct {
	valSet set.Int64SetWithMemoryUsage
}

type countOriginalWithDistinct4Int struct {
	baseCount
}

func (*countOriginalWithDistinct4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewInt64SetWithMemoryUsage()
	return PartialResult(&partialResult4CountDistinctInt{
		valSet: valSet,
	}), DefPartialResult4CountDistinctIntSize + setSize
}

func (*countOriginalWithDistinct4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctInt)(pr)
	p.valSet, _ = set.NewInt64SetWithMemoryUsage()
}

func (e *countOriginalWithDistinct4Int) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctInt)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctInt)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.valSet.Exist(input) {
			continue
		}
		memDelta += p.valSet.Insert(input)
	}

	return memDelta, nil
}

type partialResult4CountDistinctReal struct {
	valSet set.Float64SetWithMemoryUsage
}

type countOriginalWithDistinct4Real struct {
	baseCount
}

func (*countOriginalWithDistinct4Real) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewFloat64SetWithMemoryUsage()
	return PartialResult(&partialResult4CountDistinctReal{
		valSet: valSet,
	}), DefPartialResult4CountDistinctRealSize + setSize
}

func (*countOriginalWithDistinct4Real) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctReal)(pr)
	p.valSet, _ = set.NewFloat64SetWithMemoryUsage()
}

func (e *countOriginalWithDistinct4Real) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctReal)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Real) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctReal)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.valSet.Exist(input) {
			continue
		}
		memDelta += p.valSet.Insert(input)
	}

	return memDelta, nil
}

type partialResult4CountDistinctDecimal struct {
	valSet set.StringSetWithMemoryUsage
}

type countOriginalWithDistinct4Decimal struct {
	baseCount
}

func (*countOriginalWithDistinct4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewStringSetWithMemoryUsage()
	return PartialResult(&partialResult4CountDistinctDecimal{
		valSet: valSet,
	}), DefPartialResult4CountDistinctDecimalSize + setSize
}

func (*countOriginalWithDistinct4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctDecimal)(pr)
	p.valSet, _ = set.NewStringSetWithMemoryUsage()
}

func (e *countOriginalWithDistinct4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctDecimal)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctDecimal)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		hash, err := input.ToHashKey()
		if err != nil {
			return memDelta, err
		}
		decStr := string(hack.String(hash))
		if p.valSet.Exist(decStr) {
			continue
		}
		memDelta += p.valSet.Insert(decStr)
		memDelta += int64(len(decStr))
	}

	return memDelta, nil
}

type partialResult4CountDistinctDuration struct {
	valSet set.Int64SetWithMemoryUsage
}

type countOriginalWithDistinct4Duration struct {
	baseCount
}

func (*countOriginalWithDistinct4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewInt64SetWithMemoryUsage()
	return PartialResult(&partialResult4CountDistinctDuration{
		valSet: valSet,
	}), DefPartialResult4CountDistinctDurationSize + setSize
}

func (*countOriginalWithDistinct4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctDuration)(pr)
	p.valSet, _ = set.NewInt64SetWithMemoryUsage()
}

func (e *countOriginalWithDistinct4Duration) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctDuration)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctDuration)(pr)

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}

		if p.valSet.Exist(int64(input.Duration)) {
			continue
		}
		memDelta += p.valSet.Insert(int64(input.Duration))
	}

	return memDelta, nil
}

type partialResult4CountDistinctString struct {
	valSet set.StringSetWithMemoryUsage
}

type countOriginalWithDistinct4String struct {
	baseCount
}

func (*countOriginalWithDistinct4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewStringSetWithMemoryUsage()
	return PartialResult(&partialResult4CountDistinctString{
		valSet: valSet,
	}), DefPartialResult4CountDistinctStringSize + setSize
}

func (*countOriginalWithDistinct4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountDistinctString)(pr)
	p.valSet, _ = set.NewStringSetWithMemoryUsage()
}

func (e *countOriginalWithDistinct4String) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountDistinctString)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountDistinctString)(pr)
	collator := collate.GetCollator(e.args[0].GetType(sctx).GetCollate())

	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		input = string(collator.Key(input))

		if p.valSet.Exist(input) {
			continue
		}
		input = stringutil.Copy(input)
		memDelta += p.valSet.Insert(input)
		memDelta += int64(len(input))
	}

	return memDelta, nil
}

type countOriginalWithDistinct struct {
	baseCount
}

type partialResult4CountWithDistinct struct {
	valSet set.StringSetWithMemoryUsage
}

func (*countOriginalWithDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	valSet, setSize := set.NewStringSetWithMemoryUsage()
	return PartialResult(&partialResult4CountWithDistinct{
		valSet: valSet,
	}), DefPartialResult4CountWithDistinctSize + setSize
}

func (*countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountWithDistinct)(pr)
	p.valSet, _ = set.NewStringSetWithMemoryUsage()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.ordinal, int64(p.valSet.Count()))
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CountWithDistinct)(pr)

	encodedBytes := make([]byte, 0)
	collators := make([]collate.Collator, 0, len(e.args))
	for _, arg := range e.args {
		collators = append(collators, collate.GetCollator(arg.GetType(sctx).GetCollate()))
	}
	// decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		var err error
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			encodedBytes, isNull, err = evalAndEncode(sctx, e.args[i], collators[i], row, buf, encodedBytes)
			if err != nil {
				return memDelta, err
			}
			if isNull {
				hasNull = true
				break
			}
		}
		encodedString := string(encodedBytes)
		if hasNull || p.valSet.Exist(encodedString) {
			continue
		}
		memDelta += p.valSet.Insert(encodedString)
		memDelta += int64(len(encodedString))
	}

	return memDelta, nil
}

// evalAndEncode eval one row with an expression and encode value to bytes.
func evalAndEncode(
	sctx expression.EvalContext, arg expression.Expression, collator collate.Collator,
	row chunk.Row, buf, encodedBytes []byte,
) (_ []byte, isNull bool, err error) {
	switch tp := arg.GetType(sctx).EvalType(); tp {
	case types.ETInt:
		var val int64
		val, isNull, err = arg.EvalInt(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendInt64(encodedBytes, buf, val)
	case types.ETReal:
		var val float64
		val, isNull, err = arg.EvalReal(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendFloat64(encodedBytes, buf, val)
	case types.ETDecimal:
		var val *types.MyDecimal
		val, isNull, err = arg.EvalDecimal(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes, err = appendDecimal(encodedBytes, val)
	case types.ETTimestamp, types.ETDatetime:
		var val types.Time
		val, isNull, err = arg.EvalTime(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendTime(encodedBytes, buf, val)
	case types.ETDuration:
		var val types.Duration
		val, isNull, err = arg.EvalDuration(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendDuration(encodedBytes, buf, val)
	case types.ETJson:
		var val types.BinaryJSON
		val, isNull, err = arg.EvalJSON(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = val.HashValue(encodedBytes)
	case types.ETVectorFloat32:
		var val types.VectorFloat32
		val, isNull, err = arg.EvalVectorFloat32(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = val.SerializeTo(encodedBytes)
	case types.ETString:
		var val string
		val, isNull, err = arg.EvalString(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = codec.EncodeCompactBytes(encodedBytes, collator.ImmutableKey(val))
	default:
		return nil, false, errors.Errorf("unsupported column type for encode %d", tp)
	}
	return encodedBytes, isNull, err
}

func appendInt64(encodedBytes, buf []byte, val int64) []byte {
	*(*int64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendFloat64(encodedBytes, buf []byte, val float64) []byte {
	*(*float64)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:8]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDecimal(encodedBytes []byte, val *types.MyDecimal) ([]byte, error) {
	hash, err := val.ToHashKey()
	encodedBytes = append(encodedBytes, hash...)
	return encodedBytes, err
}

// WriteTime writes `t` into `buf`.
func WriteTime(buf []byte, t types.Time) {
	binary.BigEndian.PutUint16(buf, uint16(t.Year()))
	buf[2] = uint8(t.Month())
	buf[3] = uint8(t.Day())
	buf[4] = uint8(t.Hour())
	buf[5] = uint8(t.Minute())
	buf[6] = uint8(t.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Microsecond()))
	buf[12] = t.Type()
	buf[13] = uint8(t.Fsp())

	buf[7], buf[14], buf[15] = uint8(0), uint8(0), uint8(0)
}

func appendTime(encodedBytes, buf []byte, val types.Time) []byte {
	WriteTime(buf, val)
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

func appendDuration(encodedBytes, buf []byte, val types.Duration) []byte {
	*(*types.Duration)(unsafe.Pointer(&buf[0])) = val
	buf = buf[:16]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
}

