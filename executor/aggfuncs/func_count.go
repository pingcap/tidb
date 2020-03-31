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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4Count))
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Decimal struct {
	baseCount
}

func (e *countOriginal4Decimal) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Time struct {
	baseCount
}

func (e *countOriginal4Time) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4Duration struct {
	baseCount
}

func (e *countOriginal4Duration) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4JSON struct {
	baseCount
}

func (e *countOriginal4JSON) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)

	for _, row := range rowsInGroup {
		_, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p++
	}

	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return err
		}
		if isNull {
			continue
		}

		*p += input
	}
	return nil
}

func (*countPartial) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return nil
}
<<<<<<< HEAD

type countOriginalWithDistinct struct {
	baseCount
}

type partialResult4CountWithDistinct struct {
	count int64

	valSet set.StringSet
}

func (e *countOriginalWithDistinct) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4CountWithDistinct{
		count:  0,
		valSet: set.NewStringSet(),
	})
}

func (e *countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountWithDistinct)(pr)
	p.count = 0
	p.valSet = set.NewStringSet()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.ordinal, p.count)
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4CountWithDistinct)(pr)

	encodedBytes := make([]byte, 0)
	// Decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		var hasNull, isNull bool
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.args) && !hasNull; i++ {
			encodedBytes, isNull, err = e.evalAndEncode(sctx, e.args[i], row, buf, encodedBytes)
			if err != nil {
				return
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
		p.valSet.Insert(encodedString)
		p.count++
	}

	return nil
}

// evalAndEncode eval one row with an expression and encode value to bytes.
func (e *countOriginalWithDistinct) evalAndEncode(
	sctx sessionctx.Context, arg expression.Expression,
	row chunk.Row, buf, encodedBytes []byte,
) (_ []byte, isNull bool, err error) {
	switch tp := arg.GetType().EvalType(); tp {
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
		var val json.BinaryJSON
		val, isNull, err = arg.EvalJSON(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = appendJSON(encodedBytes, buf, val)
	case types.ETString:
		var val string
		val, isNull, err = arg.EvalString(sctx, row)
		if err != nil || isNull {
			break
		}
		encodedBytes = codec.EncodeBytes(encodedBytes, hack.Slice(val))
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

func writeTime(buf []byte, t types.Time) {
	binary.BigEndian.PutUint16(buf, uint16(t.Time.Year()))
	buf[2] = uint8(t.Time.Month())
	buf[3] = uint8(t.Time.Day())
	buf[4] = uint8(t.Time.Hour())
	buf[5] = uint8(t.Time.Minute())
	buf[6] = uint8(t.Time.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Time.Microsecond()))
	buf[12] = t.Type
	buf[13] = uint8(t.Fsp)
}

func appendTime(encodedBytes, buf []byte, val types.Time) []byte {
	writeTime(buf, val)
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

func appendJSON(encodedBytes, _ []byte, val json.BinaryJSON) []byte {
	encodedBytes = append(encodedBytes, val.TypeCode)
	encodedBytes = append(encodedBytes, val.Value...)
	return encodedBytes
}

func appendString(encodedBytes, _ []byte, val string) []byte {
	encodedBytes = append(encodedBytes, val...)
	return encodedBytes
}
=======
>>>>>>> 5192c26... executor: optimize count distinct with single column (#15323)
