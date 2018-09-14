package aggfuncs

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
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
	chk.AppendInt64(e.resultOrdinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
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
		if row.IsNull(e.argsOrdinal[0]) {
			continue
		}
		input := row.GetInt64(e.argsOrdinal[0])
		*p += input
	}
	return nil
}

func (*countPartial) MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) error {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return nil
}

type countOriginalWithDistinct struct {
	baseCount

	// This attribute can be removed after splitting countOriginalWithDistinct
	// into multiple struct according to the argument field types.
	argsTypes []*types.FieldType
}

type partialResult4CountWithDistinct struct {
	count int64

	valSet stringSet
}

func (e *countOriginalWithDistinct) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4CountWithDistinct{
		count:  0,
		valSet: newStringSet(),
	})
}

func (e *countOriginalWithDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CountWithDistinct)(pr)
	p.count = 0
	p.valSet = newStringSet()
}

func (e *countOriginalWithDistinct) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CountWithDistinct)(pr)
	chk.AppendInt64(e.resultOrdinal, p.count)
	return nil
}

func (e *countOriginalWithDistinct) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (err error) {
	p := (*partialResult4CountWithDistinct)(pr)

	hasNull, isNull := false, false
	encodedBytes := make([]byte, 0)
	// Decimal struct is the biggest type we will use.
	buf := make([]byte, types.MyDecimalStructSize)

	for _, row := range rowsInGroup {
		hasNull, isNull = false, false
		encodedBytes = encodedBytes[:0]

		for i := 0; i < len(e.argsOrdinal) && !hasNull; i++ {
			encodedBytes, isNull = e.evalAndEncode(sctx, e.argsOrdinal[i], e.argsTypes[i], row, buf, encodedBytes)
			if isNull {
				hasNull = true
				break
			}
		}
		encodedString := string(encodedBytes)
		if hasNull || p.valSet.exist(encodedString) {
			continue
		}
		p.valSet.insert(encodedString)
		p.count++
	}

	return nil
}

// evalAndEncode evaluates one row with an argument ordinal and encodes the
// result value to bytes.
func (e *countOriginalWithDistinct) evalAndEncode(
	sctx sessionctx.Context, argOrdinal int, tp *types.FieldType,
	row chunk.Row, buf, encodedBytes []byte,
) ([]byte, bool) {
	if row.IsNull(argOrdinal) {
		return encodedBytes, true
	}
	switch evalType := tp.EvalType(); evalType {
	case types.ETInt:
		encodedBytes = appendInt64(encodedBytes, buf, row.GetInt64(argOrdinal))
	case types.ETReal:
		var f float64
		if tp.Tp == mysql.TypeFloat {
			f = float64(row.GetFloat32(argOrdinal))
		} else {
			f = row.GetFloat64(argOrdinal)
		}
		encodedBytes = appendFloat64(encodedBytes, buf, f)
	case types.ETDecimal:
		encodedBytes = appendDecimal(encodedBytes, buf, row.GetMyDecimal(argOrdinal))
	case types.ETTimestamp, types.ETDatetime:
		encodedBytes = appendTime(encodedBytes, buf, row.GetTime(argOrdinal))
	case types.ETDuration:
		encodedBytes = appendDuration(encodedBytes, buf, row.GetDuration(argOrdinal, tp.Decimal))
	case types.ETJson:
		encodedBytes = appendJSON(encodedBytes, buf, row.GetJSON(argOrdinal))
	case types.ETString:
		encodedBytes = appendString(encodedBytes, buf, row.GetString(argOrdinal))
	}
	return encodedBytes, false
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

func appendDecimal(encodedBytes, buf []byte, val *types.MyDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&buf[0])) = *val
	buf = buf[:types.MyDecimalStructSize]
	encodedBytes = append(encodedBytes, buf...)
	return encodedBytes
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
