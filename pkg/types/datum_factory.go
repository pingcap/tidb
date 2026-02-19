// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in any) (d Datum) {
	switch x := in.(type) {
	case []any:
		d.SetValueWithDefaultCollation(MakeDatums(x...))
	default:
		d.SetValueWithDefaultCollation(in)
	}
	return d
}

// NewIntDatum creates a new Datum from an int64 value.
func NewIntDatum(i int64) (d Datum) {
	d.SetInt64(i)
	return d
}

// NewUintDatum creates a new Datum from an uint64 value.
func NewUintDatum(i uint64) (d Datum) {
	d.SetUint64(i)
	return d
}

// NewBytesDatum creates a new Datum from a byte slice.
func NewBytesDatum(b []byte) (d Datum) {
	d.SetBytes(b)
	return d
}

// NewStringDatum creates a new Datum from a string.
func NewStringDatum(s string) (d Datum) {
	d.SetString(s, mysql.DefaultCollationName)
	return d
}

// NewCollationStringDatum creates a new Datum from a string with collation.
func NewCollationStringDatum(s string, collation string) (d Datum) {
	d.SetString(s, collation)
	return d
}

// NewFloat64Datum creates a new Datum from a float64 value.
func NewFloat64Datum(f float64) (d Datum) {
	d.SetFloat64(f)
	return d
}

// NewFloat32Datum creates a new Datum from a float32 value.
func NewFloat32Datum(f float32) (d Datum) {
	d.SetFloat32(f)
	return d
}

// NewDurationDatum creates a new Datum from a Duration value.
func NewDurationDatum(dur Duration) (d Datum) {
	d.SetMysqlDuration(dur)
	return d
}

// NewTimeDatum creates a new Time from a Time value.
func NewTimeDatum(t Time) (d Datum) {
	d.SetMysqlTime(t)
	return d
}

// NewDecimalDatum creates a new Datum from a MyDecimal value.
func NewDecimalDatum(dec *MyDecimal) (d Datum) {
	d.SetMysqlDecimal(dec)
	return d
}

// NewJSONDatum creates a new Datum from a BinaryJSON value
func NewJSONDatum(j BinaryJSON) (d Datum) {
	d.SetMysqlJSON(j)
	return d
}

// NewVectorFloat32Datum creates a new Datum from a VectorFloat32 value
func NewVectorFloat32Datum(v VectorFloat32) (d Datum) {
	d.SetVectorFloat32(v)
	return d
}

// NewBinaryLiteralDatum creates a new BinaryLiteral Datum for a BinaryLiteral value.
func NewBinaryLiteralDatum(b BinaryLiteral) (d Datum) {
	d.SetBinaryLiteral(b)
	return d
}

// NewMysqlBitDatum creates a new MysqlBit Datum for a BinaryLiteral value.
func NewMysqlBitDatum(b BinaryLiteral) (d Datum) {
	d.SetMysqlBit(b)
	return d
}

// NewMysqlEnumDatum creates a new MysqlEnum Datum for a Enum value.
func NewMysqlEnumDatum(e Enum) (d Datum) {
	d.SetMysqlEnum(e, mysql.DefaultCollationName)
	return d
}

// NewCollateMysqlEnumDatum create a new MysqlEnum Datum for a Enum value with collation information.
func NewCollateMysqlEnumDatum(e Enum, collation string) (d Datum) {
	d.SetMysqlEnum(e, collation)
	return d
}

// NewMysqlSetDatum creates a new MysqlSet Datum for a Enum value.
func NewMysqlSetDatum(e Set, collation string) (d Datum) {
	d.SetMysqlSet(e, collation)
	return d
}

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...any) []Datum {
	datums := make([]Datum, len(args))
	for i, v := range args {
		datums[i] = NewDatum(v)
	}
	return datums
}

// MinNotNullDatum returns a datum represents minimum not null value.
func MinNotNullDatum() Datum {
	return Datum{k: KindMinNotNull}
}

// MaxValueDatum returns a datum represents max value.
func MaxValueDatum() Datum {
	return Datum{k: KindMaxValue}
}

// SortDatums sorts a slice of datum.
func SortDatums(ctx Context, datums []Datum) error {
	var err error
	slices.SortFunc(datums, func(a, b Datum) int {
		var cmp int
		cmp, err = a.Compare(ctx, &b, collate.GetCollator(b.Collation()))
		if err != nil {
			return 0
		}
		return cmp
	})
	if err != nil {
		err = errors.Trace(err)
	}
	return err
}

// Check if a string is considered printable
//
// Checks
// 1. Must be valid UTF-8
// 2. Must not contain control characters like NUL (0x0) and backspace (0x8)
func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// DatumsToString converts several datums to formatted string.
func DatumsToString(datums []Datum, handleSpecialValue bool) (string, error) {
	return datumsToString(datums, handleSpecialValue, false)
}

// DatumsToStringSmart is like DatumsToString, but with smart detection of non-printable data
func DatumsToStringSmart(datums []Datum, handleSpecialValue bool) (string, error) {
	return datumsToString(datums, handleSpecialValue, true)
}

func datumsToString(datums []Datum, handleSpecialValue bool, binaryAsHex bool) (string, error) {
	n := len(datums)
	builder := &strings.Builder{}
	builder.Grow(8 * n)
	if n > 1 {
		builder.WriteString("(")
	}
	for i, datum := range datums {
		if i > 0 {
			builder.WriteString(", ")
		}
		if handleSpecialValue {
			switch datum.Kind() {
			case KindNull:
				builder.WriteString("NULL")
				continue
			case KindMinNotNull:
				builder.WriteString("-inf")
				continue
			case KindMaxValue:
				builder.WriteString("+inf")
				continue
			}
		}
		str, err := datum.ToString()
		if err != nil {
			return "", errors.Trace(err)
		}
		const logDatumLen = 2048
		originalLen := -1
		if len(str) > logDatumLen {
			originalLen = len(str)
			str = str[:logDatumLen]
		}
		if datum.Kind() == KindString {
			if !binaryAsHex || isPrintable(str) {
				builder.WriteString(`"`)
				builder.WriteString(str)
				builder.WriteString(`"`)
			} else {
				// Print as hex-literal instead
				fmt.Fprintf(builder, "0x%X", str)
			}
		} else {
			builder.WriteString(str)
		}
		if originalLen != -1 {
			builder.WriteString(" len(")
			builder.WriteString(strconv.Itoa(originalLen))
			builder.WriteString(")")
		}
	}
	if n > 1 {
		builder.WriteString(")")
	}
	return builder.String(), nil
}

// DatumsToStrNoErr converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
func DatumsToStrNoErr(datums []Datum) string {
	str, err := DatumsToString(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// DatumsToStrNoErrSmart converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
// It also enables detection of non-pritable arguments
func DatumsToStrNoErrSmart(datums []Datum) string {
	str, err := DatumsToStringSmart(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// CloneRow deep copies a Datum slice.
func CloneRow(dr []Datum) []Datum {
	c := make([]Datum, len(dr))
	for i, d := range dr {
		d.Copy(&c[i])
	}
	return c
}

// GetMaxValue returns the max value datum for each type.
func GetMaxValue(ft *FieldType) (maxVal Datum) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			maxVal.SetUint64(IntegerUnsignedUpperBound(ft.GetType()))
		} else {
			maxVal.SetInt64(IntegerSignedUpperBound(ft.GetType()))
		}
	case mysql.TypeFloat:
		maxVal.SetFloat32(float32(GetMaxFloat(ft.GetFlen(), ft.GetDecimal())))
	case mysql.TypeDouble:
		maxVal.SetFloat64(GetMaxFloat(ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		// codec.Encode KindMaxValue, to avoid import circle
		bytes := []byte{250}
		maxVal.SetString(string(bytes), ft.GetCollate())
	case mysql.TypeNewDecimal:
		maxVal.SetMysqlDecimal(NewMaxOrMinDec(false, ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeDuration:
		maxVal.SetMysqlDuration(Duration{Duration: MaxTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if ft.GetType() == mysql.TypeDate || ft.GetType() == mysql.TypeDatetime {
			maxVal.SetMysqlTime(NewTime(MaxDatetime, ft.GetType(), 0))
		} else {
			maxVal.SetMysqlTime(MaxTimestamp)
		}
	}
	return
}

// GetMinValue returns the min value datum for each type.
func GetMinValue(ft *FieldType) (minVal Datum) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			minVal.SetUint64(0)
		} else {
			minVal.SetInt64(IntegerSignedLowerBound(ft.GetType()))
		}
	case mysql.TypeFloat:
		minVal.SetFloat32(float32(-GetMaxFloat(ft.GetFlen(), ft.GetDecimal())))
	case mysql.TypeDouble:
		minVal.SetFloat64(-GetMaxFloat(ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		// codec.Encode KindMinNotNull, to avoid import circle
		bytes := []byte{1}
		minVal.SetString(string(bytes), ft.GetCollate())
	case mysql.TypeNewDecimal:
		minVal.SetMysqlDecimal(NewMaxOrMinDec(true, ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeDuration:
		minVal.SetMysqlDuration(Duration{Duration: MinTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if ft.GetType() == mysql.TypeDate || ft.GetType() == mysql.TypeDatetime {
			minVal.SetMysqlTime(NewTime(MinDatetime, ft.GetType(), 0))
		} else {
			minVal.SetMysqlTime(MinTimestamp)
		}
	}
	return
}

// RoundingType is used to indicate the rounding type for reversing evaluation.
type RoundingType uint8

const (
	// Ceiling means rounding up.
	Ceiling RoundingType = iota
	// Floor means rounding down.
	Floor
)

func getDatumBound(retType *FieldType, rType RoundingType) Datum {
	if rType == Ceiling {
		return GetMaxValue(retType)
	}
	return GetMinValue(retType)
}

// ChangeReverseResultByUpperLowerBound is for expression's reverse evaluation.
// Here is an example for what's effort for the function: CastRealAsInt(t.a),
//
//			if the type of column `t.a` is mysql.TypeDouble, and there is a row that t.a == MaxFloat64
//			then the cast function will arrive a result MaxInt64. But when we do the reverse evaluation,
//	     if the result is MaxInt64, and the rounding type is ceiling. Then we should get the MaxFloat64
//	     instead of float64(MaxInt64).
//
// Another example: cast(1.1 as signed) = 1,
//
//	when we get the answer 1, we can only reversely evaluate 1.0 as the column value. So in this
//	case, we should judge whether the rounding type are ceiling. If it is, then we should plus one for
//	1.0 and get the reverse result 2.0.
func ChangeReverseResultByUpperLowerBound(
	ctx Context,
	retType *FieldType,
	res Datum,
	rType RoundingType) (Datum, error) {
	d, err := res.ConvertTo(ctx, retType)
	if terror.ErrorEqual(err, ErrOverflow) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	resRetType := FieldType{}
	switch res.Kind() {
	case KindInt64:
		resRetType.SetType(mysql.TypeLonglong)
	case KindUint64:
		resRetType.SetType(mysql.TypeLonglong)
		resRetType.AddFlag(mysql.UnsignedFlag)
	case KindFloat32:
		resRetType.SetType(mysql.TypeFloat)
	case KindFloat64:
		resRetType.SetType(mysql.TypeDouble)
	case KindMysqlDecimal:
		resRetType.SetType(mysql.TypeNewDecimal)
		resRetType.SetFlenUnderLimit(int(res.GetMysqlDecimal().GetDigitsFrac() + res.GetMysqlDecimal().GetDigitsInt()))
		resRetType.SetDecimalUnderLimit(int(res.GetMysqlDecimal().GetDigitsInt()))
	}
	bound := getDatumBound(&resRetType, rType)
	cmp, err := d.Compare(ctx, &bound, collate.GetCollator(resRetType.GetCollate()))
	if err != nil {
		return d, err
	}
	if cmp == 0 {
		d = getDatumBound(retType, rType)
	} else if rType == Ceiling {
		switch retType.GetType() {
		case mysql.TypeShort:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint16 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt16 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeLong:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint32 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt32 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeLonglong:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint64 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt64 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeFloat:
			if d.GetFloat32() != math.MaxFloat32 {
				d.SetFloat32(d.GetFloat32() + 1.0)
			}
		case mysql.TypeDouble:
			if d.GetFloat64() != math.MaxFloat64 {
				d.SetFloat64(d.GetFloat64() + 1.0)
			}
		case mysql.TypeNewDecimal:
			if d.GetMysqlDecimal().Compare(NewMaxOrMinDec(false, retType.GetFlen(), retType.GetDecimal())) != 0 {
				var decimalOne, newD MyDecimal
				one := decimalOne.FromInt(1)
				err = DecimalAdd(d.GetMysqlDecimal(), one, &newD)
				if err != nil {
					return d, err
				}
				d = NewDecimalDatum(&newD)
			}
		}
	}
	return d, nil
}

const (
	sizeOfEmptyDatum = int(unsafe.Sizeof(Datum{}))
	sizeOfMysqlTime  = int(unsafe.Sizeof(ZeroTime))
	sizeOfMyDecimal  = MyDecimalStructSize
)

// EstimatedMemUsage returns the estimated bytes consumed of a one-dimensional
// or two-dimensional datum array.
func EstimatedMemUsage(array []Datum, numOfRows int) int64 {
	if numOfRows == 0 {
		return 0
	}
	var bytesConsumed int64
	for _, d := range array {
		bytesConsumed += d.EstimatedMemUsage()
	}
	return bytesConsumed * int64(numOfRows)
}

// EstimatedMemUsage returns the estimated bytes consumed of a Datum.
func (d Datum) EstimatedMemUsage() int64 {
	bytesConsumed := sizeOfEmptyDatum
	switch d.Kind() {
	case KindMysqlDecimal:
		bytesConsumed += sizeOfMyDecimal
	case KindMysqlTime:
		bytesConsumed += sizeOfMysqlTime
	case KindVectorFloat32:
		bytesConsumed += d.GetVectorFloat32().EstimatedMemUsage()
	default:
		bytesConsumed += len(d.b)
	}
	return int64(bytesConsumed)
}

// DatumsContainNull return true if any value is null
func DatumsContainNull(vals []Datum) bool {
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}
