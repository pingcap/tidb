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
	"bytes"
	gjson "encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Kind constants.
const (
	KindNull          byte = 0
	KindInt64         byte = 1
	KindUint64        byte = 2
	KindFloat32       byte = 3
	KindFloat64       byte = 4
	KindString        byte = 5
	KindBytes         byte = 6
	KindBinaryLiteral byte = 7 // Used for BIT / HEX literals.
	KindMysqlDecimal  byte = 8
	KindMysqlDuration byte = 9
	KindMysqlEnum     byte = 10
	KindMysqlBit      byte = 11 // Used for BIT table column values.
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindInterface     byte = 14
	KindMinNotNull    byte = 15
	KindMaxValue      byte = 16
	KindRaw           byte = 17
	KindMysqlJSON     byte = 18
	KindVectorFloat32 byte = 19
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte   // datum kind.
	decimal   uint16 // decimal can hold uint16 values.
	length    uint32 // length can hold uint32 values.
	i         int64  // i can hold int64 uint64 float64 values.
	collation string // collation hold the collation information for string value.
	b         []byte // b can hold string or []byte values.
	x         any    // x hold all other types.
}

// EmptyDatumSize is the size of empty datum.
// 72 = 1 + 1 (byte) + 2 (uint16) + 4 (uint32) + 8 (int64) + 16 (string) + 24 ([]byte) + 16 (interface{})
const EmptyDatumSize = int64(unsafe.Sizeof(Datum{}))

// Clone create a deep copy of the Datum.
func (d *Datum) Clone() *Datum {
	ret := new(Datum)
	d.Copy(ret)
	return ret
}

// Copy deep copies a Datum into destination.
func (d *Datum) Copy(dst *Datum) {
	*dst = *d
	if d.b != nil {
		dst.b = make([]byte, len(d.b))
		copy(dst.b, d.b)
	}
	switch dst.Kind() {
	case KindMysqlDecimal:
		d := *d.GetMysqlDecimal()
		dst.SetMysqlDecimal(&d)
	case KindMysqlTime:
		dst.SetMysqlTime(d.GetMysqlTime())
	}
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	return d.k
}

// Collation gets the collation of the datum.
func (d *Datum) Collation() string {
	return d.collation
}

// SetCollation sets the collation of the datum.
func (d *Datum) SetCollation(collation string) {
	d.collation = collation
}

// Frac gets the frac of the datum.
func (d *Datum) Frac() int {
	return int(d.decimal)
}

// SetFrac sets the frac of the datum.
func (d *Datum) SetFrac(frac int) {
	d.decimal = uint16(frac)
}

// Length gets the length of the datum.
func (d *Datum) Length() int {
	return int(d.length)
}

// SetLength sets the length of the datum.
func (d *Datum) SetLength(l int) {
	d.length = uint32(l)
}

// IsNull checks if datum is null.
func (d *Datum) IsNull() bool {
	return d.k == KindNull
}

// GetInt64 gets int64 value.
func (d *Datum) GetInt64() int64 {
	return d.i
}

// SetInt64 sets int64 value.
func (d *Datum) SetInt64(i int64) {
	d.k = KindInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Datum) GetUint64() uint64 {
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Datum) SetUint64(i uint64) {
	d.k = KindUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Datum) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Datum) SetFloat64(f float64) {
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Datum) GetFloat32() float32 {
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetFloat32 sets float32 value.
func (d *Datum) SetFloat32(f float32) {
	d.k = KindFloat32
	d.i = int64(math.Float64bits(float64(f)))
}

// SetFloat32FromF64 sets float32 values from f64
func (d *Datum) SetFloat32FromF64(f float64) {
	d.k = KindFloat32
	d.i = int64(math.Float64bits(f))
}

// GetString gets string value.
func (d *Datum) GetString() string {
	return string(hack.String(d.b))
}

// GetBinaryStringEncoded gets the string value encoded with given charset.
func (d *Datum) GetBinaryStringEncoded() string {
	coll, err := charset.GetCollationByName(d.Collation())
	if err != nil {
		logutil.BgLogger().Warn("unknown collation", zap.Error(err))
		return d.GetString()
	}
	enc := charset.FindEncodingTakeUTF8AsNoop(coll.CharsetName)
	replace, _ := enc.Transform(nil, d.GetBytes(), charset.OpEncodeNoErr)
	return string(hack.String(replace))
}

// GetBinaryStringDecoded gets the string value decoded with given charset.
func (d *Datum) GetBinaryStringDecoded(flags Flags, chs string) (string, error) {
	enc, skip := findEncoding(flags, chs)
	if skip {
		return d.GetString(), nil
	}
	trim, err := enc.Transform(nil, d.GetBytes(), charset.OpDecode)
	return string(hack.String(trim)), err
}

// GetStringWithCheck gets the string and checks if it is valid in a given charset.
func (d *Datum) GetStringWithCheck(flags Flags, chs string) (string, error) {
	enc, skip := findEncoding(flags, chs)
	if skip {
		return d.GetString(), nil
	}
	str := d.GetBytes()
	if !enc.IsValid(str) {
		replace, err := enc.Transform(nil, str, charset.OpReplace)
		return string(hack.String(replace)), err
	}
	return d.GetString(), nil
}

func findEncoding(flags Flags, chs string) (enc charset.Encoding, skip bool) {
	enc = charset.FindEncoding(chs)
	if enc.Tp() == charset.EncodingTpUTF8 && flags.SkipUTF8Check() ||
		enc.Tp() == charset.EncodingTpASCII && flags.SkipASCIICheck() {
		return nil, true
	}
	if chs == charset.CharsetUTF8 && !flags.SkipUTF8MB4Check() {
		enc = charset.EncodingUTF8MB3StrictImpl
	}
	return enc, false
}

// SetString sets string value.
func (d *Datum) SetString(s string, collation string) {
	d.k = KindString
	sink(s)
	d.b = hack.Slice(s)
	d.collation = collation
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	if d.b != nil {
		return d.b
	}
	return []byte{}
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
	d.collation = charset.CollationBin
}

// SetBytesAsString sets bytes value to datum as string type.
func (d *Datum) SetBytesAsString(b []byte, collation string, length uint32) {
	d.k = KindString
	d.b = b
	d.length = length
	d.collation = collation
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() any {
	return d.x
}

// SetInterface sets interface to datum.
func (d *Datum) SetInterface(x any) {
	d.k = KindInterface
	d.x = x
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// SetMinNotNull sets datum to minNotNull value.
func (d *Datum) SetMinNotNull() {
	d.k = KindMinNotNull
	d.x = nil
}

// GetBinaryLiteral4Cmp gets Bit value, and remove it's prefix 0 for comparison.
func (d *Datum) GetBinaryLiteral4Cmp() BinaryLiteral {
	bitLen := len(d.b)
	if bitLen == 0 {
		return d.b
	}
	for i := range bitLen {
		// Remove the prefix 0 in the bit array.
		if d.b[i] != 0 {
			return d.b[i:]
		}
	}
	// The result is 0x000...00, we just the return 0x00.
	return d.b[bitLen-1:]
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// GetMysqlBit gets MysqlBit value
func (d *Datum) GetMysqlBit() BinaryLiteral {
	return d.GetBinaryLiteral()
}

// SetBinaryLiteral sets Bit value
func (d *Datum) SetBinaryLiteral(b BinaryLiteral) {
	d.k = KindBinaryLiteral
	d.b = b
	d.collation = charset.CollationBin
}

// SetMysqlBit sets MysqlBit value
func (d *Datum) SetMysqlBit(b BinaryLiteral) {
	d.k = KindMysqlBit
	d.b = b
}

// GetMysqlDecimal gets decimal value
func (d *Datum) GetMysqlDecimal() *MyDecimal {
	return d.x.(*MyDecimal)
}

// SetMysqlDecimal sets decimal value
func (d *Datum) SetMysqlDecimal(b *MyDecimal) {
	d.k = KindMysqlDecimal
	d.x = b
}

// GetMysqlDuration gets Duration value
func (d *Datum) GetMysqlDuration() Duration {
	return Duration{Duration: time.Duration(d.i), Fsp: int(int8(d.decimal))}
}

// SetMysqlDuration sets Duration value
func (d *Datum) SetMysqlDuration(b Duration) {
	d.k = KindMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// GetMysqlEnum gets Enum value
func (d *Datum) GetMysqlEnum() Enum {
	str := string(hack.String(d.b))
	return Enum{Value: uint64(d.i), Name: str}
}

// SetMysqlEnum sets Enum value
func (d *Datum) SetMysqlEnum(b Enum, collation string) {
	d.k = KindMysqlEnum
	d.i = int64(b.Value)
	sink(b.Name)
	d.collation = collation
	d.b = hack.Slice(b.Name)
}

// GetMysqlSet gets Set value
func (d *Datum) GetMysqlSet() Set {
	str := string(hack.String(d.b))
	return Set{Value: uint64(d.i), Name: str}
}

// SetMysqlSet sets Set value
func (d *Datum) SetMysqlSet(b Set, collation string) {
	d.k = KindMysqlSet
	d.i = int64(b.Value)
	sink(b.Name)
	d.collation = collation
	d.b = hack.Slice(b.Name)
}

// GetMysqlJSON gets json.BinaryJSON value
func (d *Datum) GetMysqlJSON() BinaryJSON {
	return BinaryJSON{TypeCode: byte(d.i), Value: d.b}
}

// SetMysqlJSON sets json.BinaryJSON value
func (d *Datum) SetMysqlJSON(b BinaryJSON) {
	d.k = KindMysqlJSON
	d.i = int64(b.TypeCode)
	d.b = b.Value
}

// SetVectorFloat32 sets VectorFloat32 value
func (d *Datum) SetVectorFloat32(vec VectorFloat32) {
	d.k = KindVectorFloat32
	d.b = vec.ZeroCopySerialize()
}

// GetVectorFloat32 gets VectorFloat32 value
func (d *Datum) GetVectorFloat32() VectorFloat32 {
	v, _, err := ZeroCopyDeserializeVectorFloat32(d.b)
	if err != nil {
		panic(err)
	}
	return v
}

// GetMysqlTime gets types.Time value
func (d *Datum) GetMysqlTime() Time {
	return d.x.(Time)
}

// SetMysqlTime sets types.Time value
func (d *Datum) SetMysqlTime(b Time) {
	d.k = KindMysqlTime
	d.x = b
}

// SetRaw sets raw value.
func (d *Datum) SetRaw(b []byte) {
	d.k = KindRaw
	d.b = b
}

// GetRaw gets raw value.
func (d *Datum) GetRaw() []byte {
	return d.b
}

// SetAutoID set the auto increment ID according to its int flag.
// Don't use it directly, useless wrapped with setDatumAutoIDAndCast.
func (d *Datum) SetAutoID(id int64, flag uint) {
	if mysql.HasUnsignedFlag(flag) {
		d.SetUint64(uint64(id))
	} else {
		d.SetInt64(id)
	}
}

// String returns a human-readable description of Datum. It is intended only for debugging.
func (d Datum) String() string {
	var t string
	switch d.k {
	case KindNull:
		t = "KindNull"
	case KindInt64:
		t = "KindInt64"
	case KindUint64:
		t = "KindUint64"
	case KindFloat32:
		t = "KindFloat32"
	case KindFloat64:
		t = "KindFloat64"
	case KindString:
		t = "KindString"
	case KindBytes:
		t = "KindBytes"
	case KindBinaryLiteral:
		t = "KindBinaryLiteral"
	case KindMysqlDecimal:
		t = "KindMysqlDecimal"
	case KindMysqlDuration:
		t = "KindMysqlDuration"
	case KindMysqlEnum:
		t = "KindMysqlEnum"
	case KindMysqlBit:
		t = "KindMysqlBit"
	case KindMysqlSet:
		t = "KindMysqlSet"
	case KindMysqlTime:
		t = "KindMysqlTime"
	case KindInterface:
		t = "KindInterface"
	case KindMinNotNull:
		t = "KindMinNotNull"
	case KindMaxValue:
		t = "KindMaxValue"
	case KindRaw:
		t = "KindRaw"
	case KindMysqlJSON:
		t = "KindMysqlJSON"
	case KindVectorFloat32:
		t = "KindVectorFloat32"
	default:
		t = "Unknown"
	}
	v := d.GetValue()
	switch v.(type) {
	case []byte, string:
		quote := `"`
		// We only need the escape functionality of %q, the quoting is not needed,
		// so we trim the \" prefix and suffix here.
		v = strings.TrimSuffix(
			strings.TrimPrefix(
				fmt.Sprintf("%q", v),
				quote),
			quote)
	}
	return fmt.Sprintf("%v %v", t, v)
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() any {
	switch d.k {
	case KindInt64:
		return d.GetInt64()
	case KindUint64:
		return d.GetUint64()
	case KindFloat32:
		return d.GetFloat32()
	case KindFloat64:
		return d.GetFloat64()
	case KindString:
		return d.GetString()
	case KindBytes:
		return d.GetBytes()
	case KindMysqlDecimal:
		return d.GetMysqlDecimal()
	case KindMysqlDuration:
		return d.GetMysqlDuration()
	case KindMysqlEnum:
		return d.GetMysqlEnum()
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral()
	case KindMysqlSet:
		return d.GetMysqlSet()
	case KindMysqlJSON:
		return d.GetMysqlJSON()
	case KindMysqlTime:
		return d.GetMysqlTime()
	case KindVectorFloat32:
		return d.GetVectorFloat32()
	default:
		return d.GetInterface()
	}
}

// TruncatedStringify returns the %v representation of the datum
// but truncated (for example, for strings, only first 64 bytes is printed).
// This function is useful in contexts like EXPLAIN.
func (d *Datum) TruncatedStringify() string {
	switch d.k {
	case KindString, KindBytes:
		return truncateStringIfNeeded(d.GetString())
	case KindMysqlJSON:
		return truncateStringIfNeeded(d.GetMysqlJSON().String())
	case KindVectorFloat32:
		// Vector supports native efficient truncation.
		return d.GetVectorFloat32().TruncatedString()
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10)
	default:
		// For other types, no truncation is needed.
		return fmt.Sprintf("%v", d.GetValue())
	}
}

func truncateStringIfNeeded(str string) string {
	const maxLen = 64
	if len(str) > maxLen {
		const suffix = "...(len:"
		lenStr := strconv.Itoa(len(str))
		buf := bytes.NewBuffer(make([]byte, 0, maxLen+len(suffix)+len(lenStr)+1))
		buf.WriteString(str[:maxLen])
		buf.WriteString(suffix)
		buf.WriteString(lenStr)
		buf.WriteByte(')')
		return buf.String()
	}
	return str
}

// SetValueWithDefaultCollation sets any kind of value.
func (d *Datum) SetValueWithDefaultCollation(val any) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, mysql.DefaultCollationName)
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, mysql.DefaultCollationName)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, mysql.DefaultCollationName)
	case BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	case VectorFloat32:
		d.SetVectorFloat32(x)
	default:
		d.SetInterface(x)
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val any, tp *types.FieldType) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, tp.GetCollate())
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, tp.GetCollate())
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, tp.GetCollate())
	case BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	case VectorFloat32:
		d.SetVectorFloat32(x)
	default:
		d.SetInterface(x)
	}
}

// Hash64ForDatum is a hash function for initialized by codec package.
var Hash64ForDatum func(h base.Hasher, d *Datum)

// Hash64 implements base.HashEquals<0th> interface.
func (d *Datum) Hash64(h base.Hasher) {
	Hash64ForDatum(h, d)
}



// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Datum) ToBool(ctx Context) (int64, error) {
	var err error
	isZero := false
	switch d.Kind() {
	case KindInt64:
		isZero = d.GetInt64() == 0
	case KindUint64:
		isZero = d.GetUint64() == 0
	case KindFloat32:
		isZero = d.GetFloat64() == 0
	case KindFloat64:
		isZero = d.GetFloat64() == 0
	case KindString, KindBytes:
		iVal, err1 := StrToFloat(ctx, d.GetString(), false)
		isZero, err = iVal == 0, err1

	case KindMysqlTime:
		isZero = d.GetMysqlTime().IsZero()
	case KindMysqlDuration:
		isZero = d.GetMysqlDuration().Duration == 0
	case KindMysqlDecimal:
		isZero = d.GetMysqlDecimal().IsZero()
	case KindMysqlEnum:
		isZero = d.GetMysqlEnum().ToNumber() == 0
	case KindMysqlSet:
		isZero = d.GetMysqlSet().ToNumber() == 0
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		isZero, err = val == 0, err1
	case KindMysqlJSON:
		val := d.GetMysqlJSON()
		isZero = val.IsZero()
	case KindVectorFloat32:
		isZero = d.GetVectorFloat32().IsZeroValue()
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	var ret int64
	if isZero {
		ret = 0
	} else {
		ret = 1
	}
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

// ConvertDatumToDecimal converts datum to decimal.
func ConvertDatumToDecimal(ctx Context, d Datum) (*MyDecimal, error) {
	dec := new(MyDecimal)
	var err error
	switch d.Kind() {
	case KindInt64:
		dec.FromInt(d.GetInt64())
	case KindUint64:
		dec.FromUint(d.GetUint64())
	case KindFloat32:
		err = dec.FromFloat64(float64(d.GetFloat32()))
	case KindFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case KindString:
		err = ctx.HandleTruncate(dec.FromString(d.GetBytes()))
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlEnum:
		dec.FromUint(d.GetMysqlEnum().Value)
	case KindMysqlSet:
		dec.FromUint(d.GetMysqlSet().Value)
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		dec.FromUint(val)
		err = err1
	case KindMysqlJSON:
		f, err1 := ConvertJSONToDecimal(ctx, d.GetMysqlJSON())
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		dec = f
	default:
		err = errors.Errorf("can't convert %v to decimal", d.GetValue())
	}
	return dec, errors.Trace(err)
}

// ToDecimal converts to a decimal.
func (d *Datum) ToDecimal(ctx Context) (*MyDecimal, error) {
	switch d.Kind() {
	case KindMysqlTime:
		return d.GetMysqlTime().ToNumber(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().ToNumber(), nil
	default:
		return ConvertDatumToDecimal(ctx, *d)
	}
}

// ToInt64 converts to a int64.
func (d *Datum) ToInt64(ctx Context) (int64, error) {
	if d.Kind() == KindMysqlBit {
		uintVal, err := d.GetBinaryLiteral().ToInt(ctx)
		return int64(uintVal), err
	}
	return d.toSignedInteger(ctx, mysql.TypeLonglong)
}

func (d *Datum) toSignedInteger(ctx Context, tp byte) (int64, error) {
	lowerBound := IntegerSignedLowerBound(tp)
	upperBound := IntegerSignedUpperBound(tp)
	switch d.Kind() {
	case KindInt64:
		return ConvertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case KindUint64:
		return ConvertUintToInt(d.GetUint64(), upperBound, tp)
	case KindFloat32:
		return ConvertFloatToInt(float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case KindFloat64:
		return ConvertFloatToInt(d.GetFloat64(), lowerBound, upperBound, tp)
	case KindString, KindBytes:
		iVal, err := StrToInt(ctx, d.GetString(), false)
		iVal, err2 := ConvertIntToInt(iVal, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return iVal, errors.Trace(err)
	case KindMysqlTime:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		// 2011-11-10 11:59:59.999999 -> 20111110120000
		t, err := d.GetMysqlTime().RoundFrac(ctx, DefaultFsp)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := t.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlDuration:
		// 11:11:11.999999 -> 111112
		// 11:59:59.999999 -> 120000
		dur, err := d.GetMysqlDuration().RoundFrac(DefaultFsp, ctx.Location())
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := dur.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlDecimal:
		var to MyDecimal
		err := d.GetMysqlDecimal().Round(&to, 0, ModeHalfUp)
		ival, err1 := to.ToInt()
		if err == nil {
			err = err1
		}
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlEnum:
		fval := d.GetMysqlEnum().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlSet:
		fval := d.GetMysqlSet().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlJSON:
		return ConvertJSONToInt(ctx, d.GetMysqlJSON(), false, tp)
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := ConvertUintToInt(val, upperBound, tp)
		return ival, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Datum) ToFloat64(ctx Context) (float64, error) {
	switch d.Kind() {
	case KindInt64:
		return float64(d.GetInt64()), nil
	case KindUint64:
		return float64(d.GetUint64()), nil
	case KindFloat32:
		return float64(d.GetFloat32()), nil
	case KindFloat64:
		return d.GetFloat64(), nil
	case KindString:
		return StrToFloat(ctx, d.GetString(), false)
	case KindBytes:
		return StrToFloat(ctx, string(d.GetBytes()), false)
	case KindMysqlTime:
		f, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDuration:
		f, err := d.GetMysqlDuration().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDecimal:
		f, err := d.GetMysqlDecimal().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlEnum:
		return d.GetMysqlEnum().ToNumber(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().ToNumber(), nil
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		return float64(val), errors.Trace(err)
	case KindMysqlJSON:
		f, err := ConvertJSONToFloat(ctx, d.GetMysqlJSON())
		return f, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", d.GetValue(), d.GetValue())
	}
}

// ToString gets the string representation of the datum.
func (d *Datum) ToString() (string, error) {
	switch d.Kind() {
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case KindFloat32:
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case KindString:
		return d.GetString(), nil
	case KindBytes:
		return d.GetString(), nil
	case KindMysqlTime:
		return d.GetMysqlTime().String(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().String(), nil
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().String(), nil
	case KindMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().String(), nil
	case KindMysqlJSON:
		return d.GetMysqlJSON().String(), nil
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral().ToString(), nil
	case KindVectorFloat32:
		return d.GetVectorFloat32().String(), nil
	case KindNull:
		return "", nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// ToBytes gets the bytes representation of the datum.
func (d *Datum) ToBytes() ([]byte, error) {
	switch d.k {
	case KindString, KindBytes:
		return d.GetBytes(), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []byte(str), nil
	}
}

// ToHashKey gets the bytes representation of the datum considering collation.
func (d *Datum) ToHashKey() ([]byte, error) {
	switch d.k {
	case KindString, KindBytes:
		return collate.GetCollator(d.Collation()).Key(d.GetString()), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return collate.GetCollator(d.Collation()).Key(str), nil
	}
}

// ToMysqlJSON is similar to convertToMysqlJSON, except the
// latter parses from string, but the former uses it as primitive.
func (d *Datum) ToMysqlJSON() (j BinaryJSON, err error) {
	var in any
	switch d.Kind() {
	case KindMysqlJSON:
		j = d.GetMysqlJSON()
		return
	case KindInt64:
		in = d.GetInt64()
	case KindUint64:
		in = d.GetUint64()
	case KindFloat32, KindFloat64:
		in = d.GetFloat64()
	case KindMysqlDecimal:
		in, err = d.GetMysqlDecimal().ToFloat64()
	case KindString, KindBytes:
		in = d.GetString()
	case KindBinaryLiteral, KindMysqlBit:
		in = d.GetBinaryLiteral().ToString()
	case KindNull:
		in = nil
	case KindMysqlTime:
		in = d.GetMysqlTime()
	case KindMysqlDuration:
		in = d.GetMysqlDuration()
	default:
		in, err = d.ToString()
	}
	if err != nil {
		err = errors.Trace(err)
		return
	}
	j = CreateBinaryJSON(in)
	return
}

// MemUsage gets the memory usage of datum.
func (d *Datum) MemUsage() (sum int64) {
	// d.x is not considered now since MemUsage is now only used by analyze samples which is bytesDatum
	return EmptyDatumSize + int64(cap(d.b)) + int64(len(d.collation))
}

type jsonDatum struct {
	K         byte       `json:"k"`
	Decimal   uint16     `json:"decimal,omitempty"`
	Length    uint32     `json:"length,omitempty"`
	I         int64      `json:"i,omitempty"`
	Collation string     `json:"collation,omitempty"`
	B         []byte     `json:"b,omitempty"`
	Time      Time       `json:"time,omitempty"`
	MyDecimal *MyDecimal `json:"mydecimal,omitempty"`
}

// MarshalJSON implements Marshaler.MarshalJSON interface.
func (d *Datum) MarshalJSON() ([]byte, error) {
	jd := &jsonDatum{
		K:         d.k,
		Decimal:   d.decimal,
		Length:    d.length,
		I:         d.i,
		Collation: d.collation,
		B:         d.b,
	}
	switch d.k {
	case KindMysqlTime:
		jd.Time = d.GetMysqlTime()
	case KindMysqlDecimal:
		jd.MyDecimal = d.GetMysqlDecimal()
	default:
		if d.x != nil {
			return nil, fmt.Errorf("unsupported type: %d", d.k)
		}
	}
	return gjson.Marshal(jd)
}

// UnmarshalJSON implements Unmarshaler.UnmarshalJSON interface.
func (d *Datum) UnmarshalJSON(data []byte) error {
	var jd jsonDatum
	if err := gjson.Unmarshal(data, &jd); err != nil {
		return err
	}
	d.k = jd.K
	d.decimal = jd.Decimal
	d.length = jd.Length
	d.i = jd.I
	d.collation = jd.Collation
	d.b = jd.B

	switch jd.K {
	case KindMysqlTime:
		d.SetMysqlTime(jd.Time)
	case KindMysqlDecimal:
		d.SetMysqlDecimal(jd.MyDecimal)
	}
	return nil
}

func invalidConv(d *Datum, tp byte) (Datum, error) {
	return Datum{}, errors.Errorf("cannot convert datum from %s to type %s", KindStr(d.Kind()), TypeStr(tp))
}

