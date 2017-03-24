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
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
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
	KindMysqlBit      byte = 7
	KindMysqlDecimal  byte = 8
	KindMysqlDuration byte = 9
	KindMysqlEnum     byte = 10
	KindMysqlHex      byte = 11
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindRow           byte = 14
	KindInterface     byte = 15
	KindMinNotNull    byte = 16
	KindMaxValue      byte = 17
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte        // datum kind.
	collation uint8       // collation can hold uint8 values.
	decimal   uint16      // decimal can hold uint16 values.
	length    uint32      // length can hold uint32 values.
	i         int64       // i can hold int64 uint64 float64 values.
	b         []byte      // b can hold string or []byte values.
	x         interface{} // x hold all other types.
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	return d.k
}

// Collation gets the collation of the datum.
func (d *Datum) Collation() byte {
	return d.collation
}

// SetCollation sets the collation of the datum.
func (d *Datum) SetCollation(collation byte) {
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

// SetLength sets the length of the datum
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

// GetString gets string value.
func (d *Datum) GetString() string {
	return hack.String(d.b)
}

// SetString sets string value.
func (d *Datum) SetString(s string) {
	d.k = KindString
	sink(s)
	d.b = hack.Slice(s)
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	return d.b
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
}

// SetBytesAsString sets bytes value to datum as string type.
func (d *Datum) SetBytesAsString(b []byte) {
	d.k = KindString
	d.b = b
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() interface{} {
	return d.x
}

// SetInterface sets interface to datum.
func (d *Datum) SetInterface(x interface{}) {
	d.k = KindInterface
	d.x = x
}

// GetRow gets row value.
func (d *Datum) GetRow() []Datum {
	return d.x.([]Datum)
}

// SetRow sets row value.
func (d *Datum) SetRow(ds []Datum) {
	d.k = KindRow
	d.x = ds
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// GetMysqlBit gets Bit value
func (d *Datum) GetMysqlBit() Bit {
	width := int(d.length)
	value := uint64(d.i)
	return Bit{Value: value, Width: width}
}

// SetMysqlBit sets Bit value
func (d *Datum) SetMysqlBit(b Bit) {
	d.k = KindMysqlBit
	d.length = uint32(b.Width)
	d.i = int64(b.Value)
}

// GetMysqlDecimal gets Decimal value
func (d *Datum) GetMysqlDecimal() *MyDecimal {
	return d.x.(*MyDecimal)
}

// SetMysqlDecimal sets Decimal value
func (d *Datum) SetMysqlDecimal(b *MyDecimal) {
	d.k = KindMysqlDecimal
	d.x = b
}

// GetMysqlDuration gets Duration value
func (d *Datum) GetMysqlDuration() Duration {
	return Duration{Duration: time.Duration(d.i), Fsp: int(d.decimal)}
}

// SetMysqlDuration sets Duration value
func (d *Datum) SetMysqlDuration(b Duration) {
	d.k = KindMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// GetMysqlEnum gets Enum value
func (d *Datum) GetMysqlEnum() Enum {
	return Enum{Value: uint64(d.i), Name: hack.String(d.b)}
}

// SetMysqlEnum sets Enum value
func (d *Datum) SetMysqlEnum(b Enum) {
	d.k = KindMysqlEnum
	d.i = int64(b.Value)
	sink(b.Name)
	d.b = hack.Slice(b.Name)
}

// GetMysqlHex gets Hex value
func (d *Datum) GetMysqlHex() Hex {
	return Hex{Value: d.i}
}

// SetMysqlHex sets Hex value
func (d *Datum) SetMysqlHex(b Hex) {
	d.k = KindMysqlHex
	d.i = b.Value
}

// GetMysqlSet gets Set value
func (d *Datum) GetMysqlSet() Set {
	return Set{Value: uint64(d.i), Name: hack.String(d.b)}
}

// SetMysqlSet sets Set value
func (d *Datum) SetMysqlSet(b Set) {
	d.k = KindMysqlSet
	d.i = int64(b.Value)
	sink(b.Name)
	d.b = hack.Slice(b.Name)
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

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() interface{} {
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
	case KindMysqlBit:
		return d.GetMysqlBit()
	case KindMysqlDecimal:
		return d.GetMysqlDecimal()
	case KindMysqlDuration:
		return d.GetMysqlDuration()
	case KindMysqlEnum:
		return d.GetMysqlEnum()
	case KindMysqlHex:
		return d.GetMysqlHex()
	case KindMysqlSet:
		return d.GetMysqlSet()
	case KindMysqlTime:
		return d.GetMysqlTime()
	default:
		return d.GetInterface()
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val interface{}) {
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
		d.SetString(x)
	case []byte:
		d.SetBytes(x)
	case Bit:
		d.SetMysqlBit(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x)
	case Hex:
		d.SetMysqlHex(x)
	case Set:
		d.SetMysqlSet(x)
	case Time:
		d.SetMysqlTime(x)
	case []Datum:
		d.SetRow(x)
	case []interface{}:
		ds := MakeDatums(x...)
		d.SetRow(ds)
	default:
		d.SetInterface(x)
	}
}

// CompareDatum compares datum to another datum.
// TODO: return error properly.
func (d *Datum) CompareDatum(sc *variable.StatementContext, ad Datum) (int, error) {
	switch ad.k {
	case KindNull:
		if d.k == KindNull {
			return 0, nil
		}
		return 1, nil
	case KindMinNotNull:
		if d.k == KindNull {
			return -1, nil
		} else if d.k == KindMinNotNull {
			return 0, nil
		}
		return 1, nil
	case KindMaxValue:
		if d.k == KindMaxValue {
			return 0, nil
		}
		return -1, nil
	case KindInt64:
		return d.compareInt64(sc, ad.GetInt64())
	case KindUint64:
		return d.compareUint64(sc, ad.GetUint64())
	case KindFloat32, KindFloat64:
		return d.compareFloat64(sc, ad.GetFloat64())
	case KindString:
		return d.compareString(sc, ad.GetString())
	case KindBytes:
		return d.compareBytes(sc, ad.GetBytes())
	case KindMysqlBit:
		return d.compareMysqlBit(sc, ad.GetMysqlBit())
	case KindMysqlDecimal:
		return d.compareMysqlDecimal(sc, ad.GetMysqlDecimal())
	case KindMysqlDuration:
		return d.compareMysqlDuration(sc, ad.GetMysqlDuration())
	case KindMysqlEnum:
		return d.compareMysqlEnum(sc, ad.GetMysqlEnum())
	case KindMysqlHex:
		return d.compareMysqlHex(sc, ad.GetMysqlHex())
	case KindMysqlSet:
		return d.compareMysqlSet(sc, ad.GetMysqlSet())
	case KindMysqlTime:
		return d.compareMysqlTime(sc, ad.GetMysqlTime())
	case KindRow:
		return d.compareRow(sc, ad.GetRow())
	default:
		return 0, nil
	}
}

func (d *Datum) compareInt64(sc *variable.StatementContext, i int64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return CompareInt64(d.i, i), nil
	case KindUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1, nil
		}
		return CompareInt64(d.i, i), nil
	default:
		return d.compareFloat64(sc, float64(i))
	}
}

func (d *Datum) compareUint64(sc *variable.StatementContext, u uint64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1, nil
		}
		return CompareInt64(d.i, int64(u)), nil
	case KindUint64:
		return CompareUint64(d.GetUint64(), u), nil
	default:
		return d.compareFloat64(sc, float64(u))
	}
}

func (d *Datum) compareFloat64(sc *variable.StatementContext, f float64) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return CompareFloat64(float64(d.i), f), nil
	case KindUint64:
		return CompareFloat64(float64(d.GetUint64()), f), nil
	case KindFloat32, KindFloat64:
		return CompareFloat64(d.GetFloat64(), f), nil
	case KindString, KindBytes:
		fVal, err := StrToFloat(sc, d.GetString())
		return CompareFloat64(fVal, f), err
	case KindMysqlBit:
		fVal := d.GetMysqlBit().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlDecimal:
		fVal, _ := d.GetMysqlDecimal().ToFloat64()
		return CompareFloat64(fVal, f), nil
	case KindMysqlDuration:
		fVal := d.GetMysqlDuration().Seconds()
		return CompareFloat64(fVal, f), nil
	case KindMysqlEnum:
		fVal := d.GetMysqlEnum().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlHex:
		fVal := d.GetMysqlHex().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlSet:
		fVal := d.GetMysqlSet().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlTime:
		fVal, _ := d.GetMysqlTime().ToNumber().ToFloat64()
		return CompareFloat64(fVal, f), nil
	default:
		return -1, nil
	}
}

func (d *Datum) compareString(sc *variable.StatementContext, s string) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		return CompareString(d.GetString(), s), nil
	case KindMysqlDecimal:
		dec := new(MyDecimal)
		err := dec.FromString([]byte(s))
		return d.GetMysqlDecimal().Compare(dec), err
	case KindMysqlTime:
		dt, err := ParseDatetime(s)
		return d.GetMysqlTime().Compare(dt), err
	case KindMysqlDuration:
		dur, err := ParseDuration(s, MaxFsp)
		return d.GetMysqlDuration().Compare(dur), err
	case KindMysqlBit:
		return CompareString(d.GetMysqlBit().ToString(), s), nil
	case KindMysqlHex:
		return CompareString(d.GetMysqlHex().ToString(), s), nil
	case KindMysqlSet:
		return CompareString(d.GetMysqlSet().String(), s), nil
	case KindMysqlEnum:
		return CompareString(d.GetMysqlEnum().String(), s), nil
	default:
		fVal, err := StrToFloat(sc, s)
		if err != nil {
			return 0, err
		}
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Datum) compareBytes(sc *variable.StatementContext, b []byte) (int, error) {
	return d.compareString(sc, hack.String(b))
}

func (d *Datum) compareMysqlBit(sc *variable.StatementContext, bit Bit) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), bit.ToString()), nil
	default:
		return d.compareFloat64(sc, bit.ToNumber())
	}
}

func (d *Datum) compareMysqlDecimal(sc *variable.StatementContext, dec *MyDecimal) (int, error) {
	switch d.k {
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Compare(dec), nil
	case KindString, KindBytes:
		dDec := new(MyDecimal)
		err := dDec.FromString(d.GetBytes())
		return dDec.Compare(dec), err
	default:
		fVal, _ := dec.ToFloat64()
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Datum) compareMysqlDuration(sc *variable.StatementContext, dur Duration) (int, error) {
	switch d.k {
	case KindMysqlDuration:
		return d.GetMysqlDuration().Compare(dur), nil
	case KindString, KindBytes:
		dDur, err := ParseDuration(d.GetString(), MaxFsp)
		return dDur.Compare(dur), err
	default:
		return d.compareFloat64(sc, dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(sc *variable.StatementContext, enum Enum) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), enum.String()), nil
	default:
		return d.compareFloat64(sc, enum.ToNumber())
	}
}

func (d *Datum) compareMysqlHex(sc *variable.StatementContext, e Hex) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), e.ToString()), nil
	default:
		return d.compareFloat64(sc, e.ToNumber())
	}
}

func (d *Datum) compareMysqlSet(sc *variable.StatementContext, set Set) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), set.String()), nil
	default:
		return d.compareFloat64(sc, set.ToNumber())
	}
}

func (d *Datum) compareMysqlTime(sc *variable.StatementContext, time Time) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		dt, err := ParseDatetime(d.GetString())
		return dt.Compare(time), err
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(time), nil
	default:
		fVal, _ := time.ToNumber().ToFloat64()
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Datum) compareRow(sc *variable.StatementContext, row []Datum) (int, error) {
	var dRow []Datum
	if d.k == KindRow {
		dRow = d.GetRow()
	} else {
		dRow = []Datum{*d}
	}
	for i := 0; i < len(row) && i < len(dRow); i++ {
		cmp, err := dRow[i].CompareDatum(sc, row[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return CompareInt64(int64(len(dRow)), int64(len(row))), nil
}

// Cast casts datum to certain types.
func (d *Datum) Cast(sc *variable.StatementContext, target *FieldType) (ad Datum, err error) {
	if !isCastType(target.Tp) {
		return ad, errors.Errorf("unknown cast type - %v", target)
	}
	return d.ConvertTo(sc, target)
}

// ConvertTo converts a datum to the target field type.
func (d *Datum) ConvertTo(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	if d.k == KindNull {
		return Datum{}, nil
	}
	switch target.Tp { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		unsigned := mysql.HasUnsignedFlag(target.Flag)
		if unsigned {
			return d.convertToUint(sc, target)
		}
		return d.convertToInt(sc, target)
	case mysql.TypeFloat, mysql.TypeDouble:
		return d.convertToFloat(sc, target)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return d.convertToString(sc, target)
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		return d.convertToMysqlTime(sc, target)
	case mysql.TypeDuration:
		return d.convertToMysqlDuration(sc, target)
	case mysql.TypeBit:
		return d.convertToMysqlBit(sc, target)
	case mysql.TypeNewDecimal:
		return d.convertToMysqlDecimal(sc, target)
	case mysql.TypeYear:
		return d.convertToMysqlYear(sc, target)
	case mysql.TypeEnum:
		return d.convertToMysqlEnum(sc, target)
	case mysql.TypeSet:
		return d.convertToMysqlSet(sc, target)
	case mysql.TypeNull:
		return Datum{}, nil
	default:
		panic("should never happen")
	}
}

func (d *Datum) convertToFloat(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var (
		f   float64
		ret Datum
		err error
	)
	switch d.k {
	case KindNull:
		return ret, nil
	case KindInt64:
		f = float64(d.GetInt64())
	case KindUint64:
		f = float64(d.GetUint64())
	case KindFloat32, KindFloat64:
		f = d.GetFloat64()
	case KindString, KindBytes:
		f, err = StrToFloat(sc, d.GetString())
	case KindMysqlTime:
		f, _ = d.GetMysqlTime().ToNumber().ToFloat64()
	case KindMysqlDuration:
		f, _ = d.GetMysqlDuration().ToNumber().ToFloat64()
	case KindMysqlDecimal:
		f, _ = d.GetMysqlDecimal().ToFloat64()
	case KindMysqlHex:
		f = d.GetMysqlHex().ToNumber()
	case KindMysqlBit:
		f = d.GetMysqlBit().ToNumber()
	case KindMysqlSet:
		f = d.GetMysqlSet().ToNumber()
	case KindMysqlEnum:
		f = d.GetMysqlEnum().ToNumber()
	default:
		return invalidConv(d, target.Tp)
	}
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
		var err1 error
		f, err1 = TruncateFloat(f, target.Flen, target.Decimal)
		if err == nil && err1 != nil {
			err = err1
			if sc.IgnoreTruncate {
				err = nil
			} else if sc.TruncateAsWarning {
				sc.AppendWarning(err)
				err = nil
			}
		}
	}
	if target.Tp == mysql.TypeFloat {
		ret.SetFloat32(float32(f))
	} else {
		ret.SetFloat64(f)
	}
	return ret, errors.Trace(err)
}

func (d *Datum) convertToString(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var ret Datum
	var s string
	switch d.k {
	case KindInt64:
		s = strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		s = strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat32:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case KindFloat64:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString, KindBytes:
		s = d.GetString()
	case KindMysqlTime:
		s = d.GetMysqlTime().String()
	case KindMysqlDuration:
		s = d.GetMysqlDuration().String()
	case KindMysqlDecimal:
		s = d.GetMysqlDecimal().String()
	case KindMysqlHex:
		s = d.GetMysqlHex().ToString()
	case KindMysqlBit:
		s = d.GetMysqlBit().ToString()
	case KindMysqlEnum:
		s = d.GetMysqlEnum().String()
	case KindMysqlSet:
		s = d.GetMysqlSet().String()
	default:
		return invalidConv(d, target.Tp)
	}

	var err error
	if target.Flen >= 0 {
		// Flen is the rune length, not binary length, for UTF8 charset, we need to calculate the
		// rune count and truncate to Flen runes if it is too long.
		if target.Charset == charset.CharsetUTF8 || target.Charset == charset.CharsetUTF8MB4 {
			var runeCount int
			var truncateLen int
			for i := range s {
				runeCount++
				if runeCount == target.Flen+1 {
					// We don't break here because we need to iterate to the end to get runeCount.
					truncateLen = i
				}
			}
			if truncateLen > 0 {
				if !sc.IgnoreTruncate {
					err = ErrDataTooLong.Gen("Data Too Long, field len %d, data len %d", target.Flen, runeCount)
				}
				s = truncateStr(s, truncateLen)
			}
		} else if len(s) > target.Flen {
			if !sc.IgnoreTruncate {
				err = ErrDataTooLong.Gen("Data Too Long, field len %d, data len %d", target.Flen, len(s))
			}
			s = truncateStr(s, target.Flen)
		}
	}
	ret.SetString(s)
	if target.Charset == charset.CharsetBin {
		ret.k = KindBytes
	}
	return ret, errors.Trace(err)
}

func (d *Datum) convertToInt(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	i64, err := d.toSignedInteger(sc, target.Tp)
	return NewIntDatum(i64), errors.Trace(err)
}

func (d *Datum) convertToUint(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	tp := target.Tp
	upperBound := unsignedUpperBound[tp]
	var (
		val uint64
		err error
		ret Datum
	)
	switch d.k {
	case KindInt64:
		val, err = convertIntToUint(d.GetInt64(), upperBound, tp)
	case KindUint64:
		val, err = convertUintToUint(d.GetUint64(), upperBound, tp)
	case KindFloat32, KindFloat64:
		val, err = convertFloatToUint(sc, d.GetFloat64(), upperBound, tp)
	case KindString, KindBytes:
		val, err = StrToUint(sc, d.GetString())
		if err != nil {
			return ret, errors.Trace(err)
		}
		val, err = convertUintToUint(val, upperBound, tp)
		if err != nil {
			return ret, errors.Trace(err)
		}
		ret.SetUint64(val)
	case KindMysqlTime:
		dec := d.GetMysqlTime().ToNumber()
		dec.Round(dec, 0)
		ival, err1 := dec.ToInt()
		val, err = convertIntToUint(ival, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlDuration:
		dec := d.GetMysqlDuration().ToNumber()
		dec.Round(dec, 0)
		var ival int64
		ival, err = dec.ToInt()
		if err == nil {
			val, err = convertIntToUint(ival, upperBound, tp)
		}
	case KindMysqlDecimal:
		fval, err1 := d.GetMysqlDecimal().ToFloat64()
		val, err = convertFloatToUint(sc, fval, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlHex:
		val, err = convertFloatToUint(sc, d.GetMysqlHex().ToNumber(), upperBound, tp)
	case KindMysqlBit:
		val, err = convertFloatToUint(sc, d.GetMysqlBit().ToNumber(), upperBound, tp)
	case KindMysqlEnum:
		val, err = convertFloatToUint(sc, d.GetMysqlEnum().ToNumber(), upperBound, tp)
	case KindMysqlSet:
		val, err = convertFloatToUint(sc, d.GetMysqlSet().ToNumber(), upperBound, tp)
	default:
		return invalidConv(d, target.Tp)
	}
	ret.SetUint64(val)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlTime(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		fsp = target.Decimal
	}
	var (
		ret Datum
		t   Time
		err error
	)
	switch d.k {
	case KindMysqlTime:
		t, err = d.GetMysqlTime().Convert(tp)
		if err != nil {
			ret.SetValue(t)
			return ret, errors.Trace(err)
		}
		t, err = t.roundFrac(fsp)
	case KindMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(tp)
		if err != nil {
			ret.SetValue(t)
			return ret, errors.Trace(err)
		}
		t, err = t.roundFrac(fsp)
	case KindString, KindBytes:
		t, err = ParseTime(d.GetString(), tp, fsp)
	case KindInt64:
		t, err = ParseTimeFromNum(d.GetInt64(), tp, fsp)
	default:
		return invalidConv(d, tp)
	}
	if tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		t.Time = FromDate(t.Time.Year(), t.Time.Month(), t.Time.Day(), 0, 0, 0, 0)
	}
	ret.SetValue(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlDuration(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		fsp = target.Decimal
	}
	var ret Datum
	switch d.k {
	case KindMysqlTime:
		dur, err := d.GetMysqlTime().ConvertToDuration()
		if err != nil {
			ret.SetValue(dur)
			return ret, errors.Trace(err)
		}
		dur, err = dur.RoundFrac(fsp)
		ret.SetValue(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlDuration:
		dur, err := d.GetMysqlDuration().RoundFrac(fsp)
		ret.SetValue(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindString, KindBytes:
		t, err := ParseDuration(d.GetString(), fsp)
		ret.SetValue(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	default:
		return invalidConv(d, tp)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlDecimal(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var ret Datum
	ret.SetLength(target.Flen)
	ret.SetFrac(target.Decimal)
	var dec = &MyDecimal{}
	var err error
	switch d.k {
	case KindInt64:
		dec.FromInt(d.GetInt64())
	case KindUint64:
		dec.FromUint(d.GetUint64())
	case KindFloat32, KindFloat64:
		dec.FromFloat64(d.GetFloat64())
	case KindString, KindBytes:
		err = dec.FromString(d.GetBytes())
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlTime:
		dec = d.GetMysqlTime().ToNumber()
	case KindMysqlDuration:
		dec = d.GetMysqlDuration().ToNumber()
	case KindMysqlBit:
		dec.FromFloat64(d.GetMysqlBit().ToNumber())
	case KindMysqlEnum:
		dec.FromFloat64(d.GetMysqlEnum().ToNumber())
	case KindMysqlHex:
		dec.FromFloat64(d.GetMysqlHex().ToNumber())
	case KindMysqlSet:
		dec.FromFloat64(d.GetMysqlSet().ToNumber())
	default:
		return invalidConv(d, target.Tp)
	}
	if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
		prec, frac := dec.PrecisionAndFrac()
		if prec-frac > target.Flen-target.Decimal {
			dec = NewMaxOrMinDec(dec.IsNegative(), target.Flen, target.Decimal)
			err = errors.Trace(ErrOverflow)
		} else if frac != target.Decimal {
			dec.Round(dec, target.Decimal)
			if frac > target.Decimal {
				err = errors.Trace(handleTruncateError(sc))
			}
		}
	}
	ret.SetValue(dec)
	return ret, err
}

func (d *Datum) convertToMysqlYear(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var (
		ret Datum
		y   int64
		err error
	)
	switch d.k {
	case KindString, KindBytes:
		y, err = StrToInt(sc, d.GetString())
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlTime:
		y = int64(d.GetMysqlTime().Time.Year())
	case KindMysqlDuration:
		y = int64(time.Now().Year())
	default:
		ret, err = d.convertToInt(sc, NewFieldType(mysql.TypeLonglong))
		if err != nil {
			return invalidConv(d, target.Tp)
		}
		y = ret.GetInt64()
	}
	y, err = AdjustYear(y)
	if err != nil {
		return invalidConv(d, target.Tp)
	}
	ret.SetInt64(y)
	return ret, nil
}

func (d *Datum) convertToMysqlBit(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	x, err := d.convertToUint(sc, target)
	if err != nil {
		return x, errors.Trace(err)
	}
	// check bit boundary, if bit has n width, the boundary is
	// in [0, (1 << n) - 1]
	width := target.Flen
	if width == 0 || width == UnspecifiedBitWidth {
		width = MinBitWidth
	}
	maxValue := uint64(1)<<uint64(width) - 1
	val := x.GetUint64()
	if val > maxValue {
		x.SetUint64(maxValue)
		return x, overflow(val, target.Tp)
	}
	var ret Datum
	ret.SetValue(Bit{Value: val, Width: width})
	return ret, nil
}

func (d *Datum) convertToMysqlEnum(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var (
		ret Datum
		e   Enum
		err error
	)
	switch d.k {
	case KindString, KindBytes:
		e, err = ParseEnumName(target.Elems, d.GetString())
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(sc, target)
		if err != nil {
			return ret, errors.Trace(err)
		}
		e, err = ParseEnumValue(target.Elems, uintDatum.GetUint64())
	}
	if err != nil {
		return invalidConv(d, target.Tp)
	}
	ret.SetValue(e)
	return ret, nil
}

func (d *Datum) convertToMysqlSet(sc *variable.StatementContext, target *FieldType) (Datum, error) {
	var (
		ret Datum
		s   Set
		err error
	)
	switch d.k {
	case KindString, KindBytes:
		s, err = ParseSetName(target.Elems, d.GetString())
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(sc, target)
		if err != nil {
			return ret, errors.Trace(err)
		}
		s, err = ParseSetValue(target.Elems, uintDatum.GetUint64())
	}

	if err != nil {
		return invalidConv(d, target.Tp)
	}
	ret.SetValue(s)
	return ret, nil
}

// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Datum) ToBool(sc *variable.StatementContext) (int64, error) {
	isZero := false
	switch d.Kind() {
	case KindInt64:
		isZero = (d.GetInt64() == 0)
	case KindUint64:
		isZero = (d.GetUint64() == 0)
	case KindFloat32:
		isZero = (RoundFloat(d.GetFloat64()) == 0)
	case KindFloat64:
		isZero = (RoundFloat(d.GetFloat64()) == 0)
	case KindString, KindBytes:
		iVal, err := StrToInt(sc, d.GetString())
		if err != nil {
			return iVal, errors.Trace(err)
		}
		isZero = iVal == 0
	case KindMysqlTime:
		isZero = d.GetMysqlTime().IsZero()
	case KindMysqlDuration:
		isZero = (d.GetMysqlDuration().Duration == 0)
	case KindMysqlDecimal:
		v, _ := d.GetMysqlDecimal().ToFloat64()
		isZero = (RoundFloat(v) == 0)
	case KindMysqlHex:
		isZero = (d.GetMysqlHex().ToNumber() == 0)
	case KindMysqlBit:
		isZero = (d.GetMysqlBit().ToNumber() == 0)
	case KindMysqlEnum:
		isZero = (d.GetMysqlEnum().ToNumber() == 0)
	case KindMysqlSet:
		isZero = (d.GetMysqlSet().ToNumber() == 0)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	if isZero {
		return 0, nil
	}
	return 1, nil
}

// ConvertDatumToDecimal converts datum to decimal.
func ConvertDatumToDecimal(sc *variable.StatementContext, d Datum) (*MyDecimal, error) {
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
		err = dec.FromString(d.GetBytes())
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlHex:
		dec.FromInt(d.GetMysqlHex().Value)
	case KindMysqlBit:
		dec.FromUint(d.GetMysqlBit().Value)
	case KindMysqlEnum:
		dec.FromUint(d.GetMysqlEnum().Value)
	case KindMysqlSet:
		dec.FromUint(d.GetMysqlSet().Value)
	default:
		err = fmt.Errorf("can't convert %v to decimal", d.GetValue())
	}
	return dec, err
}

// ToDecimal converts to a decimal.
func (d *Datum) ToDecimal(sc *variable.StatementContext) (*MyDecimal, error) {
	switch d.Kind() {
	case KindMysqlTime:
		return d.GetMysqlTime().ToNumber(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().ToNumber(), nil
	default:
		return ConvertDatumToDecimal(sc, *d)
	}
}

// ToInt64 converts to a int64.
func (d *Datum) ToInt64(sc *variable.StatementContext) (int64, error) {
	return d.toSignedInteger(sc, mysql.TypeLonglong)
}

func (d *Datum) toSignedInteger(sc *variable.StatementContext, tp byte) (int64, error) {
	lowerBound := signedLowerBound[tp]
	upperBound := signedUpperBound[tp]
	switch d.Kind() {
	case KindInt64:
		return convertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case KindUint64:
		return convertUintToInt(d.GetUint64(), upperBound, tp)
	case KindFloat32:
		return convertFloatToInt(sc, float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case KindFloat64:
		return convertFloatToInt(sc, d.GetFloat64(), lowerBound, upperBound, tp)
	case KindString, KindBytes:
		iVal, err := StrToInt(sc, d.GetString())
		if err != nil {
			return iVal, errors.Trace(err)
		}
		i64, err := convertIntToInt(iVal, lowerBound, upperBound, tp)
		return i64, errors.Trace(err)
	case KindMysqlTime:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		dec := d.GetMysqlTime().ToNumber()
		dec.Round(dec, 0)
		ival, err := dec.ToInt()
		ival, err2 := convertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, err
	case KindMysqlDuration:
		// 11:11:11.999999 -> 111112
		dec := d.GetMysqlDuration().ToNumber()
		dec.Round(dec, 0)
		ival, err := dec.ToInt()
		ival, err2 := convertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, err
	case KindMysqlDecimal:
		var to MyDecimal
		d.GetMysqlDecimal().Round(&to, 0)
		ival, err := to.ToInt()
		ival, err2 := convertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, err
	case KindMysqlHex:
		fval := d.GetMysqlHex().ToNumber()
		return convertFloatToInt(sc, fval, lowerBound, upperBound, tp)
	case KindMysqlBit:
		fval := d.GetMysqlBit().ToNumber()
		return convertFloatToInt(sc, fval, lowerBound, upperBound, tp)
	case KindMysqlEnum:
		fval := d.GetMysqlEnum().ToNumber()
		return convertFloatToInt(sc, fval, lowerBound, upperBound, tp)
	case KindMysqlSet:
		fval := d.GetMysqlSet().ToNumber()
		return convertFloatToInt(sc, fval, lowerBound, upperBound, tp)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Datum) ToFloat64(sc *variable.StatementContext) (float64, error) {
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
		return StrToFloat(sc, d.GetString())
	case KindBytes:
		return StrToFloat(sc, string(d.GetBytes()))
	case KindMysqlTime:
		f, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return f, err
	case KindMysqlDuration:
		f, _ := d.GetMysqlDuration().ToNumber().ToFloat64()
		return f, nil
	case KindMysqlDecimal:
		f, err := d.GetMysqlDecimal().ToFloat64()
		return f, err
	case KindMysqlHex:
		return d.GetMysqlHex().ToNumber(), nil
	case KindMysqlBit:
		return d.GetMysqlBit().ToNumber(), nil
	case KindMysqlEnum:
		return d.GetMysqlEnum().ToNumber(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().ToNumber(), nil
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
		return strconv.FormatFloat(float64(d.GetFloat64()), 'f', -1, 64), nil
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
	case KindMysqlHex:
		return d.GetMysqlHex().ToString(), nil
	case KindMysqlBit:
		return d.GetMysqlBit().ToString(), nil
	case KindMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().String(), nil
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

func invalidConv(d *Datum, tp byte) (Datum, error) {
	return Datum{}, errors.Errorf("cannot convert %v to type %s", d, TypeStr(tp))
}

func (d *Datum) convergeType(hasUint, hasDecimal, hasFloat *bool) (x Datum) {
	x = *d
	switch d.Kind() {
	case KindUint64:
		*hasUint = true
	case KindFloat32:
		f := d.GetFloat32()
		x.SetFloat64(float64(f))
		*hasFloat = true
	case KindFloat64:
		*hasFloat = true
	case KindMysqlDecimal:
		*hasDecimal = true
	}
	return x
}

// CoerceDatum changes type.
// If a or b is Float, changes the both to Float.
// Else if a or b is Decimal, changes the both to Decimal.
// Else if a or b is Uint and op is not div, mod, or intDiv changes the both to Uint.
func CoerceDatum(sc *variable.StatementContext, a, b Datum) (x, y Datum, err error) {
	if a.IsNull() || b.IsNull() {
		return x, y, nil
	}
	var (
		hasUint    bool
		hasDecimal bool
		hasFloat   bool
	)
	x = a.convergeType(&hasUint, &hasDecimal, &hasFloat)
	y = b.convergeType(&hasUint, &hasDecimal, &hasFloat)
	if hasFloat {
		switch x.Kind() {
		case KindInt64:
			x.SetFloat64(float64(x.GetInt64()))
		case KindUint64:
			x.SetFloat64(float64(x.GetUint64()))
		case KindMysqlHex:
			x.SetFloat64(x.GetMysqlHex().ToNumber())
		case KindMysqlBit:
			x.SetFloat64(x.GetMysqlBit().ToNumber())
		case KindMysqlEnum:
			x.SetFloat64(x.GetMysqlEnum().ToNumber())
		case KindMysqlSet:
			x.SetFloat64(x.GetMysqlSet().ToNumber())
		case KindMysqlDecimal:
			fval, err := x.ToFloat64(sc)
			if err != nil {
				return x, y, errors.Trace(err)
			}
			x.SetFloat64(fval)
		}
		switch y.Kind() {
		case KindInt64:
			y.SetFloat64(float64(y.GetInt64()))
		case KindUint64:
			y.SetFloat64(float64(y.GetUint64()))
		case KindMysqlHex:
			y.SetFloat64(y.GetMysqlHex().ToNumber())
		case KindMysqlBit:
			y.SetFloat64(y.GetMysqlBit().ToNumber())
		case KindMysqlEnum:
			y.SetFloat64(y.GetMysqlEnum().ToNumber())
		case KindMysqlSet:
			y.SetFloat64(y.GetMysqlSet().ToNumber())
		case KindMysqlDecimal:
			fval, err := y.ToFloat64(sc)
			if err != nil {
				return x, y, errors.Trace(err)
			}
			y.SetFloat64(fval)
		}
	} else if hasDecimal {
		var dec *MyDecimal
		dec, err = ConvertDatumToDecimal(sc, x)
		if err != nil {
			return x, y, errors.Trace(err)
		}
		x.SetMysqlDecimal(dec)
		dec, err = ConvertDatumToDecimal(sc, y)
		if err != nil {
			return x, y, errors.Trace(err)
		}
		y.SetMysqlDecimal(dec)
	}
	return
}

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in interface{}) (d Datum) {
	switch x := in.(type) {
	case []interface{}:
		d.SetValue(MakeDatums(x...))
	default:
		d.SetValue(in)
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
	d.SetString(s)
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

// NewDecimalDatum creates a new Datum form a MyDecimal value.
func NewDecimalDatum(dec *MyDecimal) (d Datum) {
	d.SetMysqlDecimal(dec)
	return d
}

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...interface{}) []Datum {
	datums := make([]Datum, len(args))
	for i, v := range args {
		datums[i] = NewDatum(v)
	}
	return datums
}

// DatumsToInterfaces converts a datum slice to interface slice.
func DatumsToInterfaces(datums []Datum) []interface{} {
	ins := make([]interface{}, len(datums))
	for i, v := range datums {
		ins[i] = v.GetValue()
	}
	return ins
}

// MinNotNullDatum returns a datum represents minimum not null value.
func MinNotNullDatum() Datum {
	return Datum{k: KindMinNotNull}
}

// MaxValueDatum returns a datum represents max value.
func MaxValueDatum() Datum {
	return Datum{k: KindMaxValue}
}

// EqualDatums compare if a and b contains the same datum values.
func EqualDatums(sc *variable.StatementContext, a []Datum, b []Datum) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	if a == nil && b == nil {
		return true, nil
	}
	if a == nil || b == nil {
		return false, nil
	}
	for i, ai := range a {
		v, err := ai.CompareDatum(sc, b[i])
		if err != nil {
			return false, errors.Trace(err)
		}
		if v != 0 {
			return false, nil
		}
	}
	return true, nil
}

// SortDatums sorts a slice of datum.
func SortDatums(sc *variable.StatementContext, datums []Datum) error {
	sorter := datumsSorter{datums: datums, sc: sc}
	sort.Sort(&sorter)
	return sorter.err
}

type datumsSorter struct {
	datums []Datum
	sc     *variable.StatementContext
	err    error
}

func (ds *datumsSorter) Len() int {
	return len(ds.datums)
}

func (ds *datumsSorter) Less(i, j int) bool {
	cmp, err := ds.datums[i].CompareDatum(ds.sc, ds.datums[j])
	if err != nil {
		ds.err = errors.Trace(err)
		return true
	}
	return cmp < 0
}

func (ds *datumsSorter) Swap(i, j int) {
	ds.datums[i], ds.datums[j] = ds.datums[j], ds.datums[i]
}

func handleTruncateError(sc *variable.StatementContext) error {
	if sc.IgnoreTruncate {
		return nil
	}
	if !sc.TruncateAsWarning {
		return ErrTruncated
	}
	sc.AppendWarning(ErrTruncated)
	return nil
}
