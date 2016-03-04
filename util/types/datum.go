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
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/hack"
)

// Kind constants.
const (
	KindNull  int = 0
	KindInt64 int = iota + 1
	KindUint64
	KindFloat32
	KindFloat64
	KindString
	KindBytes
	KindMysqlBit
	KindMysqlDecimal
	KindMysqlDuration
	KindMysqlEnum
	KindMysqlHex
	KindMysqlSet
	KindMysqlTime
	KindRow
	KindInterface
	KindMinNotNull
	KindMaxValue
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k int         // datum kind.
	i int64       // i can hold int64 uint64 float64 values.
	b []byte      // b can hold string or []byte values.
	x interface{} // f hold all other types.
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() int {
	return d.k
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
	d.b = hack.Slice(s)
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

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// GetMysqlBit gets mysql.Bit value
func (d *Datum) GetMysqlBit() mysql.Bit {
	return d.x.(mysql.Bit)
}

// GetMysqlDecimal gets mysql.Decimal value
func (d *Datum) GetMysqlDecimal() mysql.Decimal {
	return d.x.(mysql.Decimal)
}

// GetMysqlDuration gets mysql.Duration value
func (d *Datum) GetMysqlDuration() mysql.Duration {
	return d.x.(mysql.Duration)
}

// GetMysqlEnum gets mysql.Enum value
func (d *Datum) GetMysqlEnum() mysql.Enum {
	return d.x.(mysql.Enum)
}

// GetMysqlHex gets mysql.Hex value
func (d *Datum) GetMysqlHex() mysql.Hex {
	return d.x.(mysql.Hex)
}

// GetMysqlSet gets mysql.Set value
func (d *Datum) GetMysqlSet() mysql.Set {
	return d.x.(mysql.Set)
}

// GetMysqlTime gets mysql.Time value
func (d *Datum) GetMysqlTime() mysql.Time {
	return d.x.(mysql.Time)
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
	default:
		return d.x
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
	case mysql.Bit:
		d.x = x
		d.k = KindMysqlBit
	case mysql.Decimal:
		d.x = x
		d.k = KindMysqlDecimal
	case mysql.Duration:
		d.x = x
		d.k = KindMysqlDuration
	case mysql.Enum:
		d.x = x
		d.k = KindMysqlEnum
	case mysql.Hex:
		d.x = x
		d.k = KindMysqlHex
	case mysql.Set:
		d.x = x
		d.k = KindMysqlSet
	case mysql.Time:
		d.x = x
		d.k = KindMysqlTime
	case []Datum:
		d.x = x
		d.k = KindRow
	default:
		d.SetInterface(x)
	}
}

// CompareDatum compares datum to another datum.
// TODO: return error properly.
func (d *Datum) CompareDatum(ad Datum) (int, error) {
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
		return d.compareInt64(ad.GetInt64())
	case KindUint64:
		return d.compareUint64(ad.GetUint64())
	case KindFloat32, KindFloat64:
		return d.compareFloat64(ad.GetFloat64())
	case KindString:
		return d.compareString(ad.GetString())
	case KindBytes:
		return d.compareBytes(ad.GetBytes())
	case KindMysqlBit:
		return d.compareMysqlBit(ad.GetMysqlBit())
	case KindMysqlDecimal:
		return d.compareMysqlDecimal(ad.GetMysqlDecimal())
	case KindMysqlDuration:
		return d.compareMysqlDuration(ad.GetMysqlDuration())
	case KindMysqlEnum:
		return d.compareMysqlEnum(ad.GetMysqlEnum())
	case KindMysqlHex:
		return d.compareMysqlHex(ad.GetMysqlHex())
	case KindMysqlSet:
		return d.compareMysqlSet(ad.GetMysqlSet())
	case KindMysqlTime:
		return d.compareMysqlTime(ad.GetMysqlTime())
	case KindRow:
		return d.compareRow(ad.GetRow())
	default:
		return 0, nil
	}
}

func (d *Datum) compareInt64(i int64) (int, error) {
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
		return d.compareFloat64(float64(i))
	}
}

func (d *Datum) compareUint64(u uint64) (int, error) {
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
		return d.compareFloat64(float64(u))
	}
}

func (d *Datum) compareFloat64(f float64) (int, error) {
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
		fVal, err := parseFloat(d.GetString())
		return CompareFloat64(fVal, f), err
	case KindMysqlBit:
		fVal := d.GetMysqlBit().ToNumber()
		return CompareFloat64(fVal, f), nil
	case KindMysqlDecimal:
		fVal, _ := d.GetMysqlDecimal().Float64()
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
		fVal, _ := d.GetMysqlTime().ToNumber().Float64()
		return CompareFloat64(fVal, f), nil
	default:
		return -1, nil
	}
}

func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

func (d *Datum) compareString(s string) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		return CompareString(d.GetString(), s), nil
	case KindMysqlDecimal:
		dec, err := mysql.ParseDecimal(s)
		return d.GetMysqlDecimal().Cmp(dec), err
	case KindMysqlTime:
		dt, err := mysql.ParseDatetime(s)
		return d.GetMysqlTime().Compare(dt), err
	case KindMysqlDuration:
		dur, err := mysql.ParseDuration(s, mysql.MaxFsp)
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
		fVal, err := parseFloat(s)
		if err != nil {
			return 0, err
		}
		return d.compareFloat64(fVal)
	}
}

func (d *Datum) compareBytes(b []byte) (int, error) {
	return d.compareString(hack.String(b))
}

func (d *Datum) compareMysqlBit(bit mysql.Bit) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), bit.ToString()), nil
	default:
		return d.compareFloat64(bit.ToNumber())
	}
}

func (d *Datum) compareMysqlDecimal(dec mysql.Decimal) (int, error) {
	switch d.k {
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Cmp(dec), nil
	case KindString, KindBytes:
		dDec, err := mysql.ParseDecimal(d.GetString())
		return dDec.Cmp(dec), err
	default:
		fVal, _ := dec.Float64()
		return d.compareFloat64(fVal)
	}
}

func (d *Datum) compareMysqlDuration(dur mysql.Duration) (int, error) {
	switch d.k {
	case KindMysqlDuration:
		return d.GetMysqlDuration().Compare(dur), nil
	case KindString, KindBytes:
		dDur, err := mysql.ParseDuration(d.GetString(), mysql.MaxFsp)
		return dDur.Compare(dur), err
	default:
		return d.compareFloat64(dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(enum mysql.Enum) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), enum.String()), nil
	default:
		return d.compareFloat64(enum.ToNumber())
	}
}

func (d *Datum) compareMysqlHex(e mysql.Hex) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), e.ToString()), nil
	default:
		return d.compareFloat64(e.ToNumber())
	}
}

func (d *Datum) compareMysqlSet(set mysql.Set) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), set.String()), nil
	default:
		return d.compareFloat64(set.ToNumber())
	}
}

func (d *Datum) compareMysqlTime(time mysql.Time) (int, error) {
	switch d.k {
	case KindString, KindBytes:
		dt, err := mysql.ParseDatetime(d.GetString())
		return dt.Compare(time), err
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(time), nil
	default:
		fVal, _ := time.ToNumber().Float64()
		return d.compareFloat64(fVal)
	}
}

func (d *Datum) compareRow(row []Datum) (int, error) {
	var dRow []Datum
	if d.k == KindRow {
		dRow = d.GetRow()
	} else {
		dRow = []Datum{*d}
	}
	for i := 0; i < len(row) && i < len(dRow); i++ {
		cmp, err := dRow[i].CompareDatum(row[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return CompareInt64(int64(len(dRow)), int64(len(row))), nil
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
