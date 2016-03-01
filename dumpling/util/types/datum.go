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

	"github.com/ngaut/log"
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

// SetBytes sets bytes value to datum, if copy is true,
// creates a copy.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
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

func (d *Datum) compareDatum(ad Datum) int {
	switch ad.k {
	case KindNull:
		if d.k == KindNull {
			return 0
		}
		return 1
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
		return 0
	}
}

func (d *Datum) compareInt64(i int64) int {
	switch d.k {
	case KindInt64:
		return CompareInt64(d.i, i)
	case KindUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1
		}
		return CompareInt64(d.i, i)
	default:
		return d.compareFloat64(float64(i))
	}
}

func (d *Datum) compareUint64(u uint64) int {
	switch d.k {
	case KindInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1
		}
		return CompareInt64(d.i, int64(u))
	case KindUint64:
		return CompareUint64(d.GetUint64(), u)
	default:
		return d.compareFloat64(float64(u))
	}
}

func (d *Datum) compareFloat64(f float64) int {
	switch d.k {
	case KindNull:
		return -1
	case KindInt64:
		return CompareFloat64(float64(d.i), f)
	case KindUint64:
		return CompareFloat64(float64(d.GetUint64()), f)
	case KindFloat32, KindFloat64:
		return CompareFloat64(d.GetFloat64(), f)
	case KindString, KindBytes:
		fVal := parseFloatLoose(d.GetString())
		return CompareFloat64(fVal, f)
	case KindMysqlBit:
		fVal := d.GetMysqlBit().ToNumber()
		return CompareFloat64(fVal, f)
	case KindMysqlDecimal:
		fVal, _ := d.GetMysqlDecimal().Float64()
		return CompareFloat64(fVal, f)
	case KindMysqlDuration:
		fVal := d.GetMysqlDuration().Seconds()
		return CompareFloat64(fVal, f)
	case KindMysqlEnum:
		fVal := d.GetMysqlEnum().ToNumber()
		return CompareFloat64(fVal, f)
	case KindMysqlHex:
		fVal := d.GetMysqlHex().ToNumber()
		return CompareFloat64(fVal, f)
	case KindMysqlSet:
		fVal := d.GetMysqlSet().ToNumber()
		return CompareFloat64(fVal, f)
	case KindMysqlTime:
		fVal, _ := d.GetMysqlTime().ToNumber().Float64()
		return CompareFloat64(fVal, f)
	default:
		return -1
	}
}

func numPart(s string) string {
	s = strings.TrimSpace(s)
	var hasDot bool
	var i int
	for i = 0; i < len(s); i++ {
		if s[i] == '.' {
			if hasDot {
				break
			}
			hasDot = true
			continue
		}
		if s[i] < '0' && s[i] > '9' {
			break
		}
	}
	if i == 0 {
		return "0"
	}
	return s[:i]
}

func parseFloatLoose(s string) float64 {
	s = numPart(s)
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Errorf("should not happen!")
	}
	return val
}

func (d *Datum) compareString(s string) int {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), s)
	case KindMysqlDecimal:
		s = numPart(s)
		dec, err := mysql.ParseDecimal(s)
		if err != nil {
			log.Errorf("should not happen!")
		}
		return d.GetMysqlDecimal().Cmp(dec)
	case KindMysqlTime:
		dt, _ := mysql.ParseDatetime(s)
		return d.GetMysqlTime().Compare(dt)
	case KindMysqlDuration:
		dur, _ := mysql.ParseDuration(s, mysql.MaxFsp)
		return d.GetMysqlDuration().Compare(dur)
	case KindMysqlBit:
		return CompareString(d.GetMysqlBit().ToString(), s)
	case KindMysqlHex:
		return CompareString(d.GetMysqlHex().ToString(), s)
	case KindMysqlSet:
		return CompareString(d.GetMysqlSet().String(), s)
	case KindMysqlEnum:
		return CompareString(d.GetMysqlEnum().String(), s)
	default:
		return d.compareFloat64(parseFloatLoose(s))
	}
}

func (d *Datum) compareBytes(b []byte) int {
	return d.compareString(hack.String(b))
}

func (d *Datum) compareMysqlBit(bit mysql.Bit) int {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), bit.ToString())
	default:
		return d.compareFloat64(bit.ToNumber())
	}
}

func (d *Datum) compareMysqlDecimal(dec mysql.Decimal) int {
	switch d.k {
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Cmp(dec)
	case KindString, KindBytes:
		s := numPart(d.GetString())
		dDec, err := mysql.ParseDecimal(s)
		if err != nil {
			log.Errorf("should not happen!")
		}
		return dDec.Cmp(dec)
	default:
		fVal, _ := dec.Float64()
		return d.compareFloat64(fVal)
	}
}

func (d *Datum) compareMysqlDuration(dur mysql.Duration) int {
	switch d.k {
	case KindMysqlDuration:
		return d.GetMysqlDuration().Compare(dur)
	case KindString, KindBytes:
		dDur, _ := mysql.ParseDuration(d.GetString(), mysql.MaxFsp)
		return dDur.Compare(dur)
	default:
		return d.compareFloat64(dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(enum mysql.Enum) int {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), enum.String())
	default:
		return d.compareFloat64(enum.ToNumber())
	}
}

func (d *Datum) compareMysqlHex(e mysql.Hex) int {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), e.ToString())
	default:
		return d.compareFloat64(e.ToNumber())
	}
}

func (d *Datum) compareMysqlSet(set mysql.Set) int {
	switch d.k {
	case KindString, KindBytes:
		return CompareString(d.GetString(), set.String())
	default:
		return d.compareFloat64(set.ToNumber())
	}
}

func (d *Datum) compareMysqlTime(time mysql.Time) int {
	switch d.k {
	case KindString, KindBytes:
		dt, _ := mysql.ParseDatetime(d.GetString())
		return dt.Compare(time)
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(time)
	default:
		fVal, _ := time.ToNumber().Float64()
		return d.compareFloat64(fVal)
	}
}

func (d *Datum) compareRow(row []Datum) int {
	var dRow []Datum
	if d.k == KindRow {
		dRow = d.GetRow()
	} else {
		dRow = []Datum{*d}
	}
	for i := 0; i < len(row) && i < len(dRow); i++ {
		cmp := dRow[i].compareDatum(row[i])
		if cmp != 0 {
			return cmp
		}
	}
	return CompareInt64(int64(len(dRow)), int64(len(row)))
}

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in interface{}) (d Datum) {
	switch x := in.(type) {
	case []interface{}:
		var row []Datum
		for _, v := range x {
			row = append(row, NewDatum(v))
		}
		d.SetValue(row)
	default:
		d.SetValue(in)
	}
	return d
}
