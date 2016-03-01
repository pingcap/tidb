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

package ast

import (
	"math"

	"github.com/pingcap/tidb/util/hack"
)

// Kind constants.
const (
	KindNull  int = 0
	KindInt64 int = iota + 1
	KindUint64
	KindFloat64
	KindString
	KindBytes
	KindInterface
)

// Datum is a data box holds different kind of data.
// It has better performance and easier to use than `interface{}`.
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

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() interface{} {
	switch d.k {
	case KindInt64:
		return d.GetInt64()
	case KindUint64:
		return d.GetUint64()
	case KindFloat64:
		return d.GetFloat64()
	case KindString:
		return d.GetString()
	case KindBytes:
		return d.GetBytes()
	case KindInterface:
		return d.x
	default:
		return nil
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val interface{}) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x)
	case []byte:
		d.SetBytes(x)
	default:
		d.SetInterface(x)
	}
}
