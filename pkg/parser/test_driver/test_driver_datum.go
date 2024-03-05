// Copyright 2019 PingCAP, Inc.
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

//go:build !codes
// +build !codes

package test_driver

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
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
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k byte        // datum kind.
	i int64       // i can hold int64 uint64 float64 values.
	b []byte      // b can hold string or []byte values.
	x interface{} // x hold all other types.
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
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
	return string(d.b)
}

// SetString sets string value.
func (d *Datum) SetString(s string) {
	d.k = KindString
	d.b = []byte(s)
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

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// SetBinaryLiteral sets Bit value
func (d *Datum) SetBinaryLiteral(b BinaryLiteral) {
	d.k = KindBinaryLiteral
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
	case KindMysqlDecimal:
		return d.GetMysqlDecimal()
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral()
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
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	default:
		d.SetInterface(x)
	}
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

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...interface{}) []Datum {
	datums := make([]Datum, len(args))
	for i, v := range args {
		datums[i] = NewDatum(v)
	}
	return datums
}

// BinaryLiteral is the internal type for storing bit / hex literal type.
type BinaryLiteral []byte

// BitLiteral is the bit literal type.
type BitLiteral BinaryLiteral

// HexLiteral is the hex literal type.
type HexLiteral BinaryLiteral

// ZeroBinaryLiteral is a BinaryLiteral literal with zero value.
var ZeroBinaryLiteral = BinaryLiteral{}

// String implements fmt.Stringer interface.
func (b BinaryLiteral) String() string {
	if len(b) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

// ToString returns the string representation for the literal.
func (b BinaryLiteral) ToString() string {
	return string(b)
}

// ToBitLiteralString returns the bit literal representation for the literal.
func (b BinaryLiteral) ToBitLiteralString(trimLeadingZero bool) string {
	if len(b) == 0 {
		return "b''"
	}
	var buf bytes.Buffer
	for _, data := range b {
		fmt.Fprintf(&buf, "%08b", data)
	}
	ret := buf.Bytes()
	if trimLeadingZero {
		ret = bytes.TrimLeft(ret, "0")
		if len(ret) == 0 {
			ret = []byte{'0'}
		}
	}
	return fmt.Sprintf("b'%s'", string(ret))
}

// ParseBitStr parses bit string.
// The string format can be b'val', B'val' or 0bval, val must be 0 or 1.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-value-literals.html
func ParseBitStr(s string) (BinaryLiteral, error) {
	if len(s) == 0 {
		return nil, errors.Errorf("invalid empty string for parsing bit type")
	}

	if s[0] == 'b' || s[0] == 'B' {
		// format is b'val' or B'val'
		s = strings.Trim(s[1:], "'")
	} else if strings.HasPrefix(s, "0b") {
		s = s[2:]
	} else {
		// here means format is not b'val', B'val' or 0bval.
		return nil, errors.Errorf("invalid bit type format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinaryLiteral, nil
	}

	alignedLength := (len(s) + 7) &^ 7
	s = ("00000000" + s)[len(s)+8-alignedLength:] // Pad with zero (slice from `-alignedLength`)
	byteLength := len(s) >> 3
	buf := make([]byte, byteLength)

	for i := 0; i < byteLength; i++ {
		strPosition := i << 3
		val, err := strconv.ParseUint(s[strPosition:strPosition+8], 2, 8)
		if err != nil {
			return nil, errors.Trace(err)
		}
		buf[i] = byte(val)
	}

	return buf, nil
}

// NewBitLiteral parses bit string as BitLiteral type.
func NewBitLiteral(s string) (BitLiteral, error) {
	b, err := ParseBitStr(s)
	if err != nil {
		return BitLiteral{}, err
	}
	return BitLiteral(b), nil
}

// ToString implement ast.BinaryLiteral interface
func (b BitLiteral) ToString() string {
	return BinaryLiteral(b).ToString()
}

// ParseHexStr parses hexadecimal string literal.
// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func ParseHexStr(s string) (BinaryLiteral, error) {
	if len(s) == 0 {
		return nil, errors.Errorf("invalid empty string for parsing hexadecimal literal")
	}

	if s[0] == 'x' || s[0] == 'X' {
		// format is x'val' or X'val'
		s = strings.Trim(s[1:], "'")
		if len(s)%2 != 0 {
			return nil, errors.Errorf("invalid hexadecimal format, must even numbers, but %d", len(s))
		}
	} else if strings.HasPrefix(s, "0x") {
		s = s[2:]
	} else {
		// here means format is not x'val', X'val' or 0xval.
		return nil, errors.Errorf("invalid hexadecimal format %s", s)
	}

	if len(s) == 0 {
		return ZeroBinaryLiteral, nil
	}

	if len(s)%2 != 0 {
		s = "0" + s
	}
	buf, err := hex.DecodeString(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf, nil
}

// NewHexLiteral parses hexadecimal string as HexLiteral type.
func NewHexLiteral(s string) (HexLiteral, error) {
	h, err := ParseHexStr(s)
	if err != nil {
		return HexLiteral{}, err
	}
	return HexLiteral(h), nil
}

// ToString implement ast.BinaryLiteral interface
func (b HexLiteral) ToString() string {
	return BinaryLiteral(b).ToString()
}

// SetBinChsClnFlag sets charset, collation as 'binary' and adds binaryFlag to FieldType.
func SetBinChsClnFlag(ft *types.FieldType) {
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	ft.AddFlag(mysql.BinaryFlag)
}

// DefaultFsp is the default digit of fractional seconds part.
// MySQL use 0 as the default Fsp.
const DefaultFsp = int8(0)

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value interface{}, tp *types.FieldType, charset string, collate string) {
	switch x := value.(type) {
	case nil:
		tp.SetType(mysql.TypeNull)
		tp.SetFlen(0)
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case bool:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(1)
		tp.SetDecimal(0)
		tp.AddFlag(mysql.IsBooleanFlag)
		SetBinChsClnFlag(tp)
	case int:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(StrLenOfInt64Fast(int64(x)))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case int64:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(StrLenOfInt64Fast(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case uint64:
		tp.SetType(mysql.TypeLonglong)
		tp.AddFlag(mysql.UnsignedFlag)
		tp.SetFlen(StrLenOfUint64Fast(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case string:
		tp.SetType(mysql.TypeVarString)
		// TODO: tp.flen should be len(x) * 3 (max bytes length of CharsetUTF8)
		tp.SetFlen(len(x))
		tp.SetDecimal(types.UnspecifiedLength)
		tp.SetCharset(charset)
		tp.SetCollate(collate)
	case float32:
		tp.SetType(mysql.TypeFloat)
		s := strconv.FormatFloat(float64(x), 'f', -1, 32)
		tp.SetFlen(len(s))
		tp.SetDecimal(types.UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case float64:
		tp.SetType(mysql.TypeDouble)
		s := strconv.FormatFloat(x, 'f', -1, 64)
		tp.SetFlen(len(s))
		tp.SetDecimal(types.UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case []byte:
		tp.SetType(mysql.TypeBlob)
		tp.SetFlen(len(x))
		tp.SetDecimal(types.UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case BitLiteral:
		tp.SetType(mysql.TypeVarString)
		tp.SetFlen(len(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case HexLiteral:
		tp.SetType(mysql.TypeVarString)
		tp.SetFlen(len(x) * 3)
		tp.SetDecimal(0)
		tp.AddFlag(mysql.UnsignedFlag)
		SetBinChsClnFlag(tp)
	case BinaryLiteral:
		tp.SetType(mysql.TypeBit)
		tp.SetFlen(len(x) * 8)
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
		tp.DelFlag(mysql.BinaryFlag)
		tp.AddFlag(mysql.UnsignedFlag)
	case *MyDecimal:
		tp.SetType(mysql.TypeNewDecimal)
		tp.SetFlen(len(x.ToString()))
		tp.SetDecimal(int(x.digitsFrac))
		SetBinChsClnFlag(tp)
	default:
		tp.SetType(mysql.TypeUnspecified)
		tp.SetFlen(types.UnspecifiedLength)
		tp.SetDecimal(types.UnspecifiedLength)
	}
}
