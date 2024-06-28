// Copyright 2017 PingCAP, Inc.
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
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

/*
   The binary JSON format from MySQL 5.7 is as follows:

   JSON doc ::= type value
   type ::=
       0x01 |       // large JSON object
       0x03 |       // large JSON array
       0x04 |       // literal (true/false/null)
       0x05 |       // int16
       0x06 |       // uint16
       0x07 |       // int32
       0x08 |       // uint32
       0x09 |       // int64
       0x0a |       // uint64
       0x0b |       // double
       0x0c |       // utf8mb4 string
       0x0d |       // opaque value
       0x0e |       // date
       0x0f |       // datetime
       0x10 |       // timestamp
       0x11 |       // time

   value ::=
       object  |
       array   |
       literal |
       number  |
       string  |
       opaque  |
       time    |
       duration |

   object ::= element-count size key-entry* value-entry* key* value*

   array ::= element-count size value-entry* value*

   // number of members in object or number of elements in array
   element-count ::= uint32

   // number of bytes in the binary representation of the object or array
   size ::= uint32

   key-entry ::= key-offset key-length

   key-offset ::= uint32

   key-length ::= uint16    // key length must be less than 64KB

   value-entry ::= type offset-or-inlined-value

   // This field holds either the offset to where the value is stored,
   // or the value itself if it is small enough to be inlined (that is,
   // if it is a JSON literal or a small enough [u]int).
   offset-or-inlined-value ::= uint32

   key ::= utf8mb4-data

   literal ::=
       0x00 |   // JSON null literal
       0x01 |   // JSON true literal
       0x02 |   // JSON false literal

   number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
                       // double is stored in a platform-independent, eight-byte
                       // format using float8store()

   string ::= data-length utf8mb4-data

   data-length ::= uint8*    // If the high bit of a byte is 1, the length
                             // field is continued in the next byte,
                             // otherwise it is the last byte of the length
                             // field. So we need 1 byte to represent
                             // lengths up to 127, 2 bytes to represent
                             // lengths up to 16383, and so on...

   opaque ::= typeId data-length byte*

   time ::= uint64

   duration ::= uint64 uint32

   typeId ::= byte
*/

var jsonZero = CreateBinaryJSON(uint64(0))

const maxJSONDepth = 100

// BinaryJSON represents a binary encoded JSON object.
// It can be randomly accessed without deserialization.
type BinaryJSON struct {
	TypeCode JSONTypeCode
	Value    []byte
}

// String implements fmt.Stringer interface.
func (bj BinaryJSON) String() string {
	out, err := bj.MarshalJSON()
	terror.Log(err)
	return string(out)
}

// Copy makes a copy of the BinaryJSON
func (bj BinaryJSON) Copy() BinaryJSON {
	buf := make([]byte, len(bj.Value))
	copy(buf, bj.Value)
	return BinaryJSON{TypeCode: bj.TypeCode, Value: buf}
}

// MarshalJSON implements the json.Marshaler interface.
func (bj BinaryJSON) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, len(bj.Value)*3/2)
	return bj.marshalTo(buf)
}

func (bj BinaryJSON) marshalTo(buf []byte) ([]byte, error) {
	switch bj.TypeCode {
	case JSONTypeCodeOpaque:
		return jsonMarshalOpaqueTo(buf, bj.GetOpaque()), nil
	case JSONTypeCodeString:
		return jsonMarshalStringTo(buf, bj.GetString()), nil
	case JSONTypeCodeLiteral:
		return jsonMarshalLiteralTo(buf, bj.Value[0]), nil
	case JSONTypeCodeInt64:
		return strconv.AppendInt(buf, bj.GetInt64(), 10), nil
	case JSONTypeCodeUint64:
		return strconv.AppendUint(buf, bj.GetUint64(), 10), nil
	case JSONTypeCodeFloat64:
		return bj.marshalFloat64To(buf)
	case JSONTypeCodeArray:
		return bj.marshalArrayTo(buf)
	case JSONTypeCodeObject:
		return bj.marshalObjTo(buf)
	case JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp:
		return jsonMarshalTimeTo(buf, bj.GetTime()), nil
	case JSONTypeCodeDuration:
		return jsonMarshalDurationTo(buf, bj.GetDuration()), nil
	}
	return buf, nil
}

// IsZero return a boolean indicate whether BinaryJSON is Zero
func (bj BinaryJSON) IsZero() bool {
	// This behavior is different on MySQL 5.7 and 8.0
	//
	// In MySQL 5.7, most of these non-integer values are 0, and return a warning:
	// "Invalid JSON value for CAST to INTEGER from column j"
	//
	// In MySQL 8, most of these non-integer values are not zero, with a warning:
	// > "Evaluating a JSON value in SQL boolean context does an implicit comparison
	// > against JSON integer 0; if this is not what you want, consider converting
	// > JSON to a SQL numeric type with JSON_VALUE RETURNING"
	//
	// TODO: return a warning as MySQL 8 does

	return CompareBinaryJSON(bj, jsonZero) == 0
}

// GetInt64 gets the int64 value.
func (bj BinaryJSON) GetInt64() int64 {
	return int64(jsonEndian.Uint64(bj.Value))
}

// GetUint64 gets the uint64 value.
func (bj BinaryJSON) GetUint64() uint64 {
	return jsonEndian.Uint64(bj.Value)
}

// GetFloat64 gets the float64 value.
func (bj BinaryJSON) GetFloat64() float64 {
	return math.Float64frombits(bj.GetUint64())
}

// GetString gets the string value.
func (bj BinaryJSON) GetString() []byte {
	strLen, lenLen := binary.Uvarint(bj.Value)
	return bj.Value[lenLen : lenLen+int(strLen)]
}

// Opaque represents a raw binary type
type Opaque struct {
	// TypeCode is the same with database type code
	TypeCode byte
	// Buf is the underlying bytes of the data
	Buf []byte
}

// GetOpaque gets the opaque value
func (bj BinaryJSON) GetOpaque() Opaque {
	typ := bj.Value[0]

	strLen, lenLen := binary.Uvarint(bj.Value[1:])
	bufStart := lenLen + 1
	return Opaque{
		TypeCode: typ,
		Buf:      bj.Value[bufStart : bufStart+int(strLen)],
	}
}

// GetTime gets the time value with default fsp
//
// Deprecated: use GetTimeWithFsp instead. The `BinaryJSON` doesn't contain the fsp information, so the caller
// should always provide the fsp.
func (bj BinaryJSON) GetTime() Time {
	return bj.GetTimeWithFsp(DefaultFsp)
}

// GetTimeWithFsp gets the time value with given fsp
func (bj BinaryJSON) GetTimeWithFsp(fsp int) Time {
	coreTime := CoreTime(bj.GetUint64())

	tp := mysql.TypeDate
	if bj.TypeCode == JSONTypeCodeDatetime {
		tp = mysql.TypeDatetime
	} else if bj.TypeCode == JSONTypeCodeTimestamp {
		tp = mysql.TypeTimestamp
	}

	return NewTime(coreTime, tp, fsp)
}

// GetDuration gets the duration value
func (bj BinaryJSON) GetDuration() Duration {
	return Duration{
		time.Duration(bj.GetInt64()),
		int(jsonEndian.Uint32(bj.Value[8:])),
	}
}

// GetOpaqueFieldType returns the type of opaque value
func (bj BinaryJSON) GetOpaqueFieldType() byte {
	return bj.Value[0]
}

// GetKeys gets the keys of the object
func (bj BinaryJSON) GetKeys() BinaryJSON {
	count := bj.GetElemCount()
	ret := make([]BinaryJSON, 0, count)
	for i := 0; i < count; i++ {
		ret = append(ret, CreateBinaryJSON(string(bj.objectGetKey(i))))
	}
	return buildBinaryJSONArray(ret)
}

// GetElemCount gets the count of Object or Array.
func (bj BinaryJSON) GetElemCount() int {
	return int(jsonEndian.Uint32(bj.Value))
}

// ArrayGetElem gets the element of the index `idx`.
func (bj BinaryJSON) ArrayGetElem(idx int) BinaryJSON {
	return bj.valEntryGet(headerSize + idx*valEntrySize)
}

func (bj BinaryJSON) objectGetKey(i int) []byte {
	keyOff := int(jsonEndian.Uint32(bj.Value[headerSize+i*keyEntrySize:]))
	keyLen := int(jsonEndian.Uint16(bj.Value[headerSize+i*keyEntrySize+keyLenOff:]))
	return bj.Value[keyOff : keyOff+keyLen]
}

func (bj BinaryJSON) objectGetVal(i int) BinaryJSON {
	elemCount := bj.GetElemCount()
	return bj.valEntryGet(headerSize + elemCount*keyEntrySize + i*valEntrySize)
}

func (bj BinaryJSON) valEntryGet(valEntryOff int) BinaryJSON {
	tpCode := bj.Value[valEntryOff]
	valOff := jsonEndian.Uint32(bj.Value[valEntryOff+valTypeSize:])
	switch tpCode {
	case JSONTypeCodeLiteral:
		return BinaryJSON{TypeCode: JSONTypeCodeLiteral, Value: bj.Value[valEntryOff+valTypeSize : valEntryOff+valTypeSize+1]}
	case JSONTypeCodeUint64, JSONTypeCodeInt64, JSONTypeCodeFloat64:
		return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+8]}
	case JSONTypeCodeString:
		strLen, lenLen := binary.Uvarint(bj.Value[valOff:])
		totalLen := uint32(lenLen) + uint32(strLen)
		return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+totalLen]}
	case JSONTypeCodeOpaque:
		strLen, lenLen := binary.Uvarint(bj.Value[valOff+1:])
		totalLen := 1 + uint32(lenLen) + uint32(strLen)
		return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+totalLen]}
	case JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp:
		return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+8]}
	case JSONTypeCodeDuration:
		return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+12]}
	}
	dataSize := jsonEndian.Uint32(bj.Value[valOff+dataSizeOff:])
	return BinaryJSON{TypeCode: tpCode, Value: bj.Value[valOff : valOff+dataSize]}
}

func (bj BinaryJSON) marshalFloat64To(buf []byte) ([]byte, error) {
	// NOTE: copied from Go standard library.
	f := bj.GetFloat64()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return buf, &json.UnsupportedValueError{Str: strconv.FormatFloat(f, 'g', -1, 64)}
	}

	// Convert as if by ES6 number to string conversion.
	// This matches most other JSON generators.
	// See golang.org/issue/6384 and golang.org/issue/14135.
	// Like fmt %g, but the exponent cutoffs are different
	// and exponents themselves are not padded to two digits.
	abs := math.Abs(f)
	ffmt := byte('f')
	// Note: Must use float32 comparisons for underlying float32 value to get precise cutoffs right.
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			ffmt = 'e'
		}
	}
	buf = strconv.AppendFloat(buf, f, ffmt, -1, 64)
	if ffmt == 'e' {
		// clean up e-09 to e-9
		n := len(buf)
		if n >= 4 && buf[n-4] == 'e' && buf[n-3] == '-' && buf[n-2] == '0' {
			buf[n-2] = buf[n-1]
			buf = buf[:n-1]
		}
	}
	return buf, nil
}

func (bj BinaryJSON) marshalArrayTo(buf []byte) ([]byte, error) {
	elemCount := int(jsonEndian.Uint32(bj.Value))
	buf = append(buf, '[')
	for i := 0; i < elemCount; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		var err error
		buf, err = bj.ArrayGetElem(i).marshalTo(buf)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return append(buf, ']'), nil
}

func (bj BinaryJSON) marshalObjTo(buf []byte) ([]byte, error) {
	elemCount := int(jsonEndian.Uint32(bj.Value))
	buf = append(buf, '{')
	for i := 0; i < elemCount; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		buf = jsonMarshalStringTo(buf, bj.objectGetKey(i))
		buf = append(buf, ": "...)
		var err error
		buf, err = bj.objectGetVal(i).marshalTo(buf)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return append(buf, '}'), nil
}

func jsonMarshalStringTo(buf, s []byte) []byte {
	// NOTE: copied from Go standard library.
	// NOTE: keep in sync with string above.
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if jsonSafeSet[b] {
				i++
				continue
			}
			if start < i {
				buf = append(buf, s[start:i]...)
			}
			switch b {
			case '\\', '"':
				buf = append(buf, '\\', b)
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				buf = append(buf, `\u00`...)
				buf = append(buf, jsonHexChars[b>>4], jsonHexChars[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRune(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf = append(buf, s[start:i]...)
			}
			buf = append(buf, `\ufffd`...)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				buf = append(buf, s[start:i]...)
			}
			buf = append(buf, `\u202`...)
			buf = append(buf, jsonHexChars[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf = append(buf, s[start:]...)
	}
	buf = append(buf, '"')
	return buf
}

// opaque value will yield "base64:typeXX:<base64 encoded string>"
func jsonMarshalOpaqueTo(buf []byte, opaque Opaque) []byte {
	b64 := base64.StdEncoding.EncodeToString(opaque.Buf)
	output := fmt.Sprintf(`"base64:type%d:%s"`, opaque.TypeCode, b64)

	// as the base64 string is simple and predictable, it could be appended
	// to the buf directly.
	buf = append(buf, output...)
	return buf
}

func jsonMarshalLiteralTo(b []byte, litType byte) []byte {
	switch litType {
	case JSONLiteralFalse:
		return append(b, "false"...)
	case JSONLiteralTrue:
		return append(b, "true"...)
	case JSONLiteralNil:
		return append(b, "null"...)
	}
	return b
}

func jsonMarshalTimeTo(buf []byte, time Time) []byte {
	// printing json datetime/duration will always keep 6 fsp
	time.SetFsp(6)
	buf = append(buf, []byte(quoteJSONString(time.String()))...)
	return buf
}

func jsonMarshalDurationTo(buf []byte, duration Duration) []byte {
	// printing json datetime/duration will always keep 6 fsp
	duration.Fsp = 6
	buf = append(buf, []byte(quoteJSONString(duration.String()))...)
	return buf
}

// ParseBinaryJSONFromString parses a json from string.
func ParseBinaryJSONFromString(s string) (bj BinaryJSON, err error) {
	if len(s) == 0 {
		err = ErrInvalidJSONText.GenWithStackByArgs("The document is empty")
		return
	}
	data := hack.Slice(s)
	if !json.Valid(data) {
		err = ErrInvalidJSONText.GenWithStackByArgs("The document root must not be followed by other values.")
		return
	}
	if err = bj.UnmarshalJSON(data); err != nil && !ErrJSONObjectKeyTooLong.Equal(err) {
		err = ErrInvalidJSONText.GenWithStackByArgs(err)
	}
	return
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (bj *BinaryJSON) UnmarshalJSON(data []byte) error {
	var decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var in any
	err := decoder.Decode(&in)
	if err != nil {
		return errors.Trace(err)
	}
	newBj, err := CreateBinaryJSONWithCheck(in)
	if err != nil {
		return errors.Trace(err)
	}
	bj.TypeCode = newBj.TypeCode
	bj.Value = newBj.Value
	return nil
}

func getInt64FractionLength(i int64) int {
	absInt := uint64(0)
	if i < 0 {
		absInt = uint64(-i)
	} else {
		absInt = uint64(i)
	}
	return getUint64FractionLength(absInt)
}

func getUint64FractionLength(i uint64) int {
	lz := bits.LeadingZeros64(i)
	tz := bits.TrailingZeros64(i)
	// 64 bit removes the leading zero, removes the trailing zero and also removes the first "1".
	fraction := 64 - lz - tz - 1
	if lz == 64 && tz == 64 {
		fraction = 0
	}

	return fraction
}

// HashValue converts certain JSON values for aggregate comparisons.
// For example int64(3) == float64(3.0)
// Other than the numeric condition, this function has to construct a bidirectional map between hash value
// and the original representation
func (bj BinaryJSON) HashValue(buf []byte) []byte {
	switch bj.TypeCode {
	case JSONTypeCodeInt64:
		// Convert to a FLOAT if no precision is lost.
		// In the future, it will be better to convert to a DECIMAL value instead
		// See: https://github.com/pingcap/tidb/issues/9988

		// A double precision float can have 52-bit in fraction part.
		if getInt64FractionLength(bj.GetInt64()) <= 52 {
			buf = append(buf, JSONTypeCodeFloat64)
			buf = appendBinaryFloat64(buf, float64(bj.GetInt64()))
		} else {
			buf = append(buf, bj.TypeCode)
			buf = append(buf, bj.Value...)
		}
	case JSONTypeCodeUint64:
		// A double precision float can have 52-bit in fraction part.
		if getUint64FractionLength(bj.GetUint64()) <= 52 {
			buf = append(buf, JSONTypeCodeFloat64)
			buf = appendBinaryFloat64(buf, float64(bj.GetUint64()))
		} else {
			buf = append(buf, bj.TypeCode)
			buf = append(buf, bj.Value...)
		}
	case JSONTypeCodeArray:
		// this hash value is bidirectional, because you can get the element one-by-one
		// and you know the end of it, as the elemCount is also appended here
		buf = append(buf, bj.TypeCode)
		elemCount := int(jsonEndian.Uint32(bj.Value))
		buf = append(buf, bj.Value[0:dataSizeOff]...)
		for i := 0; i < elemCount; i++ {
			buf = bj.ArrayGetElem(i).HashValue(buf)
		}
	case JSONTypeCodeObject:
		// this hash value is bidirectional, because you can get the key using the json
		// string format, and get the value accordingly.
		buf = append(buf, bj.TypeCode)
		elemCount := int(jsonEndian.Uint32(bj.Value))
		buf = append(buf, bj.Value[0:dataSizeOff]...)
		for i := 0; i < elemCount; i++ {
			keyJSON := CreateBinaryJSON(string(bj.objectGetKey(i)))
			buf = append(buf, keyJSON.Value...)
			buf = bj.objectGetVal(i).HashValue(buf)
		}
	default:
		buf = append(buf, bj.TypeCode)
		buf = append(buf, bj.Value...)
	}
	return buf
}

// GetValue return the primitive value of the JSON.
func (bj BinaryJSON) GetValue() any {
	switch bj.TypeCode {
	case JSONTypeCodeInt64:
		return bj.GetInt64()
	case JSONTypeCodeUint64:
		return bj.GetUint64()
	case JSONTypeCodeDuration:
		return bj.GetDuration()
	case JSONTypeCodeFloat64:
		return bj.GetFloat64()
	case JSONTypeCodeString:
		return bj.GetString()
	case JSONTypeCodeDate, JSONTypeCodeDatetime:
		return bj.GetTime()
	}
	logutil.BgLogger().Error("unreachable JSON type", zap.Any("type", bj.TypeCode))
	return nil
}

// CreateBinaryJSON creates a BinaryJSON from interface.
func CreateBinaryJSON(in any) BinaryJSON {
	bj, err := CreateBinaryJSONWithCheck(in)
	if err != nil {
		panic(err)
	}
	return bj
}

// CreateBinaryJSONWithCheck creates a BinaryJSON from interface with error check.
func CreateBinaryJSONWithCheck(in any) (BinaryJSON, error) {
	typeCode, buf, err := appendBinaryJSON(nil, in)
	if err != nil {
		return BinaryJSON{}, err
	}
	bj := BinaryJSON{TypeCode: typeCode, Value: buf}
	// GetElemDepth always returns +1.
	if bj.GetElemDepth()-1 > maxJSONDepth {
		return BinaryJSON{}, ErrJSONDocumentTooDeep
	}
	return bj, nil
}

func appendBinaryJSON(buf []byte, in any) (JSONTypeCode, []byte, error) {
	var typeCode byte
	var err error
	switch x := in.(type) {
	case nil:
		typeCode = JSONTypeCodeLiteral
		buf = append(buf, JSONLiteralNil)
	case bool:
		typeCode = JSONTypeCodeLiteral
		if x {
			buf = append(buf, JSONLiteralTrue)
		} else {
			buf = append(buf, JSONLiteralFalse)
		}
	case int64:
		typeCode = JSONTypeCodeInt64
		buf = appendBinaryUint64(buf, uint64(x))
	case uint64:
		typeCode = JSONTypeCodeUint64
		buf = appendBinaryUint64(buf, x)
	case float64:
		typeCode = JSONTypeCodeFloat64
		buf = appendBinaryFloat64(buf, x)
	case json.Number:
		typeCode, buf, err = appendBinaryNumber(buf, x)
		if err != nil {
			return typeCode, nil, errors.Trace(err)
		}
	case string:
		typeCode = JSONTypeCodeString
		buf = appendBinaryString(buf, x)
	case BinaryJSON:
		typeCode = x.TypeCode
		buf = append(buf, x.Value...)
	case []any:
		typeCode = JSONTypeCodeArray
		buf, err = appendBinaryArray(buf, x)
		if err != nil {
			return typeCode, nil, errors.Trace(err)
		}
	case map[string]any:
		typeCode = JSONTypeCodeObject
		buf, err = appendBinaryObject(buf, x)
		if err != nil {
			return typeCode, nil, errors.Trace(err)
		}
	case Opaque:
		typeCode = JSONTypeCodeOpaque
		buf = appendBinaryOpaque(buf, x)
	case Time:
		typeCode = JSONTypeCodeDate
		if x.Type() == mysql.TypeDatetime {
			typeCode = JSONTypeCodeDatetime
		} else if x.Type() == mysql.TypeTimestamp {
			typeCode = JSONTypeCodeTimestamp
		}
		buf = appendBinaryUint64(buf, uint64(x.CoreTime()))
	case Duration:
		typeCode = JSONTypeCodeDuration
		buf = appendBinaryUint64(buf, uint64(x.Duration))
		buf = appendBinaryUint32(buf, uint32(x.Fsp))
	default:
		msg := fmt.Sprintf(unknownTypeErrorMsg, reflect.TypeOf(in))
		err = errors.New(msg)
	}
	return typeCode, buf, err
}

func appendZero(buf []byte, length int) []byte {
	var tmp [8]byte
	rem := length % 8
	loop := length / 8
	for i := 0; i < loop; i++ {
		buf = append(buf, tmp[:]...)
	}
	for i := 0; i < rem; i++ {
		buf = append(buf, 0)
	}
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	jsonEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendBinaryNumber(buf []byte, x json.Number) (JSONTypeCode, []byte, error) {
	// The type interpretation process is as follows:
	// - Attempt float64 if it contains Ee.
	// - Next attempt int64
	// - Then uint64 (valid in MySQL JSON, not in JSON decode library)
	// - Then float64
	// - Return an error
	if strings.Contains(x.String(), "Ee.") {
		f64, err := x.Float64()
		if err != nil {
			return JSONTypeCodeFloat64, nil, errors.Trace(err)
		}
		return JSONTypeCodeFloat64, appendBinaryFloat64(buf, f64), nil
	} else if val, err := x.Int64(); err == nil {
		return JSONTypeCodeInt64, appendBinaryUint64(buf, uint64(val)), nil
	} else if val, err := strconv.ParseUint(string(x), 10, 64); err == nil {
		return JSONTypeCodeUint64, appendBinaryUint64(buf, val), nil
	}
	val, err := x.Float64()
	if err == nil {
		return JSONTypeCodeFloat64, appendBinaryFloat64(buf, val), nil
	}
	var typeCode JSONTypeCode
	return typeCode, nil, errors.Trace(err)
}

func appendBinaryString(buf []byte, v string) []byte {
	begin := len(buf)
	buf = appendZero(buf, binary.MaxVarintLen64)
	lenLen := binary.PutUvarint(buf[begin:], uint64(len(v)))
	buf = buf[:len(buf)-binary.MaxVarintLen64+lenLen]
	buf = append(buf, v...)
	return buf
}

func appendBinaryOpaque(buf []byte, v Opaque) []byte {
	buf = append(buf, v.TypeCode)

	lenBegin := len(buf)
	buf = appendZero(buf, binary.MaxVarintLen64)
	lenLen := binary.PutUvarint(buf[lenBegin:], uint64(len(v.Buf)))

	buf = buf[:len(buf)-binary.MaxVarintLen64+lenLen]
	buf = append(buf, v.Buf...)
	return buf
}

func appendBinaryFloat64(buf []byte, v float64) []byte {
	off := len(buf)
	buf = appendZero(buf, 8)
	jsonEndian.PutUint64(buf[off:], math.Float64bits(v))
	return buf
}

func appendBinaryUint64(buf []byte, v uint64) []byte {
	off := len(buf)
	buf = appendZero(buf, 8)
	jsonEndian.PutUint64(buf[off:], v)
	return buf
}

func appendBinaryUint32(buf []byte, v uint32) []byte {
	off := len(buf)
	buf = appendZero(buf, 4)
	jsonEndian.PutUint32(buf[off:], v)
	return buf
}

func appendBinaryArray(buf []byte, array []any) ([]byte, error) {
	docOff := len(buf)
	buf = appendUint32(buf, uint32(len(array)))
	buf = appendZero(buf, dataSizeOff)
	valEntryBegin := len(buf)
	buf = appendZero(buf, len(array)*valEntrySize)
	for i, val := range array {
		var err error
		buf, err = appendBinaryValElem(buf, docOff, valEntryBegin+i*valEntrySize, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	docSize := len(buf) - docOff
	jsonEndian.PutUint32(buf[docOff+dataSizeOff:], uint32(docSize))
	return buf, nil
}

func appendBinaryValElem(buf []byte, docOff, valEntryOff int, val any) ([]byte, error) {
	var typeCode JSONTypeCode
	var err error
	elemDocOff := len(buf)
	typeCode, buf, err = appendBinaryJSON(buf, val)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if typeCode == JSONTypeCodeLiteral {
		litCode := buf[elemDocOff]
		buf = buf[:elemDocOff]
		buf[valEntryOff] = JSONTypeCodeLiteral
		buf[valEntryOff+1] = litCode
		return buf, nil
	}
	buf[valEntryOff] = typeCode
	valOff := elemDocOff - docOff
	jsonEndian.PutUint32(buf[valEntryOff+1:], uint32(valOff))
	return buf, nil
}

type field struct {
	key string
	val any
}

func appendBinaryObject(buf []byte, x map[string]any) ([]byte, error) {
	docOff := len(buf)
	buf = appendUint32(buf, uint32(len(x)))
	buf = appendZero(buf, dataSizeOff)
	keyEntryBegin := len(buf)
	buf = appendZero(buf, len(x)*keyEntrySize)
	valEntryBegin := len(buf)
	buf = appendZero(buf, len(x)*valEntrySize)

	fields := make([]field, 0, len(x))
	for key, val := range x {
		fields = append(fields, field{key: key, val: val})
	}
	slices.SortFunc(fields, func(i, j field) int {
		return cmp.Compare(i.key, j.key)
	})
	for i, field := range fields {
		keyEntryOff := keyEntryBegin + i*keyEntrySize
		keyOff := len(buf) - docOff
		keyLen := uint32(len(field.key))
		if keyLen > math.MaxUint16 {
			return nil, ErrJSONObjectKeyTooLong
		}
		jsonEndian.PutUint32(buf[keyEntryOff:], uint32(keyOff))
		jsonEndian.PutUint16(buf[keyEntryOff+keyLenOff:], uint16(keyLen))
		buf = append(buf, field.key...)
	}
	for i, field := range fields {
		var err error
		buf, err = appendBinaryValElem(buf, docOff, valEntryBegin+i*valEntrySize, field.val)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	docSize := len(buf) - docOff
	jsonEndian.PutUint32(buf[docOff+dataSizeOff:], uint32(docSize))
	return buf, nil
}
