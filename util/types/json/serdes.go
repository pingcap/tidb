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
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/hack"
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

   value ::=
       object  |
       array   |
       literal |
       number  |
       string  |

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
*/

const (
	typeCodeLen      int = 1
	compoundCountLen     = 4
	compoundSizeLen      = 4
	keyOffsetLen         = 4
	keyLengthLen         = 2
	valueInlineLen       = 4
)

// PeekBytesAsJSON trys to peek some bytes from b, until
// we can deserialize a JSON from those bytes.
func PeekBytesAsJSON(b []byte) (n int, err error) {
	if len(b) <= 0 {
		err = errors.New("Cant peek from empty bytes")
		return
	}
	switch c := TypeCode(b[0]); c {
	case TypeCodeObject, TypeCodeArray:
		if len(b) >= typeCodeLen+compoundCountLen+compoundSizeLen {
			var size uint32
			start := typeCodeLen + compoundCountLen
			end := typeCodeLen + compoundCountLen + compoundSizeLen
			err = binary.Read(bytes.NewReader(b[start:end]), binary.LittleEndian, &size)
			if err != nil {
				return n, errors.Trace(err)
			}
			n = int(size) + typeCodeLen
			return
		}
	case TypeCodeString:
		var size uint64
		reader := bytes.NewReader(b[typeCodeLen:])
		size, err = binary.ReadUvarint(reader)
		if err == nil {
			n = int(size) + int(reader.Size()) - reader.Len() + typeCodeLen
			return
		}
	case TypeCodeInt64, TypeCodeUint64, TypeCodeFloat64, TypeCodeLiteral:
		n = jsonTypeCodeLength[c] + typeCodeLen
		return
	}
	err = errors.New("Invalid JSON bytes")
	return
}

// Serialize means serialize itself into bytes.
func Serialize(j JSON) []byte {
	var buffer = new(bytes.Buffer)
	err := buffer.WriteByte(byte(j.TypeCode))
	terror.Log(errors.Trace(err))
	encode(j, buffer)
	return buffer.Bytes()
}

func encode(j JSON, buffer *bytes.Buffer) {
	switch j.TypeCode {
	case TypeCodeObject:
		encodeJSONObject(j.Object, buffer)
	case TypeCodeArray:
		encodeJSONArray(j.Array, buffer)
	case TypeCodeLiteral:
		encodeJSONLiteral(byte(j.I64), buffer)
	case TypeCodeInt64, TypeCodeUint64:
		encodeJSONInt64(j.I64, buffer)
	case TypeCodeFloat64:
		f64 := *(*float64)(unsafe.Pointer(&j.I64))
		encodeJSONFloat64(f64, buffer)
	case TypeCodeString:
		encodeJSONString(j.Str, buffer)
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, j.TypeCode)
		panic(msg)
	}
}

// Deserialize means deserialize a json from bytes.
func Deserialize(data []byte) (j JSON, err error) {
	return decode(data[0], data[1:])
}

func decode(typeCode byte, data []byte) (j JSON, err error) {
	j.TypeCode = TypeCode(typeCode)
	switch j.TypeCode {
	case TypeCodeObject:
		err = decodeJSONObject(&j.Object, data)
	case TypeCodeArray:
		err = decodeJSONArray(&j.Array, data)
	case TypeCodeLiteral:
		pbyte := (*byte)(unsafe.Pointer(&j.I64))
		err = decodeJSONLiteral(pbyte, data)
	case TypeCodeInt64, TypeCodeUint64:
		err = decodeJSONInt64(&j.I64, data)
	case TypeCodeFloat64:
		pfloat := (*float64)(unsafe.Pointer(&j.I64))
		err = decodeJSONFloat64(pfloat, data)
	case TypeCodeString:
		err = decodeJSONString(&j.Str, data)
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, typeCode)
		panic(msg)
	}
	return j, errors.Trace(err)
}

func encodeJSONLiteral(literal byte, buffer *bytes.Buffer) {
	err := buffer.WriteByte(literal)
	terror.Log(errors.Trace(err))
}

func decodeJSONLiteral(literal *byte, data []byte) error {
	var reader = bytes.NewReader(data)
	return binary.Read(reader, binary.LittleEndian, literal)
}

func encodeJSONInt64(i64 int64, buffer *bytes.Buffer) {
	err := binary.Write(buffer, binary.LittleEndian, i64)
	terror.Log(errors.Trace(err))
}

func decodeJSONInt64(i64 *int64, data []byte) error {
	var reader = bytes.NewReader(data)
	return binary.Read(reader, binary.LittleEndian, i64)
}

func encodeJSONFloat64(f64 float64, buffer *bytes.Buffer) {
	err := binary.Write(buffer, binary.LittleEndian, f64)
	terror.Log(errors.Trace(err))
}

func decodeJSONFloat64(f64 *float64, data []byte) error {
	var reader = bytes.NewReader(data)
	return binary.Read(reader, binary.LittleEndian, f64)
}

func encodeJSONString(s string, buffer *bytes.Buffer) {
	byteArray := hack.Slice(s)
	var varIntBuf = make([]byte, 9)
	var varIntLen = binary.PutUvarint(varIntBuf, uint64(len(byteArray)))
	_, err := buffer.Write(varIntBuf[0:varIntLen])
	terror.Log(errors.Trace(err))
	_, err = buffer.Write(byteArray)
	terror.Log(errors.Trace(err))
}

func decodeJSONString(s *string, data []byte) (err error) {
	var length uint64
	var reader = bytes.NewReader(data)
	length, err = binary.ReadUvarint(reader)
	if err == nil && length > 0 {
		var buf = make([]byte, length)
		_, err = reader.Read(buf)
		if err == nil {
			*s = hack.String(buf)
		}
	}
	return errors.Trace(err)
}

func encodeJSONObject(m map[string]JSON, buffer *bytes.Buffer) {
	// object ::= element-count size key-entry* value-entry* key* value*
	// key-entry ::= key-offset key-length
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = compoundCountLen + compoundSizeLen
	var keySlice = getSortedKeys(m)

	var keyEntrysLen = (keyOffsetLen + keyLengthLen) * len(m)
	var valueEntrysLen = (typeCodeLen + valueInlineLen) * len(m)
	var keyEntrys = new(bytes.Buffer)
	var valueEntrys = new(bytes.Buffer)
	var keys = new(bytes.Buffer)
	var values = new(bytes.Buffer)

	for _, key := range keySlice {
		var keyOffset = uint32(countAndSizeLen + keyEntrysLen + valueEntrysLen + keys.Len())
		var keyLength = uint16(len(hack.Slice(key)))
		err := binary.Write(keyEntrys, binary.LittleEndian, keyOffset)
		terror.Log(errors.Trace(err))
		err = binary.Write(keyEntrys, binary.LittleEndian, keyLength)
		terror.Log(errors.Trace(err))
		_, err = keys.Write(hack.Slice(key))
		terror.Log(errors.Trace(err))
	}

	for _, key := range keySlice {
		value := m[key]
		pushValueEntry(value, valueEntrys, values, countAndSizeLen+keyEntrysLen+valueEntrysLen+keys.Len())
	}

	countAndSize[0] = uint32(len(m))
	countAndSize[1] = uint32(countAndSizeLen + keyEntrysLen + valueEntrysLen + keys.Len() + values.Len())
	for _, v := range countAndSize {
		err := binary.Write(buffer, binary.LittleEndian, v)
		terror.Log(errors.Trace(err))
	}
	_, err := buffer.Write(keyEntrys.Bytes())
	terror.Log(errors.Trace(err))
	_, err = buffer.Write(valueEntrys.Bytes())
	terror.Log(errors.Trace(err))
	_, err = buffer.Write(keys.Bytes())
	terror.Log(errors.Trace(err))
	_, err = buffer.Write(values.Bytes())
	terror.Log(errors.Trace(err))
	return
}

func decodeJSONObject(m *map[string]JSON, data []byte) (err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	err = binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	if err != nil {
		return errors.Trace(err)
	}
	err = binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	if err != nil {
		return errors.Trace(err)
	}
	*m = make(map[string]JSON, countAndSize[0])

	var keyOffsets = make([]uint32, countAndSize[0])
	var keyLengths = make([]uint16, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		err = binary.Read(reader, binary.LittleEndian, &keyOffsets[i])
		if err != nil {
			return errors.Trace(err)
		}
		err = binary.Read(reader, binary.LittleEndian, &keyLengths[i])
		if err != nil {
			return errors.Trace(err)
		}
	}

	var valueTypes = make([]byte, countAndSize[0])
	var valueOffsets = make([]uint32, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		err = binary.Read(reader, binary.LittleEndian, &valueTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		err = binary.Read(reader, binary.LittleEndian, &valueOffsets[i])
		if err != nil {
			return errors.Trace(err)
		}
	}

	for i := 0; i < int(countAndSize[0]); i++ {
		var keyBuffer = make([]byte, keyLengths[i])
		if _, err = reader.Read(keyBuffer); err != nil {
			break
		}

		var key = string(keyBuffer)
		var value JSON
		typeLen, _ := jsonTypeCodeLength[TypeCode(valueTypes[i])]
		if typeLen >= 0 && typeLen <= valueInlineLen {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: valueInlineLen, Cap: valueInlineLen}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			value, err = decode(valueTypes[i], buf)
		} else {
			value, err = decode(valueTypes[i], data[valueOffsets[i]:])
		}
		if err != nil {
			break
		}
		(*m)[key] = value
	}
	return errors.Trace(err)
}

func encodeJSONArray(a []JSON, buffer *bytes.Buffer) {
	// array ::= element-count size value-entry* value*
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = compoundCountLen + compoundSizeLen

	var valueEntrysLen = (typeCodeLen + valueInlineLen) * len(a)
	var valueEntrys = new(bytes.Buffer)
	var values = new(bytes.Buffer)
	for _, value := range a {
		pushValueEntry(value, valueEntrys, values, countAndSizeLen+valueEntrysLen)
	}

	countAndSize[0] = uint32(len(a))
	countAndSize[1] = uint32(countAndSizeLen + valueEntrysLen + values.Len())
	for _, v := range countAndSize {
		err := binary.Write(buffer, binary.LittleEndian, v)
		terror.Log(errors.Trace(err))
	}
	_, err := buffer.Write(valueEntrys.Bytes())
	terror.Log(errors.Trace(err))
	_, err = buffer.Write(values.Bytes())
	terror.Log(errors.Trace(err))
}

func decodeJSONArray(a *[]JSON, data []byte) (err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	err = binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	if err != nil {
		return errors.Trace(err)
	}
	err = binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	if err != nil {
		return errors.Trace(err)
	}
	*a = make([]JSON, countAndSize[0])

	var valueTypes = make([]byte, countAndSize[0])
	var valueOffsets = make([]uint32, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		err = binary.Read(reader, binary.LittleEndian, &valueTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		err = binary.Read(reader, binary.LittleEndian, &valueOffsets[i])
		if err != nil {
			return errors.Trace(err)
		}
	}

	for i := 0; i < int(countAndSize[0]); i++ {
		var value JSON
		typeLen, _ := jsonTypeCodeLength[TypeCode(valueTypes[i])]
		if typeLen >= 0 && typeLen <= valueInlineLen {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: valueInlineLen, Cap: valueInlineLen}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			value, err = decode(valueTypes[i], buf)
		} else {
			value, err = decode(valueTypes[i], data[valueOffsets[i]:])
		}
		if err != nil {
			break
		}
		(*a)[i] = value
	}
	return errors.Trace(err)
}

// Every json type has a length which is useful for inline the value
// in value-entry. -1 means the length is variable.
var jsonTypeCodeLength = map[TypeCode]int{
	TypeCodeObject:  -1,
	TypeCodeArray:   -1,
	TypeCodeLiteral: 1,
	TypeCodeInt64:   8,
	TypeCodeUint64:  8,
	TypeCodeFloat64: 8,
	TypeCodeString:  -1,
}

// getSortedKeys returns sorted keys of a map.
func getSortedKeys(m map[string]JSON) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func pushValueEntry(value JSON, valueEntrys *bytes.Buffer, values *bytes.Buffer, prefixLen int) {
	var typeCode = value.TypeCode
	valueEntrys.WriteByte(byte(typeCode))

	typeLen, _ := jsonTypeCodeLength[typeCode]
	if typeLen > 0 && typeLen <= valueInlineLen {
		// If the value has length in (0, 4], it could be inline here.
		// And padding 0x00 to 4 bytes if needed.
		pushInlineValue(valueEntrys, value)
	} else {
		var valueOffset = uint32(prefixLen + values.Len())
		err := binary.Write(valueEntrys, binary.LittleEndian, valueOffset)
		terror.Log(errors.Trace(err))
		encode(value, values)
	}
	return
}

// pushInlineValue pushes the value into buffer first, and if its
// length < 4, pads 0x00 until there are 4 bytes written into buffer.
func pushInlineValue(buffer *bytes.Buffer, value JSON) {
	var oldLen = buffer.Len()
	switch value.TypeCode {
	case TypeCodeLiteral:
		var v = byte(value.I64)
		err := binary.Write(buffer, binary.LittleEndian, v)
		terror.Log(errors.Trace(err))
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, value.TypeCode)
		panic(msg)
	}
	var newLen = buffer.Len()
	for i := 0; i < valueInlineLen-(newLen-oldLen); i++ {
		err := buffer.WriteByte(0x00)
		terror.Log(errors.Trace(err))
	}
}
