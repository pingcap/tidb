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
	"reflect"
	"sort"
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/hack"
)

var errUnsupportedType = errors.New("Unsupported type")
var errUnknownType = errors.New("Unknown type")

var jsonTypeCodeLength = map[byte]int{
	0x00: -1,
	0x01: -1,
	0x02: -1,
	0x03: -1,
	0x04: 1,
	0x05: 2,
	0x06: 2,
	0x07: 4,
	0x08: 4,
	0x09: 8,
	0x0a: 8,
	0x0b: 8,
	0x0c: -1,
}

func jsonTypeCode(in interface{}) (code byte, err error) {
	switch in.(type) {
	case map[string]interface{}:
		code = 0x01
	case []interface{}:
		code = 0x03
	case nil, bool:
		code = 0x04
	case int16:
		code = 0x05
	case uint16:
		code = 0x06
	case int32:
		code = 0x07
	case uint32:
		code = 0x08
	case int64:
		code = 0x09
	case uint64:
		code = 0x0a
	case float64:
		code = 0x0b
	case string:
		code = 0x0c
	default:
		err = errUnsupportedType
	}
	return code, err
}

func keysInMap(m map[string]interface{}) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

func push(bytesBuffer *bytes.Buffer, in interface{}) (err error) {
	switch x := in.(type) {
	case nil:
		bytesBuffer.WriteByte(0x00)
	case bool:
		if x {
			bytesBuffer.WriteByte(0x01)
		} else {
			bytesBuffer.WriteByte(0x02)
		}
	case int16, uint16, int32, uint32, int64, uint64, float64:
		binary.Write(bytesBuffer, binary.LittleEndian, x)
	case string:
		var varIntBuf = make([]byte, 9)
		var varIntLen = binary.PutUvarint(varIntBuf, uint64(len(hack.Slice(x))))
		bytesBuffer.Write(varIntBuf[0:varIntLen])
		bytesBuffer.Write(hack.Slice(x))
	case map[string]interface{}:
		err = pushObject(bytesBuffer, x)
	case []interface{}:
		err = pushArray(bytesBuffer, x)
	default:
		err = errUnsupportedType
	}
	return
}

func pushObject(buffer *bytes.Buffer, object map[string]interface{}) (err error) {
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = len(countAndSize) * 4
	var keySlice = keysInMap(object)

	var keyEntrysLen = (4 + 2) * len(object)
	var valueEntrysLen = (1 + 4) * len(object)
	var keyEntrys = new(bytes.Buffer)
	var valueEntrys = new(bytes.Buffer)
	var keys = new(bytes.Buffer)
	var values = new(bytes.Buffer)

	for i, key := range keySlice {
		var keyOffset = uint32(countAndSizeLen + keyEntrysLen + valueEntrysLen + (4+2)*i)
		var keyLength = uint16(len(hack.Slice(key)))
		binary.Write(keyEntrys, binary.LittleEndian, keyOffset)
		binary.Write(keyEntrys, binary.LittleEndian, keyLength)
		keys.Write(hack.Slice(key))
	}

	for _, key := range keySlice {
		value, _ := object[key]
		err = pushValueEntry(value, valueEntrys, values, countAndSizeLen+keyEntrysLen+valueEntrysLen+keys.Len())
		if err != nil {
			return
		}
	}

	countAndSize[0] = uint32(len(object))
	countAndSize[1] = uint32(countAndSizeLen + keyEntrysLen + valueEntrysLen + keys.Len() + values.Len())
	for _, v := range countAndSize {
		binary.Write(buffer, binary.LittleEndian, v)
	}
	buffer.Write(keyEntrys.Bytes())
	buffer.Write(valueEntrys.Bytes())
	buffer.Write(keys.Bytes())
	buffer.Write(values.Bytes())
	return
}

func pushArray(buffer *bytes.Buffer, array []interface{}) (err error) {
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = len(countAndSize) * 4

	var valueEntrysLen = (1 + 4) * len(array)
	var valueEntrys = new(bytes.Buffer)
	var values = new(bytes.Buffer)
	for _, value := range array {
		err = pushValueEntry(value, valueEntrys, values, countAndSizeLen+valueEntrysLen)
		if err != nil {
			return
		}
	}

	countAndSize[0] = uint32(len(array))
	countAndSize[1] = uint32(countAndSizeLen + valueEntrysLen + values.Len())
	for _, v := range countAndSize {
		binary.Write(buffer, binary.LittleEndian, v)
	}
	buffer.Write(valueEntrys.Bytes())
	buffer.Write(values.Bytes())
	return
}

func pushValueEntry(value interface{}, valueEntrys *bytes.Buffer, values *bytes.Buffer, prefixLen int) (err error) {
	var typeCode byte
	if typeCode, err = jsonTypeCode(value); err != nil {
		return
	}
	valueEntrys.WriteByte(typeCode)

	typeLen, _ := jsonTypeCodeLength[typeCode]
	if typeLen > 0 && typeLen <= 4 {
		oldEntryLen := valueEntrys.Len()
		binary.Write(valueEntrys, binary.LittleEndian, value)
		newEntryLen := valueEntrys.Len()
		for i := 0; i < 4-(newEntryLen-oldEntryLen); i++ {
			valueEntrys.WriteByte(0x00)
		}
	} else {
		var valueOffset = uint32(prefixLen + values.Len())
		binary.Write(valueEntrys, binary.LittleEndian, valueOffset)
		err = push(values, value)
	}
	return
}

func pop(typeCode byte, data []byte) (out interface{}, err error) {
	var reader = bytes.NewReader(data)
	switch typeCode {
	case 0x04:
		switch literal, _ := reader.ReadByte(); literal {
		case 0x00:
			out = nil
		case 0x01:
			out = true
		case 0x02:
			out = false
		}
	case 0x05:
		var i16 int16
		binary.Read(reader, binary.LittleEndian, &i16)
		out = i16
	case 0x06:
		var u16 uint16
		binary.Read(reader, binary.LittleEndian, &u16)
		out = u16
	case 0x07:
		var i32 int32
		binary.Read(reader, binary.LittleEndian, &i32)
		out = i32
	case 0x08:
		var u32 uint32
		binary.Read(reader, binary.LittleEndian, &u32)
		out = u32
	case 0x09:
		var i64 int64
		binary.Read(reader, binary.LittleEndian, &i64)
		out = i64
	case 0x0a:
		var u64 uint64
		binary.Read(reader, binary.LittleEndian, &u64)
		out = u64
	case 0x0b:
		var f64 float64
		binary.Read(reader, binary.LittleEndian, &f64)
		out = f64
	case 0x0c:
		length, _ := binary.ReadUvarint(reader)
		var buf = make([]byte, length)
		reader.Read(buf)
		out = string(buf)
	case 0x01:
		out, err = popObject(data)
	case 0x03:
		out, err = popArray(data)
	default:
		err = errUnknownType
	}
	return out, err
}

func popObject(data []byte) (m map[string]interface{}, err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	m = make(map[string]interface{}, countAndSize[0])

	var keyOffsets = make([]uint32, countAndSize[0])
	var keyLengths = make([]uint16, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		binary.Read(reader, binary.LittleEndian, &keyOffsets[i])
		binary.Read(reader, binary.LittleEndian, &keyLengths[i])
	}

	var valueTypes = make([]byte, countAndSize[0])
	var valueOffsets = make([]uint32, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		binary.Read(reader, binary.LittleEndian, &valueTypes[i])
		binary.Read(reader, binary.LittleEndian, &valueOffsets[i])
	}

	for i := 0; i < int(countAndSize[0]); i++ {
		var keyBuffer = make([]byte, keyLengths[i])
		if _, err = reader.Read(keyBuffer); err != nil {
			return
		}

		var key = string(keyBuffer)
		var value interface{}
		typeLen, _ := jsonTypeCodeLength[valueTypes[i]]
		if typeLen >= 0 && typeLen <= 4 {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: 4, Cap: 4}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			value, err = pop(valueTypes[i], buf)
		} else {
			value, err = pop(valueTypes[i], data[valueOffsets[i]:])
		}
		if err != nil {
			return
		}
		m[key] = value
	}
	return
}

func popArray(data []byte) (a []interface{}, err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	a = make([]interface{}, countAndSize[0])

	var valueTypes = make([]byte, countAndSize[0])
	var valueOffsets = make([]uint32, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		binary.Read(reader, binary.LittleEndian, &valueTypes[i])
		binary.Read(reader, binary.LittleEndian, &valueOffsets[i])
	}

	for i := 0; i < int(countAndSize[0]); i++ {
		var value interface{}
		typeLen, _ := jsonTypeCodeLength[valueTypes[i]]
		if typeLen >= 0 && typeLen <= 4 {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: 4, Cap: 4}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			value, err = pop(valueTypes[i], buf)
		} else {
			value, err = pop(valueTypes[i], data[valueOffsets[i]:])
		}
		if err != nil {
			return
		}
		a[i] = value
	}
	return
}

/*
	The binary jSON format from MySQL 5.7 is as follows:

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

	number ::=  ....  // little-endian format for [u]int(16|32|64), whereas
                      // double is stored in a platform-independent, eight-byte
                      // format using float8store()

	string ::= data-length utf8mb4-data

	data-length ::= uint8*	// If the high bit of a byte is 1, the length
                            // field is continued in the next byte,
							// otherwise it is the last byte of the length
							// field. So we need 1 byte to represent
							// lengths up to 127, 2 bytes to represent
							// lengths up to 16383, and so on...
*/
func serialize(in interface{}) (out []byte, err error) {
	var typeCode byte
	if typeCode, err = jsonTypeCode(in); err != nil {
		return
	}
	var bytesBuffer = bytes.NewBuffer(nil)
	bytesBuffer.WriteByte(typeCode)
	push(bytesBuffer, in)
	out = bytesBuffer.Bytes()
	return
}

func deserialize(data []byte) (out interface{}, err error) {
	return pop(data[0], data[1:])
}
