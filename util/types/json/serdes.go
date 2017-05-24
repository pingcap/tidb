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

	"github.com/pingcap/tidb/util/hack"
)

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

// Serialize means serialize itself into bytes.
func Serialize(j JSON) []byte {
	var buffer = new(bytes.Buffer)
	buffer.WriteByte(j.getTypeCode())
	j.encode(buffer)
	return buffer.Bytes()
}

// Deserialize means deserialize a json from bytes.
func Deserialize(data []byte) (j JSON, err error) {
	j = jsonFromTypeCode(data[0])
	err = jsonDeserFromJSON(j).decode(data[1:])
	return
}

var (
	_ jsonDeser = new(jsonObject)
	_ jsonDeser = new(jsonArray)
	_ jsonDeser = new(jsonLiteral)
	_ jsonDeser = new(jsonInt64)
	_ jsonDeser = new(jsonDouble)
	_ jsonDeser = new(jsonString)
)

const (
	typeCodeObject  byte = 0x01
	typeCodeArray   byte = 0x03
	typeCodeLiteral byte = 0x04
	typeCodeInt64   byte = 0x09
	typeCodeDouble  byte = 0x0b
	typeCodeString  byte = 0x0c
)

const (
	jsonLiteralNil   = jsonLiteral(0x00)
	jsonLiteralTrue  = jsonLiteral(0x01)
	jsonLiteralFalse = jsonLiteral(0x02)
)

type jsonObject map[string]JSON
type jsonArray []JSON
type jsonLiteral byte
type jsonInt64 int64
type jsonDouble float64
type jsonString string

// jsonDeser is for deserialize json from bytes.
type jsonDeser interface {
	JSON
	decode([]byte) error
}

func jsonDeserFromJSON(j JSON) jsonDeser {
	if jdeser, ok := j.(jsonDeser); ok {
		return jdeser
	}
	return nil
}

func (b jsonLiteral) getTypeCode() byte {
	return typeCodeLiteral
}

func (b jsonLiteral) encode(buffer *bytes.Buffer) {
	buffer.WriteByte(byte(b))
}

func (b *jsonLiteral) decode(data []byte) error {
	var bb = (*byte)(unsafe.Pointer(b))
	*bb = data[0]
	return nil
}

func (i jsonInt64) getTypeCode() byte {
	return typeCodeInt64
}

func (i jsonInt64) encode(buffer *bytes.Buffer) {
	binary.Write(buffer, binary.LittleEndian, i)
}

func (i *jsonInt64) decode(data []byte) error {
	var reader = bytes.NewReader(data)
	return binary.Read(reader, binary.LittleEndian, i)
}

func (f jsonDouble) getTypeCode() byte {
	return typeCodeDouble
}

func (f jsonDouble) encode(buffer *bytes.Buffer) {
	binary.Write(buffer, binary.LittleEndian, f)
}

func (f *jsonDouble) decode(data []byte) error {
	var reader = bytes.NewReader(data)
	return binary.Read(reader, binary.LittleEndian, f)
}

func (s jsonString) getTypeCode() byte {
	return typeCodeString
}

func (s jsonString) encode(buffer *bytes.Buffer) {
	var ss = string(s)
	var varIntBuf = make([]byte, 9)
	var varIntLen = binary.PutUvarint(varIntBuf, uint64(len(hack.Slice(ss))))
	buffer.Write(varIntBuf[0:varIntLen])
	buffer.Write(hack.Slice(ss))
}

func (s *jsonString) decode(data []byte) error {
	var reader = bytes.NewReader(data)
	length, err := binary.ReadUvarint(reader)
	if err == nil {
		var buf = make([]byte, length)
		_, err = reader.Read(buf)
		if err == nil {
			*s = jsonString(string(buf))
		}
	}
	return err
}

func (m jsonObject) getTypeCode() byte {
	return typeCodeObject
}

func (m jsonObject) encode(buffer *bytes.Buffer) {
	// object ::= element-count size key-entry* value-entry* key* value*
	// key-entry ::= key-offset key-length
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = len(countAndSize) * 4
	var keySlice = m.getSortedKeys()

	var keyEntrysLen = (4 + 2) * len(m)
	var valueEntrysLen = (1 + 4) * len(m)
	var keyEntrys = new(bytes.Buffer)
	var valueEntrys = new(bytes.Buffer)
	var keys = new(bytes.Buffer)
	var values = new(bytes.Buffer)

	for _, key := range keySlice {
		var keyOffset = uint32(countAndSizeLen + keyEntrysLen + valueEntrysLen + keys.Len())
		var keyLength = uint16(len(hack.Slice(key)))
		binary.Write(keyEntrys, binary.LittleEndian, keyOffset)
		binary.Write(keyEntrys, binary.LittleEndian, keyLength)
		keys.Write(hack.Slice(key))
	}

	for _, key := range keySlice {
		value := m[key]
		pushValueEntry(value, valueEntrys, values, countAndSizeLen+keyEntrysLen+valueEntrysLen+keys.Len())
	}

	countAndSize[0] = uint32(len(m))
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

func (m *jsonObject) decode(data []byte) (err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	*m = make(map[string]JSON, countAndSize[0])

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
		var value = jsonFromTypeCode(valueTypes[i])
		typeLen, _ := jsonTypeCodeLength[valueTypes[i]]
		if typeLen >= 0 && typeLen <= 4 {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: 4, Cap: 4}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			jsonDeserFromJSON(value).decode(buf)
		} else {
			jsonDeserFromJSON(value).decode(data[valueOffsets[i]:])
		}
		if err != nil {
			return
		}
		(*m)[key] = value
	}
	return
}

func (a jsonArray) getTypeCode() byte {
	return typeCodeArray
}

func (a jsonArray) encode(buffer *bytes.Buffer) {
	// array ::= element-count size value-entry* value*
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = len(countAndSize) * 4

	var valueEntrysLen = (1 + 4) * len(a)
	var valueEntrys = new(bytes.Buffer)
	var values = new(bytes.Buffer)
	for _, value := range a {
		pushValueEntry(value, valueEntrys, values, countAndSizeLen+valueEntrysLen)
	}

	countAndSize[0] = uint32(len(a))
	countAndSize[1] = uint32(countAndSizeLen + valueEntrysLen + values.Len())
	for _, v := range countAndSize {
		binary.Write(buffer, binary.LittleEndian, v)
	}
	buffer.Write(valueEntrys.Bytes())
	buffer.Write(values.Bytes())
}

func (a *jsonArray) decode(data []byte) (err error) {
	var reader = bytes.NewReader(data)

	var countAndSize = make([]uint32, 2)
	binary.Read(reader, binary.LittleEndian, &countAndSize[0])
	binary.Read(reader, binary.LittleEndian, &countAndSize[1])
	*a = make([]JSON, countAndSize[0])

	var valueTypes = make([]byte, countAndSize[0])
	var valueOffsets = make([]uint32, countAndSize[0])
	for i := 0; i < int(countAndSize[0]); i++ {
		binary.Read(reader, binary.LittleEndian, &valueTypes[i])
		binary.Read(reader, binary.LittleEndian, &valueOffsets[i])
	}

	for i := 0; i < int(countAndSize[0]); i++ {
		var value = jsonFromTypeCode(valueTypes[i])
		typeLen, _ := jsonTypeCodeLength[valueTypes[i]]
		if typeLen >= 0 && typeLen <= 4 {
			var inline = valueOffsets[i]
			var hdr = reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&inline)), Len: 4, Cap: 4}
			var buf = *(*[]byte)(unsafe.Pointer(&hdr))
			err = jsonDeserFromJSON(value).decode(buf)
		} else {
			err = jsonDeserFromJSON(value).decode(data[valueOffsets[i]:])
		}
		if err != nil {
			return
		}
		(*a)[i] = value
	}
	return
}

// Every json type has a length which is useful for inline the value
// in value-entry. -1 means the length is variable.
var jsonTypeCodeLength = map[byte]int{
	typeCodeObject:  -1,
	typeCodeArray:   -1,
	typeCodeLiteral: 1,
	typeCodeInt64:   8,
	typeCodeDouble:  8,
	typeCodeString:  -1,
}

func jsonFromTypeCode(typeCode byte) JSON {
	switch typeCode {
	case typeCodeObject:
		return new(jsonObject)
	case typeCodeArray:
		return new(jsonArray)
	case typeCodeLiteral:
		return new(jsonLiteral)
	case typeCodeInt64:
		return new(jsonInt64)
	case typeCodeDouble:
		return new(jsonDouble)
	case typeCodeString:
		return new(jsonString)
	}
	panic("unknown type code")
}

// Two map are equal if they have same keys and same values.
// So we sort the keys before serialize in order to keep
// their binary representations are same.
func (m jsonObject) getSortedKeys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func pushValueEntry(value JSON, valueEntrys *bytes.Buffer, values *bytes.Buffer, prefixLen int) {
	var typeCode = value.getTypeCode()
	valueEntrys.WriteByte(typeCode)

	typeLen, _ := jsonTypeCodeLength[typeCode]
	if typeLen > 0 && typeLen <= 4 {
		// If the value has length in (0, 4], it could be inline here.
		// And padding 0x00 to 4 bytes if needed.
		oldEntryLen := valueEntrys.Len()
		binary.Write(valueEntrys, binary.LittleEndian, value)
		newEntryLen := valueEntrys.Len()
		for i := 0; i < 4-(newEntryLen-oldEntryLen); i++ {
			valueEntrys.WriteByte(0x00)
		}
	} else {
		var valueOffset = uint32(prefixLen + values.Len())
		binary.Write(valueEntrys, binary.LittleEndian, valueOffset)
		value.encode(values)
	}
	return
}
