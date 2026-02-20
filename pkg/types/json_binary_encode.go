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
	"cmp"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/size"
)

// CalculateBinaryJSONSize calculates the size of binary JSON
func CalculateBinaryJSONSize(in any) int64 {
	switch x := in.(type) {
	case nil:
		return size.SizeOfByte
	case bool:
		return size.SizeOfByte
	case int64, uint64, float64:
		return 8
	case json.Number:
		size, err := calculateBinaryNumberSize(x)
		if err != nil {
			panic(errors.Trace(err))
		}
		return size
	case string:
		return calculateBinaryStringSize(x)
	case BinaryJSON:
		return int64(len(x.Value))
	case []any:
		size, err := calculateBinaryArraySize(x)
		if err != nil {
			panic(errors.Trace(err))
		}
		return size
	case map[string]any:
		size, err := calculateBinaryObjectSize(x)
		if err != nil {
			panic(errors.Trace(err))
		}
		return size
	case Opaque:
		return calculateBinaryOpaque(x)
	case Time:
		return 8
	case Duration:
		return 12
	}
	panic(fmt.Errorf(unknownTypeErrorMsg, reflect.TypeOf(in)))
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
	for range loop {
		buf = append(buf, tmp[:]...)
	}
	for range rem {
		buf = append(buf, 0)
	}
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	jsonEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func calculateBinaryNumberSize(x json.Number) (int64, error) {
	if strings.Contains(x.String(), "Ee.") {
		_, err := x.Float64()
		if err != nil {
			return 0, errors.Trace(err)
		}
		return 8, nil
	} else if _, err := x.Int64(); err == nil {
		return 8, nil
	} else if _, err := strconv.ParseUint(string(x), 10, 64); err == nil {
		return 8, nil
	}
	_, err := x.Float64()
	if err == nil {
		return 8, nil
	}
	return 0, errors.Trace(err)
}

func calculateBinaryStringSize(v string) int64 {
	return binary.MaxVarintLen64 + int64(len(v))
}

func calculateBinaryArraySize(array []any) (int64, error) {
	arrayLen := int64(len(array))
	size := arrayLen + dataSizeOff + arrayLen*valEntrySize
	for _, val := range array {
		size += calculateBinaryValElemSize(val)
	}
	return size, nil
}

func calculateBinaryValElemSize(val any) int64 {
	return CalculateBinaryJSONSize(val)
}

func calculateBinaryObjectSize(x map[string]any) (int64, error) {
	size := 4 + dataSizeOff + int64(len(x))*keyEntrySize + int64(len(x))*valEntrySize
	for key, val := range x {
		size += int64(len(key)) + calculateBinaryValElemSize(val)
	}
	return size, nil
}

func calculateBinaryOpaque(v Opaque) int64 {
	return int64(unsafe.Sizeof(v.TypeCode)) + binary.MaxVarintLen64 + int64(len(v.Buf))
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
