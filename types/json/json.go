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
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/hack"
)

// TypeCode indicates JSON type.
type TypeCode byte

const (
	// TypeCodeObject indicates the JSON is an object.
	TypeCodeObject TypeCode = 0x01
	// TypeCodeArray indicates the JSON is an array.
	TypeCodeArray TypeCode = 0x03
	// TypeCodeLiteral indicates the JSON is a literal.
	TypeCodeLiteral TypeCode = 0x04
	// TypeCodeInt64 indicates the JSON is a signed integer.
	TypeCodeInt64 TypeCode = 0x09
	// TypeCodeUint64 indicates the JSON is a unsigned integer.
	TypeCodeUint64 TypeCode = 0x0a
	// TypeCodeFloat64 indicates the JSON is a double float number.
	TypeCodeFloat64 TypeCode = 0x0b
	// TypeCodeString indicates the JSON is a string.
	TypeCodeString TypeCode = 0x0c
)

const (
	// LiteralNil represents JSON null.
	LiteralNil byte = 0x00
	// LiteralTrue represents JSON true.
	LiteralTrue byte = 0x01
	// LiteralFalse represents JSON false.
	LiteralFalse byte = 0x02
)

const unknownTypeCodeErrorMsg = "unknown type code: %d"
const unknownTypeErrorMsg = "unknown type: %s"

// JSON is for MySQL JSON type.
type JSON struct {
	TypeCode TypeCode
	I64      int64
	Str      string
	Object   map[string]JSON
	Array    []JSON
}

// Copy deep copies a JSON.
func (j JSON) Copy() *JSON {
	ret := j
	if j.Object != nil {
		ret.Object = make(map[string]JSON, len(j.Object))
		for k, v := range j.Object {
			ret.Object[k] = *v.Copy()
		}
	}
	if j.Array != nil {
		ret.Array = make([]JSON, len(j.Array))
		for i, v := range j.Array {
			ret.Array[i] = *v.Copy()
		}
	}
	return &ret
}

// CreateJSON creates a JSON from in. Panic if any error occurs.
func CreateJSON(in interface{}) JSON {
	j, err := normalize(in)
	if err != nil {
		panic(err)
	}
	return j
}

// ParseFromString parses a json from string.
func ParseFromString(s string) (j JSON, err error) {
	// TODO: implement the decoder directly. It's important for keeping
	// keys in object have same order with the original string.
	if len(s) == 0 {
		err = ErrInvalidJSONText.GenByArgs("The document is empty")
		return
	}
	if err = j.UnmarshalJSON(hack.Slice(s)); err != nil {
		err = ErrInvalidJSONText.GenByArgs(err)
	}
	return
}

// MarshalJSON implements Marshaler interface.
func (j JSON) MarshalJSON() ([]byte, error) {
	switch j.TypeCode {
	case TypeCodeObject:
		return json.Marshal(j.Object)
	case TypeCodeArray:
		return json.Marshal(j.Array)
	case TypeCodeLiteral:
		switch byte(j.I64) {
		case LiteralNil:
			return []byte("null"), nil
		case LiteralTrue:
			return []byte("true"), nil
		default:
			return []byte("false"), nil
		}
	case TypeCodeInt64:
		return json.Marshal(j.I64)
	case TypeCodeUint64:
		u64 := *(*uint64)(unsafe.Pointer(&j.I64))
		return json.Marshal(u64)
	case TypeCodeFloat64:
		f64 := *(*float64)(unsafe.Pointer(&j.I64))
		return json.Marshal(f64)
	case TypeCodeString:
		return json.Marshal(j.Str)
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, j.TypeCode)
		panic(msg)
	}
}

// UnmarshalJSON implements Unmarshaler interface.
func (j *JSON) UnmarshalJSON(data []byte) (err error) {
	var decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var in interface{}
	if err = decoder.Decode(&in); err == nil {
		*j, err = normalize(in)
	}
	return
}

// String implements fmt.Stringer interface.
func (j JSON) String() string {
	bytes, err := json.Marshal(j)
	terror.Log(errors.Trace(err))
	return strings.TrimSpace(hack.String(bytes))
}

var (
	// ErrInvalidJSONText means invalid JSON text.
	ErrInvalidJSONText = terror.ClassJSON.New(mysql.ErrInvalidJSONText, mysql.MySQLErrName[mysql.ErrInvalidJSONText])
	// ErrInvalidJSONPath means invalid JSON path.
	ErrInvalidJSONPath = terror.ClassJSON.New(mysql.ErrInvalidJSONPath, mysql.MySQLErrName[mysql.ErrInvalidJSONPath])
	// ErrInvalidJSONData means invalid JSON data.
	ErrInvalidJSONData = terror.ClassJSON.New(mysql.ErrInvalidJSONData, mysql.MySQLErrName[mysql.ErrInvalidJSONData])
)

func init() {
	terror.ErrClassToMySQLCodes[terror.ClassJSON] = map[terror.ErrCode]uint16{
		mysql.ErrInvalidJSONText: mysql.ErrInvalidJSONText,
		mysql.ErrInvalidJSONPath: mysql.ErrInvalidJSONPath,
		mysql.ErrInvalidJSONData: mysql.ErrInvalidJSONData,
	}
}
