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

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/hack"
)

const (
	typeCodeObject  byte = 0x01
	typeCodeArray   byte = 0x03
	typeCodeLiteral byte = 0x04
	typeCodeInt64   byte = 0x09
	typeCodeFloat64 byte = 0x0b
	typeCodeString  byte = 0x0c
)

const (
	jsonLiteralNil   byte = 0x00
	jsonLiteralTrue  byte = 0x01
	jsonLiteralFalse byte = 0x02
)

const internalErrorUnknownTypeCode = "unknown type code: %d"
const internalErrorUnknownType = "unknown type: %s"

// JSON is for MySQL JSON type.
type JSON struct {
	typeCode byte
	i64      int64
	str      string
	object   map[string]JSON
	array    []JSON
}

// CreateJSON creates a JSON from in. Return nil if in is not a valid JSON type.
func CreateJSON(in interface{}) JSON {
	return normalize(in)
}

// ParseFromString parses a json from string.
func ParseFromString(s string) (j JSON, err error) {
	if len(s) == 0 {
		err = ErrInvalidJSONText.GenByArgs("The document is empty")
		return
	}
	var decoder = json.NewDecoder(bytes.NewReader(hack.Slice(s)))
	decoder.UseNumber()
	var in interface{}
	if err = decoder.Decode(&in); err != nil {
		err = ErrInvalidJSONText.GenByArgs(err)
		return
	}
	j = normalize(in)
	return
}

// MarshalJSON implements RawMessage.
func (j JSON) MarshalJSON() ([]byte, error) {
	switch j.typeCode {
	case typeCodeObject:
		return json.Marshal(j.object)
	case typeCodeArray:
		return json.Marshal(j.array)
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil:
			return hack.Slice("null"), nil
		case jsonLiteralTrue:
			return hack.Slice("true"), nil
		default:
			return hack.Slice("false"), nil
		}
	case typeCodeInt64:
		return json.Marshal(j.i64)
	case typeCodeFloat64:
		f64 := *(*float64)(unsafe.Pointer(&j.i64))
		return json.Marshal(f64)
	case typeCodeString:
		return json.Marshal(j.str)
	default:
		msg := fmt.Sprintf(internalErrorUnknownTypeCode, j.typeCode)
		panic(msg)
	}
}

// String implements fmt.Stringer interface.
func (j JSON) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// Type returns type of JSON as string.
func (j JSON) Type() string {
	switch j.typeCode {
	case typeCodeObject:
		return "OBJECT"
	case typeCodeArray:
		return "ARRAY"
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil:
			return "NULL"
		default:
			return "BOOLEAN"
		}
	case typeCodeInt64:
		return "INTEGER"
	case typeCodeFloat64:
		return "DOUBLE"
	case typeCodeString:
		return "STRING"
	default:
		msg := fmt.Sprintf(internalErrorUnknownTypeCode, j.typeCode)
		panic(msg)
	}
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
