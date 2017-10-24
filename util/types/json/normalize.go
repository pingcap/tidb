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
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/juju/errors"
)

func normalize(in interface{}) (j JSON, err error) {
	switch t := in.(type) {
	case nil:
		j.TypeCode = TypeCodeLiteral
		j.I64 = int64(LiteralNil)
	case bool:
		j.TypeCode = TypeCodeLiteral
		if t {
			j.I64 = int64(LiteralTrue)
		} else {
			j.I64 = int64(LiteralFalse)
		}
	case int64:
		j.TypeCode = TypeCodeInt64
		j.I64 = t
	case uint64:
		j.TypeCode = TypeCodeUint64
		j.I64 = *(*int64)(unsafe.Pointer(&t))
	case float64:
		j.TypeCode = TypeCodeFloat64
		*(*float64)(unsafe.Pointer(&j.I64)) = t
	case json.Number:
		if i64, errTp := t.Int64(); errTp == nil {
			j.TypeCode = TypeCodeInt64
			j.I64 = i64
		} else {
			var f64 float64
			f64, err = t.Float64()
			j.TypeCode = TypeCodeFloat64
			*(*float64)(unsafe.Pointer(&j.I64)) = f64
		}
	case string:
		j.TypeCode = TypeCodeString
		j.Str = t
	case JSON:
		j = t
	case map[string]JSON:
		j.TypeCode = TypeCodeObject
		j.Object = t
	case map[string]interface{}:
		j.TypeCode = TypeCodeObject
		j.Object = make(map[string]JSON, len(t))
		for key, value := range t {
			if j.Object[key], err = normalize(value); err != nil {
				return
			}
		}
	case []JSON:
		j.TypeCode = TypeCodeArray
		j.Array = t
	case []interface{}:
		j.TypeCode = TypeCodeArray
		j.Array = make([]JSON, 0, len(t))
		for _, elem := range t {
			var elem1 JSON
			elem1, err = normalize(elem)
			if err != nil {
				return j, err
			}
			j.Array = append(j.Array, elem1)
		}
	default:
		msg := fmt.Sprintf(unknownTypeErrorMsg, reflect.TypeOf(in))
		err = errors.New(msg)
	}
	return
}
