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
		j.typeCode = typeCodeLiteral
		j.i64 = int64(jsonLiteralNil)
	case bool:
		j.typeCode = typeCodeLiteral
		if t {
			j.i64 = int64(jsonLiteralTrue)
		} else {
			j.i64 = int64(jsonLiteralFalse)
		}
	case int64:
		j.typeCode = typeCodeInt64
		j.i64 = t
	case float64:
		j.typeCode = typeCodeFloat64
		*(*float64)(unsafe.Pointer(&j.i64)) = t
	case json.Number:
		if i64, err := t.Int64(); err == nil {
			j.typeCode = typeCodeInt64
			j.i64 = i64
		} else {
			f64, _ := t.Float64()
			j.typeCode = typeCodeFloat64
			*(*float64)(unsafe.Pointer(&j.i64)) = f64
		}
	case string:
		j.typeCode = typeCodeString
		j.str = t
	case map[string]interface{}:
		j.typeCode = typeCodeObject
		j.object = make(map[string]JSON, len(t))
		for key, value := range t {
			if j.object[key], err = normalize(value); err != nil {
				return
			}
		}
	case []interface{}:
		j.typeCode = typeCodeArray
		j.array = make([]JSON, 0, len(t))
		for _, elem := range t {
			elem1, err := normalize(elem)
			if err != nil {
				return j, err
			}
			j.array = append(j.array, elem1)
		}
	default:
		msg := fmt.Sprintf(unknownTypeErrorMsg, reflect.TypeOf(in))
		err = errors.New(msg)
	}
	return
}
