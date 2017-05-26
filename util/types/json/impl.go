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
	"reflect"
	"strings"

	"github.com/pingcap/tidb/util/hack"
)

// ParseFromString parses a json from string.
func ParseFromString(s string) (JSON, error) {
	if len(s) == 0 {
		return nil, ErrInvalidJSONText.GenByArgs("The document is empty")
	}
	var in interface{}
	var decoder = json.NewDecoder(bytes.NewReader(hack.Slice(s)))
	decoder.UseNumber()
	if err := decoder.Decode(&in); err != nil {
		return nil, ErrInvalidJSONText.GenByArgs(err)
	}
	return normalize(in), nil
}

func normalize(in interface{}) JSON {
	switch t := in.(type) {
	case bool:
		if t {
			return jsonLiteralTrue
		}
		return jsonLiteralFalse
	case nil:
		return jsonLiteralNil
	case int64:
		return jsonInt64(t)
	case float64:
		return jsonDouble(t)
	case json.Number:
		if i64, err := t.Int64(); err == nil {
			return jsonInt64(i64)
		}
		f64, _ := t.Float64()
		return jsonDouble(f64)
	case string:
		return jsonString(t)
	case map[string]interface{}:
		var object = make(map[string]JSON, len(t))
		for key, value := range t {
			object[key] = normalize(value)
		}
		return jsonObject(object)
	case []interface{}:
		var array = make([]JSON, len(t))
		for i, elem := range t {
			array[i] = normalize(elem)
		}
		return jsonArray(array)
	default:
		msg := fmt.Sprintf("unsupported json type: %s\n", reflect.TypeOf(in))
		panic(msg)
	}
}

// MarshalJSON implements RawMessage.
func (j jsonLiteral) MarshalJSON() ([]byte, error) {
	switch j {
	case jsonLiteralNil:
		return []byte("null"), nil
	case jsonLiteralTrue:
		return []byte("true"), nil
	default:
		return []byte("false"), nil
	}
}

// String implements JSON interface.
func (j jsonLiteral) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// String implements JSON interface.
func (j jsonInt64) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// String implements JSON interface.
func (j jsonDouble) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// String implements JSON interface.
func (j jsonString) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// String implements JSON interface.
func (j jsonObject) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// String implements JSON interface.
func (j jsonArray) String() string {
	bytes, _ := json.Marshal(j)
	return strings.TrimSpace(hack.String(bytes))
}

// Type implements JSON interface.
func (j jsonLiteral) Type() string {
	if j == jsonLiteralNil {
		return "NULL"
	}
	return "BOOLEAN"
}

// Type implements JSON interface.
func (j jsonInt64) Type() string {
	return "INTEGER"
}

// Type implements JSON interface.
func (j jsonDouble) Type() string {
	return "DOUBLE"
}

// Type implements JSON interface.
func (j jsonString) Type() string {
	return "STRING"
}

// Type implements JSON interface.
func (j jsonObject) Type() string {
	return "OBJECT"
}

// Type implements JSON interface.
func (j jsonArray) Type() string {
	return "ARRAY"
}
