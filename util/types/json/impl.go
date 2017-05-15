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
	"strings"
)

// ParseFromString parses a json from string.
func ParseFromString(s string) (JSON, error) {
	if len(s) == 0 {
		return nil, ErrInvalidJSONText.GenByArgs("The document is empty")
	}
	var in interface{}
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil, ErrInvalidJSONText.GenByArgs(err)
	}
	return normalize(in), nil
}

// DumpToString dumps a JSON to JSON text string.
func DumpToString(j JSON) string {
	bytes, _ := json.Marshal(j)
	return strings.Trim(string(bytes), "\n")
}

func normalize(in interface{}) JSON {
	switch t := in.(type) {
	case bool:
		if t {
			return jsonLiteral(0x01)
		} else {
			return jsonLiteral(0x02)
		}
	case nil:
		return jsonLiteral(0x00)
	case float64:
		return jsonDouble(t)
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
	}
	return nil
}

// MarshalJSON implements RawMessage.
func (u jsonLiteral) MarshalJSON() ([]byte, error) {
	switch u {
	case 0x00:
		return []byte("null"), nil
	case 0x01:
		return []byte("true"), nil
	default:
		return []byte("false"), nil
	}
}
