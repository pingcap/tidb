// Copyright 2026 PingCAP, Inc.
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
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/hack"
)

// parseBinaryJSON decodes a document validated by ParseBinaryJSONFromString.
func parseBinaryJSON(data []byte) (BinaryJSON, error) {
	// Read directly from the input and retain v1 duplicate-name and UTF-8 behavior.
	decoder := jsontext.NewDecoder(bytes.NewBuffer(data), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	value, err := readJSONValue(decoder)
	if err != nil {
		return BinaryJSON{}, err
	}
	return CreateBinaryJSONWithCheck(value)
}

// readJSONValue preserves numeric text so BinaryJSON can distinguish integers from floats.
func readJSONValue(d *jsontext.Decoder) (any, error) {
	tok, err := d.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case 'n':
		return nil, nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case '"':
		return tok.String(), nil
	case '0':
		return json.Number(tok.String()), nil
	case '[':
		values := make([]any, 0)
		for d.PeekKind() != ']' {
			v, err := readJSONValue(d)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		_, err := d.ReadToken()
		return values, err
	case '{':
		values := make(map[string]any)
		for d.PeekKind() != '}' {
			key, err := d.ReadToken()
			if err != nil {
				return nil, err
			}
			// Tokens are invalidated by the next decoder call.
			keyName := key.String()
			value, err := readJSONValue(d)
			if err != nil {
				return nil, err
			}
			values[keyName] = value
		}
		_, err := d.ReadToken()
		return values, err
	default:
		return nil, fmt.Errorf("unexpected JSON token %v", tok.Kind())
	}
}

// ExtractBinaryJSONFromText extracts a single path without materializing unrelated objects.
// Only object-member paths are streamed; other paths use the existing binary extractor.
// On the streaming path, encoding limits apply only to the selected subtree.
func ExtractBinaryJSONFromText(s string, path JSONPathExpression) (BinaryJSON, bool, error) {
	for _, leg := range path.legs {
		if leg.typ != jsonPathLegKey || leg.dotKey == "*" {
			bj, err := ParseBinaryJSONFromString(s)
			if err != nil {
				return BinaryJSON{}, false, err
			}
			result, found := bj.Extract([]JSONPathExpression{path})
			return result, found, nil
		}
	}
	data := hack.Slice(s)
	if !json.Valid(data) {
		return BinaryJSON{}, false, ErrInvalidJSONText.GenWithStackByArgs("Invalid JSON document")
	}
	d := jsontext.NewDecoder(bytes.NewBuffer(data), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	value, found, err := readJSONPathValue(d, path.legs)
	if err != nil {
		return BinaryJSON{}, false, err
	}
	if !found {
		return BinaryJSON{}, false, nil
	}
	// The whole document has been validated; only materialize the selected subtree.
	result, err := CreateBinaryJSONWithCheck(value)
	if err != nil && !ErrJSONObjectKeyTooLong.Equal(err) && !ErrJSONDocumentTooDeep.Equal(err) {
		err = ErrInvalidJSONText.GenWithStackByArgs(err)
	}
	return result, true, err
}

func readJSONPathValue(d *jsontext.Decoder, legs []jsonPathLeg) (any, bool, error) {
	if len(legs) == 0 {
		value, err := readJSONValue(d)
		return value, true, err
	}
	if d.PeekKind() != '{' {
		return nil, false, d.SkipValue()
	}
	if _, err := d.ReadToken(); err != nil {
		return nil, false, err
	}
	var selected any
	var found bool
	for d.PeekKind() != '}' {
		key, err := d.ReadToken()
		if err != nil {
			return nil, false, err
		}
		if key.String() == legs[0].dotKey {
			selected, found, err = readJSONPathValue(d, legs[1:])
		} else {
			err = d.SkipValue()
		}
		if err != nil {
			return nil, false, err
		}
	}
	_, err := d.ReadToken()
	return selected, found, err
}
