// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partialjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type topLevelJSONTokenIter struct {
	d     *json.Decoder
	level int
}

func newTopLevelJSONTokenIter(content []byte) *topLevelJSONTokenIter {
	d := json.NewDecoder(bytes.NewReader(content))
	d.UseNumber()
	return &topLevelJSONTokenIter{
		d: d,
	}
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// readName reads a name belongs to the top-level of JSON objects. Caller should
// call readOrDiscardValue to consume its value before calling next readName.
func (i *topLevelJSONTokenIter) readName() (string, error) {
	ts, err := i.next(false)
	if err != nil {
		return "", err
	}
	if len(ts) != 1 {
		return "", fmt.Errorf("unexpected JSON name, %v", ts)
	}
	name, ok := ts[0].(string)
	if !ok {
		// > An object is an unordered collection of zero or more name/value
		//   pairs, where a name is a string...
		// https://datatracker.ietf.org/doc/html/rfc8259#section-1
		return "", fmt.Errorf("unexpected JSON name, %T %v", ts, ts)
	}
	return name, nil
}

// readOrDiscardValue reads a value belongs to the top-level of JSON objects. It
// must be called after readName. If caller don't need the value, it can pass
// true to discard it.
func (i *topLevelJSONTokenIter) readOrDiscardValue(discard bool) ([]json.Token, error) {
	return i.next(discard)
}

// next is an internal method to iterate the JSON tokens. Callers should use
// readName / readOrDiscardValue instead.
func (i *topLevelJSONTokenIter) next(discard bool) ([]json.Token, error) {
	if i.level == 0 {
		t, err := i.d.Token()
		if err != nil {
			return nil, err
		}

		if t != json.Delim('{') {
			return nil, fmt.Errorf(
				"expected '{' for topLevelJSONTokenIter, got %T %v",
				t, t,
			)
		}
		i.level++
	}

	var longValue []json.Token

	if i.level == 1 {
		t, err := i.d.Token()
		if err != nil {
			return nil, unexpectedEOF(err)
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return []json.Token{t}, nil
		}

		switch delim {
		case '}', ']':
			// we are at top level and now exit this level, which means the content is end.
			i.level--
			return nil, io.EOF
		case '{', '[':
			i.level++
			// go to below loop to consume this level
			if !discard {
				longValue = make([]json.Token, 0, 16)
				longValue = append(longValue, t)
			}
		}
	}

	for i.level > 1 {
		t, err := i.d.Token()
		if err != nil {
			return nil, unexpectedEOF(err)
		}
		if !discard {
			longValue = append(longValue, t)
		}

		delim, ok := t.(json.Delim)
		if !ok {
			continue
		}

		switch delim {
		case '{', '[':
			i.level++
		case '}', ']':
			i.level--
		}
	}
	return longValue, nil
}

// ExtractTopLevelMembers extracts tokens of given top level members from a JSON
// text. It will stop parsing when all keys are found.
func ExtractTopLevelMembers(content []byte, names []string) (map[string][]json.Token, error) {
	remainNames := make(map[string]struct{}, len(names))
	for _, k := range names {
		remainNames[k] = struct{}{}
	}
	ret := make(map[string][]json.Token, len(names))
	iter := newTopLevelJSONTokenIter(content)
	for len(remainNames) > 0 {
		name, err := iter.readName()
		if err != nil {
			return nil, err
		}
		_, ok := remainNames[name]
		if ok {
			val, err2 := iter.readOrDiscardValue(false)
			if err2 != nil {
				return nil, err2
			}
			ret[name] = val
			delete(remainNames, name)
		} else {
			_, err2 := iter.readOrDiscardValue(true)
			if err2 != nil {
				return nil, err2
			}
		}
	}
	return ret, nil
}
