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

package fastjson

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

// next iterates and returns the top-level key and value as token. The odd
// invocations return key tokens, the even invocations return value tokens. If
// the value is a JSON array or object, all tokens belongs to the value will be
// returned.
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
func ExtractTopLevelMembers(content []byte, keys []string) (map[string][]json.Token, error) {
	remainKeys := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remainKeys[k] = struct{}{}
	}
	ret := make(map[string][]json.Token, len(keys))
	iter := newTopLevelJSONTokenIter(content)
	for len(remainKeys) > 0 {
		key, err := iter.next(false)
		if err != nil {
			return nil, err
		}
		if len(key) != 1 {
			return nil, fmt.Errorf("unexpected JSON key, %v", key)
		}

		keyStr, ok := key[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected JSON key, %T %v", key, key)
		}
		_, ok = remainKeys[keyStr]
		if ok {
			val, err2 := iter.next(false)
			if err2 != nil {
				return nil, err2
			}
			ret[keyStr] = val
			delete(remainKeys, keyStr)
		} else {
			_, err2 := iter.next(true)
			if err2 != nil {
				return nil, err2
			}
		}
	}
	return ret, nil
}
