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
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIter(t *testing.T) {
	failCases := [][2]string{
		{"{", "unexpected EOF"},
		{"[]", "expected '{' for topLevelJSONTokenIter, got json.Delim ["},
		{"{a}", "invalid character 'a'"},
		{"{]", "invalid character ']'"},
	}

	for _, ca := range failCases {
		i := newTopLevelJSONTokenIter([]byte(ca[0]))
		_, err := i.next(false)
		for err == nil {
			_, err = i.next(false)
		}
		require.ErrorContains(t, err, ca[1], "content: %s", ca[0])
	}

	succCases := map[string][][]json.Token{
		"{}": nil,
		`{"a": 1, "b": "val"}`: {
			[]json.Token{"a"}, []json.Token{json.Number("1")},
			[]json.Token{"b"}, []json.Token{"val"}},
		`{"a": 1, "long1": {"skip": "skip"}, "b": "val", "long2": [0,0,{"skip":2}]}`: {
			[]json.Token{"a"}, []json.Token{json.Number("1")},
			[]json.Token{"long1"}, []json.Token{
				json.Delim('{'), "skip", "skip", json.Delim('}'),
			},
			[]json.Token{"b"}, []json.Token{"val"},
			[]json.Token{"long2"}, []json.Token{
				json.Delim('['), json.Number("0"), json.Number("0"), json.Delim('{'), "skip", json.Number("2"), json.Delim('}'), json.Delim(']'),
			},
		},
	}

	for content, expected := range succCases {
		i := newTopLevelJSONTokenIter([]byte(content))
		expectedIdx := 0
		for expectedIdx < len(expected) {
			name, err := i.readName()
			require.NoError(t, err, "content: %s", content)
			require.Equal(t, expected[expectedIdx], []json.Token{name}, "content: %s", content)
			expectedIdx++

			tok, err := i.next(false)
			require.NoError(t, err, "content: %s", content)
			require.Equal(t, expected[expectedIdx], tok, "content: %s", content)
			expectedIdx++
		}
		_, err := i.next(false)
		require.ErrorIs(t, err, io.EOF, "content: %s", content)
	}
}
