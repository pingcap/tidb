// Copyright 2021 PingCAP, Inc.
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
package txn

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/assert"
)

func newRetriever(startKey, endKey kv.Key) *RangedKVRetriever {
	return NewRangeRetriever(&kv.EmptyRetriever{}, startKey, endKey)
}

func makeBytes(s interface{}) []byte {
	if s == nil {
		return nil
	}

	switch key := s.(type) {
	case string:
		return []byte(key)
	default:
		return key.([]byte)
	}
}

func TestRangedKVRetrieverValid(t *testing.T) {
	assert := assert.New(t)
	assert.True(newRetriever(nil, nil).Valid())
	assert.True(newRetriever(nil, kv.Key("0")).Valid())
	assert.True(newRetriever(kv.Key("0"), nil).Valid())
	assert.True(newRetriever(kv.Key("0"), kv.Key("01")).Valid())
	assert.True(newRetriever(kv.Key("0"), kv.Key("00")).Valid())

	assert.False(newRetriever(kv.Key("0"), kv.Key("0")).Valid())
	assert.False(newRetriever(kv.Key("00"), kv.Key("0")).Valid())
	assert.False(newRetriever(kv.Key("b"), kv.Key("a")).Valid())
}

func TestRangedKVRetrieverContains(t *testing.T) {
	assert := assert.New(t)
	cases := []struct {
		startKey    interface{}
		endKey      interface{}
		contains    []interface{}
		notContains []interface{}
	}{
		{
			startKey:    "abcdef",
			endKey:      nil,
			contains:    []interface{}{[]byte{0xFF}, "abcdef", "abcdefg", "abcdeg", "bbcdeg"},
			notContains: []interface{}{"abcdee", "abcde", "a", "0bcdefg", []byte{0x0}},
		},
		{
			startKey:    nil,
			endKey:      "abcdef",
			contains:    []interface{}{"abcdee", "abcde", "a", "0bcdefg", []byte{0x0}},
			notContains: []interface{}{[]byte{0xFF}, "abcdef", "abcdefg", "abcdeg", "bbcdeg"},
		},
		{
			startKey:    "abcdef",
			endKey:      "abcdeg",
			contains:    []interface{}{"abcdef", "abcdefg"},
			notContains: []interface{}{"abcdeg", "abcdega", "abcdee", "abcdeea", "a", []byte{0xFF}, []byte{0x0}},
		},
		{
			startKey:    "abcdef",
			endKey:      "abcdeh",
			contains:    []interface{}{"abcdef", "abcdeg", "abcdefg"},
			notContains: []interface{}{"abcdeh"},
		},
		{
			startKey: nil,
			endKey:   nil,
			contains: []interface{}{[]byte{0xFF}, "abcdef", "abcdefg", "abcdeg", "bbcdeg", "abcdee", "abcde", "a", "0bcdefg", []byte{0x0}},
		},
		{
			startKey:    "abcdef",
			endKey:      "abcdef",
			notContains: []interface{}{[]byte{0xFF}, "abcdef", "abcdefg", "abcdeg", "bbcdeg", "abcdee", "abcde", "a", "0bcdefg", []byte{0x0}},
		},
	}

	for _, c := range cases {
		startKey := makeBytes(c.startKey)
		endKey := makeBytes(c.endKey)
		retriever := newRetriever(startKey, endKey)
		if len(startKey) == 0 {
			assert.Nil(retriever.StartKey)
		} else {
			assert.Equal(kv.Key(startKey), retriever.StartKey)
		}

		if len(endKey) == 0 {
			assert.Nil(retriever.EndKey)
		} else {
			assert.Equal(kv.Key(endKey), retriever.EndKey)
		}

		for _, k := range c.contains {
			assert.True(retriever.Contains(makeBytes(k)))
		}

		for _, k := range c.notContains {
			assert.False(retriever.Contains(makeBytes(k)))
		}
	}
}

func TestRangedKVRetrieverIntersect(t *testing.T) {
	assert := assert.New(t)
	retrievers := []struct {
		startKey interface{}
		endKey   interface{}
		// {{inputStart, inputEnd}, {expectedOutputStart, expectedOutputEnd}}
		cases [][][]interface{}
	}{
		{
			startKey: nil,
			endKey:   nil,
			cases: [][][]interface{}{
				{{nil, nil}, {nil, nil}},
				{{"abcde", "abcdg"}, {"abcde", "abcdg"}},
				{{nil, "abcdg"}, {nil, "abcdg"}},
				{{"abcde", nil}, {"abcde", nil}},
			},
		},
		{
			startKey: nil,
			endKey:   "abcde",
			cases: [][][]interface{}{
				{{nil, nil}, {nil, "abcde"}},
				{{nil, "abcde"}, {nil, "abcde"}},
				{{nil, "abcdef"}, {nil, "abcde"}},
				{{nil, "abcdd"}, {nil, "abcdd"}},
				{{"abcde", nil}, nil},
				{{"abcdd", nil}, {"abcdd", "abcde"}},
				{{"abcdef", nil}, nil},
				{{"abcde", "abcdf"}, nil},
				{{"abcdd", "abcde"}, {"abcdd", "abcde"}},
				{{"abcdd", "abcdf"}, {"abcdd", "abcde"}},
				{{"abcdc", "abcdd"}, {"abcdc", "abcdd"}},
				{{"abcdef", "abcdf"}, nil},
			},
		},
		{
			startKey: "abcde",
			endKey:   nil,
			cases: [][][]interface{}{
				{{nil, nil}, {"abcde", nil}},
				{{nil, "abcde"}, nil},
				{{nil, "abcdef"}, {"abcde", "abcdef"}},
				{{nil, "abcdd"}, nil},
				{{"abcde", nil}, {"abcde", nil}},
				{{"abcdd", nil}, {"abcde", nil}},
				{{"abcdef", nil}, {"abcdef", nil}},
				{{"abcde", "abcdf"}, {"abcde", "abcdf"}},
				{{"abcdd", "abcde"}, nil},
				{{"abcdd", "abcdf"}, {"abcde", "abcdf"}},
				{{"abcdc", "abcdd"}, nil},
				{{"abcdef", "abcdf"}, {"abcdef", "abcdf"}},
			},
		},
		{
			startKey: "abcde",
			endKey:   "abcdg",
			cases: [][][]interface{}{
				{{nil, nil}, {"abcde", "abcdg"}},
				{{nil, "abcde"}, nil},
				{{nil, "abcdef"}, {"abcde", "abcdef"}},
				{{nil, "abcdd"}, nil},
				{{nil, "abcdg"}, {"abcde", "abcdg"}},
				{{nil, "abcdga"}, {"abcde", "abcdg"}},
				{{"abcde", nil}, {"abcde", "abcdg"}},
				{{"abcdd", nil}, {"abcde", "abcdg"}},
				{{"abcdef", nil}, {"abcdef", "abcdg"}},
				{{"abcdg", nil}, nil},
				{{"abcdga", nil}, nil},
				{{"abcde", "abcdf"}, {"abcde", "abcdf"}},
				{{"abcdd", "abcde"}, nil},
				{{"abcdd", "abcdf"}, {"abcde", "abcdf"}},
				{{"abcdc", "abcdd"}, nil},
				{{"abcdef", "abcdf"}, {"abcdef", "abcdf"}},
				{{"abcde", "abcdg"}, {"abcde", "abcdg"}},
				{{"abcde", "abcdh"}, {"abcde", "abcdg"}},
				{{"abcdef", "abcdg"}, {"abcdef", "abcdg"}},
				{{"abcdef", "abcdh"}, {"abcdef", "abcdg"}},
				{{"abcdg", "abcdh"}, nil},
			},
		},
	}

	for _, r := range retrievers {
		retriever := newRetriever(makeBytes(r.startKey), makeBytes(r.endKey))
		for _, c := range r.cases {
			result := retriever.Intersect(makeBytes(c[0][0]), makeBytes(c[0][1]))
			if len(c[1]) == 0 {
				assert.Nil(result)
				continue
			}

			assert.Equal(retriever.Retriever, result.Retriever)
			expectedStart := makeBytes(c[1][0])
			expectedEnd := makeBytes(c[1][1])
			if len(expectedStart) == 0 {
				assert.Nil(result.StartKey)
			} else {
				assert.Equal(kv.Key(expectedStart), result.StartKey)
			}

			if len(expectedEnd) == 0 {
				assert.Nil(result.EndKey)
			} else {
				assert.Equal(kv.Key(expectedEnd), result.EndKey)
			}
		}
	}
}
