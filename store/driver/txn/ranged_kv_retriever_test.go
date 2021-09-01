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
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/txnkv/transaction"
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
				{{"abcde", "abcdd"}, nil},
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
				{{"abcdb", "abcda"}, nil},
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
				{{"abcde", "abcdd"}, nil},
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
				{{"abcde", "abcdd"}, nil},
			},
		},
		{
			startKey: "abcde",
			endKey:   "abcdd",
			cases: [][][]interface{}{
				{{nil, nil}, nil},
				{{nil, "abcde"}, nil},
				{{nil, "abcdef"}, nil},
				{{nil, "abcdd"}, nil},
				{{"abcde", nil}, nil},
				{{"abcdd", nil}, nil},
				{{"abcdef", nil}, nil},
				{{"abcde", "abcdd"}, nil},
				{{"abcde", "abcdf"}, nil},
				{{"abcde", "abcdef"}, nil},
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

func TestRangedKVRetrieverScanCurrentRange(t *testing.T) {
	memBuffer := newMemBufferRetriever(t, [][]interface{}{{"a1", "v1"}, {"a2", "v2"}, {"a3", "v3"}, {"a4", "v4"}})
	cases := []struct {
		retriever *RangedKVRetriever
		reverse   bool
		result    [][]interface{}
	}{
		{
			retriever: NewRangeRetriever(memBuffer, nil, nil),
			result:    [][]interface{}{{"a1", "v1"}, {"a2", "v2"}, {"a3", "v3"}, {"a4", "v4"}},
		},
		{
			retriever: NewRangeRetriever(memBuffer, nil, nil),
			reverse:   true,
			result:    [][]interface{}{{"a4", "v4"}, {"a3", "v3"}, {"a2", "v2"}, {"a1", "v1"}},
		},
		{
			retriever: NewRangeRetriever(memBuffer, kv.Key("a10"), kv.Key("a4")),
			result:    [][]interface{}{{"a2", "v2"}, {"a3", "v3"}},
		},
		{
			retriever: NewRangeRetriever(memBuffer, kv.Key("a10"), kv.Key("a4")),
			reverse:   true,
			result:    [][]interface{}{{"a3", "v3"}, {"a2", "v2"}},
		},
		{
			retriever: NewRangeRetriever(memBuffer, nil, kv.Key("a4")),
			reverse:   true,
			result:    [][]interface{}{{"a3", "v3"}, {"a2", "v2"}, {"a1", "v1"}},
		},
	}

	for _, c := range cases {
		iter, err := c.retriever.ScanCurrentRange(c.reverse)
		assert.Nil(t, err)
		for i := range c.result {
			expectedKey := makeBytes(c.result[i][0])
			expectedVal := makeBytes(c.result[i][1])
			assert.True(t, iter.Valid())
			gotKey := []byte(iter.Key())
			gotVal := iter.Value()
			assert.Equal(t, expectedKey, gotKey)
			assert.Equal(t, expectedVal, gotVal)
			err = iter.Next()
			assert.Nil(t, err)
		}
		assert.False(t, iter.Valid())
	}
}

func TestSearchRetrieverByKey(t *testing.T) {
	retrievers := sortedRetrievers{
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab1"), kv.Key("ab3")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab3"), kv.Key("ab5")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab6"), kv.Key("ab7")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab8"), kv.Key("ab9")),
	}

	retrievers2 := sortedRetrievers{
		NewRangeRetriever(&kv.EmptyRetriever{}, nil, kv.Key("ab1")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab3"), kv.Key("ab5")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab6"), kv.Key("ab7")),
		NewRangeRetriever(&kv.EmptyRetriever{}, kv.Key("ab8"), nil),
	}

	cases := []struct {
		retrievers  sortedRetrievers
		search      interface{}
		expectedIdx int
	}{
		{retrievers: retrievers, search: nil, expectedIdx: 0},
		{retrievers: retrievers, search: "ab", expectedIdx: 0},
		{retrievers: retrievers, search: "ab1", expectedIdx: 0},
		{retrievers: retrievers, search: "ab2", expectedIdx: 0},
		{retrievers: retrievers, search: "ab3", expectedIdx: 1},
		{retrievers: retrievers, search: "ab31", expectedIdx: 1},
		{retrievers: retrievers, search: "ab4", expectedIdx: 1},
		{retrievers: retrievers, search: "ab5", expectedIdx: 2},
		{retrievers: retrievers, search: "ab51", expectedIdx: 2},
		{retrievers: retrievers, search: "ab71", expectedIdx: 3},
		{retrievers: retrievers, search: "ab8", expectedIdx: 3},
		{retrievers: retrievers, search: "ab81", expectedIdx: 3},
		{retrievers: retrievers, search: "ab9", expectedIdx: 4},
		{retrievers: retrievers, search: "aba", expectedIdx: 4},
		{retrievers: retrievers2, search: nil, expectedIdx: 0},
		{retrievers: retrievers2, search: "ab0", expectedIdx: 0},
		{retrievers: retrievers2, search: "ab8", expectedIdx: 3},
		{retrievers: retrievers2, search: "ab9", expectedIdx: 3},
	}

	for _, c := range cases {
		idx, r := c.retrievers.searchRetrieverByKey(makeBytes(c.search))
		assert.Equal(t, c.expectedIdx, idx)
		if idx < len(c.retrievers) {
			assert.Equal(t, c.retrievers[idx], r)
		} else {
			assert.Nil(t, r)
		}
	}
}

func TestGetScanRetrievers(t *testing.T) {
	type mockRetriever struct {
		kv.EmptyRetriever
		// Avoid zero size struct, make it can be compared for different variables
		_ interface{}
	}

	snap := &mockRetriever{}
	retrievers1 := sortedRetrievers{
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab1"), kv.Key("ab3")),
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab3"), kv.Key("ab5")),
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab6"), kv.Key("ab7")),
	}

	retrievers2 := sortedRetrievers{
		NewRangeRetriever(&mockRetriever{}, nil, kv.Key("ab1")),
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab3"), kv.Key("ab5")),
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab5"), kv.Key("ab7")),
		NewRangeRetriever(&mockRetriever{}, kv.Key("ab8"), nil),
	}

	cases := []struct {
		retrievers sortedRetrievers
		startKey   interface{}
		endKey     interface{}
		expected   sortedRetrievers
	}{
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab1",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab2",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab2")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab3",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab4",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab4")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab51",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab51")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab61",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab61")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: "ab8",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), kv.Key("ab8")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   nil, endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(snap, nil, kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab0", endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab2", endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(retrievers1[0].Retriever, kv.Key("ab2"), kv.Key("ab3")),
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab3", endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(snap, kv.Key("ab5"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab51", endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab51"), kv.Key("ab6")),
				NewRangeRetriever(retrievers1[2].Retriever, kv.Key("ab6"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab3", endKey: "ab4",
			expected: sortedRetrievers{
				NewRangeRetriever(retrievers1[1].Retriever, kv.Key("ab3"), kv.Key("ab4")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab51", endKey: "ab52",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab51"), kv.Key("ab52")),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab8", endKey: nil,
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab8"), nil),
			},
		},
		{
			retrievers: retrievers1,
			startKey:   "ab8", endKey: "ab9",
			expected: sortedRetrievers{
				NewRangeRetriever(snap, kv.Key("ab8"), kv.Key("ab9")),
			},
		},
		{
			retrievers: retrievers2,
			startKey:   "ab0", endKey: "ab9",
			expected: sortedRetrievers{
				NewRangeRetriever(retrievers2[0].Retriever, kv.Key("ab0"), kv.Key("ab1")),
				NewRangeRetriever(snap, kv.Key("ab1"), kv.Key("ab3")),
				NewRangeRetriever(retrievers2[1].Retriever, kv.Key("ab3"), kv.Key("ab5")),
				NewRangeRetriever(retrievers2[2].Retriever, kv.Key("ab5"), kv.Key("ab7")),
				NewRangeRetriever(snap, kv.Key("ab7"), kv.Key("ab8")),
				NewRangeRetriever(retrievers2[3].Retriever, kv.Key("ab8"), kv.Key("ab9")),
			},
		},
	}

	for _, c := range cases {
		result := c.retrievers.GetScanRetrievers(makeBytes(c.startKey), makeBytes(c.endKey), snap)
		expected := c.expected
		assert.Equal(t, len(expected), len(result))
		for i := range expected {
			assert.Same(t, expected[i].Retriever, result[i].Retriever)
			assert.Equal(t, expected[i].StartKey, result[i].StartKey)
			assert.Equal(t, expected[i].EndKey, result[i].EndKey)
		}

		result = c.retrievers.GetScanRetrievers(makeBytes(c.startKey), makeBytes(c.endKey), nil)
		expected = make([]*RangedKVRetriever, 0)
		for _, r := range c.expected {
			if r.Retriever != snap {
				expected = append(expected, r)
			}
		}
		assert.Equal(t, len(expected), len(result))
		for i := range expected {
			assert.Same(t, expected[i].Retriever, result[i].Retriever)
			assert.Equal(t, expected[i].StartKey, result[i].StartKey)
			assert.Equal(t, expected[i].EndKey, result[i].EndKey)
		}
	}
}

func newMemBufferRetriever(t *testing.T, data [][]interface{}) kv.MemBuffer {
	txn, err := transaction.NewTiKVTxn(nil, nil, 0, "")
	assert.Nil(t, err)
	memBuffer := NewTiKVTxn(txn).GetMemBuffer()
	for _, d := range data {
		err := memBuffer.Set(makeBytes(d[0]), makeBytes(d[1]))
		assert.Nil(t, err)
	}
	return memBuffer
}

func TestSortedRetrieversTryGet(t *testing.T) {
	retrievers := sortedRetrievers{
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab0", "v0"}, {"ab2", "v2"}, {"ab3", "v3"}, {"ab4", "v4x"}}),
			kv.Key("ab1"), kv.Key("ab3"),
		),
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab4", "v4"}, {"ab41", "v41"}}),
			kv.Key("ab3"), kv.Key("ab5"),
		),
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab7", "v7"}}),
			kv.Key("ab6"), kv.Key("ab8"),
		),
	}

	tryGetCases := [][]interface{}{
		// {key, expectedValue, fromRetriever}
		{"ab0", nil, false},
		{"ab1", kv.ErrNotExist, true},
		{"ab11", kv.ErrNotExist, true},
		{"ab2", "v2", true},
		{"ab3", kv.ErrNotExist, true},
		{"ab4", "v4", true},
		{"ab41", "v41", true},
		{"ab5", nil, false},
		{"ab7", "v7", true},
		{"ab8", nil, false},
		{"ab9", nil, false},
	}

	for _, c := range tryGetCases {
		fromRetriever, val, err := retrievers.TryGet(context.TODO(), makeBytes(c[0]))
		assert.Equal(t, c[2], fromRetriever)
		if !fromRetriever {
			assert.Nil(t, err)
			assert.Nil(t, val)
			continue
		}

		if expectedErr, ok := c[1].(error); ok {
			assert.True(t, errors.ErrorEqual(expectedErr, err))
			assert.Nil(t, val)
		} else {
			assert.Equal(t, makeBytes(c[1]), val)
			assert.Nil(t, err)
		}
	}
}

func TestSortedRetrieversTryBatchGet(t *testing.T) {
	retrievers := sortedRetrievers{
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab0", "v0"}, {"ab2", "v2"}, {"ab3", "v3"}, {"ab4", "v4x"}}),
			kv.Key("ab1"), kv.Key("ab3"),
		),
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab4", "v4"}, {"ab41", "v41"}, {"ab51", "v51"}}),
			kv.Key("ab3"), kv.Key("ab5"),
		),
		NewRangeRetriever(
			newMemBufferRetriever(t, [][]interface{}{{"ab7", "v7"}}),
			kv.Key("ab6"), kv.Key("ab8"),
		),
	}

	tryBatchGetCases := []struct {
		keys    []kv.Key
		result  map[string]string
		retKeys []kv.Key
	}{
		{
			keys:    []kv.Key{kv.Key("ab0")},
			result:  map[string]string{},
			retKeys: []kv.Key{kv.Key("ab0")},
		},
		{
			keys:    []kv.Key{kv.Key("ab0"), kv.Key("ab51"), kv.Key("ab52"), kv.Key("ab9")},
			result:  map[string]string{},
			retKeys: []kv.Key{kv.Key("ab0"), kv.Key("ab51"), kv.Key("ab52"), kv.Key("ab9")},
		},
		{
			keys: []kv.Key{kv.Key("ab21"), kv.Key("ab3"), kv.Key("ab4"), kv.Key("ab41"), kv.Key("ab7")},
			result: map[string]string{
				"ab4":  "v4",
				"ab41": "v41",
				"ab7":  "v7",
			},
			retKeys: nil,
		},
		{
			keys: []kv.Key{kv.Key("ab0"), kv.Key("ab2"), kv.Key("ab51"), kv.Key("ab7"), kv.Key("ab9")},
			result: map[string]string{
				"ab2": "v2",
				"ab7": "v7",
			},
			retKeys: []kv.Key{kv.Key("ab0"), kv.Key("ab51"), kv.Key("ab9")},
		},
		{
			keys: []kv.Key{kv.Key("ab2"), kv.Key("ab4"), kv.Key("ab51"), kv.Key("ab52"), kv.Key("ab6"), kv.Key("ab7"), kv.Key("ab9")},
			result: map[string]string{
				"ab2": "v2",
				"ab4": "v4",
				"ab7": "v7",
			},
			retKeys: []kv.Key{kv.Key("ab51"), kv.Key("ab52"), kv.Key("ab9")},
		},
		{
			keys: []kv.Key{kv.Key("ab0"), kv.Key("ab51"), kv.Key("ab6"), kv.Key("ab7"), kv.Key("ab8"), kv.Key("ab9")},
			result: map[string]string{
				"ab7": "v7",
			},
			retKeys: []kv.Key{kv.Key("ab0"), kv.Key("ab51"), kv.Key("ab8"), kv.Key("ab9")},
		},
	}

	for _, c := range tryBatchGetCases {
		got := make(map[string][]byte)
		keys, err := retrievers.TryBatchGet(context.TODO(), c.keys, func(k kv.Key, v []byte) {
			_, ok := got[string(k)]
			assert.False(t, ok)
			got[string(k)] = v
		})
		assert.Nil(t, err)
		assert.Equal(t, c.retKeys, keys)
		assert.Equal(t, len(c.result), len(got))
		for k, v := range c.result {
			val, ok := got[k]
			assert.True(t, ok)
			assert.Equal(t, []byte(v), val)
		}
	}
}
