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

func r(key, value string) *kv.Entry {
	bKey := []byte(key)
	bValue := []byte(value)
	if value == "nil" {
		bValue = nil
	}

	return &kv.Entry{Key: bKey, Value: bValue}
}

func checkExpectedIterData(t *testing.T, expected []*kv.Entry, iter kv.Iterator) {
	for _, entry := range expected {
		assert.True(t, iter.Valid())
		assert.Equal(t, entry.Key, iter.Key())
		assert.Equal(t, entry.Value, iter.Value())
		err := iter.Next()
		assert.Nil(t, err)
	}

	assert.False(t, iter.Valid())
	err := iter.Next()
	assert.NotNil(t, err)
	checkCloseIter(t, iter)
}

func checkCloseIter(t *testing.T, iter kv.Iterator) {
	iter.Close()
	assert.False(t, iter.Valid())
	err := iter.Next()
	assert.NotNil(t, err)
}

func TestOneByOneIter(t *testing.T) {
	iter1 := kv.NewSliceIter([]*kv.Entry{
		r("k1", "v1"),
		r("k5", "v3"),
	})
	iter2 := kv.NewSliceIter([]*kv.Entry{
		r("k2", "v2"),
		r("k4", "v4"),
	})
	iter3 := kv.NewSliceIter([]*kv.Entry{})
	iter4 := kv.NewSliceIter([]*kv.Entry{
		r("k3", "v3"),
		r("k6", "v6"),
	})

	// test for normal iter
	oneByOne := newOneByOneIter([]kv.Iterator{iter1, iter2, iter3, iter4})
	expected := make([]*kv.Entry, 0)
	expected = append(expected, iter1.GetSlice()...)
	expected = append(expected, iter2.GetSlice()...)
	expected = append(expected, iter3.GetSlice()...)
	expected = append(expected, iter4.GetSlice()...)
	checkExpectedIterData(t, expected, oneByOne)

	// test for close
	oneByOne = newOneByOneIter([]kv.Iterator{iter1, iter2, iter3, iter4})
	checkCloseIter(t, oneByOne)

	// test for one inner iter
	iter1 = kv.NewSliceIter([]*kv.Entry{
		r("k1", "v1"),
		r("k5", "v3"),
	})
	oneByOne = newOneByOneIter([]kv.Iterator{iter1})
	expected = make([]*kv.Entry, 0)
	expected = append(expected, iter1.GetSlice()...)
	checkExpectedIterData(t, expected, oneByOne)

	// test for empty iter
	iter3 = kv.NewSliceIter([]*kv.Entry{})
	oneByOne = newOneByOneIter([]kv.Iterator{iter3})
	checkExpectedIterData(t, nil, oneByOne)
}

func TestFilterEmptyValueIter(t *testing.T) {
	cases := []struct {
		data []*kv.Entry
	}{
		{data: nil},
		{data: []*kv.Entry{
			r("k1", "v1"),
		}},
		{data: []*kv.Entry{
			r("k1", ""),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", "v4"),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", ""),
			r("k5", "v5"),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", ""),
			r("k5", ""),
		}},
	}

	for _, c := range cases {
		data := make([]*kv.Entry, 0)
		for _, entry := range c.data {
			if len(entry.Value) > 0 {
				data = append(data, entry)
			}
		}

		iter, err := filterEmptyValue(kv.NewSliceIter(c.data))
		assert.Nil(t, err)
		checkExpectedIterData(t, data, iter)

		iter, err = filterEmptyValue(kv.NewSliceIter(c.data))
		assert.Nil(t, err)
		checkCloseIter(t, iter)
	}
}

func TestLowerBoundReverseIter(t *testing.T) {
	cases := []struct {
		data       []*kv.Entry
		lowerBound kv.Key
		expected   []*kv.Entry
	}{
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: nil,
			expected: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k1"),
			expected: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k2"),
			expected: []*kv.Entry{
				r("k2", "v2"),
			},
		},
		{
			data: []*kv.Entry{
				r("k1", "v1"),
				r("k2", "v2"),
			},
			lowerBound: kv.Key("k3"),
			expected:   nil,
		},
		{
			data: []*kv.Entry{
				r("k1", "v1"),
				r("k2", "v2"),
			},
			lowerBound: kv.Key("k0"),
			expected: []*kv.Entry{
				r("k1", "v1"),
				r("k2", "v2"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k11"),
			expected: []*kv.Entry{
				r("k2", "v2"),
			},
		},
		{
			data:       nil,
			lowerBound: nil,
			expected:   nil,
		},
		{
			data:       nil,
			lowerBound: kv.Key("k1"),
			expected:   nil,
		},
	}

	for _, c := range cases {
		iter := newLowerBoundReverseIter(kv.NewSliceIter(c.data), c.lowerBound)
		checkExpectedIterData(t, c.expected, iter)

		iter = newLowerBoundReverseIter(kv.NewSliceIter(c.data), c.lowerBound)
		checkCloseIter(t, iter)
	}
}
