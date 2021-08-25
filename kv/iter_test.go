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
package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newSliceIterWithCopy(data []*Entry) *SliceIter {
	if data == nil {
		return NewSliceIter(nil)
	}

	return NewSliceIter(append([]*Entry{}, data...))
}

func TestSliceIter(t *testing.T) {
	assert := assert.New(t)
	slices := []struct {
		data []*Entry
	}{
		{data: nil},
		{data: []*Entry{}},
		{data: []*Entry{{Key: Key("k1"), Value: []byte("v1")}}},
		{data: []*Entry{
			{Key: Key("k1"), Value: []byte("v1")},
			{Key: Key("k2"), Value: []byte("v2")},
		}},
		{data: []*Entry{
			{Key: Key("k1"), Value: []byte("v1")},
			{Key: Key("k0"), Value: []byte("")},
			{Key: Key("k2"), Value: []byte("v2")},
		}},
	}

	for _, s := range slices {
		// Normal iteration
		iter := newSliceIterWithCopy(s.data)
		for _, entry := range s.data {
			assert.True(iter.Valid())
			assert.Equal(entry.Key, iter.Key())
			assert.Equal(entry.Value, iter.Value())
			err := iter.Next()
			assert.Nil(err)
		}
		assert.False(iter.Valid())
		err := iter.Next()
		assert.NotNil(err)

		// Slice should not be modified
		slice := iter.GetSlice()
		assert.Equal(len(s.data), len(slice))
		for i := range s.data {
			assert.Equal(s.data[i].Key, slice[i].Key)
			assert.Equal(s.data[i].Value, slice[i].Value)
		}

		// Iteration after close
		iter = newSliceIterWithCopy(s.data)
		if len(s.data) == 0 {
			assert.False(iter.Valid())
		} else {
			assert.True(iter.Valid())
		}
		iter.Close()
		assert.False(iter.Valid())
		err = iter.Next()
		assert.NotNil(err)

		// Slice should not be modified
		slice = iter.GetSlice()
		assert.Equal(len(s.data), len(slice))
		for i := range s.data {
			assert.Equal(s.data[i].Key, slice[i].Key)
			assert.Equal(s.data[i].Value, slice[i].Value)
		}
	}
}
