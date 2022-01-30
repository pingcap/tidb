// Copyright 2016 PingCAP, Inc.
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
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestIncInt64(t *testing.T) {
	mb := newMockMap()
	key := Key("key")
	v, err := IncInt64(mb, key, 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), v)

	v, err = IncInt64(mb, key, 10)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), v)

	err = mb.Set(key, []byte("not int"))
	assert.Nil(t, err)

	_, err = IncInt64(mb, key, 1)
	assert.NotNil(t, err)

	// test int overflow
	maxUint32 := int64(^uint32(0))
	err = mb.Set(key, []byte(strconv.FormatInt(maxUint32, 10)))
	assert.Nil(t, err)

	v, err = IncInt64(mb, key, 1)
	assert.Nil(t, err)
	assert.Equal(t, maxUint32+1, v)
}

func TestGetInt64(t *testing.T) {
	mb := newMockMap()
	key := Key("key")
	v, err := GetInt64(context.TODO(), mb, key)
	assert.Equal(t, int64(0), v)
	assert.Nil(t, err)

	_, err = IncInt64(mb, key, 15)
	assert.Nil(t, err)
	v, err = GetInt64(context.TODO(), mb, key)
	assert.Equal(t, int64(15), v)
	assert.Nil(t, err)
}

type mockMap struct {
	index []Key
	value [][]byte
}

var _ RetrieverMutator = &mockMap{}

func newMockMap() *mockMap {
	return &mockMap{
		index: make([]Key, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockMap) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	//TODO nothing.
}

func (s *mockMap) Iter(Key, Key) (Iterator, error) {
	return nil, nil
}
func (s *mockMap) IterReverse(Key) (Iterator, error) {
	return nil, nil
}

func (s *mockMap) Get(_ context.Context, k Key) ([]byte, error) {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			return s.value[i], nil
		}
	}
	return nil, ErrNotExist
}

func (s *mockMap) Set(k Key, v []byte) error {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			s.value[i] = v
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, v)
	return nil
}

func (s *mockMap) Delete(k Key) error {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			s.index = append(s.index[:i], s.index[i+1:]...)
			s.value = append(s.value[:i], s.value[i+1:]...)
			return nil
		}
	}
	return nil
}
