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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"strconv"

	. "github.com/pingcap/check"
)

var _ = Suite(testUtilsSuite{})

type testUtilsSuite struct {
}

type mockMap struct {
	index []Key
	value [][]byte
}

func newMockMap() *mockMap {
	return &mockMap{
		index: make([]Key, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockMap) Iter(k Key, upperBound Key) (Iterator, error) {
	return nil, nil
}
func (s *mockMap) IterReverse(k Key) (Iterator, error) {
	return nil, nil
}

func (s *mockMap) Get(ctx context.Context, k Key) ([]byte, error) {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			return s.value[i], nil
		}
	}
	return nil, nil
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

func (s testUtilsSuite) TestIncInt64(c *C) {
	mb := newMockMap()
	key := Key("key")
	v, err := IncInt64(mb, key, 1)
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(1))
	v, err = IncInt64(mb, key, 10)
	c.Check(err, IsNil)
	c.Check(v, Equals, int64(11))

	err = mb.Set(key, []byte("not int"))
	c.Check(err, IsNil)
	_, err = IncInt64(mb, key, 1)
	c.Check(err, NotNil)

	// test int overflow
	maxUint32 := int64(^uint32(0))
	err = mb.Set(key, []byte(strconv.FormatInt(maxUint32, 10)))
	c.Check(err, IsNil)
	v, err = IncInt64(mb, key, 1)
	c.Check(err, IsNil)
	c.Check(v, Equals, maxUint32+1)

}

func (s testUtilsSuite) TestGetInt64(c *C) {
	mb := newMockMap()
	key := Key("key")
	v, err := GetInt64(context.TODO(), mb, key)
	c.Check(v, Equals, int64(0))
	c.Check(err, IsNil)

	_, err = IncInt64(mb, key, 15)
	c.Check(err, IsNil)
	v, err = GetInt64(context.TODO(), mb, key)
	c.Check(v, Equals, int64(15))
	c.Check(err, IsNil)
}
