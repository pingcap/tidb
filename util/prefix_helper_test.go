// Copyright 2015 PingCAP, Inc.
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

package util

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/codec"
)

const (
	startIndex = 0
	testCount  = 12
	testPow    = 10
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPrefixSuite{})

type testPrefixSuite struct {
	s kv.Storage
}

func (s *testPrefixSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := localstore.Driver{
		Driver: goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.s = store

	// must in cache
	cacheS, _ := d.Open(path)
	c.Assert(cacheS, Equals, store)
}

func (s *testPrefixSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%d", n))
}

type MockContext struct {
	prefix int
	values map[fmt.Stringer]interface{}
	kv.Storage
	txn kv.Transaction
}

func (c *MockContext) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

func (c *MockContext) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

func (c *MockContext) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

func (c *MockContext) GetTxn(forceNew bool) (kv.Transaction, error) {
	var err error
	c.txn, err = c.Begin()
	if err != nil {
		return nil, err
	}

	return c.txn, nil
}

func (c *MockContext) fillTxn() error {
	if c.txn == nil {
		return nil
	}

	var err error
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i + (c.prefix * testPow))
		err = c.txn.Set(val, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *MockContext) FinishTxn(rollback bool) error {
	if c.txn == nil {
		return nil
	}

	return c.txn.Commit()
}

func (s *testPrefixSuite) TestPrefix(c *C) {
	ctx := &MockContext{10000000, make(map[fmt.Stringer]interface{}), s.s, nil}
	ctx.fillTxn()
	err := DelKeyWithPrefix(ctx, string(encodeInt(ctx.prefix)))
	c.Assert(err, IsNil)
	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	str := "key100jfowi878230"
	err = txn.Set([]byte(str), []byte("val32dfaskli384757^*&%^"))
	c.Assert(err, IsNil)
	err = ScanMetaWithPrefix(txn, str, func([]byte, []byte) bool {
		return true
	})
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testPrefixSuite) TestCode(c *C) {
	table := []struct {
		prefix string
		h      int64
		ID     int64
	}{
		{"123abc##!@#((_)((**&&^^%$", 1234567890, 0},
		{"", 1, 0},
		{"", -1, 0},
		{"", -1, 1},
	}

	for _, t := range table {
		b := EncodeRecordKey(t.prefix, t.h, t.ID)
		handle, err := DecodeHandleFromRowKey(string(b))
		c.Assert(err, IsNil)
		c.Assert(handle, Equals, t.h)
	}

	b1 := EncodeRecordKey("aa", 1, 0)
	b2 := EncodeRecordKey("aa", 1, 1)
	c.Logf("%#v, %#v", b2, b1)
	_, err := codec.StripEnd(b1)
	c.Assert(err, IsNil)
}

func (s *testPrefixSuite) TestPrefixFilter(c *C) {
	rowKey := []byte("test@#$%l(le[0]..prefix) 2uio")
	rowKey[8] = 0x00
	rowKey[9] = 0x00
	f := RowKeyPrefixFilter(rowKey)
	b := f(append(rowKey, []byte("akjdf3*(34")...))
	c.Assert(b, IsFalse)
	buf := f([]byte("sjfkdlsaf"))
	c.Assert(buf, IsTrue)
}
