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

package structure_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/util/testleak"
)

func TestTxStructure(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTxStructureSuite{})

type testTxStructureSuite struct {
	store kv.Storage
}

func (s *testTxStructureSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testTxStructureSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func (s *testTxStructureSuite) TestString(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")
	value := []byte("1")
	err = tx.Set(key, value)
	c.Assert(err, IsNil)

	v, err := tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, value)

	n, err := tx.Inc(key, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	v, err = tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("2"))

	n, err = tx.GetInt64(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	err = tx.Clear(key)
	c.Assert(err, IsNil)

	v, err = tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	err = tx1.Set(key, value)
	c.Assert(err, NotNil)

	_, err = tx1.Inc(key, 1)
	c.Assert(err, NotNil)

	err = tx1.Clear(key)
	c.Assert(err, NotNil)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testTxStructureSuite) TestList(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")
	err = tx.LPush(key, []byte("3"), []byte("2"), []byte("1"))
	c.Assert(err, IsNil)

	// Test LGetAll.
	err = tx.LPush(key, []byte("11"))
	c.Assert(err, IsNil)
	values, err := tx.LGetAll(key)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, [][]byte{[]byte("3"), []byte("2"), []byte("1"), []byte("11")})
	value, err := tx.LPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("11"))

	l, err := tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(3))

	value, err = tx.LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	err = tx.LSet(key, 1, []byte("4"))
	c.Assert(err, IsNil)

	value, err = tx.LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	err = tx.LSet(key, 1, []byte("2"))
	c.Assert(err, IsNil)

	err = tx.LSet(key, 100, []byte("2"))
	c.Assert(err, NotNil)

	value, err = tx.LIndex(key, -1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("3"))

	value, err = tx.LPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	err = tx.RPush(key, []byte("4"))
	c.Assert(err, IsNil)

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(3))

	value, err = tx.LIndex(key, -1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("3"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.LPush(key, []byte("1"))
	c.Assert(err, IsNil)

	err = tx.LClear(key)
	c.Assert(err, IsNil)

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	err = tx1.LPush(key, []byte("1"))
	c.Assert(err, NotNil)

	_, err = tx1.RPop(key)
	c.Assert(err, NotNil)

	err = tx1.LSet(key, 1, []byte("2"))
	c.Assert(err, NotNil)

	err = tx1.LClear(key)
	c.Assert(err, NotNil)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testTxStructureSuite) TestHash(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")

	tx.EncodeHashAutoIDKeyValue(key, key, 5)

	err = tx.HSet(key, []byte("1"), []byte("1"))
	c.Assert(err, IsNil)

	err = tx.HSet(key, []byte("2"), []byte("2"))
	c.Assert(err, IsNil)

	l, err := tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	value, err := tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	value, err = tx.HGet(key, []byte("fake"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	keys, err := tx.HKeys(key)
	c.Assert(err, IsNil)
	c.Assert(keys, DeepEquals, [][]byte{[]byte("1"), []byte("2")})

	res, err := tx.HGetAll(key)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []structure.HashPair{
		{Field: []byte("1"), Value: []byte("1")},
		{Field: []byte("2"), Value: []byte("2")}})

	res, err = tx.HGetLastN(key, 1)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []structure.HashPair{
		{Field: []byte("2"), Value: []byte("2")}})

	res, err = tx.HGetLastN(key, 2)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []structure.HashPair{
		{Field: []byte("2"), Value: []byte("2")},
		{Field: []byte("1"), Value: []byte("1")}})

	err = tx.HDel(key, []byte("1"))
	c.Assert(err, IsNil)

	value, err = tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))

	n, err := tx.HInc(key, []byte("1"), 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	// Test set new value which equals to old value.
	value, err = tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	err = tx.HSet(key, []byte("1"), []byte("1"))
	c.Assert(err, IsNil)

	value, err = tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	n, err = tx.HInc(key, []byte("1"), 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	n, err = tx.HInc(key, []byte("1"), 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	n, err = tx.HGetInt64(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	err = tx.HClear(key)
	c.Assert(err, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.HDel(key, []byte("fake_key"))
	c.Assert(err, IsNil)

	// Test set nil value.
	value, err = tx.HGet(key, []byte("nil_key"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.HSet(key, []byte("nil_key"), nil)
	c.Assert(err, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.HSet(key, []byte("nil_key"), []byte("1"))
	c.Assert(err, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))

	value, err = tx.HGet(key, []byte("nil_key"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	err = tx.HSet(key, []byte("nil_key"), nil)
	c.Assert(err, NotNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))

	value, err = tx.HGet(key, []byte("nil_key"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	err = tx.HSet(key, []byte("nil_key"), []byte("2"))
	c.Assert(err, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))

	value, err = tx.HGet(key, []byte("nil_key"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	_, err = tx1.HInc(key, []byte("1"), 1)
	c.Assert(err, NotNil)

	err = tx1.HDel(key, []byte("1"))
	c.Assert(err, NotNil)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := structure.NewStructure(txn, txn, []byte{0x00})
		err = t.Set(key, []byte("abc"))
		c.Assert(err, IsNil)

		value, err = t.Get(key)
		c.Assert(err, IsNil)
		c.Assert(value, DeepEquals, []byte("abc"))
		return nil
	})
	c.Assert(err, IsNil)
}

func (*testTxStructureSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		structure.ErrInvalidHashKeyFlag,
		structure.ErrInvalidListIndex,
		structure.ErrInvalidListMetaData,
		structure.ErrWriteOnSnapshot,
	}
	for _, err := range kvErrs {
		code := err.ToSQLError().Code
		c.Assert(code != mysql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
