// Copyright 2017 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"

	storeerr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
)

type testSafePointSuite struct {
	OneByOneSuite
	store  tikv.StoreProbe
	prefix string
}

var _ = Suite(&testSafePointSuite{})

func (s *testSafePointSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = tikv.StoreProbe{KVStore: NewTestStore(c)}
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSafePointSuite) beginTxn(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func mymakeKeys(rowNum int, prefix string) [][]byte {
	keys := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSafePointSuite) waitUntilErrorPlugIn(t uint64) {
	for {
		s.store.SaveSafePoint(t + 10)
		cachedTime := time.Now()
		newSafePoint, err := s.store.LoadSafePoint()
		if err == nil {
			s.store.UpdateSPCache(newSafePoint, cachedTime)
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *testSafePointSuite) TestSafePoint(c *C) {
	txn := s.beginTxn(c)
	for i := 0; i < 10; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// for txn get normally
	txn2 := s.beginTxn(c)
	_, err = txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(err, IsNil)

	// for txn get
	s.waitUntilErrorPlugIn(txn2.StartTS())
	_, geterr := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(geterr, NotNil)
	_, isFallBehind := errors.Cause(geterr).(*tikverr.ErrGCTooEarly)
	c.Assert(isFallBehind, IsTrue)

	// for txn seek
	txn3 := s.beginTxn(c)
	s.waitUntilErrorPlugIn(txn3.StartTS())
	_, seekerr := txn3.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(seekerr, NotNil)
	_, isFallBehind = errors.Cause(seekerr).(*tikverr.ErrGCTooEarly)
	c.Assert(isFallBehind, IsTrue)

	// for snapshot batchGet
	keys := mymakeKeys(10, s.prefix)
	txn4 := s.beginTxn(c)
	s.waitUntilErrorPlugIn(txn4.StartTS())
	_, batchgeterr := toTiDBTxn(&txn4).BatchGet(context.Background(), toTiDBKeys(keys))
	c.Assert(batchgeterr, NotNil)
	// FIXME: batch get returns a TiDB error, instead of a normal error,
	// should it be unified with get/iter?
	c.Assert(terror.ErrorEqual(batchgeterr, storeerr.ErrGCTooEarly), IsTrue)

	// test safepoint cache is expired and returns expected error
	msgSPCacheExpired := "safe point cache is expired, can't check start-ts"
	txn5 := s.beginTxn(c)
	s.store.UpdateSPCache(txn5.StartTS(), time.Now().Add(-tikv.GcSafePointCacheInterval))
	_, geterr2 := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(geterr2, NotNil)
	c.Assert(geterr2, ErrorMatches, tikverr.NewErrPDServerTimeout(msgSPCacheExpired).Error())
}
