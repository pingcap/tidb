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

package tikv

import (
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/kv"
)

type testScanSuite struct {
	OneByOneSuite
	store   *tikvStore
	prefix  string
	rowNums []int
}

var _ = Suite(&testScanSuite{})

func (s *testScanSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*tikvStore)
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, scanBatchSize, scanBatchSize+1, scanBatchSize*3)
}

func (s *testScanSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testScanSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testScanSuite) TestScan(c *C) {
	check := func(c *C, scan kv.Iterator, rowNum int, keyOnly bool) {
		for i := 0; i < rowNum; i++ {
			k := scan.Key()
			c.Assert([]byte(k), BytesEquals, encodeKey(s.prefix, s08d("key", i)))
			if !keyOnly {
				v := scan.Value()
				c.Assert(v, BytesEquals, valueBytes(i))
			}
			// Because newScan return first item without calling scan.Next() just like go-hbase,
			// for-loop count will decrease 1.
			if i < rowNum-1 {
				scan.Next()
			}
		}
		scan.Next()
		c.Assert(scan.Valid(), IsFalse)
	}

	for _, rowNum := range s.rowNums {
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(context.Background())
		c.Assert(err, IsNil)

		if rowNum > 123 {
			_, err = s.store.SplitRegions(context.Background(), [][]byte{encodeKey(s.prefix, s08d("key", 123))}, false)
			c.Assert(err, IsNil)
		}

		if rowNum > 456 {
			_, err = s.store.SplitRegions(context.Background(), [][]byte{encodeKey(s.prefix, s08d("key", 456))}, false)
			c.Assert(err, IsNil)
		}

		txn2 := s.beginTxn(c)
		val, err := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, valueBytes(0))
		// Test scan without upperBound
		scan, err := txn2.Iter(encodeKey(s.prefix, ""), nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, false)
		// Test scan with upperBound
		upperBound := rowNum / 2
		scan, err = txn2.Iter(encodeKey(s.prefix, ""), encodeKey(s.prefix, s08d("key", upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, false)

		txn3 := s.beginTxn(c)
		txn3.SetOption(kv.KeyOnly, true)
		// Test scan without upper bound
		scan, err = txn3.Iter(encodeKey(s.prefix, ""), nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(encodeKey(s.prefix, ""), encodeKey(s.prefix, s08d("key", upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)

		// Restore KeyOnly to false
		txn3.SetOption(kv.KeyOnly, false)
		scan, err = txn3.Iter(encodeKey(s.prefix, ""), nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(encodeKey(s.prefix, ""), encodeKey(s.prefix, s08d("key", upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)
	}
}
