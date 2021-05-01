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

package tikv_test

import (
	"bytes"
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/unionstore"
	"github.com/pingcap/tidb/store/tikv/util"
	"go.uber.org/zap"
)

var scanBatchSize = tikv.ConfigProbe{}.GetScanBatchSize()

type testScanSuite struct {
	OneByOneSuite
	store        *tikv.KVStore
	recordPrefix []byte
	rowNums      []int
	ctx          context.Context
}

var _ = SerialSuites(&testScanSuite{})

func (s *testScanSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c)
	s.recordPrefix = []byte("prefix")
	s.rowNums = append(s.rowNums, 1, scanBatchSize, scanBatchSize+1, scanBatchSize*3)
	// Avoid using async commit logic.
	s.ctx = context.WithValue(context.Background(), util.SessionID, uint64(0))
}

func (s *testScanSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(s.recordPrefix, nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(s.ctx)
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testScanSuite) beginTxn(c *C) *tikv.KVTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func (s *testScanSuite) makeKey(i int) []byte {
	var key []byte
	key = append(key, s.recordPrefix...)
	key = append(key, []byte(fmt.Sprintf("%10d", i))...)
	return key
}

func (s *testScanSuite) makeValue(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func (s *testScanSuite) TestScan(c *C) {
	check := func(c *C, scan unionstore.Iterator, rowNum int, keyOnly bool) {
		for i := 0; i < rowNum; i++ {
			k := scan.Key()
			expectedKey := s.makeKey(i)
			if ok := bytes.Equal(k, expectedKey); !ok {
				logutil.BgLogger().Error("bytes equal check fail",
					zap.Int("i", i),
					zap.Int("rowNum", rowNum),
					zap.String("obtained key", kv.StrKey(k)),
					zap.String("obtained val", kv.StrKey(scan.Value())),
					zap.String("expected", kv.StrKey(expectedKey)),
					zap.Bool("keyOnly", keyOnly))
			}
			c.Assert(k, BytesEquals, expectedKey)
			if !keyOnly {
				v := scan.Value()
				c.Assert(v, BytesEquals, s.makeValue(i))
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
			err := txn.Set(s.makeKey(i), s.makeValue(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(s.ctx)
		c.Assert(err, IsNil)
		mockTableID := int64(999)
		if rowNum > 123 {
			_, err = s.store.SplitRegions(s.ctx, [][]byte{s.makeKey(123)}, false, &mockTableID)
			c.Assert(err, IsNil)
		}

		if rowNum > 456 {
			_, err = s.store.SplitRegions(s.ctx, [][]byte{s.makeKey(456)}, false, &mockTableID)
			c.Assert(err, IsNil)
		}

		txn2 := s.beginTxn(c)
		val, err := txn2.Get(context.TODO(), s.makeKey(0))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, s.makeValue(0))
		// Test scan without upperBound
		scan, err := txn2.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, false)
		// Test scan with upperBound
		upperBound := rowNum / 2
		scan, err = txn2.Iter(s.recordPrefix, s.makeKey(upperBound))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, false)

		txn3 := s.beginTxn(c)
		txn3.GetSnapshot().SetKeyOnly(true)
		// Test scan without upper bound
		scan, err = txn3.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, s.makeKey(upperBound))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)

		// Restore KeyOnly to false
		txn3.GetSnapshot().SetKeyOnly(false)
		scan, err = txn3.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, s.makeKey(upperBound))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)
	}
}
