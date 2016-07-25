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
	"fmt"
	"time"

	. "github.com/pingcap/check"
)

type testScanSuite struct {
	store   *tikvStore
	prefix  string
	rowNums []int
}

var _ = Suite(&testScanSuite{})

func (s *testScanSuite) SetUpSuite(c *C) {
	s.store = newTestStore(c)
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, scanBatchSize, scanBatchSize+1)
}

func (s *testScanSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Seek(encodeKey(s.prefix, ""))
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testScanSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testScanSuite) TestSeek(c *C) {
	for _, rowNum := range s.rowNums {
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit()
		c.Assert(err, IsNil)

		txn2 := s.beginTxn(c)
		val, err := txn2.Get(encodeKey(s.prefix, s08d("key", 0)))
		c.Assert(val, BytesEquals, valueBytes(0))
		scan, err := txn2.Seek(encodeKey(s.prefix, ""))
		c.Assert(err, IsNil)

		for i := 0; i < rowNum; i++ {
			k := scan.Key()
			c.Assert([]byte(k), BytesEquals, encodeKey(s.prefix, s08d("key", i)))
			v := scan.Value()
			c.Assert(v, BytesEquals, valueBytes(i))
			// Because newScan return first item without calling scan.Next() just like go-hbase,
			// for-loop count will decrease 1.
			if i < rowNum-1 {
				scan.Next()
			}
		}
		scan.Next()
		c.Assert(scan.Valid(), IsFalse)
	}
}
