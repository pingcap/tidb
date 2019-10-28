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
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type testSnapshotSuite struct {
	OneByOneSuite
	store   *tikvStore
	prefix  string
	rowNums []int
}

var _ = Suite(&testSnapshotSuite{})

func (s *testSnapshotSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*tikvStore)
	s.prefix = fmt.Sprintf("snapshot_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, 100, 191)
}

func (s *testSnapshotSuite) TearDownSuite(c *C) {
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

func (s *testSnapshotSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testSnapshotSuite) checkAll(keys []kv.Key, c *C) {
	txn := s.beginTxn(c)
	snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn.StartTS()})
	m, err := snapshot.BatchGet(keys)
	c.Assert(err, IsNil)

	scan, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	cnt := 0
	for scan.Valid() {
		cnt++
		k := scan.Key()
		v := scan.Value()
		v2, ok := m[string(k)]
		c.Assert(ok, IsTrue, Commentf("key: %q", k))
		c.Assert(v, BytesEquals, v2)
		scan.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(m, HasLen, cnt)
}

func (s *testSnapshotSuite) deleteKeys(keys []kv.Key, c *C) {
	txn := s.beginTxn(c)
	for _, k := range keys {
		err := txn.Delete(k)
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testSnapshotSuite) TestBatchGet(c *C) {
	for _, rowNum := range s.rowNums {
		logutil.Logger(context.Background()).Debug("test BatchGet",
			zap.Int("length", rowNum))
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(context.Background())
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func (s *testSnapshotSuite) TestSnapshotCache(c *C) {
	txn := s.beginTxn(c)
	c.Assert(txn.Set(kv.Key("x"), []byte("x")), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	txn = s.beginTxn(c)
	snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn.StartTS()})
	_, err := snapshot.BatchGet([]kv.Key{kv.Key("x"), kv.Key("y")})
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/snapshot-get-cache-fail", `return(true)`), IsNil)
	_, err = snapshot.Get(kv.Key("x"))
	c.Assert(err, IsNil)

	_, err = snapshot.Get(kv.Key("y"))
	c.Assert(kv.IsErrNotFound(err), IsTrue)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/snapshot-get-cache-fail"), IsNil)
}

func (s *testSnapshotSuite) TestBatchGetNotExist(c *C) {
	for _, rowNum := range s.rowNums {
		logutil.Logger(context.Background()).Debug("test BatchGetNotExist",
			zap.Int("length", rowNum))
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(context.Background())
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		keys = append(keys, kv.Key("noSuchKey"))
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func makeKeys(rowNum int, prefix string) []kv.Key {
	keys := make([]kv.Key, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSnapshotSuite) TestWriteConflictPrettyFormat(c *C) {
	conflict := &pb.WriteConflict{
		StartTs:          399402937522847774,
		ConflictTs:       399402937719455772,
		ConflictCommitTs: 399402937719455773,
		Key:              []byte{116, 128, 0, 0, 0, 0, 0, 1, 155, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 82, 87, 48, 49, 0, 0, 0, 0, 251, 1, 55, 54, 56, 50, 50, 49, 49, 48, 255, 57, 0, 0, 0, 0, 0, 0, 0, 248, 1, 0, 0, 0, 0, 0, 0, 0, 0, 247},
		Primary:          []byte{116, 128, 0, 0, 0, 0, 0, 1, 155, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 82, 87, 48, 49, 0, 0, 0, 0, 251, 1, 55, 54, 56, 50, 50, 49, 49, 48, 255, 57, 0, 0, 0, 0, 0, 0, 0, 248, 1, 0, 0, 0, 0, 0, 0, 0, 0, 247},
	}

	expectedStr := "[kv:9007]Write conflict, " +
		"txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, " +
		"key={tableID=411, indexID=1, indexValues={RW01, 768221109, , }} " +
		"primary={tableID=411, indexID=1, indexValues={RW01, 768221109, , }} " +
		kv.TxnRetryableMark
	c.Assert(newWriteConflictError(conflict).Error(), Equals, expectedStr)
}
