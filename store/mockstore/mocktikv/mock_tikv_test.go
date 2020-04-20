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

package mocktikv

import (
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

// testMockTiKVSuite tests MVCCStore interface.
// SetUpTest should set specific MVCCStore implementation.
type testMockTiKVSuite struct {
	store MVCCStore
}

type testMarshal struct{}

// testMVCCLevelDB is used to test MVCCLevelDB implementation.
type testMVCCLevelDB struct {
	testMockTiKVSuite
}

var (
	_ = Suite(&testMockTiKVSuite{})
	_ = Suite(&testMVCCLevelDB{})
	_ = Suite(testMarshal{})
)

func (s *testMockTiKVSuite) SetUpTest(c *C) {
	var err error
	s.store, err = NewMVCCLevelDB("")
	c.Assert(err, IsNil)
}

// PutMutations is exported for testing.
var PutMutations func(kvpairs ...string) []*kvrpcpb.Mutation = putMutations

func putMutations(kvpairs ...string) []*kvrpcpb.Mutation {
	var mutations []*kvrpcpb.Mutation
	for i := 0; i < len(kvpairs); i += 2 {
		mutations = append(mutations, &kvrpcpb.Mutation{
			Op:    kvrpcpb.Op_Put,
			Key:   []byte(kvpairs[i]),
			Value: []byte(kvpairs[i+1]),
		})
	}
	return mutations
}

func lock(key, primary string, ts uint64) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		Key:         []byte(key),
		PrimaryLock: []byte(primary),
		LockVersion: ts,
	}
}

func (s *testMockTiKVSuite) mustGetNone(c *C, key string, ts uint64) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetErr(c *C, key string, ts uint64) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(err, NotNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetOK(c *C, key string, ts uint64, expect string) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(err, IsNil)
	c.Assert(string(val), Equals, expect)
}

func (s *testMockTiKVSuite) mustGetRC(c *C, key string, ts uint64, expect string) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_RC, nil)
	c.Assert(err, IsNil)
	c.Assert(string(val), Equals, expect)
}

func (s *testMockTiKVSuite) mustPutOK(c *C, key, value string, startTS, commitTS uint64) {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations(key, value),
		PrimaryLock:  []byte(key),
		StartVersion: startTS,
	}
	errs := s.store.Prewrite(req)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
	err := s.store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustDeleteOK(c *C, key string, startTS, commitTS uint64) {
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Op_Del,
			Key: []byte(key),
		},
	}
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(key),
		StartVersion: startTS,
	}
	errs := s.store.Prewrite(req)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
	err := s.store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustScanOK(c *C, start string, limit int, ts uint64, expect ...string) {
	s.mustRangeScanOK(c, start, "", limit, ts, expect...)
}

func (s *testMockTiKVSuite) mustRangeScanOK(c *C, start, end string, limit int, ts uint64, expect ...string) {
	pairs := s.store.Scan([]byte(start), []byte(end), limit, ts, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(len(pairs)*2, Equals, len(expect))
	for i := 0; i < len(pairs); i++ {
		c.Assert(pairs[i].Err, IsNil)
		c.Assert(pairs[i].Key, BytesEquals, []byte(expect[i*2]))
		c.Assert(string(pairs[i].Value), Equals, expect[i*2+1])
	}
}

func (s *testMockTiKVSuite) mustReverseScanOK(c *C, end string, limit int, ts uint64, expect ...string) {
	s.mustRangeReverseScanOK(c, "", end, limit, ts, expect...)
}

func (s *testMockTiKVSuite) mustRangeReverseScanOK(c *C, start, end string, limit int, ts uint64, expect ...string) {
	pairs := s.store.ReverseScan([]byte(start), []byte(end), limit, ts, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(len(pairs)*2, Equals, len(expect))
	for i := 0; i < len(pairs); i++ {
		c.Assert(pairs[i].Err, IsNil)
		c.Assert(pairs[i].Key, BytesEquals, []byte(expect[i*2]))
		c.Assert(string(pairs[i].Value), Equals, expect[i*2+1])
	}
}

func MustPrewriteOK(c *C, store MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) {
	s := testMockTiKVSuite{store}
	s.mustPrewriteWithTTLOK(c, mutations, primary, startTS, ttl)
}

func (s *testMockTiKVSuite) mustPrewriteOK(c *C, mutations []*kvrpcpb.Mutation, primary string, startTS uint64) {
	s.mustPrewriteWithTTLOK(c, mutations, primary, startTS, 0)
}

func (s *testMockTiKVSuite) mustPrewriteWithTTLOK(c *C, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(primary),
		StartVersion: startTS,
		LockTtl:      ttl,
		MinCommitTs:  startTS + 1,
	}
	errs := s.store.Prewrite(req)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
}

func (s *testMockTiKVSuite) mustCommitOK(c *C, keys [][]byte, startTS, commitTS uint64) {
	err := s.store.Commit(keys, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustCommitErr(c *C, keys [][]byte, startTS, commitTS uint64) {
	err := s.store.Commit(keys, startTS, commitTS)
	c.Assert(err, NotNil)
}

func (s *testMockTiKVSuite) mustRollbackOK(c *C, keys [][]byte, startTS uint64) {
	err := s.store.Rollback(keys, startTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustRollbackErr(c *C, keys [][]byte, startTS uint64) {
	err := s.store.Rollback(keys, startTS)
	c.Assert(err, NotNil)
}

func (s *testMockTiKVSuite) mustScanLock(c *C, maxTs uint64, expect []*kvrpcpb.LockInfo) {
	locks, err := s.store.ScanLock(nil, nil, maxTs)
	c.Assert(err, IsNil)
	c.Assert(locks, DeepEquals, expect)
}

func (s *testMockTiKVSuite) mustResolveLock(c *C, startTS, commitTS uint64) {
	c.Assert(s.store.ResolveLock(nil, nil, startTS, commitTS), IsNil)
}

func (s *testMockTiKVSuite) mustBatchResolveLock(c *C, txnInfos map[uint64]uint64) {
	c.Assert(s.store.BatchResolveLock(nil, nil, txnInfos), IsNil)
}

func (s *testMockTiKVSuite) mustGC(c *C, safePoint uint64) {
	c.Assert(s.store.GC(nil, nil, safePoint), IsNil)
}

func (s *testMockTiKVSuite) mustDeleteRange(c *C, startKey, endKey string) {
	err := s.store.DeleteRange([]byte(startKey), []byte(endKey))
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) TestGet(c *C) {
	s.mustGetNone(c, "x", 10)
	s.mustPutOK(c, "x", "x", 5, 10)
	s.mustGetNone(c, "x", 9)
	s.mustGetOK(c, "x", 10, "x")
	s.mustGetOK(c, "x", 11, "x")
}

func (s *testMockTiKVSuite) TestGetWithLock(c *C) {
	key := "key"
	value := "value"
	s.mustPutOK(c, key, value, 5, 10)
	mutations := []*kvrpcpb.Mutation{{
		Op:  kvrpcpb.Op_Lock,
		Key: []byte(key),
	},
	}
	// test with lock's type is lock
	s.mustPrewriteOK(c, mutations, key, 20)
	s.mustGetOK(c, key, 25, value)
	s.mustCommitOK(c, [][]byte{[]byte(key)}, 20, 30)

	// test get with lock's max ts and primary key
	s.mustPrewriteOK(c, putMutations(key, "value2", "key2", "v5"), key, 40)
	s.mustGetErr(c, key, 41)
	s.mustGetErr(c, "key2", math.MaxUint64)
	s.mustGetOK(c, key, math.MaxUint64, "value")
}

func (s *testMockTiKVSuite) TestDelete(c *C) {
	s.mustPutOK(c, "x", "x5-10", 5, 10)
	s.mustDeleteOK(c, "x", 15, 20)
	s.mustGetNone(c, "x", 5)
	s.mustGetNone(c, "x", 9)
	s.mustGetOK(c, "x", 10, "x5-10")
	s.mustGetOK(c, "x", 19, "x5-10")
	s.mustGetNone(c, "x", 20)
	s.mustGetNone(c, "x", 21)
}

func (s *testMockTiKVSuite) TestCleanupRollback(c *C) {
	s.mustPutOK(c, "secondary", "s-0", 1, 2)
	s.mustPrewriteOK(c, putMutations("primary", "p-5", "secondary", "s-5"), "primary", 5)
	s.mustGetErr(c, "secondary", 8)
	s.mustGetErr(c, "secondary", 12)
	s.mustCommitOK(c, [][]byte{[]byte("primary")}, 5, 10)
	s.mustRollbackErr(c, [][]byte{[]byte("primary")}, 5)
}

func (s *testMockTiKVSuite) TestReverseScan(c *C) {
	// ver10: A(10) - B(_) - C(10) - D(_) - E(10)
	s.mustPutOK(c, "A", "A10", 5, 10)
	s.mustPutOK(c, "C", "C10", 5, 10)
	s.mustPutOK(c, "E", "E10", 5, 10)

	checkV10 := func() {
		s.mustReverseScanOK(c, "Z", 0, 10)
		s.mustReverseScanOK(c, "Z", 1, 10, "E", "E10")
		s.mustReverseScanOK(c, "Z", 2, 10, "E", "E10", "C", "C10")
		s.mustReverseScanOK(c, "Z", 3, 10, "E", "E10", "C", "C10", "A", "A10")
		s.mustReverseScanOK(c, "Z", 4, 10, "E", "E10", "C", "C10", "A", "A10")
		s.mustReverseScanOK(c, "E\x00", 3, 10, "E", "E10", "C", "C10", "A", "A10")
		s.mustReverseScanOK(c, "C\x00", 3, 10, "C", "C10", "A", "A10")
		s.mustReverseScanOK(c, "C\x00", 4, 10, "C", "C10", "A", "A10")
		s.mustReverseScanOK(c, "B", 1, 10, "A", "A10")
		s.mustRangeReverseScanOK(c, "", "E", 5, 10, "C", "C10", "A", "A10")
		s.mustRangeReverseScanOK(c, "", "C\x00", 5, 10, "C", "C10", "A", "A10")
		s.mustRangeReverseScanOK(c, "A\x00", "C", 5, 10)
	}
	checkV10()

	// ver20: A(10) - B(20) - C(10) - D(20) - E(10)
	s.mustPutOK(c, "B", "B20", 15, 20)
	s.mustPutOK(c, "D", "D20", 15, 20)

	checkV20 := func() {
		s.mustReverseScanOK(c, "Z", 5, 20, "E", "E10", "D", "D20", "C", "C10", "B", "B20", "A", "A10")
		s.mustReverseScanOK(c, "C\x00", 5, 20, "C", "C10", "B", "B20", "A", "A10")
		s.mustReverseScanOK(c, "A\x00", 1, 20, "A", "A10")
		s.mustRangeReverseScanOK(c, "B", "D", 5, 20, "C", "C10", "B", "B20")
		s.mustRangeReverseScanOK(c, "B", "D\x00", 5, 20, "D", "D20", "C", "C10", "B", "B20")
		s.mustRangeReverseScanOK(c, "B\x00", "D\x00", 5, 20, "D", "D20", "C", "C10")
	}
	checkV10()
	checkV20()

	// ver30: A(_) - B(20) - C(10) - D(_) - E(10)
	s.mustDeleteOK(c, "A", 25, 30)
	s.mustDeleteOK(c, "D", 25, 30)

	checkV30 := func() {
		s.mustReverseScanOK(c, "Z", 5, 30, "E", "E10", "C", "C10", "B", "B20")
		s.mustReverseScanOK(c, "C", 1, 30, "B", "B20")
		s.mustReverseScanOK(c, "C\x00", 5, 30, "C", "C10", "B", "B20")
	}
	checkV10()
	checkV20()
	checkV30()

	// ver40: A(_) - B(_) - C(40) - D(40) - E(10)
	s.mustDeleteOK(c, "B", 35, 40)
	s.mustPutOK(c, "C", "C40", 35, 40)
	s.mustPutOK(c, "D", "D40", 35, 40)

	checkV40 := func() {
		s.mustReverseScanOK(c, "Z", 5, 40, "E", "E10", "D", "D40", "C", "C40")
		s.mustReverseScanOK(c, "Z", 5, 100, "E", "E10", "D", "D40", "C", "C40")
	}
	checkV10()
	checkV20()
	checkV30()
	checkV40()
}

func (s *testMockTiKVSuite) TestScan(c *C) {
	// ver10: A(10) - B(_) - C(10) - D(_) - E(10)
	s.mustPutOK(c, "A", "A10", 5, 10)
	s.mustPutOK(c, "C", "C10", 5, 10)
	s.mustPutOK(c, "E", "E10", 5, 10)

	checkV10 := func() {
		s.mustScanOK(c, "", 0, 10)
		s.mustScanOK(c, "", 1, 10, "A", "A10")
		s.mustScanOK(c, "", 2, 10, "A", "A10", "C", "C10")
		s.mustScanOK(c, "", 3, 10, "A", "A10", "C", "C10", "E", "E10")
		s.mustScanOK(c, "", 4, 10, "A", "A10", "C", "C10", "E", "E10")
		s.mustScanOK(c, "A", 3, 10, "A", "A10", "C", "C10", "E", "E10")
		s.mustScanOK(c, "A\x00", 3, 10, "C", "C10", "E", "E10")
		s.mustScanOK(c, "C", 4, 10, "C", "C10", "E", "E10")
		s.mustScanOK(c, "F", 1, 10)
		s.mustRangeScanOK(c, "", "E", 5, 10, "A", "A10", "C", "C10")
		s.mustRangeScanOK(c, "", "C\x00", 5, 10, "A", "A10", "C", "C10")
		s.mustRangeScanOK(c, "A\x00", "C", 5, 10)
	}
	checkV10()

	// ver20: A(10) - B(20) - C(10) - D(20) - E(10)
	s.mustPutOK(c, "B", "B20", 15, 20)
	s.mustPutOK(c, "D", "D20", 15, 20)

	checkV20 := func() {
		s.mustScanOK(c, "", 5, 20, "A", "A10", "B", "B20", "C", "C10", "D", "D20", "E", "E10")
		s.mustScanOK(c, "C", 5, 20, "C", "C10", "D", "D20", "E", "E10")
		s.mustScanOK(c, "D\x00", 1, 20, "E", "E10")
		s.mustRangeScanOK(c, "B", "D", 5, 20, "B", "B20", "C", "C10")
		s.mustRangeScanOK(c, "B", "D\x00", 5, 20, "B", "B20", "C", "C10", "D", "D20")
		s.mustRangeScanOK(c, "B\x00", "D\x00", 5, 20, "C", "C10", "D", "D20")
	}
	checkV10()
	checkV20()

	// ver30: A(_) - B(20) - C(10) - D(_) - E(10)
	s.mustDeleteOK(c, "A", 25, 30)
	s.mustDeleteOK(c, "D", 25, 30)

	checkV30 := func() {
		s.mustScanOK(c, "", 5, 30, "B", "B20", "C", "C10", "E", "E10")
		s.mustScanOK(c, "A", 1, 30, "B", "B20")
		s.mustScanOK(c, "C\x00", 5, 30, "E", "E10")
	}
	checkV10()
	checkV20()
	checkV30()

	// ver40: A(_) - B(_) - C(40) - D(40) - E(10)
	s.mustDeleteOK(c, "B", 35, 40)
	s.mustPutOK(c, "C", "C40", 35, 40)
	s.mustPutOK(c, "D", "D40", 35, 40)

	checkV40 := func() {
		s.mustScanOK(c, "", 5, 40, "C", "C40", "D", "D40", "E", "E10")
		s.mustScanOK(c, "", 5, 100, "C", "C40", "D", "D40", "E", "E10")
	}
	checkV10()
	checkV20()
	checkV30()
	checkV40()
}

func (s *testMockTiKVSuite) TestBatchGet(c *C) {
	s.mustPutOK(c, "k1", "v1", 1, 2)
	s.mustPutOK(c, "k2", "v2", 1, 2)
	s.mustPutOK(c, "k2", "v2", 3, 4)
	s.mustPutOK(c, "k3", "v3", 1, 2)
	batchKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	pairs := s.store.BatchGet(batchKeys, 5, kvrpcpb.IsolationLevel_SI, nil)
	for _, pair := range pairs {
		c.Assert(pair.Err, IsNil)
	}
	c.Assert(string(pairs[0].Value), Equals, "v1")
	c.Assert(string(pairs[1].Value), Equals, "v2")
	c.Assert(string(pairs[2].Value), Equals, "v3")
}

func (s *testMockTiKVSuite) TestScanLock(c *C) {
	s.mustPutOK(c, "k1", "v1", 1, 2)
	s.mustPrewriteOK(c, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	s.mustPrewriteOK(c, putMutations("p2", "v10", "s2", "v10"), "p2", 10)
	s.mustPrewriteOK(c, putMutations("p3", "v20", "s3", "v20"), "p3", 20)

	locks, err := s.store.ScanLock([]byte("a"), []byte("r"), 12)
	c.Assert(err, IsNil)
	c.Assert(locks, DeepEquals, []*kvrpcpb.LockInfo{
		lock("p1", "p1", 5),
		lock("p2", "p2", 10),
	})

	s.mustScanLock(c, 10, []*kvrpcpb.LockInfo{
		lock("p1", "p1", 5),
		lock("p2", "p2", 10),
		lock("s1", "p1", 5),
		lock("s2", "p2", 10),
	})
}

func (s *testMockTiKVSuite) TestScanWithResolvedLock(c *C) {
	s.mustPrewriteOK(c, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	s.mustPrewriteOK(c, putMutations("p2", "v10", "s2", "v10"), "p1", 5)

	pairs := s.store.Scan([]byte("p1"), nil, 3, 10, kvrpcpb.IsolationLevel_SI, nil)
	lock, ok := errors.Cause(pairs[0].Err).(*ErrLocked)
	c.Assert(ok, IsTrue)
	_, ok = errors.Cause(pairs[1].Err).(*ErrLocked)
	c.Assert(ok, IsTrue)

	// Mock the request after resolving lock.
	pairs = s.store.Scan([]byte("p1"), nil, 3, 10, kvrpcpb.IsolationLevel_SI, []uint64{lock.StartTS})
	for _, pair := range pairs {
		c.Assert(pair.Err, IsNil)
	}
}

func (s *testMockTiKVSuite) TestCommitConflict(c *C) {
	// txn A want set x to A
	// txn B want set x to B
	// A prewrite.
	s.mustPrewriteOK(c, putMutations("x", "A"), "x", 5)
	// B prewrite and find A's lock.
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("x", "B"),
		PrimaryLock:  []byte("x"),
		StartVersion: 10,
	}
	errs := s.store.Prewrite(req)
	c.Assert(errs[0], NotNil)
	// B find rollback A because A exist too long.
	s.mustRollbackOK(c, [][]byte{[]byte("x")}, 5)
	// if A commit here, it would find its lock removed, report error txn not found.
	s.mustCommitErr(c, [][]byte{[]byte("x")}, 5, 10)
	// B prewrite itself after it rollback A.
	s.mustPrewriteOK(c, putMutations("x", "B"), "x", 10)
	// if A commit here, it would find its lock replaced by others and commit fail.
	s.mustCommitErr(c, [][]byte{[]byte("x")}, 5, 20)
	// B commit success.
	s.mustCommitOK(c, [][]byte{[]byte("x")}, 10, 20)
	// if B commit again, it will success because the key already committed.
	s.mustCommitOK(c, [][]byte{[]byte("x")}, 10, 20)
}

func (s *testMockTiKVSuite) TestResolveLock(c *C) {
	s.mustPrewriteOK(c, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	s.mustPrewriteOK(c, putMutations("p2", "v10", "s2", "v10"), "p2", 10)
	s.mustResolveLock(c, 5, 0)
	s.mustResolveLock(c, 10, 20)
	s.mustGetNone(c, "p1", 20)
	s.mustGetNone(c, "s1", 30)
	s.mustGetOK(c, "p2", 20, "v10")
	s.mustGetOK(c, "s2", 30, "v10")
	s.mustScanLock(c, 30, nil)
}

func (s *testMockTiKVSuite) TestBatchResolveLock(c *C) {
	s.mustPrewriteOK(c, putMutations("p1", "v11", "s1", "v11"), "p1", 11)
	s.mustPrewriteOK(c, putMutations("p2", "v12", "s2", "v12"), "p2", 12)
	s.mustPrewriteOK(c, putMutations("p3", "v13"), "p3", 13)
	s.mustPrewriteOK(c, putMutations("p4", "v14", "s3", "v14", "s4", "v14"), "p4", 14)
	s.mustPrewriteOK(c, putMutations("p5", "v15", "s5", "v15"), "p5", 15)
	txnInfos := map[uint64]uint64{
		11: 0,
		12: 22,
		13: 0,
		14: 24,
	}
	s.mustBatchResolveLock(c, txnInfos)
	s.mustGetNone(c, "p1", 20)
	s.mustGetNone(c, "p3", 30)
	s.mustGetOK(c, "p2", 30, "v12")
	s.mustGetOK(c, "s4", 30, "v14")
	s.mustScanLock(c, 30, []*kvrpcpb.LockInfo{
		lock("p5", "p5", 15),
		lock("s5", "p5", 15),
	})
	txnInfos = map[uint64]uint64{
		15: 0,
	}
	s.mustBatchResolveLock(c, txnInfos)
	s.mustScanLock(c, 30, nil)
}

func (s *testMockTiKVSuite) TestGC(c *C) {
	var safePoint uint64 = 100

	// Prepare data
	s.mustPutOK(c, "k1", "v1", 1, 2)
	s.mustPutOK(c, "k1", "v2", 11, 12)

	s.mustPutOK(c, "k2", "v1", 1, 2)
	s.mustPutOK(c, "k2", "v2", 11, 12)
	s.mustPutOK(c, "k2", "v3", 101, 102)

	s.mustPutOK(c, "k3", "v1", 1, 2)
	s.mustPutOK(c, "k3", "v2", 11, 12)
	s.mustDeleteOK(c, "k3", 101, 102)

	s.mustPutOK(c, "k4", "v1", 1, 2)
	s.mustDeleteOK(c, "k4", 11, 12)

	// Check prepared data
	s.mustGetOK(c, "k1", 5, "v1")
	s.mustGetOK(c, "k1", 15, "v2")
	s.mustGetOK(c, "k2", 5, "v1")
	s.mustGetOK(c, "k2", 15, "v2")
	s.mustGetOK(c, "k2", 105, "v3")
	s.mustGetOK(c, "k3", 5, "v1")
	s.mustGetOK(c, "k3", 15, "v2")
	s.mustGetNone(c, "k3", 105)
	s.mustGetOK(c, "k4", 5, "v1")
	s.mustGetNone(c, "k4", 105)

	s.mustGC(c, safePoint)

	s.mustGetNone(c, "k1", 5)
	s.mustGetOK(c, "k1", 15, "v2")
	s.mustGetNone(c, "k2", 5)
	s.mustGetOK(c, "k2", 15, "v2")
	s.mustGetOK(c, "k2", 105, "v3")
	s.mustGetNone(c, "k3", 5)
	s.mustGetOK(c, "k3", 15, "v2")
	s.mustGetNone(c, "k3", 105)
	s.mustGetNone(c, "k4", 5)
	s.mustGetNone(c, "k4", 105)
}

func (s *testMockTiKVSuite) TestRollbackAndWriteConflict(c *C) {
	s.mustPutOK(c, "test", "test", 1, 3)
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("lock", "lock", "test", "test1"),
		PrimaryLock:  []byte("test"),
		StartVersion: 2,
		LockTtl:      2,
	}
	errs := s.store.Prewrite(req)
	s.mustWriteWriteConflict(c, errs, 1)

	s.mustPutOK(c, "test", "test2", 5, 8)

	// simulate `getTxnStatus` for txn 2.
	err := s.store.Cleanup([]byte("test"), 2, math.MaxUint64)
	c.Assert(err, IsNil)
	req = &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("test", "test3"),
		PrimaryLock:  []byte("test"),
		StartVersion: 6,
		LockTtl:      1,
	}
	errs = s.store.Prewrite(req)
	s.mustWriteWriteConflict(c, errs, 0)
}

func (s *testMockTiKVSuite) TestDeleteRange(c *C) {
	for i := 1; i <= 5; i++ {
		key := string(byte(i) + byte('0'))
		value := "v" + key
		s.mustPutOK(c, key, value, uint64(1+2*i), uint64(2+2*i))
	}

	s.mustScanOK(c, "0", 10, 20, "1", "v1", "2", "v2", "3", "v3", "4", "v4", "5", "v5")

	s.mustDeleteRange(c, "2", "4")
	s.mustScanOK(c, "0", 10, 30, "1", "v1", "4", "v4", "5", "v5")

	s.mustDeleteRange(c, "5", "5")
	s.mustScanOK(c, "0", 10, 40, "1", "v1", "4", "v4", "5", "v5")

	s.mustDeleteRange(c, "41", "42")
	s.mustScanOK(c, "0", 10, 50, "1", "v1", "4", "v4", "5", "v5")

	s.mustDeleteRange(c, "4\x00", "5\x00")
	s.mustScanOK(c, "0", 10, 60, "1", "v1", "4", "v4")

	s.mustDeleteRange(c, "0", "9")
	s.mustScanOK(c, "0", 10, 70)
}

func (s *testMockTiKVSuite) mustWriteWriteConflict(c *C, errs []error, i int) {
	c.Assert(errs[i], NotNil)
	_, ok := errs[i].(*ErrConflict)
	c.Assert(ok, IsTrue)
}

func (s *testMockTiKVSuite) TestRC(c *C) {
	s.mustPutOK(c, "key", "v1", 5, 10)
	s.mustPrewriteOK(c, putMutations("key", "v2"), "key", 15)
	s.mustGetErr(c, "key", 20)
	s.mustGetRC(c, "key", 12, "v1")
	s.mustGetRC(c, "key", 20, "v1")
}

func (s testMarshal) TestMarshalmvccLock(c *C) {
	l := mvccLock{
		startTS:     47,
		primary:     []byte{'a', 'b', 'c'},
		value:       []byte{'d', 'e'},
		op:          kvrpcpb.Op_Put,
		ttl:         444,
		minCommitTS: 666,
	}
	bin, err := l.MarshalBinary()
	c.Assert(err, IsNil)

	var l1 mvccLock
	err = l1.UnmarshalBinary(bin)
	c.Assert(err, IsNil)

	c.Assert(l.startTS, Equals, l1.startTS)
	c.Assert(l.op, Equals, l1.op)
	c.Assert(l.ttl, Equals, l1.ttl)
	c.Assert(string(l.primary), Equals, string(l1.primary))
	c.Assert(string(l.value), Equals, string(l1.value))
	c.Assert(l.minCommitTS, Equals, l1.minCommitTS)
}

func (s testMarshal) TestMarshalmvccValue(c *C) {
	v := mvccValue{
		valueType: typePut,
		startTS:   42,
		commitTS:  55,
		value:     []byte{'d', 'e'},
	}
	bin, err := v.MarshalBinary()
	c.Assert(err, IsNil)

	var v1 mvccValue
	err = v1.UnmarshalBinary(bin)
	c.Assert(err, IsNil)

	c.Assert(v.valueType, Equals, v1.valueType)
	c.Assert(v.startTS, Equals, v1.startTS)
	c.Assert(v.commitTS, Equals, v1.commitTS)
	c.Assert(string(v.value), Equals, string(v.value))
}

func (s *testMVCCLevelDB) TestErrors(c *C) {
	c.Assert((&ErrKeyAlreadyExist{}).Error(), Equals, `key already exist, key: ""`)
	c.Assert(ErrAbort("txn").Error(), Equals, "abort: txn")
	c.Assert(ErrAlreadyCommitted(0).Error(), Equals, "txn already committed")
	c.Assert((&ErrConflict{}).Error(), Equals, "write conflict")
}

func (s *testMVCCLevelDB) TestCheckTxnStatus(c *C) {
	startTS := uint64(5 << 18)
	s.mustPrewriteWithTTLOK(c, putMutations("pk", "val"), "pk", startTS, 666)

	ttl, commitTS, action, err := s.store.CheckTxnStatus([]byte("pk"), startTS, startTS+100, 666, false)
	c.Assert(err, IsNil)
	c.Assert(ttl, Equals, uint64(666))
	c.Assert(commitTS, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)

	s.mustCommitOK(c, [][]byte{[]byte("pk")}, startTS, startTS+101)

	ttl, commitTS, _, err = s.store.CheckTxnStatus([]byte("pk"), startTS, 0, 666, false)
	c.Assert(err, IsNil)
	c.Assert(ttl, Equals, uint64(0))
	c.Assert(commitTS, Equals, startTS+101)

	s.mustPrewriteWithTTLOK(c, putMutations("pk1", "val"), "pk1", startTS, 666)
	s.mustRollbackOK(c, [][]byte{[]byte("pk1")}, startTS)

	ttl, commitTS, action, err = s.store.CheckTxnStatus([]byte("pk1"), startTS, 0, 666, false)
	c.Assert(err, IsNil)
	c.Assert(ttl, Equals, uint64(0))
	c.Assert(commitTS, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_NoAction)

	s.mustPrewriteWithTTLOK(c, putMutations("pk2", "val"), "pk2", startTS, 666)
	currentTS := uint64(777 << 18)
	ttl, commitTS, action, err = s.store.CheckTxnStatus([]byte("pk2"), startTS, 0, currentTS, false)
	c.Assert(err, IsNil)
	c.Assert(ttl, Equals, uint64(0))
	c.Assert(commitTS, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_TTLExpireRollback)

	// Cover the TxnNotFound case.
	_, _, _, err = s.store.CheckTxnStatus([]byte("txnNotFound"), 5, 0, 666, false)
	c.Assert(err, NotNil)
	notFound, ok := errors.Cause(err).(*ErrTxnNotFound)
	c.Assert(ok, IsTrue)
	c.Assert(notFound.StartTs, Equals, uint64(5))
	c.Assert(string(notFound.PrimaryKey), Equals, "txnNotFound")

	ttl, commitTS, action, err = s.store.CheckTxnStatus([]byte("txnNotFound"), 5, 0, 666, true)
	c.Assert(err, IsNil)
	c.Assert(ttl, Equals, uint64(0))
	c.Assert(commitTS, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_LockNotExistRollback)

	// Check the rollback tombstone blocks this prewrite which comes with a smaller startTS.
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("txnNotFound", "val"),
		PrimaryLock:  []byte("txnNotFound"),
		StartVersion: 4,
		MinCommitTs:  6,
	}
	errs := s.store.Prewrite(req)
	c.Assert(errs, NotNil)
}

func (s *testMVCCLevelDB) TestRejectCommitTS(c *C) {
	s.mustPrewriteOK(c, putMutations("x", "A"), "x", 5)
	// Push the minCommitTS
	_, _, _, err := s.store.CheckTxnStatus([]byte("x"), 5, 100, 100, false)
	c.Assert(err, IsNil)
	err = s.store.Commit([][]byte{[]byte("x")}, 5, 10)
	e, ok := errors.Cause(err).(*ErrCommitTSExpired)
	c.Assert(ok, IsTrue)
	c.Assert(e.MinCommitTs, Equals, uint64(101))
}

func (s *testMVCCLevelDB) TestMvccGetByKey(c *C) {
	s.mustPrewriteOK(c, putMutations("q1", "v5"), "p1", 5)
	debugger, ok := s.store.(MVCCDebugger)
	c.Assert(ok, IsTrue)
	mvccInfo := debugger.MvccGetByKey([]byte("q1"))
	except := &kvrpcpb.MvccInfo{
		Lock: &kvrpcpb.MvccLock{
			Type:       kvrpcpb.Op_Put,
			StartTs:    5,
			Primary:    []byte("p1"),
			ShortValue: []byte("v5"),
		},
	}
	c.Assert(mvccInfo, DeepEquals, except)
}

func (s *testMVCCLevelDB) TestTxnHeartBeat(c *C) {
	s.mustPrewriteWithTTLOK(c, putMutations("pk", "val"), "pk", 5, 666)

	// Update the ttl
	ttl, err := s.store.TxnHeartBeat([]byte("pk"), 5, 888)
	c.Assert(err, IsNil)
	c.Assert(ttl, Greater, uint64(666))

	// Advise ttl is small
	ttl, err = s.store.TxnHeartBeat([]byte("pk"), 5, 300)
	c.Assert(err, IsNil)
	c.Assert(ttl, Greater, uint64(300))

	// The lock has already been clean up
	c.Assert(s.store.Cleanup([]byte("pk"), 5, math.MaxUint64), IsNil)
	_, err = s.store.TxnHeartBeat([]byte("pk"), 5, 1000)
	c.Assert(err, NotNil)
}
