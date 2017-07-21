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
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testMockTiKVSuite struct {
	store MVCCStore
}

var _ = Suite(&testMockTiKVSuite{})

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

func (s *testMockTiKVSuite) SetUpTest(c *C) {
	s.store = NewMvccStore()
}

func (s *testMockTiKVSuite) mustGetNone(c *C, key string, ts uint64) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI)
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetErr(c *C, key string, ts uint64) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI)
	c.Assert(err, NotNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetOK(c *C, key string, ts uint64, expect string) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI)
	c.Assert(err, IsNil)
	c.Assert(string(val), Equals, expect)
}

func (s *testMockTiKVSuite) mustGetRC(c *C, key string, ts uint64, expect string) {
	val, err := s.store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_RC)
	c.Assert(err, IsNil)
	c.Assert(string(val), Equals, expect)
}

func (s *testMockTiKVSuite) mustPutOK(c *C, key, value string, startTS, commitTS uint64) {
	errs := s.store.Prewrite(putMutations(key, value), []byte(key), startTS, 0)
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
	errs := s.store.Prewrite(mutations, []byte(key), startTS, 0)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
	err := s.store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustScanOK(c *C, start string, limit int, ts uint64, expect ...string) {
	pairs := s.store.Scan([]byte(start), nil, limit, ts, kvrpcpb.IsolationLevel_SI)
	c.Assert(len(pairs)*2, Equals, len(expect))
	for i := 0; i < len(pairs); i++ {
		c.Assert(pairs[i].Err, IsNil)
		c.Assert(pairs[i].Key, BytesEquals, []byte(expect[i*2]))
		c.Assert(string(pairs[i].Value), Equals, expect[i*2+1])
	}
}

func (s *testMockTiKVSuite) mustPrewriteOK(c *C, mutations []*kvrpcpb.Mutation, primary string, startTS uint64) {
	errs := s.store.Prewrite(mutations, []byte(primary), startTS, 0)
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

func (s *testMockTiKVSuite) TestGet(c *C) {
	s.mustGetNone(c, "x", 10)
	s.mustPutOK(c, "x", "x", 5, 10)
	s.mustGetNone(c, "x", 9)
	s.mustGetOK(c, "x", 10, "x")
	s.mustGetOK(c, "x", 11, "x")
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
	}
	checkV10()

	// ver20: A(10) - B(20) - C(10) - D(20) - E(10)
	s.mustPutOK(c, "B", "B20", 15, 20)
	s.mustPutOK(c, "D", "D20", 15, 20)

	checkV20 := func() {
		s.mustScanOK(c, "", 5, 20, "A", "A10", "B", "B20", "C", "C10", "D", "D20", "E", "E10")
		s.mustScanOK(c, "C", 5, 20, "C", "C10", "D", "D20", "E", "E10")
		s.mustScanOK(c, "D\x00", 1, 20, "E", "E10")
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
	pairs := s.store.BatchGet(batchKeys, 5, kvrpcpb.IsolationLevel_SI)
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
	s.mustScanLock(c, 10, []*kvrpcpb.LockInfo{
		lock("p1", "p1", 5),
		lock("p2", "p2", 10),
		lock("s1", "p1", 5),
		lock("s2", "p2", 10),
	})
}

func (s *testMockTiKVSuite) TestCommitConflict(c *C) {
	// txn A want set x to A
	// txn B want set x to B
	// A prewrite.
	s.mustPrewriteOK(c, putMutations("x", "A"), "x", 5)
	// B prewrite and find A's lock.
	errs := s.store.Prewrite(putMutations("x", "B"), []byte("x"), 10, 0)
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

func (s *testMockTiKVSuite) TestRollbackAndWriteConflict(c *C) {
	s.mustPutOK(c, "test", "test", 1, 3)

	errs := s.store.Prewrite(putMutations("lock", "lock", "test", "test1"), []byte("test"), 2, 2)
	s.mustWriteWriteConflict(c, errs, 1)

	s.mustPutOK(c, "test", "test2", 5, 8)

	// simulate `getTxnStatus` for txn 2.
	err := s.store.Cleanup([]byte("test"), 2)
	c.Assert(err, IsNil)

	errs = s.store.Prewrite(putMutations("test", "test3"), []byte("test"), 6, 1)
	s.mustWriteWriteConflict(c, errs, 0)
}

func (s *testMockTiKVSuite) mustWriteWriteConflict(c *C, errs []error, i int) {
	c.Assert(errs[i], NotNil)
	c.Assert(strings.Contains(errs[i].Error(), "write conflict"), IsTrue)
}

func (s *testMockTiKVSuite) TestRC(c *C) {
	s.mustPutOK(c, "key", "v1", 5, 10)
	s.mustPrewriteOK(c, putMutations("key", "v2"), "key", 15)
	s.mustGetErr(c, "key", 20)
	s.mustGetRC(c, "key", 12, "v1")
	s.mustGetRC(c, "key", 20, "v1")
}
