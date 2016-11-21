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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testMockTiKVSuite struct {
	store *MvccStore
}

var _ = Suite(&testMockTiKVSuite{})

func encodeKey(s string) []byte {
	return codec.EncodeBytes(nil, []byte(s))
}

func encodeKeys(ss []string) [][]byte {
	var keys [][]byte
	for _, s := range ss {
		keys = append(keys, encodeKey(s))
	}
	return keys
}

func putMutations(kvpairs ...string) []*kvrpcpb.Mutation {
	var mutations []*kvrpcpb.Mutation
	for i := 0; i < len(kvpairs); i += 2 {
		mutations = append(mutations, &kvrpcpb.Mutation{
			Op:    kvrpcpb.Op_Put,
			Key:   encodeKey(kvpairs[i]),
			Value: []byte(kvpairs[i+1]),
		})
	}
	return mutations
}

func lock(key, primary string, ts uint64) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		Key:         encodeKey(key),
		PrimaryLock: encodeKey(primary),
		LockVersion: ts,
	}
}

func (s *testMockTiKVSuite) SetUpTest(c *C) {
	s.store = NewMvccStore()
}

func (s *testMockTiKVSuite) mustGetNone(c *C, key string, ts uint64) {
	val, err := s.store.Get(encodeKey(key), ts)
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetErr(c *C, key string, ts uint64) {
	val, err := s.store.Get(encodeKey(key), ts)
	c.Assert(err, NotNil)
	c.Assert(val, IsNil)
}

func (s *testMockTiKVSuite) mustGetOK(c *C, key string, ts uint64, expect string) {
	val, err := s.store.Get(encodeKey(key), ts)
	c.Assert(err, IsNil)
	c.Assert(string(val), Equals, expect)
}

func (s *testMockTiKVSuite) mustPutOK(c *C, key, value string, startTS, commitTS uint64) {
	errs := s.store.Prewrite(putMutations(key, value), encodeKey(key), startTS, 0)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
	err := s.store.Commit([][]byte{encodeKey(key)}, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustDeleteOK(c *C, key string, startTS, commitTS uint64) {
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Op_Del,
			Key: encodeKey(key),
		},
	}
	errs := s.store.Prewrite(mutations, encodeKey(key), startTS, 0)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
	err := s.store.Commit([][]byte{encodeKey(key)}, startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustScanOK(c *C, start string, limit int, ts uint64, expect ...string) {
	pairs := s.store.Scan(encodeKey(start), nil, limit, ts)
	c.Assert(len(pairs)*2, Equals, len(expect))
	for i := 0; i < len(pairs); i++ {
		c.Assert(pairs[i].Err, IsNil)
		c.Assert(pairs[i].Key, BytesEquals, encodeKey(expect[i*2]))
		c.Assert(string(pairs[i].Value), Equals, expect[i*2+1])
	}
}

func (s *testMockTiKVSuite) mustPrewriteOK(c *C, mutations []*kvrpcpb.Mutation, primary string, startTS uint64) {
	errs := s.store.Prewrite(mutations, encodeKey(primary), startTS, 0)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}
}

func (s *testMockTiKVSuite) mustCommitOK(c *C, keys []string, startTS, commitTS uint64) {
	err := s.store.Commit(encodeKeys(keys), startTS, commitTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustCommitErr(c *C, keys []string, startTS, commitTS uint64) {
	err := s.store.Commit(encodeKeys(keys), startTS, commitTS)
	c.Assert(err, NotNil)
}

func (s *testMockTiKVSuite) mustRollbackOK(c *C, keys []string, startTS uint64) {
	err := s.store.Rollback(encodeKeys(keys), startTS)
	c.Assert(err, IsNil)
}

func (s *testMockTiKVSuite) mustRollbackErr(c *C, keys []string, startTS uint64) {
	err := s.store.Rollback(encodeKeys(keys), startTS)
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
	s.mustCommitOK(c, []string{"primary"}, 5, 10)
	s.mustRollbackErr(c, []string{"primary"}, 5)
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
