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
	"bytes"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"golang.org/x/net/context"
)

type testRawKVSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	client  *RawKVClient
	bo      *Backoffer
}

var _ = Suite(&testRawKVSuite{})

func (s *testRawKVSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	pdClient := mocktikv.NewPDClient(s.cluster)
	s.client = &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(pdClient),
		pdClient:    pdClient,
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mocktikv.NewMvccStore()),
	}
	s.bo = NewBackoffer(context.Background(), 5000)
}

func (s *testRawKVSuite) TearDownTest(c *C) {
	s.client.Close()
}

func (s *testRawKVSuite) mustNotExist(c *C, key []byte) {
	v, err := s.client.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}

func (s *testRawKVSuite) mustGet(c *C, key, value []byte) {
	v, err := s.client.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, NotNil)
	c.Assert(v, BytesEquals, value)
}

func (s *testRawKVSuite) mustPut(c *C, key, value []byte) {
	err := s.client.Put(key, value)
	c.Assert(err, IsNil)
}

func (s *testRawKVSuite) mustDelete(c *C, key []byte) {
	err := s.client.Delete(key)
	c.Assert(err, IsNil)
}

func (s *testRawKVSuite) mustScan(c *C, startKey string, limit int, expect ...string) {
	keys, values, err := s.client.Scan([]byte(startKey), limit)
	c.Assert(err, IsNil)
	c.Assert(len(keys)*2, Equals, len(expect))
	for i := range keys {
		c.Assert(string(keys[i]), Equals, expect[i*2])
		c.Assert(string(values[i]), Equals, expect[i*2+1])
	}
}

func (s *testRawKVSuite) mustDeleteRange(c *C, startKey, endKey []byte, expected map[string]string) {
	err := s.client.DeleteRange(startKey, endKey)
	c.Assert(err, IsNil)

	for keyStr := range expected {
		key := []byte(keyStr)
		if bytes.Compare(startKey, key) <= 0 && bytes.Compare(key, endKey) < 0 {
			delete(expected, keyStr)
		}
	}

	s.checkData(c, expected)
}

func (s *testRawKVSuite) checkData(c *C, expected map[string]string) {
	keys, values, err := s.client.Scan([]byte(""), len(expected)+1)
	c.Assert(err, IsNil)

	c.Assert(len(expected), Equals, len(keys))
	for i, key := range keys {
		c.Assert(expected[string(key)], Equals, string(values[i]))
	}
}

func (s *testRawKVSuite) split(c *C, regionKey, splitKey string) {
	loc, err := s.client.regionCache.LocateKey(s.bo, []byte(regionKey))
	c.Assert(err, IsNil)

	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.SplitRaw(loc.Region.id, newRegionID, []byte(splitKey), []uint64{peerID}, peerID)
}

func (s *testRawKVSuite) TestSimple(c *C) {
	s.mustNotExist(c, []byte("key"))
	s.mustPut(c, []byte("key"), []byte("value"))
	s.mustGet(c, []byte("key"), []byte("value"))
	s.mustDelete(c, []byte("key"))
	s.mustNotExist(c, []byte("key"))
	err := s.client.Put([]byte("key"), []byte(""))
	c.Assert(err, NotNil)
}

func (s *testRawKVSuite) TestSplit(c *C) {
	s.mustPut(c, []byte("k1"), []byte("v1"))
	s.mustPut(c, []byte("k3"), []byte("v3"))

	s.split(c, "k", "k2")

	s.mustGet(c, []byte("k1"), []byte("v1"))
	s.mustGet(c, []byte("k3"), []byte("v3"))
}

func (s *testRawKVSuite) TestScan(c *C) {
	s.mustPut(c, []byte("k1"), []byte("v1"))
	s.mustPut(c, []byte("k3"), []byte("v3"))
	s.mustPut(c, []byte("k5"), []byte("v5"))
	s.mustPut(c, []byte("k7"), []byte("v7"))

	check := func() {
		s.mustScan(c, "", 1, "k1", "v1")
		s.mustScan(c, "k1", 2, "k1", "v1", "k3", "v3")
		s.mustScan(c, "", 10, "k1", "v1", "k3", "v3", "k5", "v5", "k7", "v7")
		s.mustScan(c, "k2", 2, "k3", "v3", "k5", "v5")
		s.mustScan(c, "k2", 3, "k3", "v3", "k5", "v5", "k7", "v7")
	}

	check()
	s.split(c, "k", "k2")
	check()
	s.split(c, "k2", "k5")
	check()
}

func (s *testRawKVSuite) TestDeleteRange(c *C) {
	// Init data
	testData := map[string]string{}
	for _, i := range []byte("abcd") {
		for j := byte('0'); j <= byte('9'); j++ {
			key := []byte{i, j}
			value := []byte{'v', i, j}
			s.mustPut(c, key, value)

			testData[string(key)] = string(value)
		}
	}

	s.split(c, "b", "b")
	s.split(c, "c", "c")
	s.split(c, "d", "d")

	s.checkData(c, testData)
	s.mustDeleteRange(c, []byte("b"), []byte("c0"), testData)
	s.mustDeleteRange(c, []byte("c11"), []byte("c12"), testData)
	s.mustDeleteRange(c, []byte("d0"), []byte("d0"), testData)
	s.mustDeleteRange(c, []byte("c5"), []byte("d5"), testData)
	s.mustDeleteRange(c, []byte("a"), []byte("z"), testData)
}
