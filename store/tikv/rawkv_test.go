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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	goctx "golang.org/x/net/context"
)

type testRawKVSuite struct {
	cluster *mocktikv.Cluster
	client  *RawKVClient
	bo      *Backoffer
}

var _ = Suite(&testRawKVSuite{})

func (s *testRawKVSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.client = &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mocktikv.NewMvccStore()),
	}
	s.bo = NewBackoffer(5000, goctx.Background())
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

func (s *testRawKVSuite) TestSimple(c *C) {
	s.mustNotExist(c, []byte("key"))
	s.mustPut(c, []byte("key"), []byte("value"))
	s.mustGet(c, []byte("key"), []byte("value"))
	s.mustDelete(c, []byte("key"))
	s.mustNotExist(c, []byte("key"))

	s.mustPut(c, []byte("key"), []byte(""))
	s.mustGet(c, []byte("key"), []byte(""))
	s.mustPut(c, []byte("key"), nil)
	s.mustGet(c, []byte("key"), []byte(""))
}

func (s *testRawKVSuite) TestSplit(c *C) {
	loc, err := s.client.regionCache.LocateKey(s.bo, []byte("k"))
	c.Assert(err, IsNil)
	s.mustPut(c, []byte("k1"), []byte("v1"))
	s.mustPut(c, []byte("k3"), []byte("v3"))

	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, []byte("k2"), []uint64{peerID}, peerID)

	s.mustGet(c, []byte("k1"), []byte("v1"))
	s.mustGet(c, []byte("k3"), []byte("v3"))
}
