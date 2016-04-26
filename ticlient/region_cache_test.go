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

package ticlient

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type testRegionCacheSuite struct {
	pd    *mockPDClient
	cache *RegionCache
}

var _ = Suite(&testRegionCacheSuite{})

func (s *testRegionCacheSuite) SetUpTest(c *C) {
	s.pd = newMockPDClient()
	s.cache = NewRegionCache(s.pd)

	s.pd.setStore(1, "addr1")
	s.pd.setStore(2, "addr2")
	s.pd.setRegion(3, nil, nil, []uint64{1, 2})
}

func (s *testRegionCacheSuite) TestSimple(c *C) {
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr1")
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestDropRegion(c *C) {
	s.cache.GetRegion([]byte("a"))

	// remove from pd
	s.pd.removeRegion(3)

	// read from cache
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr1")
	c.Assert(s.cache.regions, HasLen, 1)

	// drop from cache
	s.cache.DropRegion(3)

	r, err = s.cache.GetRegion([]byte("a"))
	c.Assert(r, IsNil)
	c.Assert(err, NotNil)
	c.Assert(s.cache.regions, HasLen, 0)
}

func (s *testRegionCacheSuite) TestDropRegion2(c *C) {
	// remove store 1 from PD, should cause an error in `loadRegion()`
	// Why 1? Because GetRegion will pick 1 "randomly".
	s.pd.removeStore(1)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(r, IsNil)
	c.Assert(s.cache.regions, HasLen, 0)
}

func (s *testRegionCacheSuite) TestUpdateLeader(c *C) {
	s.cache.GetRegion([]byte("a"))
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(3, 2)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.curPeerIdx, Equals, 1)
	c.Assert(r.GetAddress(), Equals, "addr2")
}

func (s *testRegionCacheSuite) TestUpdateLeader2(c *C) {
	s.cache.GetRegion([]byte("a"))
	// store4 becomes leader
	s.pd.setStore(4, "addr4")
	s.pd.setRegion(3, nil, nil, []uint64{4, 1, 2})
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(3, 4)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.curPeerIdx, Equals, 0)
	c.Assert(r.GetAddress(), Equals, "addr4")
}

func (s *testRegionCacheSuite) TestUpdateLeader3(c *C) {
	s.cache.GetRegion([]byte("a"))
	// store2 becomes leader
	s.pd.setRegion(3, nil, nil, []uint64{2, 1})
	// store2 gone, store4 becomes leader
	s.pd.removeStore(2)
	s.pd.setStore(4, "addr4")
	// tikv-server reports `NotLeader`(store2 is the leader)
	s.cache.UpdateLeader(3, 2)

	s.pd.setRegion(3, nil, nil, []uint64{4, 1})
	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr4")
}

func (s *testRegionCacheSuite) TestSplit(c *C) {
	r, err := s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr1")

	// split to ['' - 'm' - 'z']
	s.pd.setRegion(3, nil, []byte("m"), []uint64{1})
	s.pd.setRegion(4, []byte("m"), nil, []uint64{2})

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(r.GetID())
	c.Assert(s.cache.regions, HasLen, 0)

	r, err = s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, uint64(4))
	c.Assert(r.GetAddress(), Equals, "addr2")
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestMerge(c *C) {
	// ['' - 'm' - 'z']
	s.pd.setRegion(3, nil, []byte("m"), []uint64{1})
	s.pd.setRegion(4, []byte("m"), nil, []uint64{2})

	r, err := s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, uint64(4))
	c.Assert(r.GetAddress(), Equals, "addr2")

	// merge to single region
	s.pd.removeRegion(4)
	s.pd.setRegion(3, nil, nil, []uint64{1, 2})

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(r.GetID())
	c.Assert(s.cache.regions, HasLen, 0)

	r, err = s.cache.GetRegion([]byte("x"))
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr1")
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestReconnect(c *C) {
	s.cache.GetRegion([]byte("a"))

	// connect tikv-server failed, cause drop cache
	s.cache.DropRegion(3)

	r, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, uint64(3))
	c.Assert(r.GetAddress(), Equals, "addr1")
	c.Assert(s.cache.regions, HasLen, 1)
}

func (s *testRegionCacheSuite) TestNextPeer(c *C) {
	region, err := s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(region.curPeerIdx, Equals, 0)

	s.cache.NextPeer(3)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(region.curPeerIdx, Equals, 1)

	s.cache.NextPeer(3)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	// Out of range of Peers, so get Region again and pick Peers[0] as leader.
	c.Assert(region.curPeerIdx, Equals, 0)

	s.pd.removeRegion(3)
	// regionCache still has more Peers, so pick next peer.
	s.cache.NextPeer(3)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(region.curPeerIdx, Equals, 1)

	// region 3 is removed so can't get Region from pd.
	s.cache.NextPeer(3)
	region, err = s.cache.GetRegion([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(region, IsNil)

}

type mockPDClient struct {
	ts      int64
	stores  map[uint64]*metapb.Store
	regions map[uint64]*metapb.Region
}

func newMockPDClient() *mockPDClient {
	return &mockPDClient{
		stores:  make(map[uint64]*metapb.Store),
		regions: make(map[uint64]*metapb.Region),
	}
}

func (c *mockPDClient) setStore(id uint64, addr string) {
	c.stores[id] = &metapb.Store{
		Id:      proto.Uint64(id),
		Address: proto.String(addr),
	}
}

func (c *mockPDClient) removeStore(id uint64) {
	delete(c.stores, id)
}

func (c *mockPDClient) setRegion(id uint64, startKey, endKey []byte, peers []uint64) {
	var metaPeers []*metapb.Peer
	for _, id := range peers {
		metaPeers = append(metaPeers, &metapb.Peer{
			Id:      proto.Uint64(id),
			StoreId: proto.Uint64(id),
		})
	}
	c.regions[id] = &metapb.Region{
		Id:       proto.Uint64(id),
		StartKey: startKey,
		EndKey:   endKey,
		Peers:    metaPeers,
	}
}

func (c *mockPDClient) removeRegion(id uint64) {
	delete(c.regions, id)
}

func (c *mockPDClient) GetTS() (int64, int64, error) {
	c.ts++
	return c.ts, 0, nil
}

func (c *mockPDClient) GetRegion(key []byte) (*metapb.Region, error) {
	for _, r := range c.regions {
		if bytes.Compare(r.GetStartKey(), key) <= 0 &&
			(bytes.Compare(key, r.GetEndKey()) < 0 || len(r.GetEndKey()) == 0) {
			return r, nil
		}
	}
	return nil, errors.Errorf("region not found for key %q", key)
}

func (c *mockPDClient) GetStore(storeID uint64) (*metapb.Store, error) {
	s, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store %d not found", storeID)
	}
	return s, nil
}

func (c *mockPDClient) Close() {}
