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

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct{}

func (s *testRegionSuite) TestRegionInfo(c *C) {
	n := uint64(3)

	peers := make([]*metapb.Peer, 0, n)
	for i := uint64(0); i < n; i++ {
		p := &metapb.Peer{
			Id:      i,
			StoreId: i,
		}
		peers = append(peers, p)
	}
	region := &metapb.Region{
		Peers: peers,
	}
	downPeer, pendingPeer := peers[0], peers[1]

	info := NewRegionInfo(region, peers[0])
	info.DownPeers = []*pdpb.PeerStats{{Peer: downPeer}}
	info.PendingPeers = []*metapb.Peer{pendingPeer}

	r := info.Clone()
	c.Assert(r, DeepEquals, info)

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetPeer(i), Equals, r.Peers[i])
	}
	c.Assert(r.GetPeer(n), IsNil)
	c.Assert(r.GetDownPeer(n), IsNil)
	c.Assert(r.GetDownPeer(downPeer.GetId()), DeepEquals, downPeer)
	c.Assert(r.GetPendingPeer(n), IsNil)
	c.Assert(r.GetPendingPeer(pendingPeer.GetId()), DeepEquals, pendingPeer)

	for i := uint64(0); i < n; i++ {
		c.Assert(r.GetStorePeer(i).GetStoreId(), Equals, i)
	}
	c.Assert(r.GetStorePeer(n), IsNil)

	removePeer := &metapb.Peer{
		Id:      n,
		StoreId: n,
	}
	r.Peers = append(r.Peers, removePeer)
	c.Assert(DiffRegionPeersInfo(info, r), Matches, "Add peer.*")
	c.Assert(DiffRegionPeersInfo(r, info), Matches, "Remove peer.*")
	c.Assert(r.GetStorePeer(n), DeepEquals, removePeer)
	r.RemoveStorePeer(n)
	c.Assert(DiffRegionPeersInfo(r, info), Equals, "")
	c.Assert(r.GetStorePeer(n), IsNil)
	r.Region.StartKey = []byte{0}
	c.Assert(DiffRegionKeyInfo(r, info), Matches, "StartKey Changed.*")
	r.Region.EndKey = []byte{1}
	c.Assert(DiffRegionKeyInfo(r, info), Matches, ".*EndKey Changed.*")

	stores := r.GetStoreIds()
	c.Assert(stores, HasLen, int(n))
	for i := uint64(0); i < n; i++ {
		_, ok := stores[i]
		c.Assert(ok, IsTrue)
	}

	followers := r.GetFollowers()
	c.Assert(followers, HasLen, int(n-1))
	for i := uint64(1); i < n; i++ {
		c.Assert(followers[peers[i].GetStoreId()], DeepEquals, peers[i])
	}
}

func (s *testRegionSuite) TestRegionItem(c *C) {
	item := newRegionItem([]byte("b"), []byte{})

	c.Assert(item.Less(newRegionItem([]byte("a"), []byte{})), IsTrue)
	c.Assert(item.Less(newRegionItem([]byte("b"), []byte{})), IsFalse)
	c.Assert(item.Less(newRegionItem([]byte("c"), []byte{})), IsFalse)

	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)

	item = newRegionItem([]byte("b"), []byte("d"))
	c.Assert(item.Contains([]byte("a")), IsFalse)
	c.Assert(item.Contains([]byte("b")), IsTrue)
	c.Assert(item.Contains([]byte("c")), IsTrue)
	c.Assert(item.Contains([]byte("d")), IsFalse)
}

func (s *testRegionSuite) TestRegionTree(c *C) {
	tree := newRegionTree()

	c.Assert(tree.search([]byte("a")), IsNil)

	regionA := NewRegion([]byte("a"), []byte("b"))
	regionB := NewRegion([]byte("b"), []byte("c"))
	regionC := NewRegion([]byte("c"), []byte("d"))
	regionD := NewRegion([]byte("d"), []byte{})

	tree.update(regionA)
	tree.update(regionC)
	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), IsNil)
	c.Assert(tree.search([]byte("c")), Equals, regionC)
	c.Assert(tree.search([]byte("d")), IsNil)

	tree.update(regionB)
	tree.remove(regionC)
	tree.update(regionD)
	c.Assert(tree.search([]byte{}), IsNil)
	c.Assert(tree.search([]byte("a")), Equals, regionA)
	c.Assert(tree.search([]byte("b")), Equals, regionB)
	c.Assert(tree.search([]byte("c")), IsNil)
	c.Assert(tree.search([]byte("d")), Equals, regionD)

	// region with the same range and different region id will not be delete.
	region0 := newRegionItem([]byte{}, []byte("a")).region
	tree.update(region0)
	c.Assert(tree.search([]byte{}), Equals, region0)
	anotherRegion0 := newRegionItem([]byte{}, []byte("a")).region
	anotherRegion0.Id = 123
	tree.remove(anotherRegion0)
	c.Assert(tree.search([]byte{}), Equals, region0)

	// overlaps with 0, A, B, C.
	region0D := newRegionItem([]byte(""), []byte("d")).region
	tree.update(region0D)
	c.Assert(tree.search([]byte{}), Equals, region0D)
	c.Assert(tree.search([]byte("a")), Equals, region0D)
	c.Assert(tree.search([]byte("b")), Equals, region0D)
	c.Assert(tree.search([]byte("c")), Equals, region0D)
	c.Assert(tree.search([]byte("d")), Equals, regionD)

	// overlaps with D.
	regionE := newRegionItem([]byte("e"), []byte{}).region
	tree.update(regionE)
	c.Assert(tree.search([]byte{}), Equals, region0D)
	c.Assert(tree.search([]byte("a")), Equals, region0D)
	c.Assert(tree.search([]byte("b")), Equals, region0D)
	c.Assert(tree.search([]byte("c")), Equals, region0D)
	c.Assert(tree.search([]byte("d")), IsNil)
	c.Assert(tree.search([]byte("e")), Equals, regionE)
}

func updateRegions(c *C, tree *regionTree, regions []*metapb.Region) {
	for _, region := range regions {
		tree.update(region)
		c.Assert(tree.search(region.StartKey), Equals, region)
		if len(region.EndKey) > 0 {
			end := region.EndKey[0]
			c.Assert(tree.search([]byte{end - 1}), Equals, region)
			c.Assert(tree.search([]byte{end + 1}), Not(Equals), region)
		}
	}
}

func (s *testRegionSuite) TestRegionTreeSplitAndMerge(c *C) {
	tree := newRegionTree()
	regions := []*metapb.Region{newRegionItem([]byte{}, []byte{}).region}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = SplitRegions(regions)
		updateRegions(c, tree, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = MergeRegions(regions)
		updateRegions(c, tree, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = MergeRegions(regions)
		} else {
			regions = SplitRegions(regions)
		}
		updateRegions(c, tree, regions)
	}
}

func newRegionItem(start, end []byte) *regionItem {
	return &regionItem{region: NewRegion(start, end)}
}
