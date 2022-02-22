// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pdtypes

import (
	"bytes"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// Region is a mock of PD's core.RegionInfo. For testing purpose.
type Region struct {
	Meta   *metapb.Region
	Leader *metapb.Peer
}

// NewRegionInfo returns a new RegionInfo.
func NewRegionInfo(meta *metapb.Region, leader *metapb.Peer) *Region {
	return &Region{Meta: meta, Leader: leader}
}

// RegionTree is a mock of PD's region tree. For testing purpose.
type RegionTree struct {
	Regions []*Region
}

// SetRegion puts a region to region tree.
func (t *RegionTree) SetRegion(region *Region) {
	rs := t.Regions[:0]
	for _, r := range t.Regions {
		if !overlap(r, region) {
			rs = append(rs, r)
		}
	}
	rs = append(rs, region)
	t.Regions = rs
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (t *RegionTree) ScanRange(startKey, endKey []byte, limit int) []*Region {
	sort.Slice(t.Regions, func(i, j int) bool {
		return bytes.Compare(t.Regions[i].Meta.StartKey, t.Regions[j].Meta.StartKey) < 0
	})
	pivot := NewRegionInfo(&metapb.Region{StartKey: startKey, EndKey: endKey}, nil)
	var res []*Region
	for _, r := range t.Regions {
		if overlap(r, pivot) && (limit == 0 || len(res) < limit) {
			res = append(res, r)
		}
	}
	return res
}

func overlap(a, b *Region) bool {
	//            |----a----|
	// |----b----|
	if len(b.Meta.EndKey) > 0 && bytes.Compare(b.Meta.EndKey, a.Meta.StartKey) <= 0 {
		return false
	}

	// |----a----|
	// 		      |----b----|
	if len(a.Meta.EndKey) > 0 && bytes.Compare(a.Meta.EndKey, b.Meta.StartKey) <= 0 {
		return false
	}

	return true
}
