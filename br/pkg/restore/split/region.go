// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// RegionInfo includes a region and the leader of the region.
type RegionInfo struct {
	Region       *metapb.Region
	Leader       *metapb.Peer
	PendingPeers []*metapb.Peer
	DownPeers    []*metapb.Peer
}

// ContainsInterior returns whether the region contains the given key, and also
// that the key does not fall on the boundary (start key) of the region.
func (region *RegionInfo) ContainsInterior(key []byte) bool {
	return bytes.Compare(key, region.Region.GetStartKey()) > 0 &&
		(len(region.Region.GetEndKey()) == 0 ||
			bytes.Compare(key, region.Region.GetEndKey()) < 0)
}
