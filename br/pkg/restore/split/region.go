// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
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
		beforeEnd(key, region.Region.GetEndKey())
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

// ToZapFields returns zap fields for the RegionInfo. It can handle nil RegionInfo.
func (region *RegionInfo) ToZapFields() zap.Field {
	if region == nil {
		return zap.Skip()
	}
	return logutil.Region(region.Region)
}
