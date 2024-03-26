// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockPDClientForSplit struct {
	pd.Client

	regions     *pdtypes.RegionTree
	scanRegions struct {
		errors     []error
		beforeHook func()
	}
	splitRegions struct {
		fn func() (bool, *kvrpcpb.SplitRegionResponse, error)
	}
	scatterRegion struct {
		eachRegionFailBefore int
		count                map[uint64]int
	}
	scatterRegions struct {
		notImplemented bool
	}
	getOperator struct {
		responses map[uint64][]*pdpb.GetOperatorResponse
	}
}

func newMockPDClientForSplit() *mockPDClientForSplit {
	ret := &mockPDClientForSplit{}
	ret.regions = &pdtypes.RegionTree{}
	ret.scatterRegion.count = make(map[uint64]int)
	return ret
}

func newRegionNotFullyReplicatedErr(regionID uint64) error {
	return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionID)
}

func (c *mockPDClientForSplit) SetRegions(boundaries [][]byte) {
	for i := 1; i < len(boundaries); i++ {
		c.regions.SetRegion(&pdtypes.Region{
			Meta: &metapb.Region{
				Id:       uint64(i),
				StartKey: boundaries[i-1],
				EndKey:   boundaries[i],
			},
		})
	}
}

func (c *mockPDClientForSplit) ScanRegions(
	_ context.Context,
	key, endKey []byte,
	limit int,
	_ ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	if len(c.scanRegions.errors) > 0 {
		err := c.scanRegions.errors[0]
		c.scanRegions.errors = c.scanRegions.errors[1:]
		return nil, err
	}

	if c.scanRegions.beforeHook != nil {
		c.scanRegions.beforeHook()
	}

	regions := c.regions.ScanRange(key, endKey, limit)
	ret := make([]*pd.Region, 0, len(regions))
	for _, r := range regions {
		ret = append(ret, &pd.Region{
			Meta:   r.Meta,
			Leader: r.Leader,
		})
	}
	return ret, nil
}

func (c *mockPDClientForSplit) ScatterRegion(_ context.Context, regionID uint64) error {
	c.scatterRegion.count[regionID]++
	if c.scatterRegion.count[regionID] > c.scatterRegion.eachRegionFailBefore {
		return nil
	}
	return newRegionNotFullyReplicatedErr(regionID)
}

func (c *mockPDClientForSplit) ScatterRegions(context.Context, []uint64, ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	if c.scatterRegions.notImplemented {
		return nil, status.Error(codes.Unimplemented, "Ah, yep")
	}
	return nil, nil
}

func (c *mockPDClientForSplit) GetOperator(_ context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	ret := c.getOperator.responses[regionID][0]
	c.getOperator.responses[regionID] = c.getOperator.responses[regionID][1:]
	return ret, nil
}
