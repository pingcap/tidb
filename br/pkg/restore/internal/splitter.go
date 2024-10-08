// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package internal

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/restore/split"
)

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client split.SplitClient
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client split.SplitClient) *RegionSplitter {
	return &RegionSplitter{
		client: client,
	}
}

// SplitWaitAndScatter expose the function `SplitWaitAndScatter` of split client.
func (rs *RegionSplitter) SplitWaitAndScatter(ctx context.Context, region *split.RegionInfo, keys [][]byte) ([]*split.RegionInfo, error) {
	return rs.client.SplitWaitAndScatter(ctx, region, keys)
}
