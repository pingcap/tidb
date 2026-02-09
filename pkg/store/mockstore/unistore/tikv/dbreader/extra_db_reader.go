package dbreader

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/kv"
)

// LocateExtraRegionResult is the result of locating a region for accessing extra data in uniStore.
// It's only used by mock MPP executors in tests.
type LocateExtraRegionResult struct {
	Found  bool
	Region *metapb.Region
	Peer   *metapb.Peer
}

// GetExtraDBReaderContext contains the region and key ranges to create an extra DB reader.
// It's only used by mock MPP executors in tests.
type GetExtraDBReaderContext struct {
	Region *metapb.Region
	Peer   *metapb.Peer
	Ranges []kv.KeyRange
}

// ExtraDbReaderProvider provides DBReader to read extra data in uniStore.
// It's only used by mock MPP executors in tests.
type ExtraDbReaderProvider interface {
	LocateExtraRegion(ctx context.Context, key []byte) (LocateExtraRegionResult, error)
	GetExtraDBReaderByRegion(ctx GetExtraDBReaderContext) (*DBReader, error)
}
