// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
)

// Splitter defines the interface for basic splitting strategies.
type Splitter interface {
	// The execution will split the keys on one region, and starts scatter after the region finish split.
	ExecuteOneRegion(ctx context.Context, region *RegionInfo, keys [][]byte) ([]*RegionInfo, error)
	// The execution will split all privided keys
	// and make sure new splitted regions are balance.
	// It will split regions by the rewrite rules,
	// then it will split regions by the end key of each range.
	// tableRules includes the prefix of a table, since some ranges may have
	// a prefix with record sequence or index sequence.
	// note: all ranges and rewrite rules must have raw key.
	ExecuteSortedKeys(ctx context.Context, keys [][]byte) error
}

// PipelineSplitter defines the interface for advanced (pipeline) splitting strategies.
// log / compacted sst files restore need to use this to split after full restore.
// and the splitter must perform with a control.
// so we choose to split and restore in a continuous flow.
type PipelineSplitter interface {
	Splitter
	ExecutePipelineSplit(ctx context.Context) error // Method for executing pipeline-based splitting
}
