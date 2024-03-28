// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
)

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	Start []byte
	End   []byte
}

// SortRanges checks if the range overlapped and sort them.
func SortRanges(ranges []rtree.Range) ([]rtree.Range, error) {
	rangeTree := rtree.NewRangeTree()
	for _, rg := range ranges {
		if out := rangeTree.InsertRange(rg); out != nil {
			log.Error("insert ranges overlapped",
				logutil.Key("startKeyOut", out.StartKey),
				logutil.Key("endKeyOut", out.EndKey),
				logutil.Key("startKeyIn", rg.StartKey),
				logutil.Key("endKeyIn", rg.EndKey))
			return nil, errors.Annotatef(berrors.ErrInvalidRange, "ranges overlapped")
		}
	}
	sortedRanges := rangeTree.GetSortedRanges()
	return sortedRanges, nil
}

// RewriteRules contains rules for rewriting keys of tables.
type RewriteRules struct {
	Data        []*import_sstpb.RewriteRule
	OldKeyspace []byte
	NewKeyspace []byte
}

// Append append its argument to this rewrite rules.
func (r *RewriteRules) Append(other RewriteRules) {
	r.Data = append(r.Data, other.Data...)
}

// EmptyRewriteRule make a map of new, empty rewrite rules.
func EmptyRewriteRulesMap() map[int64]*RewriteRules {
	return make(map[int64]*RewriteRules)
}

// EmptyRewriteRule make a new, empty rewrite rule.
func EmptyRewriteRule() *RewriteRules {
	return &RewriteRules{
		Data: []*import_sstpb.RewriteRule{},
	}
}
