// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"
)

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	Start []byte
	End   []byte
}

// SortRanges checks if the range overlapped and sort them.
func SortRanges(ranges []rtree.Range, rewriteRules *RewriteRules) ([]rtree.Range, error) {
	rangeTree := rtree.NewRangeTree()
	for _, rg := range ranges {
		if rewriteRules != nil {
			startID := tablecodec.DecodeTableID(rg.StartKey)
			endID := tablecodec.DecodeTableID(rg.EndKey)
			var rule *import_sstpb.RewriteRule
			if startID == endID {
				rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", logutil.Key("key", rg.StartKey))
				} else {
					log.Debug(
						"rewrite start key",
						logutil.Key("key", rg.StartKey), logutil.RewriteRule(rule))
				}
				oldKey := rg.EndKey
				rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", logutil.Key("key", rg.EndKey))
				} else {
					log.Debug(
						"rewrite end key",
						logutil.Key("origin-key", oldKey),
						logutil.Key("key", rg.EndKey),
						logutil.RewriteRule(rule))
				}
			} else {
				log.Warn("table id does not match",
					logutil.Key("startKey", rg.StartKey),
					logutil.Key("endKey", rg.EndKey),
					zap.Int64("startID", startID),
					zap.Int64("endID", endID))
				return nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch")
			}
		}
		if out := rangeTree.InsertRange(rg); out != nil {
			log.Error("insert ranges overlapped",
				logutil.Key("startKeyOut", out.StartKey),
				logutil.Key("endKeyOut", out.EndKey),
				logutil.Key("startKeyIn", rg.StartKey),
				logutil.Key("endKeyIn", rg.EndKey))
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidRange, "ranges overlapped")
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

// EmptyRewriteRule make a new, empty rewrite rule.
func EmptyRewriteRule() *RewriteRules {
	return &RewriteRules{
		Data: []*import_sstpb.RewriteRule{},
	}
}
