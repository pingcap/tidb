// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"fmt"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"go.uber.org/zap"
)

// The impact factor is used to reduce the size and number of MVCC entries
// in SST files, helping to optimize performance and resource usage.
const impactFactor = 16

type CompactedFileSplitStrategy struct {
	*split.BaseSplitStrategy
	checkpointSets           map[string]struct{}
	checkpointFileProgressFn func(uint64, uint64)
}

var _ split.SplitStrategy[SSTs] = &CompactedFileSplitStrategy{}

func NewCompactedFileSplitStrategy(
	rules map[int64]*utils.RewriteRules,
	checkpointsSet map[string]struct{},
	updateStatsFn func(uint64, uint64),
) *CompactedFileSplitStrategy {
	return &CompactedFileSplitStrategy{
		BaseSplitStrategy:        split.NewBaseSplitStrategy(rules),
		checkpointSets:           checkpointsSet,
		checkpointFileProgressFn: updateStatsFn,
	}
}

type sstIdentity struct {
	EffectiveID     int64
	RewriteBoundary *utils.RewriteRules
}

func (cs *CompactedFileSplitStrategy) inspect(ssts SSTs) sstIdentity {
	r, ok := ssts.(RewrittenSSTs)
	if !ok || r.RewrittenTo() == ssts.TableID() {
		return sstIdentity{
			EffectiveID:     ssts.TableID(),
			RewriteBoundary: nil,
		}
	}

	rule := utils.GetRewriteRuleOfTable(ssts.TableID(), r.RewrittenTo(), 0, map[int64]int64{}, false)

	return sstIdentity{
		EffectiveID:     r.RewrittenTo(),
		RewriteBoundary: rule,
	}
}

func (cs *CompactedFileSplitStrategy) Accumulate(ssts SSTs) {
	identity := cs.inspect(ssts)

	splitHelper, exist := cs.TableSplitter[identity.EffectiveID]
	if !exist {
		splitHelper = split.NewSplitHelper()
		log.Info("Initialized splitter for table.",
			zap.Int64("table-id", ssts.TableID()), zap.Int64("effective-id", identity.EffectiveID), zap.Stringer("rewrite-boundary", identity.RewriteBoundary))
		cs.TableSplitter[identity.EffectiveID] = splitHelper
	}

	for _, f := range ssts.GetSSTs() {
		startKey, endKey, err := utils.GetRewriteRawKeys(f, identity.RewriteBoundary)
		if err != nil {
			log.Panic("[unreachable] the rewrite rule doesn't match the SST file, this shouldn't happen...",
				logutil.ShortError(err), zap.Stringer("rule", identity.RewriteBoundary), zap.Int64("effective-id", identity.EffectiveID),
				zap.Stringer("file", f),
			)
		}
		cs.AccumulateCount += 1
		if f.TotalKvs == 0 || f.Size_ == 0 {
			log.Warn("No key-value pairs in sst files", zap.String("name", f.Name))
			continue
		}
		// The number of MVCC entries in the compacted SST files can be excessive.
		// This calculation takes the MVCC impact into account to optimize performance.
		calculateCount := int64(f.TotalKvs) / impactFactor
		if calculateCount == 0 {
			// at least consider as 1 key impact
			log.Warn(fmt.Sprintf("less than %d key-value pairs in subcompaction", impactFactor), zap.String("name", f.Name))
			calculateCount = 1
		}
		calculateSize := f.Size_ / impactFactor
		if calculateSize == 0 {
			log.Warn(fmt.Sprintf("less than %d key-value size in subcompaction", impactFactor), zap.String("name", f.Name))
			calculateSize = 1
		}
		splitHelper.Merge(split.Valued{
			Key: split.Span{
				StartKey: startKey,
				EndKey:   endKey,
			},
			Value: split.Value{
				Size:   calculateSize,
				Number: calculateCount,
			},
		})
	}
}

func (cs *CompactedFileSplitStrategy) ShouldSplit() bool {
	return cs.AccumulateCount > (4096 / impactFactor)
}

func hasRule[T any](ssts SSTs, rules map[int64]T) bool {
	if r, ok := ssts.(RewrittenSSTs); ok {
		_, exist := rules[r.RewrittenTo()]
		// If the SST has been rewritten (logically has another table ID),
		// don't check table ID in its physical file, or we may mistakenly match it
		// with another table that has the same ID.
		//
		// An example, if there are tables:
		//
		// - Foo.ID = 1  (Backup Data)
		// - Foo.ID = 10 (Upstream after Rewriting)
		// - Bar.ID = 1  (Upstream Natively)
		//
		// If we treat `Foo` in the backup data as if it had table ID `1`,
		// the restore progress may match it with `Bar`.
		return exist
	}

	if _, exist := rules[ssts.TableID()]; exist {
		return true
	}

	return false
}

func (cs *CompactedFileSplitStrategy) ShouldSkip(ssts SSTs) bool {
	if !hasRule(ssts, cs.Rules) {
		log.Warn("skip for no rule files", zap.Int64("tableID", ssts.TableID()), zap.Any("ssts", ssts))
		return true
	}
	sstOutputs := make([]*backuppb.File, 0, len(ssts.GetSSTs()))
	for _, sst := range ssts.GetSSTs() {
		if _, ok := cs.checkpointSets[sst.Name]; !ok {
			sstOutputs = append(sstOutputs, sst)
		} else {
			// This file is recorded in the checkpoint, indicating that it has
			// already been restored to the cluster. Therefore, we will skip
			// processing this file and only update the statistics.
			cs.checkpointFileProgressFn(sst.TotalKvs, sst.Size_)
		}
	}
	if len(sstOutputs) == 0 {
		log.Info("all files in SST set skipped", zap.Stringer("ssts", ssts))
		return true
	}
	if len(sstOutputs) != len(ssts.GetSSTs()) {
		log.Info(
			"partial files in SST set skipped due to checkpoint",
			zap.Stringer("ssts", ssts), zap.Int("origin", len(ssts.GetSSTs())), zap.Int("output", len(sstOutputs)),
		)
		ssts.SetSSTs(sstOutputs)
		return false
	}
	return false
}
