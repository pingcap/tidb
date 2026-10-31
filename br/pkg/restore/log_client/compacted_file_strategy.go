// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"fmt"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	rules map[int64]*restoreutils.RewriteRules,
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
	RewriteBoundary *restoreutils.RewriteRules
}

func (cs *CompactedFileSplitStrategy) inspect(ssts SSTs) sstIdentity {
	r, ok := ssts.(RewrittenSSTs)
	if !ok || r.RewrittenTo() == ssts.TableID() {
		return sstIdentity{
			EffectiveID:     ssts.TableID(),
			RewriteBoundary: nil,
		}
	}

	rule := restoreutils.GetRewriteRuleOfTable(ssts.TableID(), r.RewrittenTo(), map[int64]int64{}, false)

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
		cs.TableSplitter[ssts.TableID()] = splitHelper
	}

	for _, f := range ssts.GetSSTs() {
		startKey := codec.EncodeBytes(nil, f.StartKey)
		endKey := codec.EncodeBytes(nil, f.EndKey)
		cs.AccumulateCount += 1
		if f.TotalKvs == 0 || f.Size_ == 0 {
			log.Error("No key-value pairs in subcompaction", zap.String("name", f.Name))
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

func (cs *CompactedFileSplitStrategy) ShouldSkip(subCompaction SSTs) bool {
	_, exist := cs.Rules[subCompaction.TableID()]
	if !exist {
		log.Info("skip for no rule files", zap.Int64("tableID", subCompaction.TableID()))
		return true
	}
	sstOutputs := make([]*backuppb.File, 0, len(subCompaction.GetSSTs()))
	for _, sst := range subCompaction.GetSSTs() {
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
		log.Info("all files in sub compaction skipped")
		return true
	}
	if len(sstOutputs) != len(subCompaction.GetSSTs()) {
		log.Info("partial files in sub compaction skipped due to checkpoint")
		subCompaction.SetSSTs(sstOutputs)
		return false
	}
	return false
}
