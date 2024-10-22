// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

type CompactedFileSplitStrategy struct {
	*split.BaseSplitStrategy
	checkpointSets map[string]struct{}
	updateStatusFn func(uint64, uint64)
}

var _ split.SplitStrategy[*backuppb.LogFileSubcompaction] = &CompactedFileSplitStrategy{}

func NewCompactedFileSplitStrategy(
	rules map[int64]*restoreutils.RewriteRules,
	checkpointsSet map[string]struct{},
	updateStatsFn func(uint64, uint64),
) *CompactedFileSplitStrategy {
	return &CompactedFileSplitStrategy{
		BaseSplitStrategy: split.NewBaseSplitStrategy(rules),
		checkpointSets:    checkpointsSet,
		updateStatusFn:    updateStatsFn,
	}
}

func (l *CompactedFileSplitStrategy) Accumulate(subCompaction *backuppb.LogFileSubcompaction) {
	splitHelper, exist := l.TableSplitter[subCompaction.Meta.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		l.TableSplitter[subCompaction.Meta.TableId] = splitHelper
	}

	for _, f := range subCompaction.SstOutputs {
		startKey := codec.EncodeBytes(nil, f.StartKey)
		endKey := codec.EncodeBytes(nil, f.EndKey)
		l.AccumulateCount += 1
		splitHelper.Merge(split.Valued{
			Key: split.Span{
				StartKey: startKey,
				EndKey:   endKey,
			},
			Value: split.Value{
				// because we have too many mvcc in the sst files.
				// consider the MVCC impact here.
				Size:   f.Size_ / 16,
				Number: int64(f.TotalKvs) / 16,
			},
		})
	}
}

func (l *CompactedFileSplitStrategy) ShouldSplit() bool {
	return l.AccumulateCount > 128
}

func (l *CompactedFileSplitStrategy) ShouldSkip(subCompaction *backuppb.LogFileSubcompaction) bool {
	_, exist := l.Rules[subCompaction.Meta.TableId]
	if !exist {
		log.Info("skip for no rule files", zap.Int64("tableID", subCompaction.Meta.TableId))
		return true
	}
	sstOutputs := make([]*backuppb.File, 0, len(subCompaction.SstOutputs))
	for _, sst := range subCompaction.SstOutputs {
		if _, ok := l.checkpointSets[sst.Name]; !ok {
			sstOutputs = append(sstOutputs, sst)
		} else {
			l.updateStatusFn(sst.TotalKvs, sst.Size_)
		}
	}
	if len(sstOutputs) == 0 {
		log.Info("all files in sub compaction skipped")
		return true
	}
	if len(sstOutputs) != len(subCompaction.SstOutputs) {
		log.Info("partial files in sub compaction skipped")
		subCompaction.SstOutputs = sstOutputs
		return false
	}
	return false
}
