// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type LogSplitStrategy struct {
	*split.BaseSplitStrategy
	checkpointSkipMap        *LogFilesSkipMap
	checkpointFileProgressFn func(uint64, uint64)
}

var _ split.SplitStrategy[*LogDataFileInfo] = &LogSplitStrategy{}

func NewLogSplitStrategy(
	ctx context.Context,
	useCheckpoint bool,
	execCtx sqlexec.RestrictedSQLExecutor,
	rules map[int64]*restoreutils.RewriteRules,
	updateStatsFn func(uint64, uint64),
) (*LogSplitStrategy, error) {
	downstreamIdset := make(map[int64]struct{})
	for _, rule := range rules {
		downstreamIdset[rule.NewTableID] = struct{}{}
	}
	skipMap := NewLogFilesSkipMap()
	if useCheckpoint {
		t, err := checkpoint.LoadCheckpointDataForLogRestore(
			ctx, execCtx, func(groupKey checkpoint.LogRestoreKeyType, off checkpoint.LogRestoreValueMarshaled) {
				for tableID, foffs := range off.Foffs {
					// filter out the checkpoint data of dropped table
					if _, exists := downstreamIdset[tableID]; exists {
						for _, foff := range foffs {
							skipMap.Insert(groupKey, off.Goff, foff)
						}
					}
				}
			})

		if err != nil {
			return nil, errors.Trace(err)
		}
		summary.AdjustStartTimeToEarlierTime(t)
	}
	return &LogSplitStrategy{
		BaseSplitStrategy:        split.NewBaseSplitStrategy(rules),
		checkpointSkipMap:        skipMap,
		checkpointFileProgressFn: updateStatsFn,
	}, nil
}

const splitFileThreshold = 1024 * 1024 // 1 MB

func (ls *LogSplitStrategy) Accumulate(file *LogDataFileInfo) {
	if file.Length > splitFileThreshold {
		ls.AccumulateCount += 1
	}
	splitHelper, exist := ls.TableSplitter[file.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		ls.TableSplitter[file.TableId] = splitHelper
	}

	splitHelper.Merge(split.Valued{
		Key: split.Span{
			StartKey: file.StartKey,
			EndKey:   file.EndKey,
		},
		Value: split.Value{
			Size:   file.Length,
			Number: file.NumberOfEntries,
		},
	})
}

func (ls *LogSplitStrategy) ShouldSplit() bool {
	return ls.AccumulateCount > 4096
}

func (ls *LogSplitStrategy) ShouldSkip(file *LogDataFileInfo) bool {
	if file.IsMeta {
		return true
	}
	_, exist := ls.Rules[file.TableId]
	if !exist {
		log.Info("skip for no rule files", zap.Int64("tableID", file.TableId))
		return true
	}

	if ls.checkpointSkipMap.NeedSkip(file.MetaDataGroupName, file.OffsetInMetaGroup, file.OffsetInMergedGroup) {
		//onPcheckpointSkipMaprogress()
		ls.checkpointFileProgressFn(uint64(file.NumberOfEntries), file.Length)
		return true
	}
	return false
}
