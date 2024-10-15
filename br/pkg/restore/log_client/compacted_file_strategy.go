// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
)

type CompactedFileSplitStrategy struct {
	*split.BaseSplitStrategy
	checkpointSets map[string]struct{}
}

var _ split.SplitStrategy[*backuppb.LogFileSubcompaction] = &CompactedFileSplitStrategy{}

func NewCompactedFileSplitStrategy(rules map[int64]*restoreutils.RewriteRules, checkpointsSet map[string]struct{}) CompactedFileSplitStrategy {
	return CompactedFileSplitStrategy{
		BaseSplitStrategy: split.NewBaseSplitStrategy(rules),
		checkpointSets:    checkpointsSet,
	}
}

func (l CompactedFileSplitStrategy) Accumulate(file *backuppb.LogFileSubcompaction) {
	sstOutputs := make([]*backuppb.File, 0, len(file.SstOutputs))
	for _, sst := range file.SstOutputs {
		if _, ok := l.checkpointSets[sst.Name]; !ok {
			sstOutputs = append(sstOutputs, sst)
		}
	}
	if len(sstOutputs) == 0 {
		return
	}

	splitHelper, exist := l.TableSplitter[file.Meta.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		l.TableSplitter[file.Meta.TableId] = splitHelper
	}

	for _, f := range sstOutputs {
		splitHelper.Merge(split.Valued{
			Key: split.Span{
				StartKey: f.StartKey,
				EndKey:   f.EndKey,
			},
			Value: split.Value{
				Size:   f.Size_,
				Number: int64(f.TotalKvs),
			},
		})
	}
}

func (l CompactedFileSplitStrategy) ShouldSplit() bool {
	return len(l.TableSplitter) > 128
}
