// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package logclient

import (
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
)

type LogSplitStrategy struct {
	*split.BaseSplitStrategy
}

var _ split.SplitStrategy[*LogDataFileInfo] = &LogSplitStrategy{}

func NewLogSplitStrategy(rules map[int64]*restoreutils.RewriteRules) LogSplitStrategy {
	return LogSplitStrategy{
		BaseSplitStrategy: split.NewBaseSplitStrategy(rules),
	}
}

const splitFileThreshold = 1024 * 1024 // 1 MB

func (l LogSplitStrategy) skipFile(file *LogDataFileInfo) bool {
	_, exist := l.Rules[file.TableId]
	return file.Length < splitFileThreshold || file.IsMeta || !exist
}

func (l LogSplitStrategy) Accumulate(file *LogDataFileInfo) {
	if l.skipFile(file) {
		return
	}
	splitHelper, exist := l.TableSplitter[file.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		l.TableSplitter[file.TableId] = splitHelper
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

func (l LogSplitStrategy) ShouldSplit() bool {
	return len(l.TableSplitter) > 4096
}
