package logclient

import (
	"bytes"
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

type LogSplitStrategy struct {
	tableSplitter map[int64]*split.SplitHelper
	rules         map[int64]*restoreutils.RewriteRules
}

var _ split.SplitStrategy[*LogDataFileInfo] = &LogSplitStrategy{}

func NewLogSplitStrategy(rules map[int64]*restoreutils.RewriteRules) LogSplitStrategy {
	return LogSplitStrategy{rules: rules}
}

const splitFileThreshold = 1024 * 1024 // 1 MB

func (l LogSplitStrategy) skipFile(file *LogDataFileInfo) bool {
	_, exist := l.rules[file.TableId]
	return file.Length < splitFileThreshold || file.IsMeta || !exist
}

func (l LogSplitStrategy) Accumulate(file *LogDataFileInfo) {
	if l.skipFile(file) {
		return
	}
	splitHelper, exist := l.tableSplitter[file.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		l.tableSplitter[file.TableId] = splitHelper
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
	return len(l.tableSplitter) > 4096
}

// TODO refine the output
func (l LogSplitStrategy) AccumlationsIter() *split.SplitHelperIterator {
	tableSplitters := make([]*split.RewriteSplitter, 0, len(l.tableSplitter))
	for tableID, splitter := range l.tableSplitter {
		delete(l.tableSplitter, tableID)
		rewriteRule, exists := l.rules[tableID]
		if !exists {
			log.Info("skip splitting due to no table id matched", zap.Int64("tableID", tableID))
			continue
		}
		newTableID := restoreutils.GetRewriteTableID(tableID, rewriteRule)
		if newTableID == 0 {
			log.Warn("failed to get the rewrite table id", zap.Int64("tableID", tableID))
			continue
		}
		tableSplitters = append(tableSplitters, split.NewRewriteSpliter(
			codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(newTableID)),
			newTableID,
			rewriteRule,
			splitter,
		))
	}
	sort.Slice(tableSplitters, func(i, j int) bool {
		return bytes.Compare(tableSplitters[i].RewriteKey, tableSplitters[j].RewriteKey) < 0
	})
	return split.NewSplitHelperIterator(tableSplitters)
}
