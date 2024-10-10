package logclient

import (
	"bytes"
	"sort"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

type CompactedFileSplitStrategy struct {
	tableSplitter map[int64]*split.SplitHelper
	rules         map[int64]*restoreutils.RewriteRules
}

var _ split.SplitStrategy[*backuppb.LogFileSubcompaction] = &CompactedFileSplitStrategy{}

func NewCompactedFileSplitStrategy(rules map[int64]*restoreutils.RewriteRules) CompactedFileSplitStrategy {
	return CompactedFileSplitStrategy{rules: rules}
}

func (l CompactedFileSplitStrategy) Accumulate(file *backuppb.LogFileSubcompaction) {
	splitHelper, exist := l.tableSplitter[file.Meta.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		l.tableSplitter[file.Meta.TableId] = splitHelper
	}

	for _, f := range file.SstOutputs {
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
	return len(l.tableSplitter) > 128
}

// TODO refine the output
func (l CompactedFileSplitStrategy) AccumlationsIter() *split.SplitHelperIterator {
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
