package restore

import (
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils/pipeline"
	"go.uber.org/zap"
)

func NewValidateAndMerge(filesOfTable map[int64][]*backuppb.File, splitSizeBytes, splitKeyCount uint64) pipeline.Worker[CreatedTable, TableWithRange] {
	return validateAndMerge{
		fileOfTable:    filesOfTable,
		splitSizeBytes: splitSizeBytes,
		splitKeyCount:  splitKeyCount,
	}
}

type validateAndMerge struct {
	fileOfTable    map[int64][]*backuppb.File
	splitSizeBytes uint64
	splitKeyCount  uint64
}

func (v validateAndMerge) MainLoop(ctx pipeline.Context[TableWithRange], tableStream <-chan CreatedTable) {
	// Could we have a smaller outCh size?
	outCh := make(chan TableWithRange, len(v.fileOfTable))
	defer ctx.Finish()
	defer log.Info("all range generated")

	for {
		select {
		case <-ctx.Done():
			ctx.EmitErr(ctx.Err())
			return
		case t, ok := <-tableStream:
			if !ok {
				return
			}
			files := v.fileOfTable[t.OldTable.Info.ID]
			if partitions := t.OldTable.Info.Partition; partitions != nil {
				log.Debug("table partition",
					zap.Stringer("database", t.OldTable.DB.Name),
					zap.Stringer("table", t.Table.Name),
					zap.Any("partition info", partitions),
				)
				for _, partition := range partitions.Definitions {
					files = append(files, v.fileOfTable[partition.ID]...)
				}
			}
			for _, file := range files {
				err := ValidateFileRewriteRule(file, t.RewriteRule)
				if err != nil {
					ctx.EmitErr(err)
					return
				}
			}
			// Merge small ranges to reduce split and scatter regions.
			ranges, stat, err := MergeFileRanges(
				files, v.splitSizeBytes, v.splitKeyCount)
			if err != nil {
				ctx.EmitErr(err)
				return
			}
			log.Info("merge and validate file",
				zap.Stringer("database", t.OldTable.DB.Name),
				zap.Stringer("table", t.Table.Name),
				zap.Int("Files(total)", stat.TotalFiles),
				zap.Int("File(write)", stat.TotalWriteCFFile),
				zap.Int("File(default)", stat.TotalDefaultCFFile),
				zap.Int("Region(total)", stat.TotalRegions),
				zap.Int("Regoin(keys avg)", stat.RegionKeysAvg),
				zap.Int("Region(bytes avg)", stat.RegionBytesAvg),
				zap.Int("Merged(regions)", stat.MergedRegions),
				zap.Int("Merged(keys avg)", stat.MergedRegionKeysAvg),
				zap.Int("Merged(bytes avg)", stat.MergedRegionBytesAvg))

			tableWithRange := TableWithRange{
				CreatedTable: t,
				Range:        ranges,
			}
			log.Debug("sending range info",
				zap.Stringer("table", t.Table.Name),
				zap.Int("files", len(files)),
				zap.Int("range size", len(ranges)),
				zap.Int("output channel size", len(outCh)))
			ctx.Emit(tableWithRange)
		}
	}
}
