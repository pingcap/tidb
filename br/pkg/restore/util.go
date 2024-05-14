// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Granularity string

const (
	FineGrained   Granularity = "fine-grained"
	CoarseGrained Granularity = "coarse-grained"

	maxSplitKeysOnce = 10240
)

// GoValidateFileRanges validate files by a stream of tables and yields
// tables with range.
func GoValidateFileRanges(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	fileOfTable map[int64][]*backuppb.File,
	splitSizeBytes, splitKeyCount uint64,
	errCh chan<- error,
) <-chan TableWithRange {
	// Could we have a smaller outCh size?
	outCh := make(chan TableWithRange, len(fileOfTable))
	go func() {
		defer close(outCh)
		defer log.Info("all range generated")
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case t, ok := <-tableStream:
				if !ok {
					return
				}
				files := fileOfTable[t.OldTable.Info.ID]
				if partitions := t.OldTable.Info.Partition; partitions != nil {
					log.Debug("table partition",
						zap.Stringer("database", t.OldTable.DB.Name),
						zap.Stringer("table", t.Table.Name),
						zap.Any("partition info", partitions),
					)
					for _, partition := range partitions.Definitions {
						files = append(files, fileOfTable[partition.ID]...)
					}
				}
				for _, file := range files {
					err := restoreutils.ValidateFileRewriteRule(file, t.RewriteRule)
					if err != nil {
						errCh <- err
						return
					}
				}
				// Merge small ranges to reduce split and scatter regions.
				ranges, stat, err := restoreutils.MergeAndRewriteFileRanges(
					files, t.RewriteRule, splitSizeBytes, splitKeyCount)
				if err != nil {
					errCh <- err
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
				outCh <- tableWithRange
			}
		}
	}()
	return outCh
}

// SplitRanges splits region by
// 1. data range after rewrite.
// 2. rewrite rules.
func SplitRanges(
	ctx context.Context,
	client *Client,
	ranges []rtree.Range,
	updateCh glue.Progress,
	isRawKv bool,
) error {
	splitClientOpts := make([]split.ClientOptionalParameter, 0, 2)
	splitClientOpts = append(splitClientOpts, split.WithOnSplit(func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	}))
	if isRawKv {
		splitClientOpts = append(splitClientOpts, split.WithRawKV())
	}

	splitter := restoreutils.NewRegionSplitter(split.NewClient(
		client.GetPDClient(),
		client.pdHTTPClient,
		client.GetTLSConfig(),
		maxSplitKeysOnce,
		client.GetStoreCount()+1,
		splitClientOpts...,
	))

	return splitter.ExecuteSplit(ctx, ranges)
}

// ZapTables make zap field of table for debuging, including table names.
func ZapTables(tables []CreatedTable) zapcore.Field {
	return logutil.AbbreviatedArray("tables", tables, func(input any) []string {
		tables := input.([]CreatedTable)
		names := make([]string, 0, len(tables))
		for _, t := range tables {
			names = append(names, fmt.Sprintf("%s.%s",
				utils.EncloseName(t.OldTable.DB.Name.String()),
				utils.EncloseName(t.OldTable.Info.Name.String())))
		}
		return names
	})
}
