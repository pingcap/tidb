// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapclient

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	snapsplit "github.com/pingcap/tidb/br/pkg/restore/internal/snap_split"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// mapTableToFiles makes a map that mapping table ID to its backup files.
// aware that one file can and only can hold one table.
func mapTableToFiles(files []*backuppb.File) (map[int64][]*backuppb.File, int) {
	result := map[int64][]*backuppb.File{}
	// count the write cf file that hint for split key slice size
	maxSplitKeyCount := 0
	for _, file := range files {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		tableEndID := tablecodec.DecodeTableID(file.GetEndKey())
		if tableID != tableEndID {
			log.Panic("key range spread between many files.",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		if tableID == 0 {
			log.Panic("invalid table key of file",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		result[tableID] = append(result[tableID], file)
		if file.Cf == restoreutils.WriteCFName {
			maxSplitKeyCount += 1
		}
	}
	return result, maxSplitKeyCount
}

// SortAndValidateFileRanges sort, merge and validate files by tables and yields tables with range.
func SortAndValidateFileRanges(
	createdTables []*CreatedTable,
	allFiles []*backuppb.File,
	splitSizeBytes, splitKeyCount uint64,
) ([][]byte, []TableWithRange, error) {
	// sort the created table by downstream stream table id
	sort.Slice(createdTables, func(a, b int) bool {
		return createdTables[a].Table.ID < createdTables[b].Table.ID
	})
	// mapping table ID to its backup files
	fileOfTable, hintSplitKeyCount := mapTableToFiles(allFiles)
	// sort, merge, and validate files in each tables, and generate split keys by the way
	var (
		// to generate region split keys, merge the small ranges over the adjacent tables
		sortedSplitKeys = make([][]byte, 0, hintSplitKeyCount)

		tableWithRanges = make([]TableWithRange, 0, len(createdTables))
	)

	log.Info("start to merge ranges", zap.Uint64("kv size threshold", splitSizeBytes), zap.Uint64("kv count threshold", splitKeyCount))
	for _, table := range createdTables {
		files := fileOfTable[table.OldTable.Info.ID]
		if partitions := table.OldTable.Info.Partition; partitions != nil {
			for _, partition := range partitions.Definitions {
				files = append(files, fileOfTable[partition.ID]...)
			}
		}
		for _, file := range files {
			if err := restoreutils.ValidateFileRewriteRule(file, table.RewriteRule); err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
		// Merge small ranges to reduce split and scatter regions.
		// Notice that the files having the same start key and end key are in the same range.
		sortedRanges, stat, err := restoreutils.MergeAndRewriteFileRanges(
			files, table.RewriteRule, splitSizeBytes, splitKeyCount)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		log.Info("merge and validate file",
			zap.Stringer("database", table.OldTable.DB.Name),
			zap.Stringer("table", table.Table.Name),
			zap.Int("Files(total)", stat.TotalFiles),
			zap.Int("File(write)", stat.TotalWriteCFFile),
			zap.Int("File(default)", stat.TotalDefaultCFFile),
			zap.Int("Region(total)", stat.TotalRegions),
			zap.Int("Regoin(keys avg)", stat.RegionKeysAvg),
			zap.Int("Region(bytes avg)", stat.RegionBytesAvg),
			zap.Int("Merged(regions)", stat.MergedRegions),
			zap.Int("Merged(keys avg)", stat.MergedRegionKeysAvg),
			zap.Int("Merged(bytes avg)", stat.MergedRegionBytesAvg))

		for _, rg := range sortedRanges {
			sortedSplitKeys = append(sortedSplitKeys, rg.EndKey)
		}

		tableWithRanges = append(tableWithRanges, TableWithRange{
			CreatedTable: *table,
			Range:        sortedRanges,
		})
	}
	return sortedSplitKeys, tableWithRanges, nil
}

func (rc *SnapClient) RestoreTables(
	ctx context.Context,
	placementRuleManager PlacementRuleManager,
	createdTables []*CreatedTable,
	allFiles []*backuppb.File,
	checkpointSetWithTableID map[int64]map[string]struct{},
	splitSizeBytes, splitKeyCount uint64,
	updateCh glue.Progress,
) error {
	if err := placementRuleManager.SetPlacementRule(ctx, createdTables); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err := placementRuleManager.ResetPlacementRules(ctx)
		if err != nil {
			log.Warn("failed to reset placement rules", zap.Error(err))
		}
	}()

	start := time.Now()
	sortedSplitKeys, tableWithRanges, err := SortAndValidateFileRanges(createdTables, allFiles, splitSizeBytes, splitKeyCount)
	if err != nil {
		return errors.Trace(err)
	}
	drainResult := drainRanges(tableWithRanges, checkpointSetWithTableID, updateCh)
	log.Info("Merge ranges", zap.Duration("take", time.Since(start)))

	start = time.Now()
	if err = rc.SplitPoints(ctx, sortedSplitKeys, updateCh, false); err != nil {
		return errors.Trace(err)
	}
	log.Info("Split regions", zap.Duration("take", time.Since(start)))

	start = time.Now()
	if err = rc.RestoreSSTFiles(ctx, drainResult.Files(), updateCh); err != nil {
		return errors.Trace(err)
	}
	elapsed := time.Since(start)
	log.Info("Retore files", zap.Duration("take", elapsed))

	summary.CollectSuccessUnit("files", len(allFiles), elapsed)
	return nil
}

// SplitRanges implements TiKVRestorer. It splits region by
// data range after rewrite.
func (rc *SnapClient) SplitPoints(
	ctx context.Context,
	sortedSplitKeys [][]byte,
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

	splitter := snapsplit.NewRegionSplitter(split.NewClient(
		rc.pdClient,
		rc.pdHTTPClient,
		rc.tlsConf,
		maxSplitKeysOnce,
		rc.storeCount+1,
		splitClientOpts...,
	))

	return splitter.ExecuteSplit(ctx, sortedSplitKeys)
}

func getFileRangeKey(f string) string {
	// the backup date file pattern is `{store_id}_{region_id}_{epoch_version}_{key}_{ts}_{cf}.sst`
	// so we need to compare with out the `_{cf}.sst` suffix
	idx := strings.LastIndex(f, "_")
	if idx < 0 {
		panic(fmt.Sprintf("invalid backup data file name: '%s'", f))
	}

	return f[:idx]
}

// isFilesBelongToSameRange check whether two files are belong to the same range with different cf.
func isFilesBelongToSameRange(f1, f2 string) bool {
	return getFileRangeKey(f1) == getFileRangeKey(f2)
}

func drainFilesByRange(files []*backuppb.File) ([]*backuppb.File, []*backuppb.File) {
	if len(files) == 0 {
		return nil, nil
	}
	idx := 1
	for idx < len(files) {
		if !isFilesBelongToSameRange(files[idx-1].Name, files[idx].Name) {
			break
		}
		idx++
	}

	return files[:idx], files[idx:]
}

// RestoreSSTFiles tries to restore the files.
func (rc *SnapClient) RestoreSSTFiles(
	ctx context.Context,
	tableIDWithFiles []TableIDWithFiles,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	fileCount := 0
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore files", zap.Duration("take", elapsed))
			summary.CollectSuccessUnit("files", fileCount, elapsed)
		}
	}()

	log.Debug("start to restore files", zap.Int("files", fileCount))

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreSSTFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	eg, ectx := errgroup.WithContext(ctx)
	err = rc.setSpeedLimit(ctx, rc.rateLimit)
	if err != nil {
		return errors.Trace(err)
	}

	var rangeFiles []*backuppb.File
	var leftFiles []*backuppb.File
LOOPFORTABLE:
	for _, tableIDWithFile := range tableIDWithFiles {
		tableID := tableIDWithFile.TableID
		files := tableIDWithFile.Files
		rules := tableIDWithFile.RewriteRules
		fileCount += len(files)
		for rangeFiles, leftFiles = drainFilesByRange(files); len(rangeFiles) != 0; rangeFiles, leftFiles = drainFilesByRange(leftFiles) {
			if ectx.Err() != nil {
				log.Warn("Restoring encountered error and already stopped, give up remained files.",
					zap.Int("remained", len(leftFiles)),
					logutil.ShortError(ectx.Err()))
				// We will fetch the error from the errgroup then (If there were).
				// Also note if the parent context has been canceled or something,
				// breaking here directly is also a reasonable behavior.
				break LOOPFORTABLE
			}
			filesReplica := rangeFiles
			rc.fileImporter.WaitUntilUnblock()
			rc.workerPool.ApplyOnErrorGroup(eg, func() (restoreErr error) {
				fileStart := time.Now()
				defer func() {
					if restoreErr == nil {
						log.Info("import files done", logutil.Files(filesReplica),
							zap.Duration("take", time.Since(fileStart)))
						updateCh.Inc()
					}
				}()
				if importErr := rc.fileImporter.ImportSSTFiles(ectx, filesReplica, rules, rc.cipher, rc.dom.Store().GetCodec().GetAPIVersion()); importErr != nil {
					return errors.Trace(importErr)
				}

				// the data of this range has been import done
				if rc.checkpointRunner != nil && len(filesReplica) > 0 {
					rangeKey := getFileRangeKey(filesReplica[0].Name)
					// The checkpoint range shows this ranges of kvs has been restored into
					// the table corresponding to the table-id.
					if err := checkpoint.AppendRangesForRestore(ectx, rc.checkpointRunner, tableID, rangeKey); err != nil {
						return errors.Trace(err)
					}
				}
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error(
			"restore files failed",
			zap.Error(err),
		)
		return errors.Trace(err)
	}
	// Once the parent context canceled and there is no task running in the errgroup,
	// we may break the for loop without error in the errgroup. (Will this happen?)
	// At that time, return the error in the context here.
	return ctx.Err()
}
