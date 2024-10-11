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

package restore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/opentracing/opentracing-go"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/util"
)

// RestoreFilesInfo represents the batch files to be restored for a table. Current, we have 5 type files
// 1. Raw KV(sst files)
// 2. Txn KV(sst files)
// 3. Databse KV backup(sst files)
// 4. Log backup changes(dataFileInfo)
// 5. Compacted Log backups(sst files)
type RestoreFilesInfo struct {
	// TableID only valid in 3.4.5.
	// For Raw/Txn KV, table id is always 0
	TableID int64

	// For log Backup Changes, this field is null.
	SSTFiles []*backuppb.File

	// Only used for log Backup Changes, for other types this field is null.
	LogFiles []*backuppb.DataFileInfo

	// RewriteRules is the rewrite rules for the specify table.
	// because these rules belongs to the *one table*.
	// we can hold them here.
	RewriteRules *utils.RewriteRules
}

type BatchRestoreFilesInfo []RestoreFilesInfo

// NewEmptyRuleSSTFilesInfo is a wrapper of Raw/Txn non-tableID files.
func NewEmptyRuleSSTFilesInfos(files []*backuppb.File) []RestoreFilesInfo {
	return []RestoreFilesInfo{{
		SSTFiles: files,
	}}
}

func NewSSTFilesInfo(files []*backuppb.File, rules *utils.RewriteRules) RestoreFilesInfo {
	return RestoreFilesInfo{
		SSTFiles:     files,
		RewriteRules: rules,
	}
}

// FileRestorer is the minimal methods required for restoring sst, including
// 1. Raw backup ssts
// 2. Txn backup ssts
// 3. TiDB backup ssts
// 4. Log Compacted ssts
type FileRestorer interface {
	// Restore import the files to the TiKV.
	Restore(ctx context.Context, onProgress func(), files ...BatchRestoreFilesInfo) error

	// Close release the resources.
	Close() error
}

type FileImporter interface {
	Import(ctx context.Context, filesGroup ...RestoreFilesInfo) error

	// Close release the resources.
	Close() error
}

type ConcurrentlFileImporter interface {
	FileImporter
	// control the concurrency of importer
	// to make the restore balance on every store.
	WaitUntilUnblock()
}

type SimpleRestorer struct {
	workerPool   *util.WorkerPool
	fileImporter FileImporter
}

func NewSimpleFileRestorer(
	fileImporter FileImporter,
	workerPool *util.WorkerPool,
) FileRestorer {
	return &SimpleRestorer{
		workerPool:   workerPool,
		fileImporter: fileImporter,
	}
}

func (s *SimpleRestorer) Close() error {
	return s.fileImporter.Close()
}

func (s *SimpleRestorer) Restore(ctx context.Context, onProgress func(), batchFilesInfo ...BatchRestoreFilesInfo) error {
	errCh := make(chan error, len(batchFilesInfo))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)

	for _, info := range batchFilesInfo {
		for _, fileGroup := range info {
			s.workerPool.ApplyOnErrorGroup(eg,
				func() (restoreErr error) {
					fileStart := time.Now()
					defer func() {
						if restoreErr == nil {
							log.Info("import sst files done", logutil.Files(fileGroup.SSTFiles),
								zap.Duration("take", time.Since(fileStart)))
							onProgress()
						}
					}()
					return s.fileImporter.Import(ectx, fileGroup)
				})
		}
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type MultiTablesRestorer struct {
	workerPool       *util.WorkerPool
	fileImporter     ConcurrentlFileImporter
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
}

func NewMultiTablesRestorer(
	fileImporter ConcurrentlFileImporter,
	workerPool *util.WorkerPool,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType],
) FileRestorer {
	return &MultiTablesRestorer{
		workerPool:       workerPool,
		fileImporter:     fileImporter,
		checkpointRunner: checkpointRunner,
	}
}

func (m *MultiTablesRestorer) Close() error {
	return m.fileImporter.Close()
}

func (m *MultiTablesRestorer) Restore(ctx context.Context, onProgress func(), batchFilesInfo ...BatchRestoreFilesInfo) (err error) {
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
	for _, tableIDWithFiles := range batchFilesInfo {
		if ectx.Err() != nil {
			log.Warn("Restoring encountered error and already stopped, give up remained files.",
				logutil.ShortError(ectx.Err()))
			// We will fetch the error from the errgroup then (If there were).
			// Also note if the parent context has been canceled or something,
			// breaking here directly is also a reasonable behavior.
			break
		}
		filesReplica := tableIDWithFiles
		m.fileImporter.WaitUntilUnblock()
		m.workerPool.ApplyOnErrorGroup(eg, func() (restoreErr error) {
			// fileStart := time.Now()
			defer func() {
				if restoreErr == nil {
					// log.Info("import files done", zapFilesGroup(filesReplica), zap.Duration("take", time.Since(fileStart)))
					onProgress()
				}
			}()
			if importErr := m.fileImporter.Import(ectx, filesReplica...); importErr != nil {
				return errors.Trace(importErr)
			}

			// the data of this range has been import done
			if m.checkpointRunner != nil && len(filesReplica) > 0 {
				for _, filesGroup := range filesReplica {
					rangeKeySet := make(map[string]struct{})
					for _, file := range filesGroup.SSTFiles {
						rangeKey := GetFileRangeKey(file.Name)
						// Assert that the files having the same rangeKey are all in the current filesGroup.Files
						rangeKeySet[rangeKey] = struct{}{}
					}
					for rangeKey := range rangeKeySet {
						// The checkpoint range shows this ranges of kvs has been restored into
						// the table corresponding to the table-id.
						if err := checkpoint.AppendRangesForRestore(ectx, m.checkpointRunner, filesGroup.TableID, rangeKey); err != nil {
							return errors.Trace(err)
						}
					}
				}
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error("restore files failed", zap.Error(err))
		return errors.Trace(err)
	}
	// Once the parent context canceled and there is no task running in the errgroup,
	// we may break the for loop without error in the errgroup. (Will this happen?)
	// At that time, return the error in the context here.
	return ctx.Err()
}

func GetFileRangeKey(f string) string {
	// the backup date file pattern is `{store_id}_{region_id}_{epoch_version}_{key}_{ts}_{cf}.sst`
	// so we need to compare with out the `_{cf}.sst` suffix
	idx := strings.LastIndex(f, "_")
	if idx < 0 {
		panic(fmt.Sprintf("invalid backup data file name: '%s'", f))
	}

	return f[:idx]
}

// PipelineFileRestorer will try to do the restore and split in pipeline
// used in log backup and compacted sst backup
// because of unable to split all regions before restore these data.
// we just can restore as well as split.
type PipelineFileRestorer[T any] interface {
	// Raw/Txn Restore, full Restore
	FileRestorer
	split.MultiRegionsSplitter

	// Log Restore, Compacted Restore
	// split when Iter until condition satified
	WrapIter(iter.TryNextor[T], split.SplitStrategy[T]) iter.TryNextor[T]
}

type PipelineFileRestorerWrapper[T any] struct {
	split.RegionsSplitter
}

func (p *PipelineFileRestorerWrapper[T]) WrapIter(ctx context.Context, i iter.TryNextor[T], strategy split.SplitStrategy[T]) iter.TryNextor[T] {
	return iter.Map(i, func(item T) T {
		strategy.Accumulate(item)
		// Check if we need to split
		if strategy.ShouldSplit() { // Assuming ShouldSplit returns true when split is needed
			log.Info("start to split the regions in pipeline")
			startTime := time.Now()
			s := strategy.AccumulationsIter()
			err := p.ExecuteRegions(ctx, s)
			if err != nil {
				log.Error("failed to split regions in pipeline, anyway we can still doing restore")
			}
			log.Info("end to split the regions in pipeline", zap.Duration("takes", time.Since(startTime)))
		}
		return item
	})
}
