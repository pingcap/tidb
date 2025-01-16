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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

// BackupFileSet represents the batch files to be restored for a table. Current, we have 5 type files
// 1. Raw KV(sst files)
// 2. Txn KV(sst files)
// 3. Database KV backup(sst files)
// 4. Compacted Log backups(sst files)
type BackupFileSet struct {
	// TableID only valid in 3.4.5.
	// For Raw/Txn KV, table id is always 0
	TableID int64

	// For log Backup Changes, this field is null.
	SSTFiles []*backuppb.File

	// RewriteRules is the rewrite rules for the specify table.
	// because these rules belongs to the *one table*.
	// we can hold them here.
	RewriteRules *utils.RewriteRules
}

type BatchBackupFileSet []BackupFileSet

type zapBatchBackupFileSetMarshaler BatchBackupFileSet

// MarshalLogObjectForFiles is an internal util function to zap something having `Files` field.
func MarshalLogObjectForFiles(batchFileSet BatchBackupFileSet, encoder zapcore.ObjectEncoder) error {
	return zapBatchBackupFileSetMarshaler(batchFileSet).MarshalLogObject(encoder)
}

func (fgs zapBatchBackupFileSetMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	elements := make([]string, 0)
	total := 0
	totalKVs := uint64(0)
	totalBytes := uint64(0)
	totalSize := uint64(0)
	for _, fg := range fgs {
		for _, f := range fg.SSTFiles {
			total += 1
			elements = append(elements, f.GetName())
			totalKVs += f.GetTotalKvs()
			totalBytes += f.GetTotalBytes()
			totalSize += f.GetSize_()
		}
	}
	encoder.AddInt("total", total)
	_ = encoder.AddArray("files", logutil.AbbreviatedArrayMarshaler(elements))
	encoder.AddUint64("totalKVs", totalKVs)
	encoder.AddUint64("totalBytes", totalBytes)
	encoder.AddUint64("totalSize", totalSize)
	return nil
}

func ZapBatchBackupFileSet(batchFileSet BatchBackupFileSet) zap.Field {
	return zap.Object("fileset", zapBatchBackupFileSetMarshaler(batchFileSet))
}

// CreateUniqueFileSets used for Raw/Txn non-tableID files
// converts a slice of files into a slice of unique BackupFileSets,
// where each BackupFileSet contains a single file.
func CreateUniqueFileSets(files []*backuppb.File) []BackupFileSet {
	newSet := make([]BackupFileSet, len(files))
	for i, f := range files {
		newSet[i].SSTFiles = []*backuppb.File{f}
	}
	return newSet
}

func NewFileSet(files []*backuppb.File, rules *utils.RewriteRules) BackupFileSet {
	return BackupFileSet{
		SSTFiles:     files,
		RewriteRules: rules,
	}
}

// SstRestorer defines the essential methods required for restoring SST files in various backup formats:
// 1. Raw backup SST files
// 2. Transactional (Txn) backup SST files
// 3. TiDB backup SST files
// 4. Log-compacted SST files
//
// It serves as a high-level interface for restoration, supporting implementations such as simpleRestorer
// and MultiTablesRestorer. SstRestorer includes FileImporter for handling raw, transactional, and compacted SSTs,
// and MultiTablesRestorer for TiDB-specific backups.
type SstRestorer interface {
	// GoRestore imports the specified backup file sets into TiKV asynchronously.
	// The onProgress function is called with progress updates as files are processed.
	GoRestore(onProgress func(int64), batchFileSets ...BatchBackupFileSet) error

	// WaitUntilFinish blocks until all pending restore files have completed processing.
	WaitUntilFinish() error

	// Close releases any resources associated with the restoration process.
	Close() error
}

// FileImporter is a low-level interface for handling the import of backup files into storage (e.g., TiKV).
// It is primarily used by the importer client to manage raw and transactional SST file imports.
type FileImporter interface {
	// Import uploads and imports the provided backup file sets into storage.
	// The ctx parameter provides context for managing request scope.
	Import(ctx context.Context, fileSets ...BackupFileSet) error

	// Close releases any resources used by the importer client.
	Close() error
}

// BalancedFileImporter is a wrapper around FileImporter that adds concurrency controls.
// It ensures that file imports are balanced across storage nodes, which is particularly useful
// in MultiTablesRestorer scenarios where concurrency management is critical for efficiency.
type BalancedFileImporter interface {
	FileImporter

	// PauseForBackpressure manages concurrency by controlling when imports can proceed,
	// ensuring load is distributed evenly across storage nodes.
	PauseForBackpressure()
}

type SimpleRestorer struct {
	eg               *errgroup.Group
	ectx             context.Context
	workerPool       *util.WorkerPool
	fileImporter     FileImporter
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
}

func NewSimpleSstRestorer(
	ctx context.Context,
	fileImporter FileImporter,
	workerPool *util.WorkerPool,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType],
) SstRestorer {
	eg, ectx := errgroup.WithContext(ctx)
	return &SimpleRestorer{
		eg:               eg,
		ectx:             ectx,
		workerPool:       workerPool,
		fileImporter:     fileImporter,
		checkpointRunner: checkpointRunner,
	}
}

func (s *SimpleRestorer) Close() error {
	return s.fileImporter.Close()
}

func (s *SimpleRestorer) WaitUntilFinish() error {
	return s.eg.Wait()
}

func (s *SimpleRestorer) GoRestore(onProgress func(int64), batchFileSets ...BatchBackupFileSet) error {
	for _, sets := range batchFileSets {
		for _, set := range sets {
			s.workerPool.ApplyOnErrorGroup(s.eg,
				func() (restoreErr error) {
					fileStart := time.Now()
					defer func() {
						if restoreErr == nil {
							log.Info("import sst files done", logutil.Files(set.SSTFiles),
								zap.Duration("take", time.Since(fileStart)))
							for _, f := range set.SSTFiles {
								onProgress(int64(f.TotalKvs))
							}
						}
					}()
					err := s.fileImporter.Import(s.ectx, set)
					if err != nil {
						return errors.Trace(err)
					}
					if s.checkpointRunner != nil {
						// The checkpoint shows this ranges of files has been restored into
						// the table corresponding to the table-id.
						for _, f := range set.SSTFiles {
							if err := checkpoint.AppendRangesForRestore(s.ectx, s.checkpointRunner,
								checkpoint.NewCheckpointFileItem(set.TableID, f.GetName())); err != nil {
								return errors.Trace(err)
							}
						}
					}
					return nil
				})
		}
	}
	return nil
}

type MultiTablesRestorer struct {
	eg               *errgroup.Group
	ectx             context.Context
	workerPool       *util.WorkerPool
	fileImporter     BalancedFileImporter
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
}

func NewMultiTablesRestorer(
	ctx context.Context,
	fileImporter BalancedFileImporter,
	workerPool *util.WorkerPool,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType],
) SstRestorer {
	eg, ectx := errgroup.WithContext(ctx)
	return &MultiTablesRestorer{
		eg:               eg,
		ectx:             ectx,
		workerPool:       workerPool,
		fileImporter:     fileImporter,
		checkpointRunner: checkpointRunner,
	}
}

func (m *MultiTablesRestorer) Close() error {
	return m.fileImporter.Close()
}

func (m *MultiTablesRestorer) WaitUntilFinish() error {
	if err := m.eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error("restore files failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (m *MultiTablesRestorer) GoRestore(onProgress func(int64), batchFileSets ...BatchBackupFileSet) (err error) {
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

	if span := opentracing.SpanFromContext(m.ectx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreSSTFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		m.ectx = opentracing.ContextWithSpan(m.ectx, span1)
	}

	for i, batchFileSet := range batchFileSets {
		if m.ectx.Err() != nil {
			log.Warn("Restoring encountered error and already stopped, give up remained files.",
				logutil.ShortError(m.ectx.Err()))
			// We will fetch the error from the errgroup then (If there were).
			// Also note if the parent context has been canceled or something,
			// breaking here directly is also a reasonable behavior.
			break
		}
		filesReplica := batchFileSet
		m.fileImporter.PauseForBackpressure()
		cx := logutil.ContextWithField(m.ectx, zap.Int("sn", i))
		m.workerPool.ApplyOnErrorGroup(m.eg, func() (restoreErr error) {
			fileStart := time.Now()
			defer func() {
				if restoreErr == nil {
					logutil.CL(cx).Info("import files done", zap.Duration("take", time.Since(fileStart)))
					onProgress(int64(len(filesReplica)))
				}
			}()
			if importErr := m.fileImporter.Import(cx, filesReplica...); importErr != nil {
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
						if err := checkpoint.AppendRangesForRestore(m.ectx, m.checkpointRunner,
							checkpoint.NewCheckpointRangeKeyItem(filesGroup.TableID, rangeKey)); err != nil {
							return errors.Trace(err)
						}
					}
				}
			}
			return nil
		})
	}
	// Once the parent context canceled and there is no task running in the errgroup,
	// we may break the for loop without error in the errgroup. (Will this happen?)
	// At that time, return the error in the context here.
	return m.ectx.Err()
}

// GetFileRangeKey is used to reduce the checkpoint number, because we combine the write cf/default cf into one restore file group.
// during full restore, so we can reduce the checkpoint number with the common prefix of the file.
func GetFileRangeKey(f string) string {
	// the backup date file pattern is `{store_id}_{region_id}_{epoch_version}_{key}_{ts}_{cf}.sst`
	// so we need to compare without the `_{cf}.sst` suffix
	idx := strings.LastIndex(f, "_")
	if idx < 0 {
		panic(fmt.Sprintf("invalid backup data file name: '%s'", f))
	}

	return f[:idx]
}

type PipelineRestorerWrapper[T any] struct {
	split.PipelineRegionsSplitter
}

// WithSplit processes items using a split strategy within a pipeline.
// It iterates over items, accumulating them until a split condition is met.
// When a split is required, it executes the split operation on the accumulated items.
func (p *PipelineRestorerWrapper[T]) WithSplit(ctx context.Context, i iter.TryNextor[T], strategy split.SplitStrategy[T]) iter.TryNextor[T] {
	return iter.TryMap(
		iter.FilterOut(i, func(item T) bool {
			// Skip items based on the strategy's criteria.
			// Non-skip iterms should be filter out.
			return strategy.ShouldSkip(item)
		}), func(item T) (T, error) {
			// Accumulate the item for potential splitting.
			strategy.Accumulate(item)

			// Check if the accumulated items meet the criteria for splitting.
			if strategy.ShouldSplit() {
				startTime := time.Now()

				// Execute the split operation on the accumulated items.
				accumulations := strategy.GetAccumulations()
				err := p.ExecuteRegions(ctx, accumulations)
				if err != nil {
					log.Error("Failed to split regions in pipeline; exit restore", zap.Error(err), zap.Duration("duration", time.Since(startTime)))
					return item, errors.Annotate(err, "Execute region split on accmulated files failed")
				}
				// Reset accumulations after the split operation.
				strategy.ResetAccumulations()
				log.Info("Completed region split in pipeline", zap.Duration("duration", time.Since(startTime)))
			}
			// Return the item without filtering it out.
			return item, nil
		})
}
