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

// Restorer is the minimal methods required for restoring.
// It contains the primitive APIs extract from `restore.Client`, so some of arguments may seem redundant.
// Maybe TODO: make a better abstraction?
package sstfiles

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/logutil"
	snapsplit "github.com/pingcap/tidb/br/pkg/restore/internal/snap_split"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"

	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type MultiTablesRestorer struct {
	workerPool       *tidbutil.WorkerPool
	splitter         split.SplitClient
	fileImporter     *SnapFileImporter
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType]
}

func NewMultiTablesRestorer(
	fileImporter *SnapFileImporter,
	splitter split.SplitClient,
	workerPool *tidbutil.WorkerPool,
	checkpointRunner *checkpoint.CheckpointRunner[checkpoint.RestoreKeyType, checkpoint.RestoreValueType],
) FileRestorer {
	return &MultiTablesRestorer{
		workerPool:       workerPool,
		splitter:         splitter,
		fileImporter:     fileImporter,
		checkpointRunner: checkpointRunner,
	}
}

func (s *MultiTablesRestorer) Close() error {
	return s.fileImporter.Close()
}

// SplitRanges implements FileRestorer. It splits region by
// data range after rewrite.
// updateCh is used to record progress.
func (s *MultiTablesRestorer) SplitRanges(ctx context.Context, ranges []rtree.Range, onProgress func()) error {
	splitClientOpt := split.WithOnSplit(func(keys [][]byte) {
		for range keys {
			onProgress()
		}
	})
	s.splitter.ApplyOptions(splitClientOpt)
	splitter := snapsplit.NewRegionSplitter(s.splitter)
	return splitter.ExecuteSplit(ctx, nil)
}

// RestoreSSTFiles tries to restore the files.
// updateCh is used to record progress.
func (s *MultiTablesRestorer) RestoreFiles(ctx context.Context, sstFiles []SstFilesInfo, onProgress func()) (err error) {
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

	var rangeFiles []*backuppb.File
	var leftFiles []*backuppb.File
LOOPFORTABLE:
	for _, sstFile := range sstFiles {
		tableID := sstFile.TableID
		files := sstFile.Files
		rules := sstFile.RewriteRules
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
			s.fileImporter.WaitUntilUnblock()
			s.workerPool.ApplyOnErrorGroup(eg, func() (restoreErr error) {
				fileStart := time.Now()
				defer func() {
					if restoreErr == nil {
						log.Info("import files done", logutil.Files(filesReplica),
							zap.Duration("take", time.Since(fileStart)))
						onProgress()
					}
				}()
				if importErr := s.fileImporter.ImportSSTFiles(ectx, filesReplica, rules); importErr != nil {
					return errors.Trace(importErr)
				}

				// the data of this range has been import done
				if s.checkpointRunner != nil && len(filesReplica) > 0 {
					rangeKey := GetFileRangeKey(filesReplica[0].Name)
					// The checkpoint range shows this ranges of kvs has been restored into
					// the table corresponding to the table-id.
					if err := checkpoint.AppendRangesForRestore(ectx, s.checkpointRunner, tableID, rangeKey); err != nil {
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

func GetFileRangeKey(f string) string {
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
	return GetFileRangeKey(f1) == GetFileRangeKey(f2)
}
