// Copyright 2026 PingCAP, Inc.
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

package logclient

import (
	"context"
	"runtime"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"go.uber.org/zap"
)

// preSplitMergeMaxWorkers caps the merge goroutines; with dynamic per-table
// scheduling more workers only help while there are more busy tables than
// workers, and the largest table bounds the speedup anyway.
const preSplitMergeMaxWorkers = 16

// preSplitMergeWorkers is the number of goroutines merging pre-split files.
func preSplitMergeWorkers() int {
	return max(1, min(preSplitMergeMaxWorkers, runtime.GOMAXPROCS(0)))
}

// mergeDMLFilesByTable merges every DML file of a table that has a rewrite rule
// into that table's SplitHelper. Merging is CPU bound, so tables are merged
// concurrently by `workers` goroutines while this goroutine keeps consuming the
// iterator. Each table sees its files in iteration order, so the result is the
// same as merging every file on one goroutine.
func mergeDMLFilesByTable(
	ctx context.Context,
	logIter LogIter,
	rules map[int64]*restoreutils.RewriteRules,
	workers int,
) (map[int64]*split.SplitHelper, int, error) {
	acc := split.NewTableSplitAccumulator(ctx, workers)
	var fileCount, scanned int
	startTime := time.Now()
	lastLog := startTime
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		if r.Err != nil {
			acc.Abort()
			return nil, 0, errors.Annotate(r.Err, "iterate DML files")
		}
		scanned++
		if scanned&0x3ff == 0 {
			// Add only blocks (and sees a cancel) when the workers fall behind,
			// and skipped files never reach it.
			if err := ctx.Err(); err != nil {
				acc.Abort()
				return nil, 0, errors.Trace(err)
			}
			if time.Since(lastLog) >= 30*time.Second {
				log.Info("pre-split: merging files",
					zap.Int("file-count", fileCount),
					zap.Int("scanned", scanned),
					zap.Duration("elapsed", time.Since(startTime)))
				lastLog = time.Now()
			}
		}
		file := r.Item
		if file.IsMeta {
			continue
		}
		if _, exist := rules[file.TableId]; !exist {
			continue
		}
		if err := acc.Add(file.TableId, split.Valued{
			Key: split.Span{
				StartKey: file.StartKey,
				EndKey:   file.EndKey,
			},
			Value: split.Value{
				Size:   file.Length,
				Number: file.NumberOfEntries,
			},
		}); err != nil {
			acc.Abort()
			return nil, 0, errors.Annotate(err, "merge DML files")
		}
		fileCount++
	}
	failpoint.InjectCall("pre-split-merge-before-wait")
	helpers, err := acc.Wait()
	if err != nil {
		return nil, 0, errors.Annotate(err, "merge DML files")
	}
	return helpers, fileCount, nil
}
