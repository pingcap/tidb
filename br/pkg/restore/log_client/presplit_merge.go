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
	"maps"
	"runtime"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
)

const (
	preSplitMergeMaxWorkers = 16
	preSplitMergeBatchSize  = 4096
	preSplitMergeQueueDepth = 4
)

type preSplitFile struct {
	tableID int64
	valued  split.Valued
}

func preSplitMergeWorkers() int {
	return max(1, min(preSplitMergeMaxWorkers, runtime.GOMAXPROCS(0)))
}

// mergeDMLFilesByTable merges every DML file of a table with a rewrite rule
// into that table's split helper. Each table is owned by one worker and its
// files reach that worker in iteration order, so the helpers are the same as
// merging every file on one goroutine.
func mergeDMLFilesByTable(
	ctx context.Context,
	logIter LogIter,
	rules map[int64]*restoreutils.RewriteRules,
	workers int,
) (map[int64]*split.SplitHelper, int, error) {
	workers = max(1, workers)
	queues := make([]chan []preSplitFile, workers)
	helpers := make([]map[int64]*split.SplitHelper, workers)
	var wg sync.WaitGroup
	for i := range workers {
		queues[i] = make(chan []preSplitFile, preSplitMergeQueueDepth)
		helpers[i] = make(map[int64]*split.SplitHelper)
		wg.Add(1)
		go func(queue <-chan []preSplitFile, owned map[int64]*split.SplitHelper) {
			defer wg.Done()
			for batch := range queue {
				if ctx.Err() != nil {
					continue
				}
				for _, f := range batch {
					helper, ok := owned[f.tableID]
					if !ok {
						helper = split.NewSplitHelper()
						owned[f.tableID] = helper
					}
					helper.Merge(f.valued)
				}
			}
		}(queues[i], helpers[i])
	}

	pending := make([][]preSplitFile, workers)
	flush := func(i int) error {
		if len(pending[i]) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case queues[i] <- pending[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
		pending[i] = make([]preSplitFile, 0, preSplitMergeBatchSize)
		return nil
	}

	var fileCount int
	var iterErr error
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		if r.Err != nil {
			iterErr = r.Err
			break
		}
		file := r.Item
		if file.IsMeta {
			continue
		}
		if _, exist := rules[file.TableId]; !exist {
			continue
		}
		i := int(uint64(file.TableId) % uint64(workers))
		pending[i] = append(pending[i], preSplitFile{
			tableID: file.TableId,
			valued: split.Valued{
				Key: split.Span{
					StartKey: file.StartKey,
					EndKey:   file.EndKey,
				},
				Value: split.Value{
					Size:   file.Length,
					Number: file.NumberOfEntries,
				},
			},
		})
		fileCount++
		if len(pending[i]) >= preSplitMergeBatchSize {
			if iterErr = flush(i); iterErr != nil {
				break
			}
		}
	}
	for i := 0; iterErr == nil && i < workers; i++ {
		iterErr = flush(i)
	}
	failpoint.InjectCall("pre-split-merge-before-wait")
	for _, queue := range queues {
		close(queue)
	}
	wg.Wait()
	if iterErr == nil {
		iterErr = ctx.Err()
	}
	if iterErr != nil {
		return nil, 0, iterErr
	}

	merged := make(map[int64]*split.SplitHelper)
	for _, owned := range helpers {
		maps.Copy(merged, owned)
	}
	return merged, fileCount, nil
}
