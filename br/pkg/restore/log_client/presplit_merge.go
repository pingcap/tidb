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
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
)

const (
	preSplitMergeMaxWorkers = 16
	preSplitMergeBatchSize  = 1024
	// preSplitMergeMaxBatches bounds the batches handed to the workers and
	// preSplitMergeMaxPartial the spans still collected per table, so a slow
	// table holds the iteration back instead of buffering the metadata.
	preSplitMergeMaxBatches = 64
	preSplitMergeMaxPartial = 1 << 16
	preSplitMergeCheckEvery = 1024
)

func preSplitMergeWorkers() int {
	return max(1, min(preSplitMergeMaxWorkers, runtime.GOMAXPROCS(0)))
}

// preSplitTable is one table's split helper and the batches waiting for it.
// While scheduled is set the table is in the ready queue or held by a worker,
// so at most one worker merges into the helper at a time.
type preSplitTable struct {
	mu        sync.Mutex
	batches   [][]split.Valued
	scheduled bool
	helper    *split.SplitHelper
}

// mergeDMLFilesByTable merges every DML file of a table with a rewrite rule
// into that table's split helper. Any idle worker takes any table with queued
// batches, one worker per table at a time, and a table's batches are merged in
// iteration order, so the helpers are the same as merging every file on one
// goroutine.
func mergeDMLFilesByTable(
	ctx context.Context,
	logIter LogIter,
	rules map[int64]*restoreutils.RewriteRules,
	workers int,
) (map[int64]*split.SplitHelper, int, error) {
	workers = max(1, workers)
	// workerCtx is also canceled when the iteration fails, so the workers drop
	// the batches already queued.
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tables := make(map[int64]*preSplitTable)
	// A table is sent to ready only when it holds a batch, and every queued
	// batch holds a token, so ready never has more entries than tokens.
	tokens := make(chan struct{}, preSplitMergeMaxBatches)
	ready := make(chan *preSplitTable, preSplitMergeMaxBatches)
	var scheduled, wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var batches [][]split.Valued
			for t := range ready {
				for {
					t.mu.Lock()
					batches, t.batches = t.batches, batches[:0]
					if len(batches) == 0 {
						t.scheduled = false
						t.mu.Unlock()
						scheduled.Done()
						break
					}
					t.mu.Unlock()
					for i, batch := range batches {
						if workerCtx.Err() == nil {
							for _, v := range batch {
								t.helper.Merge(v)
							}
						}
						batches[i] = nil
						<-tokens
					}
				}
			}
		}()
	}

	submit := func(tableID int64, batch []split.Valued) error {
		select {
		case tokens <- struct{}{}:
		case <-workerCtx.Done():
			return workerCtx.Err()
		}
		t := tables[tableID]
		t.mu.Lock()
		t.batches = append(t.batches, batch)
		wake := !t.scheduled
		t.scheduled = true
		t.mu.Unlock()
		if wake {
			scheduled.Add(1)
			ready <- t
		}
		return nil
	}

	partial := make(map[int64][]split.Valued)
	partialSpans := 0
	flushPartial := func() error {
		for tableID, batch := range partial {
			delete(partial, tableID)
			partialSpans -= len(batch)
			if err := submit(tableID, batch); err != nil {
				return err
			}
		}
		return nil
	}

	var fileCount, scanned int
	var iterErr error
	for r := logIter.TryNext(ctx); !r.Finished; r = logIter.TryNext(ctx) {
		if r.Err != nil {
			iterErr = r.Err
			break
		}
		scanned++
		if scanned%preSplitMergeCheckEvery == 0 {
			if iterErr = ctx.Err(); iterErr != nil {
				break
			}
		}
		file := r.Item
		if file.IsMeta {
			continue
		}
		if _, exist := rules[file.TableId]; !exist {
			continue
		}
		if _, ok := tables[file.TableId]; !ok {
			tables[file.TableId] = &preSplitTable{helper: split.NewSplitHelper()}
		}
		batch := append(partial[file.TableId], split.Valued{
			Key: split.Span{
				StartKey: file.StartKey,
				EndKey:   file.EndKey,
			},
			Value: split.Value{
				Size:   file.Length,
				Number: file.NumberOfEntries,
			},
		})
		fileCount++
		partialSpans++
		if len(batch) < preSplitMergeBatchSize {
			partial[file.TableId] = batch
			if partialSpans >= preSplitMergeMaxPartial {
				if iterErr = flushPartial(); iterErr != nil {
					break
				}
			}
			continue
		}
		delete(partial, file.TableId)
		partialSpans -= len(batch)
		if iterErr = submit(file.TableId, batch); iterErr != nil {
			break
		}
	}
	if iterErr == nil {
		iterErr = flushPartial()
	}
	if iterErr != nil {
		cancel()
	}
	failpoint.InjectCall("pre-split-merge-before-wait")
	scheduled.Wait()
	close(ready)
	wg.Wait()
	if iterErr == nil {
		iterErr = ctx.Err()
	}
	if iterErr != nil {
		return nil, 0, iterErr
	}

	merged := make(map[int64]*split.SplitHelper, len(tables))
	for tableID, t := range tables {
		merged[tableID] = t.helper
	}
	return merged, fileCount, nil
}
