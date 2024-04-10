// Copyright 2023 PingCAP, Inc.
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

package duplicate

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"go.uber.org/zap"
)

type task struct {
	startKey internalKey
	endKey   internalKey
}

type worker struct {
	sorter  extsort.ExternalSorter
	taskCh  chan task
	taskWg  *sync.WaitGroup
	numDups *atomic.Int64
	handler Handler
	logger  log.Logger
}

func (w *worker) run(ctx context.Context) error {
	for {
		select {
		case t, ok := <-w.taskCh:
			if !ok {
				return nil
			}
			if err := w.runTask(ctx, t); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *worker) runTask(ctx context.Context, t task) (retErr error) {
	logTask := w.logger.With(
		zap.Stringer("startKey", t.startKey),
		zap.Stringer("initialEndKey", t.endKey),
	).Begin(zap.InfoLevel, "run task")

	var processedKeys int64

	defer func() {
		w.taskWg.Done()
		err := w.handler.Close()
		if retErr == nil {
			retErr = err
		}
		logTask.End(zap.ErrorLevel, retErr,
			zap.Stringer("endKey", t.endKey),
			zap.Int64("processedKeys", processedKeys),
		)
	}()

	it, err := w.sorter.NewIterator(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = it.Close()
	}()

	// Periodically check context cancellation or task splitting during iteration.
	const checkInterval = 1000
	numIterations := 0

	inDup := false
	var prevKey, curKey internalKey
	for it.Seek(encodeInternalKey(nil, t.startKey)); it.Valid(); it.Next() {
		if err := decodeInternalKey(it.UnsafeKey(), &curKey); err != nil {
			return err
		}
		if compareInternalKey(curKey, t.endKey) >= 0 {
			break
		}
		processedKeys++

		if bytes.Equal(curKey.key, prevKey.key) {
			if inDup {
				if err := w.handler.Append(curKey.keyID); err != nil {
					return err
				}
			} else {
				if err := w.handler.Begin(curKey.key); err != nil {
					return err
				}
				if err := w.handler.Append(prevKey.keyID); err != nil {
					return err
				}
				if err := w.handler.Append(curKey.keyID); err != nil {
					return err
				}
				inDup = true
				w.numDups.Add(1)
			}
		} else if inDup {
			if err := w.handler.End(); err != nil {
				return err
			}
			inDup = false
		}

		numIterations++
		// Try to split the task and send the new task to the task channel
		// so that other idle workers can process it.
		if numIterations%checkInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(w.taskCh) == 0 {
				userSplitKey := genSplitKey(curKey.key, t.endKey.key)
				if bytes.Compare(userSplitKey, curKey.key) > 0 {
					splitKey := internalKey{key: userSplitKey}
					w.taskWg.Add(1)
					select {
					case w.taskCh <- task{startKey: splitKey, endKey: t.endKey}:
						t.endKey = splitKey
					default:
						w.taskWg.Done()
					}
				}
			}
		}
		// prevKey and curKey are reused in the loop to avoid memory allocation.
		// In next iteration, curKey will be the prevKey and prevKey will be reused
		// as the new curKey.
		prevKey, curKey = curKey, prevKey
	}
	if err := it.Error(); err != nil {
		return err
	}
	if inDup {
		if err := w.handler.End(); err != nil {
			return err
		}
	}
	return nil
}

func genSplitKey(startKey, endKey []byte) []byte {
	if bytes.Equal(startKey, endKey) {
		return slices.Clone(startKey)
	}

	l := commonPrefixLen(startKey, endKey)
	splitKey := slices.Clone(startKey[:l])

	if l == len(startKey) {
		// The start key is a prefix of the end key.
		splitKey = append(splitKey, endKey[l]/2)
		return splitKey
	}

	c1, c2 := startKey[l], endKey[l]
	if c1+1 < c2 {
		return append(splitKey, c1+(c2-c1)/2)
	}
	splitKey = append(splitKey, c1)
	// Ensure the split key is greater than the start key.
	for _, c := range startKey[l+1:] {
		splitKey = append(splitKey, 0xff)
		if c != 0xff {
			return splitKey
		}
	}
	return append(splitKey, 0xff)
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
