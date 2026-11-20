// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pingcap/tidb/pkg/tablecodec"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
)

func transitionIndexRetry(ctx context.Context, c *txnkv.Client, p splitPlan, global bool, from, to string) error {
	_, err := retryWriteConflict(ctx, func() error { return transitionIndex(ctx, c, p, global, from, to) })
	return err
}

// These seed handles are disjoint from the controlled move/rollback fixtures.
var indexStressHandles = [...]int64{5, 6, 8, 9, 10, 11, 12, 13}

type indexWriterResult struct {
	count int
	err   error
}
type indexWriter struct {
	cancel context.CancelFunc
	done   <-chan indexWriterResult
	once   sync.Once
	result indexWriterResult
}

func (w *indexWriter) wait() (int, error) {
	w.once.Do(func() { w.result = <-w.done })
	return w.result.count, w.result.err
}

// startIndexWriter runs bounded foreground DML. The caller waits for completion
// on success, or cancels and joins the goroutine on failure.
func startIndexWriter(ctx context.Context, c *txnkv.Client, p splitPlan, iterations int) (*indexWriter, error) {
	ctx, cancel := context.WithCancel(ctx)
	ready := make(chan error, 1)
	done := make(chan indexWriterResult, 1)
	writer := &indexWriter{cancel: cancel, done: done}
	go func() {
		for i := range iterations {
			h := indexStressHandles[i%len(indexStressHandles)]
			next := row{Route: int64(25 + 50*((i/len(indexStressHandles))%2)), Value: int64(10000 + i), Identity: h}
			err := mutateWithRetry(ctx, c, p, 1000, h, &next)
			if i == 0 {
				ready <- err
			}
			if err != nil {
				done <- indexWriterResult{i, err}
				return
			}
		}
		done <- indexWriterResult{iterations, nil}
	}()
	if err := <-ready; err != nil {
		cancel()
		_, _ = writer.wait()
		return nil, err
	}
	return writer, nil
}

func buildOnlineIndexGroup(ctx context.Context, im *importer, p splitPlan, global, exercise bool, out *report) (retErr error) {
	kind := "local"
	step := 0
	if global {
		kind = "global"
		step = 1
	}
	h := int64(200000 + p.Generation*1000 + step*100)
	identity := int64(1000000 + p.Generation*1000 + step*100)
	a := row{Route: 10, Value: 42, Identity: identity}
	cRow := row{Route: 110, Value: 42, Identity: identity + 1}
	d := row{Route: 11, Value: 43, Identity: identity + 2}
	if exercise {
		for _, m := range []struct {
			handle int64
			r      row
		}{{h, a}, {h, cRow}, {h + 1, d}} {
			if err := mutateWithRetry(ctx, im.client, p, 0, m.handle, &m.r); err != nil {
				return err
			}
		}
	}

	if exercise {
		stale, err := im.client.Begin()
		if err != nil {
			return err
		}
		changed := a
		changed.Value = 100
		if err = applyMutation(ctx, stale, p, 1000, h, &changed, true); err != nil {
			_ = stale.Rollback()
			return err
		}
		if err = transitionIndexRetry(ctx, im.client, p, global, indexInactive, indexRunning); err != nil {
			_ = stale.Rollback()
			return err
		}
		if err = stale.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
			return fmt.Errorf("uncaptured DML crossed activation barrier: %v", err)
		}
		out.PhaseConflicts++
	} else if err := transitionIndexRetry(ctx, im.client, p, global, indexInactive, indexRunning); err != nil {
		return err
	}

	ts, err := im.client.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	rows, err := records(ctx, im.client, ts, p.Targets)
	if err != nil {
		return err
	}
	var input []pair
	for _, v := range rows {
		pid, handle, err := tablecodec.DecodeRecordKey(v.Key)
		if err != nil {
			return err
		}
		r, err := decodeRow(v.Value)
		if err != nil {
			return err
		}
		for _, idx := range indexes(p, global) {
			item, err := indexPair(p, pid, handle.IntValue(), r, idx)
			if err != nil {
				return err
			}
			input = append(input, item)
		}
	}
	var writer *indexWriter
	if exercise {
		// A -> B -> deleted B -> E reuses one unique key with several handles.
		b := row{Route: 12, Value: 44, Identity: identity}
		e := row{Route: 112, Value: 45, Identity: identity}
		moved := cRow
		moved.Route = 20
		d.Null = true
		d.Route = 75
		for _, m := range []struct {
			pid, handle int64
			next        *row
		}{
			{1000, h, nil}, {0, h + 2, &b}, {1000, h + 2, nil}, {0, h + 3, &e},
			{1001, h, &moved}, {1000, h + 1, &d},
		} {
			if err := mutateWithRetry(ctx, im.client, p, m.pid, m.handle, m.next); err != nil {
				return err
			}
		}
		// Force a duplicate violation against the canonical constraint. Incomplete
		// shadow uniqueness must never be used as the business authority.
		duplicate := row{Route: 15, Value: 46, Identity: identity}
		if err := mutate(ctx, im.client, p, 0, h+4, &duplicate, true); !errors.Is(err, errDuplicateIdentity) {
			return fmt.Errorf("canonical duplicate rejection: %v", err)
		}
		if err = verifyOnlineRollback(ctx, im.client, p, 1001, h+3, e); err != nil {
			return err
		}
		writer, err = startIndexWriter(ctx, im.client, p, im.mutations)
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				writer.cancel()
			}
			count, err := writer.wait()
			writer.cancel()
			out.OnlineCommits += count
			retErr = errors.Join(retErr, err)
		}()
	}
	if err = im.bulk(ctx, p, kind, input, false); err != nil {
		return err
	}
	if exercise {
		e := row{Route: 112, Value: 909, Identity: identity}
		stale, err := im.client.Begin()
		if err != nil {
			return err
		}
		if err = applyMutation(ctx, stale, p, 1001, h+3, &e, true); err != nil {
			_ = stale.Rollback()
			return err
		}
		if err = transitionIndexRetry(ctx, im.client, p, global, indexRunning, indexMerging); err != nil {
			_ = stale.Rollback()
			return err
		}
		if err = stale.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
			return fmt.Errorf("Running DML crossed phase barrier: %v", err)
		}
		out.PhaseConflicts++
		// An m-version put supersedes E's earlier b-version, while A/B deletion
		// history remains. It must not delete E's now-live unique entry.
		if err = mutateWithRetry(ctx, im.client, p, 1001, h+3, &e); err != nil {
			return err
		}
		d.Null = false
		d.Route = 15
		if err = mutateWithRetry(ctx, im.client, p, 1000, h+1, &d); err != nil {
			return err
		}
		q, err := p.target(e.Route)
		if err != nil {
			return err
		}
		entry, err := indexPair(p, q, h+3, e, indexes(p, global)[1])
		if err != nil {
			return err
		}
		tempKey := bytes.Clone(entry.Key)
		tablecodec.IndexKey2TempIndexKey(tempKey)
		staleMerge, err := prepareIndexMerge(ctx, im.client, p, global, tempKey)
		if err != nil {
			return err
		}
		e.Value++
		if err = mutateWithRetry(ctx, im.client, p, 1001, h+3, &e); err != nil {
			_ = staleMerge.Rollback()
			return err
		}
		if err = staleMerge.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
			return fmt.Errorf("merge missed foreground conflict: %v", err)
		}
		out.IndexConflicts++
	} else if err = transitionIndexRetry(ctx, im.client, p, global, indexRunning, indexMerging); err != nil {
		return err
	}
	if exercise {
		if err := im.exportMergeFixture(ctx, p, global); err != nil {
			return err
		}
	}
	conflicts, err := mergeIndexes(ctx, im.client, p, global)
	if err != nil {
		return err
	}
	out.IndexConflicts += conflicts
	if writer != nil {
		if _, err = writer.wait(); err != nil {
			return err
		}
	}
	if exercise {
		ts, err := im.client.GetTimestamp(ctx)
		if err != nil {
			return err
		}
		for slot, h := range indexStressHandles {
			if slot >= im.mutations {
				continue
			}
			last := im.mutations - 1 - ((im.mutations - 1 - slot) % len(indexStressHandles))
			value, err := im.client.GetSnapshot(ts).Get(ctx, recordKey(1000, h))
			if err != nil {
				return err
			}
			final, err := decodeRow(value.Value)
			if err != nil {
				return err
			}
			if final.Value != int64(10000+last) || final.Route != int64(25+50*((last/len(indexStressHandles))%2)) {
				return fmt.Errorf("background terminal write missing for handle %d", h)
			}
		}
		// Open a worker on an m-only key, then close the group. Even when there is
		// nothing to replay, its temp deletion must not commit across retirement.
		e := row{Route: 112, Value: 912, Identity: identity}
		if err = mutateWithRetry(ctx, im.client, p, 1001, h+3, &e); err != nil {
			return err
		}
		q, err := p.target(e.Route)
		if err != nil {
			return err
		}
		entry, err := indexPair(p, q, h+3, e, indexes(p, global)[1])
		if err != nil {
			return err
		}
		key := bytes.Clone(entry.Key)
		tablecodec.IndexKey2TempIndexKey(key)
		late, err := prepareIndexMerge(ctx, im.client, p, global, key)
		if err != nil {
			return err
		}
		if err = transitionIndexRetry(ctx, im.client, p, global, indexMerging, indexDirect); err != nil {
			_ = late.Rollback()
			return err
		}
		if err = late.Commit(ctx); !tikverr.IsErrWriteConflict(err) {
			return fmt.Errorf("late index worker crossed Direct barrier: %v", err)
		}
		if txn, err := prepareIndexMerge(ctx, im.client, p, global, key); err == nil {
			_ = txn.Rollback()
			return fmt.Errorf("new worker entered retired group")
		}
		out.PhaseConflicts++
	} else if err = transitionIndexRetry(ctx, im.client, p, global, indexMerging, indexDirect); err != nil {
		return err
	}
	return verifyIndexGroup(ctx, im.client, p, global)
}

// verifyOnlineRollback checks every KV the mutation can touch, including absent
// new index keys. It runs before the background writer starts.
func verifyOnlineRollback(ctx context.Context, c *txnkv.Client, p splitPlan, pid, h int64, before row) error {
	after := before
	after.Value = 999999
	q, err := p.target(before.Route)
	if err != nil {
		return err
	}
	keys := [][]byte{recordKey(pid, h), recordKey(q, h), markerKey(p, q, h), uniqueKey(before.Identity)}
	for _, global := range []bool{false, true} {
		keys = append(keys, indexStateKey(p, global))
		for _, idx := range indexes(p, global) {
			for _, r := range []row{before, after} {
				entry, err := indexPair(p, q, h, r, idx)
				if err != nil {
					return err
				}
				temp := bytes.Clone(entry.Key)
				tablecodec.IndexKey2TempIndexKey(temp)
				keys = append(keys, entry.Key, temp)
			}
		}
	}
	capture := func() ([]pair, error) {
		ts, err := c.GetTimestamp(ctx)
		if err != nil {
			return nil, err
		}
		entries, err := c.GetSnapshot(ts).BatchGet(ctx, keys)
		if err != nil {
			return nil, err
		}
		var values []pair
		for key, value := range entries {
			values = append(values, pair{[]byte(key), bytes.Clone(value.Value)})
		}
		return values, nil
	}
	original, err := capture()
	if err != nil {
		return err
	}
	txn, err := c.Begin()
	if err != nil {
		return err
	}
	if err = applyMutation(ctx, txn, p, pid, h, &after, true); err != nil {
		_ = txn.Rollback()
		return err
	}
	if err = txn.Rollback(); err != nil {
		return err
	}
	actual, err := capture()
	if err != nil {
		return err
	}
	if err = equalPairs(original, actual); err != nil {
		return fmt.Errorf("online mutation rollback changed data: %w", err)
	}
	return nil
}
