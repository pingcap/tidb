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
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

const (
	indexInactive = "inactive"
	indexRunning  = "running"
	indexMerging  = "merging"
	indexDirect   = "direct"
)

var errDuplicateIdentity = errors.New("canonical unique identity already exists")

type indexRow struct {
	PID, Handle int64
	Row         row
}

func uniqueKey(identity int64) []byte {
	return codec.EncodeInt([]byte("prototype/canonical-unique/"), identity)
}
func indexStateKey(p splitPlan, global bool) []byte {
	kind := "local"
	if global {
		kind = "global"
	}
	return []byte(fmt.Sprintf("prototype/g%d/index-state/%s", p.Generation, kind))
}

func transitionIndex(ctx context.Context, c *txnkv.Client, p splitPlan, global bool, from, to string) error {
	return transact(ctx, c, func(txn *transaction.KVTxn) error {
		state, err := getOptional(ctx, txn, indexStateKey(p, global))
		if err != nil {
			return err
		}
		if string(state) != from {
			return fmt.Errorf("index transition expected %q, got %q", from, state)
		}
		if !(from == indexInactive && to == indexRunning || from == indexRunning && to == indexMerging || from == indexMerging && to == indexDirect) {
			return fmt.Errorf("illegal index transition %s -> %s", from, to)
		}
		if to == indexMerging {
			kind := "local"
			if global {
				kind = "global"
			}
			sealed, err := getOptional(ctx, txn, []byte(fmt.Sprintf("prototype/g%d/sealed/%s", p.Generation, kind)))
			if err != nil {
				return err
			}
			if string(sealed) != "sealed" {
				return fmt.Errorf("cannot merge before import is sealed")
			}
		}
		return txn.Set(indexStateKey(p, global), []byte(to))
	})
}

// maintainIndexes uses the production temp value codec and b/m version protocol.
// Unique validation belongs to the canonical source guard in applyMutation;
// incomplete shadow data is never allowed to reject an otherwise valid DML.
func maintainIndexes(ctx context.Context, txn *transaction.KVTxn, p splitPlan, before, after *indexRow) error {
	for _, global := range []bool{false, true} {
		key := indexStateKey(p, global)
		state, err := getOptional(ctx, txn, key)
		if err != nil {
			return err
		}
		if state == nil {
			return fmt.Errorf("index state missing")
		}

		if string(state) != indexInactive && string(state) != indexRunning && string(state) != indexMerging && string(state) != indexDirect {
			return fmt.Errorf("invalid index state %q", state)
		}
		// Coarse prototype barrier: a phase change conflicts with every transaction
		// retaining the previous state. Production SQL must use its schema/MDL contract.
		if err = txn.Set(key, state); err != nil {
			return err
		}
		if string(state) == indexInactive {
			continue
		}

		for _, idx := range indexes(p, global) {
			if before != nil {
				if err = writeIndexEvent(ctx, txn, p, idx, before, true, string(state)); err != nil {
					return err
				}
			}
			if after != nil {
				if err = writeIndexEvent(ctx, txn, p, idx, after, false, string(state)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeIndexEvent(ctx context.Context, txn *transaction.KVTxn, p splitPlan, idx *model.IndexInfo, r *indexRow, deleted bool, state string) error {
	entry, err := indexPair(p, r.PID, r.Handle, r.Row, idx)
	if err != nil {
		return err
	}
	var handle kv.Handle = kv.IntHandle(r.Handle)
	if idx.Global {
		handle = kv.NewPartitionHandle(r.PID, handle)
	}
	distinct := idx.Unique && !r.Row.Null
	if state != indexRunning {
		if deleted {
			deleteKey := true
			if distinct {
				current, err := getOptional(ctx, txn, entry.Key)
				if err != nil {
					return err
				}
				if current != nil {
					owner, err := tablecodec.DecodeHandleInIndexValue(current)
					if err != nil {
						return err
					}
					deleteKey = equalIndexHandles(owner, handle)
				}
			}
			if deleteKey {
				if err = txn.Delete(entry.Key); err != nil {
					return err
				}
			}
		} else if err = txn.Set(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	if state == indexDirect {
		return nil
	}
	tempKey := bytes.Clone(entry.Key)
	tablecodec.IndexKey2TempIndexKey(tempKey)
	version := tablecodec.TempIndexKeyTypeBackfill
	if state == indexMerging {
		version = tablecodec.TempIndexKeyTypeMerge
	}
	elem := tablecodec.TempIndexValueElem{Value: entry.Value, Handle: handle, KeyVer: version, Delete: deleted, Distinct: distinct, Global: idx.Global}
	var previous []byte
	if distinct {
		previous, err = getOptional(ctx, txn, tempKey)
		if err != nil {
			return err
		}
	}
	return txn.Set(tempKey, elem.Encode(bytes.Clone(previous)))
}

func equalIndexHandles(a, b kv.Handle) bool {
	pa, aPart := a.(kv.PartitionHandle)
	pb, bPart := b.(kv.PartitionHandle)
	if aPart != bPart {
		return false
	}
	if aPart {
		return pa.PartitionID == pb.PartitionID && pa.Handle.Equal(pb.Handle)
	}
	return a.Equal(b)
}

// applyTempHistory adapts the decisions in ddl/checkTempIndexKey without exposing
// a new production API. It reuses the actual codec and FilterOverwritten, but is
// not the production DDL merge worker. exists must use the same transaction.
func applyTempHistory(key, raw, origin []byte, exists func(kv.Handle) (bool, error)) ([]byte, error) {
	history, err := tablecodec.DecodeTempIndexValue(raw)
	if err != nil {
		return nil, err
	}
	for _, elem := range history {
		if elem.Handle == nil {
			elem.Handle, err = tablecodec.DecodeIndexHandle(key, elem.Value, 1)
			if err != nil {
				return nil, err
			}
		}
	}
	history = history.FilterOverwritten()
	value := bytes.Clone(origin)
	for _, elem := range history {
		if elem.KeyVer == tablecodec.TempIndexKeyTypeMerge || elem.KeyVer == tablecodec.TempIndexKeyTypeDelete {
			continue
		}
		if elem.KeyVer != tablecodec.TempIndexKeyTypeBackfill {
			return nil, fmt.Errorf("unsupported temp key version %d", elem.KeyVer)
		}
		if elem.Delete {
			if value == nil {
				continue
			}
			if tablecodec.IndexKVIsUnique(value) {
				owner, err := tablecodec.DecodeHandleInIndexValue(value)
				if err != nil {
					return nil, err
				}
				if !equalIndexHandles(owner, elem.Handle) {
					live, err := exists(owner)
					if err != nil {
						return nil, err
					}
					if live {
						continue
					}
				}
			}
			value = nil
		} else {
			if value != nil {
				if tablecodec.IndexKVIsUnique(elem.Value) && !bytes.Equal(value, elem.Value) {
					return nil, fmt.Errorf("duplicate unique key in temp merge")
				}
				continue
			}
			value = bytes.Clone(elem.Value)
		}
	}
	return value, nil
}

func prepareIndexMerge(ctx context.Context, c *txnkv.Client, p splitPlan, global bool, key []byte) (*transaction.KVTxn, error) {
	txn, err := c.Begin()
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*transaction.KVTxn, error) { _ = txn.Rollback(); return nil, e }
	state, err := getOptional(ctx, txn, indexStateKey(p, global))
	if err != nil {
		return fail(err)
	}
	if string(state) != indexMerging {
		return fail(fmt.Errorf("index merge is not open"))
	}
	raw, err := getOptional(ctx, txn, key)
	if err != nil {
		return fail(err)
	}
	if raw == nil {
		return fail(fmt.Errorf("temp index key absent"))
	}
	originKey := bytes.Clone(key)
	tablecodec.TempIndexKey2IndexKey(originKey)
	origin, err := getOptional(ctx, txn, originKey)
	if err != nil {
		return fail(err)
	}
	value, err := applyTempHistory(key, raw, origin, func(h kv.Handle) (bool, error) {
		pid := tablecodec.DecodeTableID(key)
		if ph, ok := h.(kv.PartitionHandle); ok {
			pid = ph.PartitionID
			h = ph.Handle
		}
		if _, ok := p.Parents[pid]; !ok {
			return false, fmt.Errorf("index handle outside final layout")
		}
		b, err := getOptional(ctx, txn, recordKey(pid, h.IntValue()))
		return b != nil, err
	})
	if err != nil {
		return fail(err)
	}
	if !bytes.Equal(value, origin) {
		if value == nil {
			err = txn.Delete(originKey)
		} else {
			err = txn.Set(originKey, value)
		}
		if err != nil {
			return fail(err)
		}
	}
	if err = txn.Delete(key); err != nil {
		return fail(err)
	}
	if err = txn.Set(indexStateKey(p, global), state); err != nil {
		return fail(err)
	}
	return txn, nil
}

func tempPrefixes(p splitPlan, global bool) [][]byte {
	pids := p.Targets
	if global {
		pids = []int64{logicalID}
	}
	var prefixes [][]byte
	for _, pid := range pids {
		for _, idx := range indexes(p, global) {
			prefixes = append(prefixes, tablecodec.EncodeIndexSeekKey(pid, tablecodec.TempIndexPrefix|idx.ID, nil))
		}
	}
	return prefixes
}

func mergeIndexes(ctx context.Context, c *txnkv.Client, p splitPlan, global bool) (int, error) {
	// A fresh snapshot after the merging barrier covers every b-version commit.
	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	conflicts := 0
	for _, prefix := range tempPrefixes(p, global) {
		entries, err := scan(ctx, c, ts, prefix)
		if err != nil {
			return conflicts, err
		}
		for _, entry := range entries {
			count, err := retryWriteConflict(ctx, func() error {
				txn, err := prepareIndexMerge(ctx, c, p, global, entry.Key)
				if err != nil {
					return err
				}
				return txn.Commit(ctx)
			})
			conflicts += count
			if err != nil {
				return conflicts, err
			}
		}
	}
	return conflicts, nil
}

func mutateWithRetry(ctx context.Context, c *txnkv.Client, p splitPlan, pid, h int64, next *row) error {
	_, err := retryWriteConflict(ctx, func() error { return mutate(ctx, c, p, pid, h, next, true) })
	return err
}

// Only a confirmed write conflict is safe to rerun. Fresh transactions and a
// short bounded delay avoid starvation; the caller's deadline bounds total work.
func retryWriteConflict(ctx context.Context, operation func() error) (int, error) {
	conflicts := 0
	delay := time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return conflicts, err
		}
		err := operation()
		if !tikverr.IsErrWriteConflict(err) {
			return conflicts, err
		}
		conflicts++
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return conflicts, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 20*time.Millisecond)
	}
}
