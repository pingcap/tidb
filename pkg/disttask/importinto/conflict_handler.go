// Copyright 2025 PingCAP, Inc.
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

package importinto

import (
	"bytes"
	"context"
	goerrors "errors"
	"io"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
)

const (
	// (1+2+4+8)*0.1s + (10-4)*1s = 7.5s
	storeOpMinBackoff       = 100 * time.Millisecond
	storeOpMaxBackoff       = time.Second
	storeOpMaxRetryCnt      = 10
	snapshotRefreshInterval = 15 * time.Second
	// we define those limit to be within how client define big transaction, see
	// https://github.com/tikv/client-go/blob/3150e385e39fbbb324fe975d68abe4fdf5dbd6ba/txnkv/transaction/2pc.go#L695-L696
	bufferedKeySizeLimit  = 2 * units.MiB
	bufferedKeyCountLimit = 9600
	bufferedHandleLimit   = 256
)

type conflictKVHandler interface {
	init() error
	run(context.Context, chan *external.KVPair) error
	getCollectResult() *collectConflictResult
	close(context.Context) error
}

var _ conflictKVHandler = (*baseConflictKVHandler)(nil)

type baseConflictKVHandler struct {
	tableImporter *importer.TableImporter
	store         tidbkv.Storage
	logger        *zap.Logger
	kvGroup       string

	collector       *conflictRowCollector
	deleter         *conflictKVDeleter
	encoder         *importer.TableKVEncoder
	lastRefreshTime time.Time
	snapshot        tidbkv.Snapshot

	handleFn func(context.Context, *external.KVPair) error

	// we delete keys in batch
	bufferedKeys []tidbkv.Key
	bufSize      int
}

func (h *baseConflictKVHandler) init() error {
	if err := h.refreshSnapshotAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	encoder, err := h.tableImporter.GetKVEncoderForDupResolve()
	if err != nil {
		return err
	}
	h.encoder = encoder
	return nil
}

func (h *baseConflictKVHandler) run(ctx context.Context, pairCh chan *external.KVPair) error {
	for kvPair := range pairCh {
		if err := h.handleFn(ctx, kvPair); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (h *baseConflictKVHandler) getCollectResult() *collectConflictResult {
	if h.collector != nil {
		return h.collector.collectConflictResult
	}
	return nil
}

func (h *baseConflictKVHandler) close(ctx context.Context) error {
	var firstErr common.OnceError
	if h.collector != nil {
		firstErr.Set(h.collector.close(ctx))
	}
	firstErr.Set(h.encoder.Close())
	firstErr.Set(h.sendKeysToDelete(ctx))

	return firstErr.Get()
}

func (h *baseConflictKVHandler) refreshSnapshotAsNeeded() error {
	if h.snapshot != nil && time.Since(h.lastRefreshTime) < snapshotRefreshInterval {
		return nil
	}
	// we refresh it to avoid fall behind GC safe point.
	// it's not necessary to update this version too frequently, even though we
	// will delete KVs during conflict KV handing, as this handler is used to handle
	// conflicts of the same KV group, the data KVs corresponding to any 2 conflict
	// KVs are either conflicts with each other too and recorded in the conflict
	// KV file, or they are not conflicted and are either recorded or ingested,
	// so for a single data KV found in this handler cannot be deleted twice.
	ver, err := h.store.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	h.snapshot = h.store.GetSnapshot(ver)
	h.lastRefreshTime = time.Now()
	return nil
}

// re-encode the row from the handle and value of data KV, then we either delete
// all encoded keys or call handleConflictRowFn, it's possible that part or all
// of the keys are already deleted.
func (h *baseConflictKVHandler) encodeAndHandleRow(ctx context.Context,
	handle tidbkv.Handle, val []byte) (err error) {
	tbl := h.tableImporter.Table
	tblMeta := tbl.Meta()
	decodedData, _, err := tables.DecodeRawRowData(h.encoder.SessionCtx.GetExprCtx(),
		tblMeta, handle, tbl.Cols(), val)
	if err != nil {
		return errors.Trace(err)
	}
	var autoRowID int64
	if !tblMeta.HasClusteredIndex() {
		autoRowID = handle.IntValue()
	}
	kvPairs, err := h.encoder.Encode(decodedData, autoRowID)
	if err != nil {
		return errors.Trace(err)
	}

	if h.collector != nil {
		err = h.collector.recordConflictRow(ctx, h.kvGroup, handle, decodedData, kvPairs)
	} else {
		err = h.gatherAndDeleteKeysWithRetry(ctx, kvPairs.Pairs)
	}
	kvPairs.Clear()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *baseConflictKVHandler) gatherAndDeleteKeysWithRetry(ctx context.Context, pairs []common.KvPair) error {
	backoffer := backoff.NewExponential(storeOpMinBackoff, 2, storeOpMaxBackoff)
	if err := dxfhandle.RunWithRetry(ctx, storeOpMaxRetryCnt, backoffer, h.logger, func(ctx context.Context) (bool, error) {
		err := h.gatherKeysToDelete(ctx, pairs)
		if err != nil {
			return common.IsRetryableError(err), err
		}
		return true, nil
	}); err != nil {
		return err
	}

	if h.bufSize >= bufferedKeySizeLimit || len(h.bufferedKeys) >= bufferedKeyCountLimit {
		return h.sendKeysToDelete(ctx)
	}
	return nil
}

func (h *baseConflictKVHandler) sendKeysToDelete(ctx context.Context) error {
	if len(h.bufferedKeys) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.deleter.getCh() <- h.bufferedKeys:
		h.bufferedKeys = make([]tidbkv.Key, 0, len(h.bufferedKeys))
		h.bufSize = 0
		return nil
	}
}

// we are deleting keys related to a single row in one transaction, and a normal
// 'insert SQL' will also generate this mount of data, so we shouldn't meet the
// 'transaction too large' issue in normal case.
// as all duplicate KVs are either removed or recorded during importing, and we
// only delete existing KVs, so there will be no overlap in the KVs to be deleted
// for any 2 conflict KVs in a single KV group, it's safe to resolve a single KV
// group in multiple routines, and we can use a relatively stale snapshot to check
// existence of the KVs to be deleted.
func (h *baseConflictKVHandler) gatherKeysToDelete(ctx context.Context, pairs []common.KvPair) (err error) {
	if err = h.refreshSnapshotAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	allKeys := make([]tidbkv.Key, 0, len(pairs))
	for _, p := range pairs {
		allKeys = append(allKeys, p.Key)
	}
	res, err := h.snapshot.BatchGet(ctx, allKeys)
	if err != nil {
		return errors.Trace(err)
	}
	if len(res) == 0 {
		return nil
	}

	for k := range res {
		h.bufferedKeys = append(h.bufferedKeys, []byte(k))
		h.bufSize += len(k)
	}

	return nil
}

type conflictDataKVHandler struct {
	*baseConflictKVHandler
}

func (h *conflictDataKVHandler) handle(ctx context.Context, kv *external.KVPair) error {
	handle, err := tablecodec.DecodeRowKey(kv.Key)
	if err != nil {
		return err
	}
	return h.encodeAndHandleRow(ctx, handle, kv.Value)
}

type handleOfTable struct {
	tableID int64
	handle  tidbkv.Handle
}

type conflictIndexKVHandler struct {
	*baseConflictKVHandler
	targetIdx *model.IndexInfo

	bufferedHandles []handleOfTable
	isRowHandledFn  func(handle tidbkv.Handle) bool
}

func (h *conflictIndexKVHandler) init() error {
	indexID, err := kvGroup2IndexID(h.kvGroup)
	if err != nil {
		return errors.Trace(err)
	}
	tbl := h.tableImporter.Table
	tblMeta := tbl.Meta()
	targetIdx := model.FindIndexInfoByID(tblMeta.Indices, indexID)
	if targetIdx == nil {
		// should not happen
		return errors.Errorf("index %d in table %s", indexID, tblMeta.Name)
	}

	if err = h.baseConflictKVHandler.init(); err != nil {
		return err
	}

	h.targetIdx = targetIdx
	return nil
}

func (h *conflictIndexKVHandler) handle(ctx context.Context, kv *external.KVPair) error {
	tableID := tablecodec.DecodeTableID(kv.Key)
	if tableID == 0 {
		// should not happen
		return errors.Errorf("invalid table ID in key %v", redact.Key(kv.Key))
	}
	handle, err := tablecodec.DecodeIndexHandle(kv.Key, kv.Value, len(h.targetIdx.Columns))
	if err != nil {
		return err
	}
	if h.isRowHandledFn != nil && h.isRowHandledFn(handle) {
		return nil
	}

	h.bufferedHandles = append(h.bufferedHandles, handleOfTable{handle: handle, tableID: tableID})

	if len(h.bufferedHandles) >= bufferedHandleLimit {
		return h.handleBufferedHandles(ctx)
	}
	return nil
}

func (h *conflictIndexKVHandler) handleBufferedHandles(ctx context.Context) error {
	if len(h.bufferedHandles) == 0 {
		return nil
	}
	rowKeys := make([]tidbkv.Key, 0, len(h.bufferedHandles))
	rowKeys2Handle := make(map[string]tidbkv.Handle, len(h.bufferedHandles))
	for _, hdl := range h.bufferedHandles {
		rowKey := tablecodec.EncodeRowKeyWithHandle(hdl.tableID, hdl.handle)
		rowKeys = append(rowKeys, rowKey)
		rowKeys2Handle[string(rowKey)] = hdl.handle
	}

	if err := h.refreshSnapshotAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	res, err := h.snapshot.BatchGet(ctx, rowKeys)
	if err != nil {
		return errors.Trace(err)
	}
	for rowKey, val := range res {
		handle := rowKeys2Handle[rowKey]
		if err := h.encodeAndHandleRow(ctx, handle, val.Value); err != nil {
			return errors.Trace(err)
		}
	}
	h.bufferedHandles = h.bufferedHandles[:0]
	return nil
}

func (h *conflictIndexKVHandler) close(ctx context.Context) error {
	var firstErr common.OnceError
	firstErr.Set(h.handleBufferedHandles(ctx))
	firstErr.Set(h.baseConflictKVHandler.close(ctx))
	return firstErr.Get()
}

func startReadFiles(ctx context.Context, eg *tidbutil.ErrorGroupWithRecover,
	store storage.ExternalStorage, files []string) chan *external.KVPair {
	pairCh := make(chan *external.KVPair)
	eg.Go(func() error {
		defer close(pairCh)
		for _, file := range files {
			if err := readOneFile(ctx, store, file, pairCh); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return pairCh
}

func readOneFile(ctx context.Context, store storage.ExternalStorage, file string, outCh chan *external.KVPair) error {
	reader, err := external.NewKVReader(ctx, file, store, 0, 3*external.DefaultReadBufferSize)
	if err != nil {
		return err
	}
	//nolint: errcheck
	defer reader.Close()
	for {
		key, val, err := reader.NextKV()
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- &external.KVPair{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(val),
		}:
		}
	}
	return nil
}
