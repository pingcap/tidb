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
	"context"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	dxfhandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/redact"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// (1+2+4+8)*0.1s + (10-4)*1s = 7.5s
	storeOpMinBackoff       = 100 * time.Millisecond
	storeOpMaxBackoff       = time.Second
	storeOpMaxRetryCnt      = 10
	snapshotRefreshInterval = 15 * time.Second
)

type conflictKVHandler interface {
	init() error
	run(context.Context, chan *external.KVPair) error
	close() error
}

var _ conflictKVHandler = (*baseConflictKVHandler)(nil)

type baseConflictKVHandler struct {
	tableImporter       *importer.TableImporter
	store               tidbkv.Storage
	logger              *zap.Logger
	kvGroup             string
	handleConflictRowFn func(ctx context.Context, kvGroup string, handle tidbkv.Handle, row []types.Datum, pairs *kv.Pairs) error

	encoder         *importer.TableKVEncoder
	lastRefreshTime time.Time
	snapshot        tidbkv.Snapshot

	handleFn func(context.Context, *external.KVPair) error
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

func (h *baseConflictKVHandler) close() error {
	return h.encoder.Close()
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
	decodedData, _, err := tables.DecodeRawRowData(h.encoder.SessionCtx,
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

	if h.handleConflictRowFn != nil {
		err = h.handleConflictRowFn(ctx, h.kvGroup, handle, decodedData, kvPairs)
	} else {
		err = h.deleteKeysWithRetry(ctx, kvPairs.Pairs)
	}
	kvPairs.Clear()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *baseConflictKVHandler) deleteKeysWithRetry(ctx context.Context, pairs []common.KvPair) error {
	backoffer := backoff.NewExponential(storeOpMinBackoff, 2, storeOpMaxBackoff)
	return dxfhandle.RunWithRetry(ctx, storeOpMaxRetryCnt, backoffer, h.logger, func(ctx context.Context) (bool, error) {
		err := h.deleteKeys(ctx, pairs)
		if err != nil {
			return common.IsRetryableError(err), err
		}
		return true, nil
	})
}

// we are deleting keys related to a single row in one transaction, and a normal
// 'insert SQL' will also generate this mount of data, so we shouldn't meet the
// 'transaction too large' issue in normal case.
// as all duplicate KVs are either removed or recorded during importing, and we
// only delete existing KVs, so there will be no overlap in the KVs to be deleted
// for any 2 conflict KVs in a single KV group, it's safe to resolve a single KV
// group in multiple routines, and we can use a relatively stale snapshot to check
// existence of the KVs to be deleted.
func (h *baseConflictKVHandler) deleteKeys(ctx context.Context, pairs []common.KvPair) (err error) {
	if err = h.refreshSnapshotAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	existingPairs := make([]common.KvPair, 0, len(pairs))
	for _, p := range pairs {
		// TODO test if BatchGet performs better
		_, err = h.snapshot.Get(ctx, p.Key)
		if err != nil {
			if isKeyNotFoundErr(err) {
				// not ingested, or already deleted when resolving other KV groups.
				continue
			}
			return errors.Trace(err)
		}
		existingPairs = append(existingPairs, p)
	}

	if len(existingPairs) == 0 {
		return nil
	}

	txn, err := h.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				h.logger.Warn("failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	for _, p := range existingPairs {
		if err = txn.Delete(p.Key); err != nil {
			return errors.Trace(err)
		}
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

type conflictIndexKVHandler struct {
	*baseConflictKVHandler
	targetIdx *model.IndexInfo

	isRowHandledFn func(handle tidbkv.Handle) bool
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
	rowKey := tablecodec.EncodeRowKeyWithHandle(tableID, handle)

	if err = h.refreshSnapshotAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	val, err := h.snapshot.Get(ctx, rowKey)
	// either the data KV is deleted by handing conflicts in other KV group or the
	// data KV itself is conflicted and not ingested.
	if err != nil {
		if isKeyNotFoundErr(err) {
			return nil
		}
		return errors.Trace(err)
	}
	if h.isRowHandledFn != nil && h.isRowHandledFn(handle) {
		return nil
	}
	return h.encodeAndHandleRow(ctx, handle, val)
}

func handleKVGroupConflicts(
	ctx context.Context,
	logger *zap.Logger,
	concurrency int,
	newHandlerFn func(string) conflictKVHandler,
	store storage.ExternalStorage,
	kvGroup string,
	ci *common.ConflictInfo,
) (err error) {
	task := log.BeginTask(logger.With(
		zap.String("kvGroup", kvGroup),
		zap.Uint64("duplicates", ci.Count),
		zap.Int("file-count", len(ci.Files)),
	), "handle kv group conflicts")

	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	pairCh := make(chan *external.KVPair)
	eg, egCtx := tidbutil.NewErrorGroupWithRecoverWithCtx(ctx)
	eg.Go(func() error {
		defer close(pairCh)
		for _, file := range ci.Files {
			if err = readOneFile(egCtx, store, file, pairCh); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	for i := 0; i < concurrency; i++ {
		handler := newHandlerFn(kvGroup)
		eg.Go(func() error {
			if err = handler.init(); err != nil {
				return errors.Trace(err)
			}
			defer handler.close()
			return handler.run(egCtx, pairCh)
		})
	}
	return eg.Wait()
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
			if err == io.EOF {
				break
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- &external.KVPair{Key: key, Value: val}:
		}
	}
	return nil
}

func isKeyNotFoundErr(err error) bool {
	return tidbkv.IsErrNotFound(err) || tikverr.IsErrNotFound(err)
}
