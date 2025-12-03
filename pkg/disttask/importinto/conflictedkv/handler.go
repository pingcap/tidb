// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvconflicts

import (
	"context"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
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

// handler is the conflict KV handler, either collecting info about those KVs or
// delete those KVs from the cluster.
type handler interface {
	preRun() error
	run(context.Context, chan *external.KVPair) error
	close(context.Context) error
}

type kvHandler interface {
	handle(context.Context, *external.KVPair) error
}

type encodedRowHandler interface {
	handleEncodedRow(ctx context.Context, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error
}

var _ handler = (*baseHandler)(nil)

type baseHandler struct {
	targetTable table.Table
	logger      *zap.Logger
	kvGroup     string
	encoder     *importer.TableKVEncoder

	kvHandler
	encodedRowHandler
}

func (h *baseHandler) preRun() error {
	return nil
}

func (h *baseHandler) run(ctx context.Context, pairCh chan *external.KVPair) error {
	for kvPair := range pairCh {
		if err := h.handle(ctx, kvPair); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (h *baseHandler) close(context.Context) error {
	return h.encoder.Close()
}

// re-encode the row from the handle and value of data KV, then we either delete
// all encoded keys or call handleConflictRowFn, it's possible that part or all
// of the keys are already deleted.
func (h *baseHandler) encodeAndHandleRow(ctx context.Context,
	handle tidbkv.Handle, val []byte) (err error) {
	tblMeta := h.targetTable.Meta()
	decodedData, _, err := tables.DecodeRawRowData(h.encoder.SessionCtx.GetExprCtx(),
		tblMeta, handle, h.targetTable.Cols(), val)
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

	err = h.handleEncodedRow(ctx, handle, decodedData, kvPairs)
	kvPairs.Clear()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type dataKVHandler struct {
	*baseHandler
}

var (
	_ handler   = (*dataKVHandler)(nil)
	_ kvHandler = (*dataKVHandler)(nil)
)

func newDataKVHandler(base *baseHandler) *dataKVHandler {
	h := &dataKVHandler{baseHandler: base}
	base.kvHandler = h
	return h
}

func (h *dataKVHandler) handle(ctx context.Context, kv *external.KVPair) error {
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

type indexKVHandler struct {
	*baseHandler
	snapshot  *lazyRefreshedSnapshot
	hdlFilter *handleFilter

	targetIdx       *model.IndexInfo
	bufferedHandles []handleOfTable
}

var (
	_ handler   = (*indexKVHandler)(nil)
	_ kvHandler = (*indexKVHandler)(nil)
)

func newIndexKVHandler(base *baseHandler, snapshot *lazyRefreshedSnapshot, filter *handleFilter) *indexKVHandler {
	h := &indexKVHandler{
		baseHandler: base,
		snapshot:    snapshot,
		hdlFilter:   filter,
	}
	base.kvHandler = h
	return h
}

func (h *indexKVHandler) preRun() error {
	indexID, err := external.KVGroup2IndexID(h.kvGroup)
	if err != nil {
		return errors.Trace(err)
	}
	tblMeta := h.targetTable.Meta()
	targetIdx := model.FindIndexInfoByID(tblMeta.Indices, indexID)
	if targetIdx == nil {
		// should not happen
		return errors.Errorf("index %d in table %s", indexID, tblMeta.Name)
	}

	if err = h.baseHandler.preRun(); err != nil {
		return err
	}

	h.targetIdx = targetIdx
	return nil
}

func (h *indexKVHandler) handle(ctx context.Context, kv *external.KVPair) error {
	tableID := tablecodec.DecodeTableID(kv.Key)
	if tableID == 0 {
		// should not happen
		return errors.Errorf("invalid table ID in key %v", redact.Key(kv.Key))
	}
	handle, err := tablecodec.DecodeIndexHandle(kv.Key, kv.Value, len(h.targetIdx.Columns))
	if err != nil {
		return err
	}
	if h.hdlFilter.needSkip(handle) {
		return nil
	}

	h.bufferedHandles = append(h.bufferedHandles, handleOfTable{handle: handle, tableID: tableID})

	if len(h.bufferedHandles) >= bufferedHandleLimit {
		return h.handleBufferedHandles(ctx)
	}
	return nil
}

func (h *indexKVHandler) handleBufferedHandles(ctx context.Context) error {
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

	if err := h.snapshot.refreshAsNeeded(); err != nil {
		return errors.Trace(err)
	}
	res, err := h.snapshot.BatchGet(ctx, rowKeys)
	if err != nil {
		return errors.Trace(err)
	}
	for rowKey, val := range res {
		handle := rowKeys2Handle[rowKey]
		if err := h.encodeAndHandleRow(ctx, handle, val); err != nil {
			return errors.Trace(err)
		}
	}
	h.bufferedHandles = h.bufferedHandles[:0]
	return nil
}

func (h *indexKVHandler) close(ctx context.Context) error {
	var firstErr common.OnceError
	firstErr.Set(h.handleBufferedHandles(ctx))
	firstErr.Set(h.baseHandler.close(ctx))
	return firstErr.Get()
}

type lazyRefreshedSnapshot struct {
	tidbkv.Snapshot
	store           tidbkv.Storage
	lastRefreshTime time.Time
}

func (s *lazyRefreshedSnapshot) refreshAsNeeded() error {
	if s.Snapshot != nil && time.Since(s.lastRefreshTime) < snapshotRefreshInterval {
		return nil
	}
	// we refresh it to avoid fall behind GC safe point.
	// it's not necessary to update this version too frequently, even though we
	// will delete KVs during conflict KV handing, as this handler is used to handle
	// conflicts of the same KV group, the data KVs corresponding to any 2 conflict
	// KVs are either conflicts with each other too and recorded in the conflict
	// KV file, or they are not conflicted and are either recorded or ingested.
	// and as we handle index KV group one by one,
	// so for a single data KV found here won't be deleted by 2 parallel handlers.
	ver, err := s.store.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	s.Snapshot = s.store.GetSnapshot(ver)
	s.lastRefreshTime = time.Now()
	return nil
}
