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

package conflictedkv

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const snapshotRefreshInterval = 15 * time.Second

var (
	// BufferedHandleLimit is the max number of handles buffered before processing.
	// exported for test.
	BufferedHandleLimit = 256
)

// TrafficRecorder records the best-effort traffic between TiDB and TiKV.
// It's used to report metering data for conflict handling without introducing
// a dependency on the metering package.
type TrafficRecorder interface {
	IncClusterReadBytes(uint64)
	IncClusterWriteBytes(uint64)
}

// Handler is the conflict KV Handler, either collecting info about those KVs or
// delete those KVs from the cluster.
type Handler interface {
	// PreRun is called before Run.
	// if it failed, Close still need to be called.
	PreRun() error
	// Run processes the conflicted KV pairs from the channel.
	Run(context.Context, chan *simplesst.KVPair) error
	// Close must be called regardless of PreRun/Run result.
	Close(context.Context) error
}

// KVHandler handles a single conflict KV pair.
// exported for test.
type KVHandler interface {
	Handle(context.Context, *simplesst.KVPair) error
}

// EncodedRowHandler handles the re-encoded row from conflict KV.
// exported for test.
type EncodedRowHandler interface {
	HandleEncodedRow(ctx context.Context, rowKey tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs) error
}

var _ Handler = (*BaseHandler)(nil)

// BaseHandler is the base struct for conflict KV handlers.
// exported for test.
type BaseHandler struct {
	targetTable table.Table
	kvGroup     string
	// the codec used to decoded encoded keys.
	// in next-gen, the encoded key is prepended with keyspace prefix before
	// store to object store to make later ingest step easier to process. but
	// when resolving conflicts, we need to use transaction to access those keys,
	// and the keys must not have the keyspace prefix.
	codec     tikv.Codec
	encoder   *importer.TableKVEncoder
	collector execute.Collector
	logger    *zap.Logger
	EncodedRowHandler

	KVHandler
}

// NewBaseHandler creates a new BaseHandler.
func NewBaseHandler(
	targetTable table.Table,
	kvGroup string,
	codec tikv.Codec,
	encoder *importer.TableKVEncoder,
	encodedRowHdl EncodedRowHandler,
	collector execute.Collector,
	logger *zap.Logger,
) *BaseHandler {
	if collector == nil {
		collector = &execute.NoopCollector{}
	}
	return &BaseHandler{
		targetTable:       targetTable,
		kvGroup:           kvGroup,
		codec:             codec,
		encoder:           encoder,
		collector:         collector,
		logger:            logger,
		EncodedRowHandler: encodedRowHdl,
	}
}

// PreRun implements Handler interface.
func (*BaseHandler) PreRun() error {
	return nil
}

// Run implements Handler interface.
func (h *BaseHandler) Run(ctx context.Context, pairCh chan *simplesst.KVPair) error {
	for kvPair := range pairCh {
		if err := h.Handle(ctx, kvPair); err != nil {
			return errors.Trace(err)
		}
		// Each item in pairCh is one conflict KV pair.
		h.collector.Processed(1, 0)
	}
	return nil
}

// Close implements Handler interface.
func (h *BaseHandler) Close(context.Context) (err error) {
	if h.encoder != nil {
		// in some test, we don't set encoder
		err = h.encoder.Close()
	}
	return err
}

// re-encode the row from the handle and value of data KV into KV pairs and handle
// them using the EncodedRowHandler.
func (h *BaseHandler) encodeAndHandleRow(ctx context.Context,
	rowKey tidbkv.Key, handle tidbkv.Handle, val []byte) (err error) {
	tblMeta := h.targetTable.Meta()
	decodedData, _, err := tables.DecodeRawRowData(h.encoder.SessionCtx.GetExprCtx(),
		h.targetTable, handle, h.targetTable.VisibleCols(), val)
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

	err = h.HandleEncodedRow(ctx, rowKey, decodedData, kvPairs)
	kvPairs.Clear()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DataKVHandler handles conflicted data KVs.
type DataKVHandler struct {
	*BaseHandler
}

var (
	_ Handler   = (*DataKVHandler)(nil)
	_ KVHandler = (*DataKVHandler)(nil)
)

// NewDataKVHandler creates a new DataKVHandler.
func NewDataKVHandler(base *BaseHandler) *DataKVHandler {
	h := &DataKVHandler{BaseHandler: base}
	base.KVHandler = h
	return h
}

// Handle implements KVHandler interface.
func (h *DataKVHandler) Handle(ctx context.Context, kv *simplesst.KVPair) error {
	key, err := h.codec.DecodeKey(kv.Key)
	if err != nil {
		return err
	}
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return err
	}
	return h.encodeAndHandleRow(ctx, key, handle, kv.Value)
}

type rowKeyWithHandle struct {
	rowKey tidbkv.Key
	handle tidbkv.Handle
}

func encodeDataRowKey(tableID int64, handle tidbkv.Handle) tidbkv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(tableID)
	return tablecodec.EncodeRecordKey(recordPrefix, handle)
}

// IndexKVHandler handles conflicted index KVs.
// exported for test.
type IndexKVHandler struct {
	*BaseHandler
	snapshot     *LazyRefreshedSnapshot
	rowKeyFilter *KeyFilter

	targetIdx    *model.IndexInfo
	bufferedRows []rowKeyWithHandle
}

var (
	_ Handler   = (*IndexKVHandler)(nil)
	_ KVHandler = (*IndexKVHandler)(nil)
)

// NewIndexKVHandler creates a new IndexKVHandler.
// exported for test.
func NewIndexKVHandler(base *BaseHandler, snapshot *LazyRefreshedSnapshot, filter *KeyFilter) *IndexKVHandler {
	h := &IndexKVHandler{
		BaseHandler:  base,
		snapshot:     snapshot,
		rowKeyFilter: filter,
	}
	base.KVHandler = h
	return h
}

// PreRun implements Handler interface.
func (h *IndexKVHandler) PreRun() error {
	indexID, err := globalsort.KVGroup2IndexID(h.kvGroup)
	if err != nil {
		return errors.Trace(err)
	}
	tblMeta := h.targetTable.Meta()
	targetIdx := model.FindIndexInfoByID(tblMeta.Indices, indexID)
	if targetIdx == nil {
		// should not happen
		return errors.Errorf("index %d in table %s", indexID, tblMeta.Name)
	}

	if err = h.BaseHandler.PreRun(); err != nil {
		return err
	}

	h.targetIdx = targetIdx
	return nil
}

// Handle implements KVHandler interface.
func (h *IndexKVHandler) Handle(ctx context.Context, kv *simplesst.KVPair) error {
	key, err := h.codec.DecodeKey(kv.Key)
	if err != nil {
		return err
	}
	// we should use the table ID from the key, in case of partition table
	tableID := tablecodec.DecodeTableID(key)
	if tableID == 0 {
		// should not happen
		return errors.Errorf("invalid table ID in key %v", redact.Key(kv.Key))
	}
	handle, err := tablecodec.DecodeIndexHandle(key, kv.Value, len(h.targetIdx.Columns))
	if err != nil {
		return err
	}
	// The filter and snapshot lookup need the data row key, not this index key.
	rowKey := encodeDataRowKey(tableID, handle)
	if h.rowKeyFilter.isHandledGlobally(rowKey) {
		return nil
	}

	h.bufferedRows = append(h.bufferedRows, rowKeyWithHandle{rowKey: rowKey, handle: handle})

	if len(h.bufferedRows) >= BufferedHandleLimit {
		return h.handleBufferedHandles(ctx)
	}
	return nil
}

func (h *IndexKVHandler) handleBufferedHandles(ctx context.Context) error {
	if len(h.bufferedRows) == 0 {
		return nil
	}
	rowKeys := make([]tidbkv.Key, 0, len(h.bufferedRows))
	rowKeys2Handle := make(map[string]tidbkv.Handle, len(h.bufferedRows))
	for _, row := range h.bufferedRows {
		rowKeys = append(rowKeys, row.rowKey)
		rowKeys2Handle[string(row.rowKey)] = row.handle
	}

	res, err := h.snapshot.BatchGet(ctx, rowKeys)
	if err != nil {
		return errors.Trace(err)
	}
	for rowKey, val := range res {
		// when it's MV index, 2 index keys might point to the same row key.
		if h.rowKeyFilter.isHandledLocally(rowKey) {
			continue
		}
		handle := rowKeys2Handle[rowKey]
		if err := h.encodeAndHandleRow(ctx, tidbkv.Key(rowKey), handle, val.Value); err != nil {
			return errors.Trace(err)
		}

		// every conflicted row from data KV group must be recorded, but for index KV
		// group, they might come from the same row, so we only need to record it on
		// the first time we meet it.
		// currently, we use memory to do this check, if it's too large, we just skip
		// the checking and skip later checksum.
		//
		// an alternative solution is to upload those row keys to sort storage and
		// check them in another pass later.
		h.rowKeyFilter.addLocal(rowKey)
	}
	h.bufferedRows = h.bufferedRows[:0]
	return nil
}

// Close implements Handler interface.
func (h *IndexKVHandler) Close(ctx context.Context) error {
	var firstErr common.OnceError
	firstErr.Set(h.handleBufferedHandles(ctx))
	firstErr.Set(h.BaseHandler.Close(ctx))
	return firstErr.Get()
}

// LazyRefreshedSnapshot is a snapshot that refreshes its version lazily.
// exported for test.
type LazyRefreshedSnapshot struct {
	tidbkv.Snapshot
	store           tidbkv.Storage
	lastRefreshTime time.Time
	trafficRec      TrafficRecorder
}

// NewLazyRefreshedSnapshot creates a new LazyRefreshedSnapshot.
// exported for test.
func NewLazyRefreshedSnapshot(store tidbkv.Storage, rec TrafficRecorder) *LazyRefreshedSnapshot {
	return &LazyRefreshedSnapshot{
		store:      store,
		trafficRec: rec,
	}
}

func (s *LazyRefreshedSnapshot) refreshAsNeeded() error {
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

// BatchGet implements Snapshot interface.
func (s *LazyRefreshedSnapshot) BatchGet(
	ctx context.Context,
	keys []tidbkv.Key,
	options ...tidbkv.BatchGetOption,
) (map[string]tidbkv.ValueEntry, error) {
	if err := s.refreshAsNeeded(); err != nil {
		return nil, errors.Trace(err)
	}
	res, err := s.Snapshot.BatchGet(ctx, keys, options...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if s.trafficRec != nil {
		var readBytes uint64
		for k, v := range res {
			readBytes += uint64(len(k) + len(v.Value))
		}
		s.trafficRec.IncClusterReadBytes(readBytes)
	}
	return res, nil
}
