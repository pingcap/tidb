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
	"encoding/json"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/redact"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type conflictResolutionStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID               int64
	store                tidbkv.Storage
	taskMeta             *TaskMeta
	logger               *zap.Logger
	tableImporter        *importer.TableImporter
	conflictRowsChecksum *verification.KVChecksum
	conflictedRowCount   int64
}

var _ execute.StepExecutor = &conflictResolutionStepExecutor{}

func (e *conflictResolutionStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *conflictResolutionStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := ConflictResolutionStepMeta{}
	if err = json.Unmarshal(subtask.Meta, &stepMeta); err != nil {
		return errors.Trace(err)
	}
	e.conflictedRowCount = 0
	e.conflictRowsChecksum = verification.NewKVChecksumWithKeyspace(e.store.GetCodec().GetKeyspace())
	for kvGroup, ci := range stepMeta.ConflictInfos {
		if err = e.handleKVGroupConflicts(ctx, kvGroup, ci); err != nil {
			return err
		}
	}
	// TODO record conflicts to s3
	return nil
}

func (e *conflictResolutionStepExecutor) OnFinished(_ context.Context, subtask *proto.Subtask) error {
	subtaskMeta := &ConflictResolutionStepMeta{}
	if err := json.Unmarshal(subtask.Meta, subtaskMeta); err != nil {
		return errors.Trace(err)
	}
	subtaskMeta.Checksum = newFromKVChecksum(e.conflictRowsChecksum)
	subtaskMeta.ConflictedRowCount = e.conflictedRowCount
	newMeta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	subtask.Meta = newMeta
	return nil
}

func (e *conflictResolutionStepExecutor) handleKVGroupConflicts(ctx context.Context, kvGroup string, ci *common.ConflictInfo) (err error) {
	task := log.BeginTask(e.logger.With(
		zap.String("kvGroup", kvGroup),
		zap.Uint64("duplicates", ci.Count),
		zap.Int("file-count", len(ci.Files)),
	), "handle kv group conflicts")

	checksum := verification.NewKVChecksumWithKeyspace(e.store.GetCodec().GetKeyspace())
	var handler conflictKVHandler
	if kvGroup == dataKVGroup {
		handler = &conflictDataKVHandler{
			conflictResolutionStepExecutor: e,
			checksum:                       checksum,
		}
	} else {
		handler = &conflictIndexKVHandler{
			conflictDataKVHandler: &conflictDataKVHandler{
				conflictResolutionStepExecutor: e,
				checksum:                       checksum,
			},
		}
	}
	defer func() {
		conflictedRowCount := handler.getConflictedRowCount()
		e.conflictedRowCount += conflictedRowCount
		e.conflictRowsChecksum.Add(checksum)
		task.End(zapcore.ErrorLevel, err, zap.Stringer("conflictedRowsSum", checksum),
			zap.Int64("conflictedRowCount", conflictedRowCount))
	}()

	if err = handler.init(kvGroup); err != nil {
		return errors.Trace(err)
	}
	defer handler.close()

	for _, file := range ci.Files {
		if err = e.handleConflictFile(ctx, handler, file); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *conflictResolutionStepExecutor) handleConflictFile(ctx context.Context, handler conflictKVHandler, file string) (err error) {
	reader, err := external.NewKVReader(ctx, file, e.tableImporter.GlobalSortStore, 0, 3*external.DefaultReadBufferSize)
	if err != nil {
		return err
	}
	defer reader.Close()

	for {
		key, val, err := reader.NextKV()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err = handler.handle(ctx, key, val); err != nil {
			return err
		}
	}
	return nil
}

func (e *conflictResolutionStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}

type conflictKVHandler interface {
	init(kvGroup string) error
	handle(ctx context.Context, key, val []byte) error
	close() error
	getConflictedRowCount() int64
}

type conflictDataKVHandler struct {
	*conflictResolutionStepExecutor
	encoder *importer.TableKVEncoder
	// record the checksum of the all KVs of the conflicted row.
	checksum           *verification.KVChecksum
	conflictedRowCount int64
}

func (h *conflictDataKVHandler) init(string) error {
	encoder, err := h.tableImporter.GetKVEncoder(&checkpoints.ChunkCheckpoint{})
	if err != nil {
		return err
	}
	h.encoder = encoder
	return nil
}

func (h *conflictDataKVHandler) handle(ctx context.Context, key, val []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return err
	}
	return h.encodeAndDeleteRow(ctx, h.encoder, handle, val)
}

func (h *conflictDataKVHandler) getConflictedRowCount() int64 {
	return h.conflictedRowCount
}

// re-encode the row from the handle and value of data KV, and delete all encoded keys.
// it's possible that part or all of the keys are already deleted.
func (h *conflictDataKVHandler) encodeAndDeleteRow(ctx context.Context,
	encoder *importer.TableKVEncoder, handle tidbkv.Handle, val []byte) (err error) {
	tbl := h.tableImporter.Table
	tblMeta := tbl.Meta()
	decodedData, _, err := tables.DecodeRawRowData(encoder.SessionCtx,
		tblMeta, handle, tbl.Cols(), val)
	if err != nil {
		return errors.Trace(err)
	}
	if !tblMeta.HasClusteredIndex() {
		// for non-clustered PK, need to append handle
		decodedData = append(decodedData, types.NewIntDatum(handle.IntValue()))
	}
	_, err = encoder.Table.AddRecord(encoder.SessionCtx.GetTableCtx(), decodedData)
	if err != nil {
		return errors.Trace(err)
	}
	kvPairs := encoder.SessionCtx.TakeKvPairs()
	h.checksum.Update(kvPairs.Pairs)
	h.conflictedRowCount++

	err = h.deleteKeys(ctx, kvPairs.Pairs)
	kvPairs.Clear()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *conflictDataKVHandler) deleteKeys(ctx context.Context, pairs []common.KvPair) (err error) {
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

	for _, p := range pairs {
		if err = txn.Delete(p.Key); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (h *conflictDataKVHandler) close() error {
	return h.encoder.Close()
}

type conflictIndexKVHandler struct {
	*conflictDataKVHandler
	targetIdx *model.IndexInfo
	snapshot  tidbkv.Snapshot
}

func (h *conflictIndexKVHandler) init(kvGroup string) error {
	indexID, err := kvGroup2IndexID(kvGroup)
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

	// it's not necessary to update this version even though we will delete KVs
	// during conflict KV handing, as this handler is used to handle conflicts of
	// the same KV group, the data KVs corresponding to any 2 conflict KVs are
	// either conflicts with each other too and recorded in the conflict KV file,
	// or they are not conflicted and are either recorded or ingested, so for a
	// single data KV found in this handler cannot be deleted twice.
	ver, err := h.store.CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snapshot := h.store.GetSnapshot(ver)

	if err = h.conflictDataKVHandler.init(kvGroup); err != nil {
		return err
	}

	h.targetIdx = targetIdx
	h.snapshot = snapshot
	return nil
}

func (h *conflictIndexKVHandler) handle(ctx context.Context, key, val []byte) error {
	tableID := tablecodec.DecodeTableID(key)
	if tableID == 0 {
		// should not happen
		return errors.Errorf("invalid table ID in key %v", redact.Key(key))
	}
	handle, err := tablecodec.DecodeIndexHandle(key, val, len(h.targetIdx.Columns))
	if err != nil {
		return err
	}
	rowKey := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	val, err = h.snapshot.Get(ctx, rowKey)
	// either the data KV is deleted by handing conflicts in other KV group or the
	// data KV itself is conflicted and not ingested.
	if err != nil {
		if tidbkv.IsErrNotFound(err) || tikverr.IsErrNotFound(err) {
			return nil
		}
		return errors.Trace(err)
	}
	return h.encodeAndDeleteRow(ctx, h.encoder, handle, val)
}
