// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func writeVectorIndexRecord(
	ctx context.Context,
	writer ingest.SPFreshBuildRecordWriter,
	vecInfo *model.VectorIndexInfo,
	idxData []types.Datum,
	handle kv.Handle,
	loc *time.Location,
	bufs *spfreshVectorRecordBuffers,
) error {
	// SPFresh remote `/spfresh/build` expects handle bytes plus a vector build
	// record payload, not a normal encoded index KV pair.
	if vecInfo == nil {
		return errors.New("vector index info is not set")
	}
	expectedIdxDataLen := 1 + len(vecInfo.StoredColumnIDs)
	if len(idxData) != expectedIdxDataLen {
		return errors.Errorf("invalid vector index columns length %d", len(idxData))
	}
	if idxData[0].IsNull() {
		return nil
	}
	vec := idxData[0].GetVectorFloat32()
	expectedDims := int(vecInfo.Dimension)
	if vec.Len() != expectedDims {
		return errors.Errorf("vector dimension mismatch, expected %d, got %d", expectedDims, vec.Len())
	}
	serialized := vec.ZeroCopySerialize()
	expectedLen := 4 + expectedDims*4
	if len(serialized) != expectedLen {
		return errors.Errorf("invalid vector encoding length %d, expected %d", len(serialized), expectedLen)
	}
	vectorPayload := serialized[4:]
	storedValues := idxData[1:]
	var storedPayload []byte
	if len(storedValues) > 0 {
		var err error
		storedPayload = bufs.storedPayload[:0]
		storedPayload, err = tablecodec.EncodeOldRow(
			loc,
			storedValues,
			vecInfo.StoredColumnIDs,
			storedPayload,
			bufs.storedValues[:len(storedValues)*2],
		)
		if err != nil {
			return errors.Trace(err)
		}
		bufs.storedPayload = storedPayload
	}
	if len(storedPayload) > spfreshMaxStoredPayloadBytes {
		return errors.Errorf("SPFresh stored payload length %d exceeds limit %d",
			len(storedPayload), spfreshMaxStoredPayloadBytes)
	}
	var encodedHandle []byte
	if handle.IsInt() {
		encodedHandle = codec.EncodeInt(bufs.encodedIntHandle[:0], handle.IntValue())
	} else {
		encodedHandle = handle.Encoded()
	}
	return errors.Trace(writer.WriteSPFreshBuildRecord(ctx, encodedHandle, vectorPayload, storedPayload))
}

type spfreshVectorRecordBuffers struct {
	storedPayload    []byte
	storedValues     []types.Datum
	encodedIntHandle [8]byte
}

func initializeSPFreshBuild(
	ctx context.Context,
	store kv.Storage,
	tableInfo *model.TableInfo,
	routePlan *backend.SPFreshBuildRoutePlan,
) error {
	failpoint.Inject("mockSPFreshBuildTransport", func() { failpoint.Return(nil) })
	targets, err := resolveRoutedSPFreshWorkerTargets(ctx, store, routePlan)
	if err != nil {
		return err
	}
	return remote.InitializeSPFreshBuild(
		ctx,
		util.InternalHTTPClient(),
		targets.clusterID,
		tableInfo,
		routePlan,
	)
}

func hasSPFreshIndex(indexes []*model.IndexInfo) bool {
	for _, idx := range indexes {
		if idx != nil && idx.IsSPFreshIndex() {
			return true
		}
	}
	return false
}

type spfreshBuildFinisher interface {
	FinishSPFreshBuild() (bool, error)
}

func (r *readIndexStepExecutor) finishRemoteSortReadIndex(
	subtask *proto.Subtask,
	sm *BackfillSubTaskMeta,
	backendCtx ingest.BackendCtx,
) error {
	finisher, ok := backendCtx.(spfreshBuildFinisher)
	if !ok {
		return errors.New("SPFresh build finisher is unavailable")
	}
	hasData, err := finisher.FinishSPFreshBuild()
	if err != nil {
		return errors.Trace(err)
	}
	sm.SPFreshBuildHasData = &hasData
	meta, err := sm.Marshal()
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func singleSPFreshReadIndex(indexes []*model.IndexInfo) (*model.IndexInfo, error) {
	if len(indexes) != 1 {
		return nil, errors.New("read-index SPFresh build metadata supports one SPFresh index and no other indexes")
	}
	index := indexes[0]
	if index == nil || !index.IsSPFreshIndex() {
		return nil, errors.New("read-index SPFresh build metadata requires an SPFresh index")
	}
	return index, nil
}

func spfreshBuildRecordWriters(writers []ingest.Writer, writePlan *indexWritePlan) ([]ingest.SPFreshBuildRecordWriter, error) {
	var spfreshWriters []ingest.SPFreshBuildRecordWriter
	for i, indexPlan := range writePlan.indexes {
		if indexPlan.spfreshVectorInfo == nil {
			continue
		}
		if spfreshWriters == nil {
			spfreshWriters = make([]ingest.SPFreshBuildRecordWriter, len(writers))
		}
		writer, ok := writers[i].(ingest.SPFreshBuildRecordWriter)
		if !ok {
			return nil, errors.Errorf("writer %T does not support SPFresh build records", writers[i])
		}
		spfreshWriters[i] = writer
	}
	return spfreshWriters, nil
}
