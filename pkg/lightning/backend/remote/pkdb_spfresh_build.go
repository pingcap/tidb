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

package remote

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	maxSPFreshBuildChunkBytes    = 16 * 1024 * 1024
	spfreshBuildRecordHeaderSize = 2
	spfreshBuildShardInitLimit   = 16
)

type spfreshIndexRequest struct {
	KeyspaceID     uint32                           `json:"keyspace_id"`
	TableID        int64                            `json:"table_id"`
	IndexID        int64                            `json:"index_id"`
	Dims           uint64                           `json:"dims"`
	DistanceMetric string                           `json:"distance_metric"`
	Seed           uint64                           `json:"seed"`
	VectorColumnID int64                            `json:"vector_column_id"`
	StoredColumns  []spfreshStoredColumn            `json:"stored_columns"`
	HandleColumn   *spfreshHandleColumn             `json:"handle_column"`
	BaseSnapshotTS uint64                           `json:"base_snapshot_ts"`
	IndexOptions   *spfreshStaticIndexOptions       `json:"index_options,omitempty"`
	RuntimeConfig  model.SPFreshVectorRuntimeConfig `json:"runtime_config"`
}

type spfreshBuildInitRequest struct {
	BuildID  string   `json:"build_id"`
	ShardIDs []string `json:"shard_ids"`
	spfreshIndexRequest
}

type spfreshStoredColumn = pkdbspfreshbulk.StoredColumn
type spfreshHandleColumn = pkdbspfreshbulk.HandleColumn
type spfreshJSONBytes = pkdbspfreshbulk.JSONBytes

func spfreshStoredColumnsToAPI(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) ([]spfreshStoredColumn, error) {
	storedColumns, err := model.ResolveSPFreshStoredColumns(tblInfo, idxInfo)
	if err != nil {
		return nil, err
	}
	if len(storedColumns) == 0 {
		return []spfreshStoredColumn{}, nil
	}
	defaultCtx := metabuild.NewNonStrictContext().GetExprCtx()
	cols := make([]spfreshStoredColumn, 0, len(storedColumns))
	for _, colInfo := range storedColumns {
		fieldType, err := spfreshFieldTypeToJSONBytes(
			&colInfo.FieldType,
			fmt.Sprintf("SPFresh stored column %d field_type", colInfo.ID),
		)
		if err != nil {
			return nil, err
		}
		originDefaultValue, err := spfreshOriginDefaultValueToAPI(defaultCtx, colInfo)
		if err != nil {
			return nil, err
		}
		cols = append(cols, spfreshStoredColumn{
			ColumnID:           colInfo.ID,
			FieldType:          fieldType,
			OriginDefaultValue: originDefaultValue,
		})
	}
	return cols, nil
}

func spfreshOriginDefaultValueToAPI(ctx expression.BuildContext, colInfo *model.ColumnInfo) (*spfreshJSONBytes, error) {
	if colInfo.GetOriginDefaultValue() == nil {
		if mysql.HasNotNullFlag(colInfo.GetFlag()) {
			return nil, nil
		}
		encoded := spfreshJSONBytes{codec.NilFlag}
		return &encoded, nil
	}
	defaultValue, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx, colInfo)
	if err != nil {
		return nil, errors.Annotatef(err, "get SPFresh stored column %s origin default", colInfo.Name.O)
	}
	encoded, err := codec.EncodeValue(ctx.GetEvalCtx().Location(), nil, defaultValue)
	if err != nil {
		return nil, errors.Annotatef(err, "encode SPFresh stored column %s origin default", colInfo.Name.O)
	}
	value := spfreshJSONBytes(encoded)
	return &value, nil
}

func spfreshHandleColumnToAPI(tblInfo *model.TableInfo) (*spfreshHandleColumn, error) {
	if tblInfo == nil {
		return nil, errors.New("table info is not set")
	}
	handleColID, handleFieldType, ok := model.SPFreshHandleColumnInfo(tblInfo)
	if !ok {
		if tblInfo.PKIsHandle {
			return nil, errors.New("SPFresh PKIsHandle table has no primary-key column")
		}
		return nil, nil
	}
	if !mysql.IsIntegerType(handleFieldType.GetType()) {
		return nil, errors.Errorf("SPFresh handle column %d must have an integer field_type", handleColID)
	}
	fieldType, err := spfreshFieldTypeToJSONBytes(&handleFieldType, "SPFresh handle column field_type")
	if err != nil {
		return nil, err
	}
	return &spfreshHandleColumn{
		ColumnID:  handleColID,
		FieldType: fieldType,
	}, nil
}

func spfreshFieldTypeToJSONBytes(fieldType *types.FieldType, label string) (spfreshJSONBytes, error) {
	encoded, err := proto.Marshal(expression.ToPBFieldType(fieldType))
	if err != nil {
		return nil, errors.Annotatef(err, "marshal %s", label)
	}
	if len(encoded) == 0 {
		return nil, errors.Errorf("%s is empty", label)
	}
	return spfreshJSONBytes(encoded), nil
}

func initializeSPFreshBuildShard(
	ctx context.Context,
	httpClient *http.Client,
	logger log.Logger,
	workerURL string,
	clusterID uint64,
	taskID string,
	body []byte,
) error {
	url := fmt.Sprintf("%s/spfresh/build?cluster_id=%d&task_id=%s", workerURL, clusterID, taskID)
	resp, err := sendLongOwnerBoundRequestWithRetryWithHeaders(
		ctx,
		httpClient,
		http.MethodPost,
		url,
		body,
		map[string]string{"Content-Type": "application/json"},
		nil,
	)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		msg, readErr := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshControlBodyBytes))
		_ = resp.Body.Close()
		if readErr == nil {
			if strings.TrimSpace(string(msg)) == taskExitsMsg {
				logger.Info("SPFresh build task already initialized in SPFresh build worker",
					zap.String("taskID", taskID),
					zap.String("worker addr", workerURL))
				return nil
			}
			if trimmed := strings.TrimSpace(string(msg)); trimmed != "" {
				logger.Error("failed to init spfresh build task",
					zap.String("taskID", taskID),
					zap.String("status", resp.Status),
					zap.ByteString("msg", msg))
				return dbterror.ErrIngestFailed.FastGenByArgs(trimmed)
			}
		}
		logger.Error("failed to init spfresh build task",
			zap.String("taskID", taskID),
			zap.String("status", resp.Status),
			zap.ByteString("msg", msg))
		return ErrInitTask
	}
	_ = resp.Body.Close()
	return nil
}

func isSPFreshBuildEngineConfig(cfg *backend.EngineConfig) bool {
	if cfg == nil || cfg.TableInfo == nil || cfg.TableInfo.Core == nil {
		return false
	}
	idxInfo := model.FindIndexInfoByID(cfg.TableInfo.Core.Indices, int64(cfg.EngineID))
	return idxInfo != nil && idxInfo.IsSPFreshIndex()
}

// SPFreshBuildCommitRequest is the metadata-only SPFresh build commit payload.
type SPFreshBuildCommitRequest struct {
	BuildID    string `json:"build_id"`
	KeyspaceID uint32 `json:"keyspace_id"`
	TableID    int64  `json:"table_id"`
	IndexID    int64  `json:"index_id"`
}

// CommitSPFreshBuildReady publishes Ready metadata without endpoint-local retry.
// Callers can use IsRetryableSPFreshBuildCommitError to try another worker.
func CommitSPFreshBuildReady(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	req SPFreshBuildCommitRequest,
) error {
	if req.BuildID == "" {
		return errors.New("SPFresh build commit build ID is not set")
	}
	body, err := json.Marshal(&req)
	if err != nil {
		return err
	}
	return sendOneShotSPFreshBuildCommitRequest(ctx, httpClient, workerURL, clusterID, body)
}

// DeleteSPFreshBuildTasksByPrefix deletes routed SPFresh build shard tasks
// whose task IDs have the given prefix.
func DeleteSPFreshBuildTasksByPrefix(ctx context.Context, httpClient *http.Client, workerURL string, clusterID uint64, taskIDPrefix string) error {
	if taskIDPrefix == "" {
		return errors.New("SPFresh build task ID prefix is not set")
	}
	_, err := sendSPFreshBuildDeleteRequest(ctx, httpClient, workerURL, clusterID, taskIDPrefix)
	return err
}

func (w *writer) prepareSPFreshBuildRecord(ctx context.Context, handle []byte, payloadLen int) error {
	if len(handle) > (1<<16 - 1) {
		return errors.Errorf("spfresh build handle length %d exceeds max uint16", len(handle))
	}
	recordSize := spfreshBuildRecordHeaderSize + len(handle) + payloadLen
	if recordSize > maxSPFreshBuildChunkBytes {
		return errors.Errorf("spfresh build record size %d exceeds max chunk size %d", recordSize, maxSPFreshBuildChunkBytes)
	}
	if len(w.buf) > 0 && len(w.buf)+recordSize > batchSize {
		if err := w.addChunk(ctx); err != nil {
			return err
		}
	}
	var lenBuf [2]byte
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(handle)))
	w.buf = append(w.buf, lenBuf[:]...)
	w.buf = append(w.buf, handle...)
	return nil
}

func (w *writer) appendSPFreshBuildRecord(ctx context.Context, handle, vectorPayload, storedPayload []byte) error {
	payloadLen := len(vectorPayload) + 4 + len(storedPayload)
	if err := w.prepareSPFreshBuildRecord(ctx, handle, payloadLen); err != nil {
		return err
	}
	var storedPayloadLen [4]byte
	binary.LittleEndian.PutUint32(storedPayloadLen[:], uint32(len(storedPayload)))
	w.buf = append(w.buf, vectorPayload...)
	w.buf = append(w.buf, storedPayloadLen[:]...)
	w.buf = append(w.buf, storedPayload...)
	return nil
}

// AppendSPFreshBuildRecord appends one encoded SPFresh build record without
// wrapping it in generic import KV rows.
func (w *writer) AppendSPFreshBuildRecord(ctx context.Context, handle, vectorPayload, storedPayload []byte) error {
	if w.e.taskType != taskTypeSPFreshBuild {
		return errors.New("SPFresh build record writer used for non-SPFresh build task")
	}
	return w.appendSPFreshBuildRecord(ctx, handle, vectorPayload, storedPayload)
}

// InitializeSPFreshBuild initializes every routed build shard before
// read-index subtasks start writing chunks.
func InitializeSPFreshBuild(
	ctx context.Context,
	httpClient *http.Client,
	clusterID uint64,
	tableInfo *model.TableInfo,
	routePlan *backend.SPFreshBuildRoutePlan,
) error {
	routes, err := routePlan.AllBuildShardOwners()
	if err != nil {
		return err
	}
	shardIDs := make([]string, len(routes))
	for i, route := range routes {
		shardIDs[i] = route.BuildShardID
	}
	sort.Strings(shardIDs)
	indexRequest, err := newSPFreshTableIndexRequest(
		routePlan.KeyspaceID,
		tableInfo,
		routePlan.IndexID,
		routePlan.BaseSnapshotTS,
	)
	if err != nil {
		return err
	}
	body, err := json.Marshal(&spfreshBuildInitRequest{
		BuildID:             routePlan.IndexBuildID(),
		ShardIDs:            shardIDs,
		spfreshIndexRequest: indexRequest,
	})
	if err != nil {
		return err
	}
	logger := log.L()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(min(len(routes), spfreshBuildShardInitLimit))
	for _, route := range routes {
		route := route
		g.Go(func() error {
			return initializeSPFreshBuildShard(
				gCtx,
				httpClient,
				logger,
				route.Owner,
				clusterID,
				route.BuildShardID,
				body,
			)
		})
	}
	return g.Wait()
}

// SPFreshBulkSchema resolves the same stored and handle schema used by routed builds.
func SPFreshBulkSchema(tbl *model.TableInfo, idx *model.IndexInfo) ([]pkdbspfreshbulk.StoredColumn, *pkdbspfreshbulk.HandleColumn, error) {
	columns, err := spfreshStoredColumnsToAPI(tbl, idx)
	if err != nil {
		return nil, nil, err
	}
	handle, err := spfreshHandleColumnToAPI(tbl)
	return columns, handle, err
}
