// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	spfreshStatusBatchPath             = "/spfresh/status/batch"
	maxSPFreshStatusBatchIndexes       = 4096
	maxSPFreshStatusBatchResponseBytes = 4 << 20 // 4MiB
	spfreshIndexStateUnknown           = "UNKNOWN"
	spfreshIndexStateReady             = "READY"
)

var spfreshStatusEndpointTimeout = 3 * time.Second

type spfreshIndexStatusBaseRow struct {
	tableSchema    string
	tableName      string
	indexName      string
	tableID        int64
	indexID        int64
	dimensions     uint64
	distanceMetric string
}

type spfreshStatusBatchRequest struct {
	KeyspaceID uint32                    `json:"keyspace_id"`
	Indexes    []spfreshStatusBatchIndex `json:"indexes"`
}

type spfreshStatusBatchIndex struct {
	TableID int64 `json:"table_id"`
	IndexID int64 `json:"index_id"`
}

type spfreshStatusBatchResponse struct {
	Statuses []spfreshStatusBatchStatus `json:"statuses"`
}

type spfreshStatusBatchStatus struct {
	KeyspaceID         uint32  `json:"keyspace_id"`
	TableID            int64   `json:"table_id"`
	IndexID            int64   `json:"index_id"`
	Dimensions         uint64  `json:"dimensions"`
	DistanceMetric     string  `json:"distance_metric"`
	AppliedBaseTS      uint64  `json:"applied_base_ts"`
	LagSeconds         *uint64 `json:"lag_seconds"`
	OwnerLeaseExpireTS *uint64 `json:"owner_lease_expire_ts"`
}

type spfreshIndexStatusKey struct {
	tableID int64
	indexID int64
}

func isSPFreshIndexStatusTarget(tbl *model.TableInfo, idx *model.IndexInfo) bool {
	return tbl != nil &&
		tbl.State == model.StatePublic &&
		idx != nil &&
		idx.State == model.StatePublic &&
		idx.IsSPFreshIndex()
}

func (e *memtableRetriever) setDataFromSPFreshIndexStatus(ctx context.Context, sctx sessionctx.Context) error {
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaTiDBIndexUsageExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaTiDBIndexUsageExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}

	keyspaceID := uint32(sctx.GetStore().GetCodec().GetKeyspaceID())
	schemas, tbls, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}

	baseRows := make([]spfreshIndexStatusBaseRow, 0, 16)
	statusIndexes := make([]spfreshStatusBatchIndex, 0, 16)
	for i, tbl := range tbls {
		schema := schemas[i]
		if checker != nil && !checker.RequestVerification(
			sctx.GetSessionVars().ActiveRoles,
			schema.L, tbl.Name.L, "", mysql.AllPrivMask) {
			continue
		}

		for _, idx := range ex.ListIndexes(tbl) {
			if !isSPFreshIndexStatusTarget(tbl, idx) {
				continue
			}
			distanceMetric, ok := model.SPFreshDistanceMetricAPIString(idx.VectorInfo.DistanceMetric)
			if !ok {
				return errors.Errorf("unknown vector distance metric %q", idx.VectorInfo.DistanceMetric)
			}
			baseRows = append(baseRows, spfreshIndexStatusBaseRow{
				tableSchema:    schema.O,
				tableName:      tbl.Name.O,
				indexName:      idx.Name.O,
				tableID:        tbl.ID,
				indexID:        idx.ID,
				dimensions:     idx.VectorInfo.Dimension,
				distanceMetric: distanceMetric,
			})
			statusIndexes = append(statusIndexes, spfreshStatusBatchIndex{
				TableID: tbl.ID,
				IndexID: idx.ID,
			})
		}
	}

	if len(baseRows) == 0 {
		e.rows = nil
		return nil
	}

	observedAt := time.Now()
	statuses, fetchErr := fetchSPFreshStatusBatch(ctx, sctx, keyspaceID, statusIndexes)
	if fetchErr != nil {
		return fetchErr
	}

	rows := make([][]types.Datum, 0, len(baseRows))
	for _, baseRow := range baseRows {
		statusKey := spfreshIndexStatusKey{
			tableID: baseRow.tableID,
			indexID: baseRow.indexID,
		}
		status, ok := statuses[statusKey]
		if ok && !spfreshStatusMatchesBaseRow(baseRow, status) {
			logutil.BgLogger().Warn("ignore spfresh status row with mismatched config",
				zap.Uint32("keyspace_id", keyspaceID),
				zap.Int64("table_id", baseRow.tableID),
				zap.Int64("index_id", baseRow.indexID),
				zap.Uint64("expected_dimensions", baseRow.dimensions),
				zap.Uint64("actual_dimensions", status.Dimensions),
				zap.String("expected_distance_metric", baseRow.distanceMetric),
				zap.String("actual_distance_metric", status.DistanceMetric))
			status = spfreshStatusBatchStatus{}
		} else if ok && (status.AppliedBaseTS == 0 || status.LagSeconds == nil) {
			logutil.BgLogger().Warn("ignore incomplete spfresh status row",
				zap.Uint32("keyspace_id", keyspaceID),
				zap.Int64("table_id", baseRow.tableID),
				zap.Int64("index_id", baseRow.indexID))
			status = spfreshStatusBatchStatus{}
		}
		row := buildSPFreshIndexStatusRow(keyspaceID, baseRow, status, observedAt)
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func fetchSPFreshStatusBatch(
	ctx context.Context,
	sctx sessionctx.Context,
	keyspaceID uint32,
	indexes []spfreshStatusBatchIndex,
) (map[spfreshIndexStatusKey]spfreshStatusBatchStatus, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	endpoints, err := spfreshRouter.statusEndpointOrder()
	if err != nil {
		return nil, errors.Trace(err)
	}

	clusterID, err := getSPFreshClusterID(ctx, sctx)
	if err != nil {
		return nil, err
	}

	statuses := make(map[spfreshIndexStatusKey]spfreshStatusBatchStatus, len(indexes))
	duplicateKeys := make(map[spfreshIndexStatusKey]struct{})
	for start := 0; start < len(indexes); start += maxSPFreshStatusBatchIndexes {
		end := min(start+maxSPFreshStatusBatchIndexes, len(indexes))
		batchStatuses, err := fetchSPFreshStatusBatchOnce(ctx, endpoints, clusterID, keyspaceID, indexes[start:end])
		if err != nil {
			return nil, err
		}
		mergeSPFreshStatusMaps(keyspaceID, statuses, duplicateKeys, batchStatuses)
	}
	return statuses, nil
}

func fetchSPFreshStatusBatchOnce(
	ctx context.Context,
	endpoints []*spfreshEndpointState,
	clusterID uint64,
	keyspaceID uint32,
	indexes []spfreshStatusBatchIndex,
) (map[spfreshIndexStatusKey]spfreshStatusBatchStatus, error) {
	body, err := json.Marshal(&spfreshStatusBatchRequest{
		KeyspaceID: keyspaceID,
		Indexes:    indexes,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	client := *util.InternalHTTPClient()
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	var lastWorkerErr error
	for _, endpoint := range endpoints {
		url := fmt.Sprintf("%s?cluster_id=%d", util.ComposeURL(endpoint.endpoint, spfreshStatusBatchPath), clusterID)
		reqCtx, cancel := context.WithTimeout(ctx, spfreshStatusEndpointTimeout)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(httpReq)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, errors.Trace(err)
			}
			endpoint.markFailure(time.Now())
			lastWorkerErr = errors.Trace(err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshErrorBodyBytes))
			resp.Body.Close()
			cancel()
			if isRetryableSPFreshWorkerStatus(resp.StatusCode) {
				endpoint.markFailure(time.Now())
				lastWorkerErr = errors.Errorf("spfresh %s failed on %s: http %d: %s",
					spfreshStatusBatchPath, endpoint.endpoint, resp.StatusCode, strings.TrimSpace(string(msg)))
				continue
			}
			if resp.StatusCode >= http.StatusInternalServerError {
				endpoint.markFailure(time.Now())
			}
			return nil, errors.Errorf("spfresh %s failed on %s: http %d: %s",
				spfreshStatusBatchPath, endpoint.endpoint, resp.StatusCode, strings.TrimSpace(string(msg)))
		}

		respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxSPFreshStatusBatchResponseBytes+1))
		resp.Body.Close()
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, errors.Trace(err)
			}
			endpoint.markFailure(time.Now())
			lastWorkerErr = errors.Trace(err)
			continue
		}
		if len(respBody) > maxSPFreshStatusBatchResponseBytes {
			endpoint.markFailure(time.Now())
			lastWorkerErr = errors.Errorf("spfresh %s response exceeds %d bytes",
				spfreshStatusBatchPath, maxSPFreshStatusBatchResponseBytes)
			continue
		}

		var payload spfreshStatusBatchResponse
		if err := json.Unmarshal(respBody, &payload); err != nil {
			endpoint.markFailure(time.Now())
			lastWorkerErr = errors.Trace(err)
			continue
		}
		endpoint.markSuccess()
		return spfreshStatusMap(keyspaceID, payload.Statuses), nil
	}

	logutil.BgLogger().Warn("collect spfresh index status degraded to UNKNOWN after worker failures",
		zap.Uint32("keyspace_id", keyspaceID),
		zap.Int("endpoint_count", len(endpoints)),
		zap.Error(lastWorkerErr))
	return nil, nil
}

func mergeSPFreshStatusMaps(
	keyspaceID uint32,
	statuses map[spfreshIndexStatusKey]spfreshStatusBatchStatus,
	duplicateKeys map[spfreshIndexStatusKey]struct{},
	batchStatuses map[spfreshIndexStatusKey]spfreshStatusBatchStatus,
) {
	for key, status := range batchStatuses {
		if _, duplicated := duplicateKeys[key]; duplicated {
			continue
		}
		if _, exists := statuses[key]; exists {
			logutil.BgLogger().Warn("ignore duplicate spfresh status rows",
				zap.Uint32("keyspace_id", keyspaceID),
				zap.Int64("table_id", key.tableID),
				zap.Int64("index_id", key.indexID))
			delete(statuses, key)
			duplicateKeys[key] = struct{}{}
			continue
		}
		statuses[key] = status
	}
}

func spfreshStatusMap(keyspaceID uint32, batchStatuses []spfreshStatusBatchStatus) map[spfreshIndexStatusKey]spfreshStatusBatchStatus {
	statuses := make(map[spfreshIndexStatusKey]spfreshStatusBatchStatus, len(batchStatuses))
	duplicateKeys := make(map[spfreshIndexStatusKey]struct{})
	for _, status := range batchStatuses {
		if status.KeyspaceID != keyspaceID {
			logutil.BgLogger().Warn("ignore spfresh status row with mismatched keyspace",
				zap.Uint32("expected_keyspace_id", keyspaceID),
				zap.Uint32("actual_keyspace_id", status.KeyspaceID),
				zap.Int64("table_id", status.TableID),
				zap.Int64("index_id", status.IndexID))
			continue
		}
		key := spfreshIndexStatusKey{
			tableID: status.TableID,
			indexID: status.IndexID,
		}
		if _, duplicated := duplicateKeys[key]; duplicated {
			continue
		}
		if _, exists := statuses[key]; exists {
			logutil.BgLogger().Warn("ignore duplicate spfresh status rows",
				zap.Uint32("keyspace_id", keyspaceID),
				zap.Int64("table_id", status.TableID),
				zap.Int64("index_id", status.IndexID))
			delete(statuses, key)
			duplicateKeys[key] = struct{}{}
			continue
		}
		statuses[key] = status
	}
	return statuses
}

func isRetryableSPFreshWorkerStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

func spfreshStatusMatchesBaseRow(
	baseRow spfreshIndexStatusBaseRow,
	status spfreshStatusBatchStatus,
) bool {
	return status.Dimensions == baseRow.dimensions &&
		strings.EqualFold(status.DistanceMetric, baseRow.distanceMetric)
}

func buildSPFreshIndexStatusRow(
	keyspaceID uint32,
	baseRow spfreshIndexStatusBaseRow,
	status spfreshStatusBatchStatus,
	observedAt time.Time,
) []types.Datum {
	indexState := spfreshIndexStateUnknown
	appliedBaseTS := any(nil)
	lagSeconds := any(nil)
	ownerLeaseExpireTS := any(nil)
	isReady := int64(0)
	if status.AppliedBaseTS != 0 {
		indexState = spfreshIndexStateReady
		isReady = 1
		appliedBaseTS = status.AppliedBaseTS
		lagSeconds = *status.LagSeconds
		if status.OwnerLeaseExpireTS != nil {
			ownerLeaseExpireTS = *status.OwnerLeaseExpireTS
		}
	}

	observedTime := types.NewTime(types.FromGoTime(observedAt), mysql.TypeTimestamp, types.MaxFsp)
	return types.MakeDatums(
		baseRow.tableSchema,
		baseRow.tableName,
		baseRow.indexName,
		uint64(keyspaceID),
		baseRow.tableID,
		baseRow.indexID,
		baseRow.dimensions,
		baseRow.distanceMetric,
		appliedBaseTS,
		lagSeconds,
		indexState,
		isReady,
		ownerLeaseExpireTS,
		observedTime,
	)
}
