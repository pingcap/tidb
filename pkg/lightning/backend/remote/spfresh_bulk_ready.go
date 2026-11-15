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

package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
)

const spfreshBulkReadyPath = "/spfresh/bulk-ready"

// SPFreshBulkReadyRequest identifies the imported index and its fixed MVP
// configuration before publication.
type SPFreshBulkReadyRequest struct {
	HandleColumn       *pkdbspfreshbulk.HandleColumn  `json:"handle_column,omitempty"`
	StoredColumns      []pkdbspfreshbulk.StoredColumn `json:"stored_columns,omitempty"`
	BuildID            string                         `json:"build_id"`
	KeyspaceID         uint32                         `json:"keyspace_id"`
	TableID            int64                          `json:"table_id"`
	IndexID            int64                          `json:"index_id"`
	SourceSnapshotTS   uint64                         `json:"source_snapshot_ts"`
	Dimension          uint64                         `json:"dimension"`
	Distance           string                         `json:"distance"`
	AlgorithmSeed      uint64                         `json:"algorithm_seed"`
	VectorColumnID     int64                          `json:"vector_column_id"`
	BulkImportTS       uint64                         `json:"bulk_import_ts"`
	MaxPartitionKey    uint64                         `json:"max_partition_key"`
	PartitionCount     uint64                         `json:"partition_count"`
	LeafPartitionCount uint64                         `json:"leaf_partition_count"`
	LeafEntryCount     uint64                         `json:"leaf_entry_count"`
	RootLevel          uint32                         `json:"root_level"`
}

// SPFreshBulkReadyResponse reports the committed Ready timestamp and decoded
// root summary.
type SPFreshBulkReadyResponse struct {
	BuildID          string `json:"build_id"`
	SourceSnapshotTS uint64 `json:"source_snapshot_ts"`
	IndexTS          uint64 `json:"index_ts"`
	RootLevel        uint32 `json:"root_level"`
	RootIsLeaf       bool   `json:"root_is_leaf"`
}

// CommitSPFreshBulkReady validates the imported root and publishes Ready(B)
// with one request. Redirects are rejected and the request is never retried.
func CommitSPFreshBulkReady(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	req SPFreshBulkReadyRequest,
) (SPFreshBulkReadyResponse, error) {
	if httpClient == nil {
		return SPFreshBulkReadyResponse{}, errors.New("spfresh bulk Ready requires http client")
	}
	workerURL = strings.TrimSuffix(workerURL, "/")
	if workerURL == "" {
		return SPFreshBulkReadyResponse{}, errors.New("spfresh bulk Ready requires worker URL")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return SPFreshBulkReadyResponse{}, errors.Trace(err)
	}
	resp, err := sendOneShotSPFreshBulkRequest(
		ctx,
		httpClient,
		http.MethodPost,
		workerURL+spfreshBulkReadyPath,
		body,
		map[string]string{"Content-Type": "application/json"},
	)
	if err != nil {
		return SPFreshBulkReadyResponse{}, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return SPFreshBulkReadyResponse{}, spfreshControlRequestError(resp, "publish bulk Ready checkpoint")
	}
	defer resp.Body.Close()
	var result SPFreshBulkReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return SPFreshBulkReadyResponse{}, errors.Annotate(err, "decode bulk Ready response")
	}
	if result.BuildID != req.BuildID || result.SourceSnapshotTS != req.SourceSnapshotTS ||
		result.IndexTS != req.BulkImportTS || result.RootLevel != req.RootLevel ||
		result.RootLevel == 0 || result.RootIsLeaf != (req.RootLevel == 1) {
		return SPFreshBulkReadyResponse{}, errors.New("SPFresh bulk Ready response does not match request")
	}
	return result, nil
}
