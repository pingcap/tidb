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
)

const spfreshBulkBuildingPath = "/spfresh/bulk-building"

type spfreshBulkBuildingRequest struct {
	BuildID          string `json:"build_id"`
	KeyspaceID       uint32 `json:"keyspace_id"`
	TableID          int64  `json:"table_id"`
	IndexID          int64  `json:"index_id"`
	SourceSnapshotTS uint64 `json:"source_snapshot_ts"`
}

// InitSPFreshBulkBuilding writes the one Building(B) checkpoint used by the
// MVP bulk path. The request is sent exactly once and redirects are rejected.
func InitSPFreshBulkBuilding(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	buildID string,
	keyspaceID uint32,
	tableID int64,
	indexID int64,
	sourceSnapshotTS uint64,
) error {
	if httpClient == nil {
		return errors.New("spfresh bulk Building requires http client")
	}
	workerURL = strings.TrimSuffix(workerURL, "/")
	if workerURL == "" {
		return errors.New("spfresh bulk Building requires worker URL")
	}
	body, err := json.Marshal(spfreshBulkBuildingRequest{
		BuildID:          buildID,
		KeyspaceID:       keyspaceID,
		TableID:          tableID,
		IndexID:          indexID,
		SourceSnapshotTS: sourceSnapshotTS,
	})
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := sendOneShotSPFreshBulkRequest(
		ctx,
		httpClient,
		http.MethodPost,
		workerURL+spfreshBulkBuildingPath,
		body,
		map[string]string{"Content-Type": "application/json"},
	)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.StatusCode == http.StatusOK {
		_ = resp.Body.Close()
		return nil
	}
	return spfreshControlRequestError(resp, "initialize bulk Building checkpoint")
}
