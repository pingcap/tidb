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

	"github.com/pingcap/tidb/pkg/meta/model"
)

const spfreshRuntimeConfigPath = "/spfresh/runtime-config"

type spfreshRuntimeConfigUpdateRequest struct {
	KeyspaceID uint32                           `json:"keyspace_id"`
	TableID    int64                            `json:"table_id"`
	IndexID    int64                            `json:"index_id"`
	Expected   model.SPFreshVectorRuntimeConfig `json:"expected"`
	Desired    model.SPFreshVectorRuntimeConfig `json:"desired"`
}

// UpdateSPFreshRuntimeConfig atomically changes an index's persisted runtime configuration.
func UpdateSPFreshRuntimeConfig(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	keyspaceID uint32,
	tableID int64,
	indexID int64,
	expected model.SPFreshVectorRuntimeConfig,
	desired model.SPFreshVectorRuntimeConfig,
) error {
	body, err := json.Marshal(&spfreshRuntimeConfigUpdateRequest{
		KeyspaceID: keyspaceID,
		TableID:    tableID,
		IndexID:    indexID,
		Expected:   expected,
		Desired:    desired,
	})
	if err != nil {
		return err
	}
	return postShortSPFreshControlRequest(ctx, httpClient, workerURL, clusterID, spfreshRuntimeConfigPath, body, map[string]string{
		"Content-Type": "application/json",
	}, "runtime config update")
}
