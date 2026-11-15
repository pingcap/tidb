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
	stdErrors "errors"
	"fmt"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// SPFreshBuildSealRequest binds a shard barrier to the immutable build manifest.
// Workers use this identity to acknowledge durable proof after local task loss.
type SPFreshBuildSealRequest struct {
	BuildID        string `json:"build_id"`
	KeyspaceID     uint32 `json:"keyspace_id"`
	TableID        int64  `json:"table_id"`
	IndexID        int64  `json:"index_id"`
	BaseSnapshotTS uint64 `json:"base_snapshot_ts"`
}

// SealSPFreshBuildShard waits for an SPFresh build task's durable sealed proof.
func SealSPFreshBuildShard(ctx context.Context, httpClient *http.Client, workerURL string, clusterID uint64, buildShardID string, req SPFreshBuildSealRequest) error {
	body, err := sendSPFreshBuildSealRequest(ctx, httpClient, workerURL, clusterID, buildShardID, req)
	if err != nil {
		var controlErr *SPFreshControlRequestError
		if stdErrors.As(err, &controlErr) && !controlErr.Retryable() {
			// DDL retries unknown Go errors even when the HTTP client stops.
			// Reorg checks retryable text before error codes. Keep arbitrary
			// worker diagnostics out of the normalized message, including after
			// DXF serialization. Retain them in Body and the diagnostic log.
			log.FromContext(ctx).Warn("SPFresh build seal rejected by worker",
				zap.String("taskID", buildShardID),
				zap.String("buildID", req.BuildID),
				zap.String("workerURL", workerURL),
				zap.Int("statusCode", controlErr.StatusCode),
				zap.String("body", controlErr.Body))
			terminal := *controlErr
			terminal.cause = dbterror.ErrIngestFailed.GenWithStackByArgs(
				fmt.Sprintf("SPFresh build seal failed (HTTP %d); see TiDB log for worker response", controlErr.StatusCode),
			)
			return errors.WithStack(&terminal)
		}
		return err
	}
	return validateSPFreshBuildSealedResponse(body, "seal")
}
