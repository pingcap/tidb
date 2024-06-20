// Copyright 2024 PingCAP, Inc.
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

package systable

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"go.uber.org/zap"
)

var (
	refreshInterval = time.Second
)

// MinJobIDRefresher is used to maintain the minimal job ID in tidb_ddl_job table.
// we use it to mitigate this issue https://github.com/pingcap/tidb/issues/52905
type MinJobIDRefresher struct {
	sysTblMgr       Manager
	currMinJobID    int64
	lastRefreshTime time.Time
}

// NewMinJobIDRefresher creates a new MinJobIDRefresher.
func NewMinJobIDRefresher(sysTblMgr Manager) *MinJobIDRefresher {
	return &MinJobIDRefresher{
		sysTblMgr: sysTblMgr,
	}
}

// GetCurrMinJobID gets the minimal job ID in tidb_ddl_job table.
func (r *MinJobIDRefresher) GetCurrMinJobID() int64 {
	return r.currMinJobID
}

// Refresh refreshes the minimal job ID in tidb_ddl_job table.
func (r *MinJobIDRefresher) Refresh(ctx context.Context) {
	now := time.Now()
	if now.Sub(r.lastRefreshTime) < refreshInterval {
		return
	}
	r.lastRefreshTime = now
	minID, err := r.sysTblMgr.GetMinJobID(ctx, r.currMinJobID)
	if err != nil {
		logutil.DDLLogger().Info("get min job ID failed", zap.Error(err))
		return
	}
	// use max, in case all job are finished to avoid the currMinJobID go back.
	r.currMinJobID = max(r.currMinJobID, minID)
}
