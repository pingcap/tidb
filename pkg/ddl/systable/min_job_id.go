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
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"go.uber.org/zap"
)

var (
	refreshInterval = 10 * time.Second
)

// MinJobIDRefresher is used to maintain the minimal job ID in tidb_ddl_job table.
// we use it to mitigate this issue https://github.com/pingcap/tidb/issues/52905
// by querying since min job ID, TiKV can seek to the position where rows exists
// to avoid scanning and skipping all the deleted rows.
type MinJobIDRefresher struct {
	sysTblMgr    Manager
	currMinJobID atomic.Int64
}

// NewMinJobIDRefresher creates a new MinJobIDRefresher.
func NewMinJobIDRefresher(sysTblMgr Manager) *MinJobIDRefresher {
	return &MinJobIDRefresher{
		sysTblMgr: sysTblMgr,
	}
}

// GetCurrMinJobID gets the minimal job ID in tidb_ddl_job table.
func (r *MinJobIDRefresher) GetCurrMinJobID() int64 {
	return r.currMinJobID.Load()
}

// Start refreshes the minimal job ID in tidb_ddl_job table.
func (r *MinJobIDRefresher) Start(ctx context.Context) {
	for {
		r.refresh(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(refreshInterval):
		}
	}
}

func (r *MinJobIDRefresher) refresh(ctx context.Context) {
	currMinID := r.currMinJobID.Load()
	nextMinID, err := r.sysTblMgr.GetMinJobID(ctx, currMinID)
	if err != nil {
		logutil.DDLLogger().Info("get min job ID failed", zap.Error(err))
		return
	}
	// use max, in case all job are finished to avoid the currMinJobID go back.
	r.currMinJobID.Store(max(currMinID, nextMinID))
}
