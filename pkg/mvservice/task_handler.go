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

package mvservice

import (
	"context"
	"errors"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
)

var (
	// ErrMVRefreshHandlerNotRegistered means refresh logic has not been wired in yet.
	ErrMVRefreshHandlerNotRegistered = errors.New("mv refresh handler is not registered")
	// ErrMVLogPurgeHandlerNotRegistered means purge logic has not been wired in yet.
	ErrMVLogPurgeHandlerNotRegistered = errors.New("mvlog purge handler is not registered")
)

// MVRefreshHandler defines the refresh contract for one MV ID.
type MVRefreshHandler interface {
	RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID int64) (nextRefresh time.Time, err error)
}

// MVLogPurgeHandler defines the purge contract for one MVLog ID.
type MVLogPurgeHandler interface {
	PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error)
}

// MVMetaFetchHandler defines the metadata fetch contract for MV scheduler bootstrap.
type MVMetaFetchHandler interface {
	fetchAllTiDBMVLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error)
	fetchAllTiDBMVRefresh(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error)
}

// MVHistoryGCHandler defines history cleanup contract for MV maintenance history tables.
type MVHistoryGCHandler interface {
	GetCurrentTSO(ctx context.Context, sysSessionPool basic.SessionPool) (uint64, error)
	PurgeMVHistoryBeforeTSO(ctx context.Context, sysSessionPool basic.SessionPool, cutoffTSO uint64) error
}

// MVTaskHandler is a convenience interface that implements both refresh and purge.
type MVTaskHandler interface {
	MVRefreshHandler
	MVLogPurgeHandler
	MVMetaFetchHandler
	MVHistoryGCHandler
}
