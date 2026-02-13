package mvs

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
	fetchAllTiDBMLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error)
	fetchAllTiDBMViews(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error)
}

// MVTaskHandler is a convenience interface that implements both refresh and purge.
type MVTaskHandler interface {
	MVRefreshHandler
	MVLogPurgeHandler
	MVMetaFetchHandler
}
