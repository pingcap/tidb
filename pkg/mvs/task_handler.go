package utils

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrMVRefreshHandlerNotRegistered means refresh logic has not been wired in yet.
	ErrMVRefreshHandlerNotRegistered = errors.New("mv refresh handler is not registered")
	// ErrMVLogPurgeHandlerNotRegistered means purge logic has not been wired in yet.
	ErrMVLogPurgeHandlerNotRegistered = errors.New("mvlog purge handler is not registered")
)

// MVRefreshHandler defines the refresh contract for one MV ID.
type MVRefreshHandler interface {
	RefreshMV(ctx context.Context, mvID string) (relatedMVLog []string, nextRefresh time.Time, err error)
}

// MVLogPurgeHandler defines the purge contract for one MVLog ID.
type MVLogPurgeHandler interface {
	PurgeMVLog(ctx context.Context, mvLogID string) (nextPurge time.Time, err error)
}

// MVTaskHandler is a convenience interface that implements both refresh and purge.
type MVTaskHandler interface {
	MVRefreshHandler
	MVLogPurgeHandler
}

type noopMVTaskHandler struct{}

func (noopMVTaskHandler) RefreshMV(_ context.Context, _ string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	return nil, time.Time{}, ErrMVRefreshHandlerNotRegistered
}

func (noopMVTaskHandler) PurgeMVLog(_ context.Context, _ string) (nextPurge time.Time, err error) {
	return time.Time{}, ErrMVLogPurgeHandlerNotRegistered
}
