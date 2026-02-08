package utils

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/stretchr/testify/require"
)

type mockSessionPool struct{}

func (mockSessionPool) Get() (pools.Resource, error) { return nil, nil }
func (mockSessionPool) Put(pools.Resource)           {}
func (mockSessionPool) Close()                       {}

type mockTaskHandlerServerHelper struct{}

func (mockTaskHandlerServerHelper) serverFilter(serverInfo) bool { return true }
func (mockTaskHandlerServerHelper) getServerInfo() (serverInfo, error) {
	return serverInfo{ID: "test-server"}, nil
}
func (mockTaskHandlerServerHelper) getAllServerInfo(context.Context) (map[string]serverInfo, error) {
	return map[string]serverInfo{"test-server": {ID: "test-server"}}, nil
}

type mockMVServiceHelper struct {
	mockTaskHandlerServerHelper
	taskHandler MVTaskHandler
}

func (m mockMVServiceHelper) RefreshMV(ctx context.Context, mvID string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	return m.taskHandler.RefreshMV(ctx, mvID)
}

func (m mockMVServiceHelper) PurgeMVLog(ctx context.Context, mvLogID string) (nextPurge time.Time, err error) {
	return m.taskHandler.PurgeMVLog(ctx, mvLogID)
}

type mockMVTaskHandler struct {
	refreshRelated []string
	refreshNext    time.Time
	purgeNext      time.Time
	refreshErr     error
	purgeErr       error

	lastRefreshID string
	lastPurgeID   string
}

func (m *mockMVTaskHandler) RefreshMV(_ context.Context, mvID string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	m.lastRefreshID = mvID
	return m.refreshRelated, m.refreshNext, m.refreshErr
}

func (m *mockMVTaskHandler) PurgeMVLog(_ context.Context, mvLogID string) (nextPurge time.Time, err error) {
	m.lastPurgeID = mvLogID
	return m.purgeNext, m.purgeErr
}

func TestMVServiceDefaultTaskHandler(t *testing.T) {
	svc := NewMVJobsManager(mockSessionPool{}, mockMVServiceHelper{taskHandler: noopMVTaskHandler{}})
	defer svc.Close()

	_, _, err := svc.executeMVRefresh(context.Background(), &mv{ID: "mv-1"})
	require.ErrorIs(t, err, ErrMVRefreshHandlerNotRegistered)

	_, err = svc.executeMVLogPurge(context.Background(), &mvLog{ID: "mlog-1"})
	require.ErrorIs(t, err, ErrMVLogPurgeHandlerNotRegistered)
}

func TestMVServiceUseInjectedTaskHandler(t *testing.T) {
	nextRefresh := time.Now().Add(time.Minute).Round(0)
	nextPurge := time.Now().Add(2 * time.Minute).Round(0)
	handler := &mockMVTaskHandler{
		refreshRelated: []string{"mlog-a", "mlog-b"},
		refreshNext:    nextRefresh,
		purgeNext:      nextPurge,
	}
	svc := NewMVJobsManager(mockSessionPool{}, mockMVServiceHelper{taskHandler: handler})
	defer svc.Close()

	related, gotNextRefresh, err := svc.executeMVRefresh(context.Background(), &mv{ID: "mv-2"})
	require.NoError(t, err)
	require.Equal(t, []string{"mlog-a", "mlog-b"}, related)
	require.True(t, nextRefresh.Equal(gotNextRefresh))
	require.Equal(t, "mv-2", handler.lastRefreshID)

	gotNextPurge, err := svc.executeMVLogPurge(context.Background(), &mvLog{ID: "mlog-2"})
	require.NoError(t, err)
	require.True(t, nextPurge.Equal(gotNextPurge))
	require.Equal(t, "mlog-2", handler.lastPurgeID)
}
