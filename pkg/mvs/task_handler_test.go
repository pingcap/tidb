package mvs

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	basic "github.com/pingcap/tidb/pkg/util"
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
	refreshRelated []string
	refreshNext    time.Time
	purgeNext      time.Time
	refreshErr     error
	purgeErr       error

	lastRefreshID string
	lastPurgeID   string
}

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	m.lastRefreshID = mvID
	return m.refreshRelated, m.refreshNext, m.refreshErr
}

func (m *mockMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mvLogID string) (nextPurge time.Time, err error) {
	m.lastPurgeID = mvLogID
	return m.purgeNext, m.purgeErr
}

func (*mockMVServiceHelper) fetchAllTiDBMLogPurge(context.Context, basic.SessionPool) (map[string]*mvLog, error) {
	return nil, nil
}

func (*mockMVServiceHelper) fetchAllTiDBMViews(context.Context, basic.SessionPool) (map[string]*mv, error) {
	return nil, nil
}

func TestMVServiceDefaultTaskHandler(t *testing.T) {
	helper := &mockMVServiceHelper{
		refreshErr: ErrMVRefreshHandlerNotRegistered,
		purgeErr:   ErrMVLogPurgeHandlerNotRegistered,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)

	_, _, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, "mv-1")
	require.ErrorIs(t, err, ErrMVRefreshHandlerNotRegistered)

	_, err = svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-1")
	require.ErrorIs(t, err, ErrMVLogPurgeHandlerNotRegistered)
}

func TestMVServiceUseInjectedTaskHandler(t *testing.T) {
	nextRefresh := time.Now().Add(time.Minute).Round(0)
	nextPurge := time.Now().Add(2 * time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshRelated: []string{"mlog-a", "mlog-b"},
		refreshNext:    nextRefresh,
		purgeNext:      nextPurge,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)

	related, gotNextRefresh, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, "mv-2")
	require.NoError(t, err)
	require.Equal(t, []string{"mlog-a", "mlog-b"}, related)
	require.True(t, nextRefresh.Equal(gotNextRefresh))
	require.Equal(t, "mv-2", helper.lastRefreshID)

	gotNextPurge, err := svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-2")
	require.NoError(t, err)
	require.True(t, nextPurge.Equal(gotNextPurge))
	require.Equal(t, "mlog-2", helper.lastPurgeID)
}
