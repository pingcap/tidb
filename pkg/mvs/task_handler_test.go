package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	svc := NewMVJobsManager(nil, nil, noopMVTaskHandler{})
	defer svc.Close()

	_, _, err := svc.executeMVRefresh(context.Background(), &mv{ID: "mv-1"})
	require.ErrorIs(t, err, ErrMVRefreshHandlerNotRegistered)

	_, err = svc.executeMVLogPurge(context.Background(), &mvLog{ID: "mlog-1"})
	require.ErrorIs(t, err, ErrMVLogPurgeHandlerNotRegistered)
}

func TestMVServiceSetTaskHandler(t *testing.T) {
	svc := NewMVJobsManager(nil, nil, noopMVTaskHandler{})
	defer svc.Close()

	nextRefresh := time.Now().Add(time.Minute).Round(0)
	nextPurge := time.Now().Add(2 * time.Minute).Round(0)
	handler := &mockMVTaskHandler{
		refreshRelated: []string{"mlog-a", "mlog-b"},
		refreshNext:    nextRefresh,
		purgeNext:      nextPurge,
	}
	svc.SetTaskHandler(handler)

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
