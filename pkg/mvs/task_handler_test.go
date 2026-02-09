package mvs

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type mockSessionPool struct{}

func (mockSessionPool) Get() (pools.Resource, error) { return nil, nil }
func (mockSessionPool) Put(pools.Resource)           {}
func (mockSessionPool) Close()                       {}

type recordingSessionPool struct {
	se pools.Resource
}

func (p recordingSessionPool) Get() (pools.Resource, error) { return p.se, nil }
func (recordingSessionPool) Put(pools.Resource)             {}
func (recordingSessionPool) Close()                         {}

type recordingSessionContext struct {
	*mock.Context
	executedSQL []string
	execErrs    map[string]error
}

func newRecordingSessionContext() *recordingSessionContext {
	return &recordingSessionContext{
		Context:  mock.NewContext(),
		execErrs: make(map[string]error),
	}
}

func (s *recordingSessionContext) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *recordingSessionContext) ExecuteInternal(_ context.Context, sql string, _ ...any) (sqlexec.RecordSet, error) {
	s.executedSQL = append(s.executedSQL, sql)
	if err, ok := s.execErrs[sql]; ok {
		return nil, err
	}
	return nil, nil
}

func (s *recordingSessionContext) Close() {}

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
	fetchLogs      map[string]*mvLog
	fetchViews     map[string]*mv
	fetchLogsErr   error
	fetchViewsErr  error
	fetchLogsCalls atomic.Int32
	fetchViewCalls atomic.Int32

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

func (m *mockMVServiceHelper) fetchAllTiDBMLogPurge(context.Context, basic.SessionPool) (map[string]*mvLog, error) {
	m.fetchLogsCalls.Add(1)
	if m.fetchLogsErr != nil {
		return nil, m.fetchLogsErr
	}
	return m.fetchLogs, nil
}

func (m *mockMVServiceHelper) fetchAllTiDBMViews(context.Context, basic.SessionPool) (map[string]*mv, error) {
	m.fetchViewCalls.Add(1)
	if m.fetchViewsErr != nil {
		return nil, m.fetchViewsErr
	}
	return m.fetchViews, nil
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

func TestMVServiceNotifyDDLChangeTriggersFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	helper := &mockMVServiceHelper{
		fetchLogs:  map[string]*mvLog{},
		fetchViews: map[string]*mv{},
	}
	svc := NewMVJobsManager(ctx, mockSessionPool{}, helper)
	svc.lastRefresh.Store(time.Now().UnixMilli())

	done := make(chan struct{})
	go func() {
		svc.Run()
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(0), helper.fetchLogsCalls.Load())
	require.Equal(t, int32(0), helper.fetchViewCalls.Load())

	svc.NotifyDDLChange()
	require.Eventually(t, func() bool {
		return helper.fetchLogsCalls.Load() > 0 && helper.fetchViewCalls.Load() > 0
	}, time.Second, 20*time.Millisecond)
}

func TestRegisterMVSBootstrapAndDDLHandler(t *testing.T) {
	called := atomic.Int32{}
	var (
		gotHandlerID notifier.HandlerID
		gotHandler   notifier.SchemaChangeHandler
	)
	svc := RegisterMVS(context.Background(), func(id notifier.HandlerID, handler notifier.SchemaChangeHandler) {
		gotHandlerID = id
		gotHandler = handler
	}, mockSessionPool{}, func() {
		called.Add(1)
	})
	require.NotNil(t, svc)
	require.True(t, svc.ddlDirty.Load())
	require.True(t, svc.notifier.isAwake())
	require.Equal(t, notifier.MVJobsHandlerID, gotHandlerID)
	require.NotNil(t, gotHandler)

	createTable := notifier.NewCreateTableEvent(&meta.TableInfo{ID: 1})
	require.NoError(t, gotHandler(context.Background(), nil, createTable))
	require.Equal(t, int32(0), called.Load())

	mvLogEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvLogEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedViewLog))))
	require.NoError(t, gotHandler(context.Background(), nil, mvLogEvent))
	require.Equal(t, int32(1), called.Load())
}

func TestCalcNextPurgeTime(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.Local)
	interval := 10 * time.Second
	cases := []struct {
		name     string
		now      time.Time
		interval int64
		expect   time.Time
	}{
		{
			name:     "before start",
			now:      time.Date(2026, 1, 2, 3, 4, 4, 0, time.Local),
			interval: int64(interval / time.Second),
			expect:   time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local),
		},
		{
			name:     "exactly on boundary",
			now:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local),
			interval: int64(interval / time.Second),
			expect:   time.Date(2026, 1, 2, 3, 4, 15, 0, time.Local),
		},
		{
			name:     "between boundaries",
			now:      time.Date(2026, 1, 2, 3, 4, 22, 500000000, time.Local),
			interval: int64(interval / time.Second),
			expect:   time.Date(2026, 1, 2, 3, 4, 25, 0, time.Local),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, calcNextExecTime(start, tc.interval, tc.now))
		})
	}
}

func TestCalcNextRefreshTime(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local)
	now := time.Date(2026, 1, 2, 3, 4, 22, 0, time.Local)
	require.Equal(t, time.Date(2026, 1, 2, 3, 4, 25, 0, time.Local), calcNextExecTime(start, 10, now))
}

func TestCalcNextScheduleTimeZeroInterval(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local)
	now := time.Date(2026, 1, 2, 3, 4, 22, 0, time.Local)
	require.Equal(t, now, calcNextExecTime(start, 0, now))
	require.Equal(t, now, calcNextExecTime(start, -1, now))
}

func TestWithRCRestrictedTxnCommit(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	err := withRCRestrictedTxn(context.Background(), pool, func(_ context.Context, _ sessionctx.Context) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
}

func TestWithRCRestrictedTxnRollbackOnError(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}
	injectedErr := errors.New("injected error")

	err := withRCRestrictedTxn(context.Background(), pool, func(_ context.Context, _ sessionctx.Context) error {
		return injectedErr
	})
	require.ErrorIs(t, err, injectedErr)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
}

func TestWithRCRestrictedTxnRollbackOnPanic(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	require.PanicsWithValue(t, "panic in txn body", func() {
		_ = withRCRestrictedTxn(context.Background(), pool, func(_ context.Context, _ sessionctx.Context) error {
			panic("panic in txn body")
		})
	})
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
}
