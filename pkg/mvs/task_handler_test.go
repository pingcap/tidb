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
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	executedSQL           []string
	execErrs              map[string]error
	executedRestrictedSQL []string
	restrictedRows        map[string][]chunk.Row
	restrictedErrs        map[string]error
}

func newRecordingSessionContext() *recordingSessionContext {
	return &recordingSessionContext{
		Context:        mock.NewContext(),
		execErrs:       make(map[string]error),
		restrictedRows: make(map[string][]chunk.Row),
		restrictedErrs: make(map[string]error),
	}
}

func (s *recordingSessionContext) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *recordingSessionContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *recordingSessionContext) ExecuteInternal(_ context.Context, sql string, _ ...any) (sqlexec.RecordSet, error) {
	s.executedSQL = append(s.executedSQL, sql)
	if err, ok := s.execErrs[sql]; ok {
		return nil, err
	}
	return nil, nil
}

func (s *recordingSessionContext) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, _ ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	s.executedRestrictedSQL = append(s.executedRestrictedSQL, sql)
	if err, ok := s.restrictedErrs[sql]; ok {
		return nil, nil, err
	}
	if rows, ok := s.restrictedRows[sql]; ok {
		return rows, nil, nil
	}
	return nil, nil, nil
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
	refreshNext    time.Time
	refreshDeleted bool
	purgeNext      time.Time
	purgeDeleted   bool
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

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID string) (nextRefresh time.Time, deleted bool, err error) {
	m.lastRefreshID = mvID
	return m.refreshNext, m.refreshDeleted, m.refreshErr
}

func (m *mockMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mvLogID string) (nextPurge time.Time, deleted bool, err error) {
	m.lastPurgeID = mvLogID
	return m.purgeNext, m.purgeDeleted, m.purgeErr
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

	_, _, err = svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-1")
	require.ErrorIs(t, err, ErrMVLogPurgeHandlerNotRegistered)
}

func TestMVServiceUseInjectedTaskHandler(t *testing.T) {
	nextRefresh := time.Now().Add(time.Minute).Round(0)
	nextPurge := time.Now().Add(2 * time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshNext: nextRefresh,
		purgeNext:   nextPurge,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)

	gotNextRefresh, deleted, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, "mv-2")
	require.NoError(t, err)
	require.False(t, deleted)
	require.True(t, nextRefresh.Equal(gotNextRefresh))
	require.Equal(t, "mv-2", helper.lastRefreshID)

	gotNextPurge, deleted, err := svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-2")
	require.NoError(t, err)
	require.False(t, deleted)
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

func TestMVServicePurgeMVLogRemoveOnDeleted(t *testing.T) {
	helper := &mockMVServiceHelper{
		purgeDeleted: true,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)
	svc.executor.Run()
	defer svc.executor.Close()

	l := &mvLog{
		ID:        "mlog-remove-1",
		nextPurge: time.Now(),
	}
	require.NoError(t, svc.buildMLogPurgeTasks(map[string]*mvLog{l.ID: l}))

	svc.purgeMVLog([]*mvLog{l})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), svc.executor.metrics.failedCount.Load())

	require.Eventually(t, func() bool {
		svc.mvLogPurgeMu.Lock()
		_, ok := svc.mvLogPurgeMu.pending[l.ID]
		svc.mvLogPurgeMu.Unlock()
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), l.retryCount.Load())
	require.Equal(t, int64(0), svc.metrics.mvLogCount.Load())
}

func TestMVServicePurgeMVLogSuccessUpdatesNextPurgeAndOrderTS(t *testing.T) {
	nextPurge := time.Now().Add(time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		purgeNext: nextPurge,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)
	svc.executor.Run()
	defer svc.executor.Close()

	l := &mvLog{
		ID:        "mlog-reschedule-1",
		nextPurge: time.Now().Add(-time.Minute).Round(0),
	}
	require.NoError(t, svc.buildMLogPurgeTasks(map[string]*mvLog{l.ID: l}))

	svc.purgeMVLog([]*mvLog{l})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), svc.executor.metrics.failedCount.Load())

	svc.mvLogPurgeMu.Lock()
	item, ok := svc.mvLogPurgeMu.pending[l.ID]
	require.True(t, ok)
	require.True(t, item.Value.nextPurge.Equal(nextPurge))
	require.Equal(t, nextPurge.UnixMilli(), item.Value.orderTs)
	svc.mvLogPurgeMu.Unlock()
}

func TestMVServiceRefreshMVRemoveOnDeleted(t *testing.T) {
	helper := &mockMVServiceHelper{
		refreshDeleted: true,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-remove-1",
		nextRefresh: time.Now(),
	}
	require.NoError(t, svc.buildMVRefreshTasks(map[string]*mv{m.ID: m}))

	svc.refreshMV([]*mv{m})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), svc.executor.metrics.failedCount.Load())

	require.Eventually(t, func() bool {
		svc.mvRefreshMu.Lock()
		_, ok := svc.mvRefreshMu.pending[m.ID]
		svc.mvRefreshMu.Unlock()
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), m.retryCount.Load())
	require.Equal(t, int64(0), svc.metrics.mvCount.Load())
}

func TestMVServiceRefreshMVSuccessUpdatesNextRefreshAndOrderTS(t *testing.T) {
	nextRefresh := time.Now().Add(time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshNext: nextRefresh,
	}
	svc := NewMVJobsManager(context.Background(), mockSessionPool{}, helper)
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-reschedule-1",
		nextRefresh: time.Now().Add(-time.Minute).Round(0),
	}
	require.NoError(t, svc.buildMVRefreshTasks(map[string]*mv{m.ID: m}))

	svc.refreshMV([]*mv{m})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), svc.executor.metrics.failedCount.Load())

	svc.mvRefreshMu.Lock()
	item, ok := svc.mvRefreshMu.pending[m.ID]
	require.True(t, ok)
	require.True(t, item.Value.nextRefresh.Equal(nextRefresh))
	require.Equal(t, nextRefresh.UnixMilli(), item.Value.orderTs)
	svc.mvRefreshMu.Unlock()
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

func TestServerHelperFetchAllTiDBMLogPurge(t *testing.T) {
	const fetchMLogPurgeSQL = `SELECT t.MLOG_ID, UNIX_TIMESTAMP(l.PURGE_START), l.PURGE_INTERVAL, UNIX_TIMESTAMP(t.LAST_PURGE_TIME) FROM mysql.tidb_mlog_purge t JOIN mysql.tidb_mlogs l ON t.MLOG_ID = l.MLOG_ID`

	purgeStartSec := int64(600)
	intervalSec := int64(60)
	lastPurgeSec := int64(720)

	se := newRecordingSessionContext()
	se.restrictedRows[fetchMLogPurgeSQL] = []chunk.Row{
		// LAST_PURGE_TIME is NULL -> should use time.Time{} as calculation base.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("201"),
			types.NewIntDatum(purgeStartSec),
			types.NewIntDatum(intervalSec),
			types.NewDatum(nil),
		}).ToRow(),
		// LAST_PURGE_TIME exists -> should use the returned timestamp.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("202"),
			types.NewIntDatum(purgeStartSec),
			types.NewIntDatum(intervalSec),
			types.NewIntDatum(lastPurgeSec),
		}).ToRow(),
		// invalid row with empty ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum(""),
			types.NewIntDatum(purgeStartSec),
			types.NewIntDatum(intervalSec),
			types.NewIntDatum(lastPurgeSec),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serverHelper{}).fetchAllTiDBMLogPurge(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{fetchMLogPurgeSQL}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	l201 := got["201"]
	require.NotNil(t, l201)
	expect201 := calcNextExecTime(time.Unix(purgeStartSec, 0), intervalSec, time.Time{})
	require.Equal(t, expect201, l201.nextPurge)
	require.Equal(t, expect201.UnixMilli(), l201.orderTs)
	require.Equal(t, intervalSec*int64(time.Second), int64(l201.purgeInterval))

	l202 := got["202"]
	require.NotNil(t, l202)
	expect202 := calcNextExecTime(time.Unix(purgeStartSec, 0), intervalSec, time.Unix(lastPurgeSec, 0))
	require.Equal(t, expect202, l202.nextPurge)
	require.Equal(t, expect202.UnixMilli(), l202.orderTs)
	require.Equal(t, intervalSec*int64(time.Second), int64(l202.purgeInterval))
}

func TestServerHelperFetchAllTiDBMViews(t *testing.T) {
	const fetchMViewsSQL = `SELECT t.MVIEW_ID, UNIX_TIMESTAMP(v.REFRESH_START), v.REFRESH_INTERVAL, UNIX_TIMESTAMP(t.LAST_REFRESH_TIME) FROM mysql.tidb_mview_refresh t JOIN mysql.tidb_mviews v ON t.MVIEW_ID = v.MVIEW_ID`

	refreshStartSec := int64(900)
	intervalSec := int64(120)
	lastRefreshSec := int64(1200)

	se := newRecordingSessionContext()
	se.restrictedRows[fetchMViewsSQL] = []chunk.Row{
		// LAST_REFRESH_TIME is NULL -> should use time.Time{} as calculation base.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("101"),
			types.NewIntDatum(refreshStartSec),
			types.NewIntDatum(intervalSec),
			types.NewDatum(nil),
		}).ToRow(),
		// LAST_REFRESH_TIME exists -> should use the returned timestamp.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("102"),
			types.NewIntDatum(refreshStartSec),
			types.NewIntDatum(intervalSec),
			types.NewIntDatum(lastRefreshSec),
		}).ToRow(),
		// invalid row with empty ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum(""),
			types.NewIntDatum(refreshStartSec),
			types.NewIntDatum(intervalSec),
			types.NewIntDatum(lastRefreshSec),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serverHelper{}).fetchAllTiDBMViews(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{fetchMViewsSQL}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	m101 := got["101"]
	require.NotNil(t, m101)
	expect101 := calcNextExecTime(time.Unix(refreshStartSec, 0), intervalSec, time.Time{})
	require.Equal(t, expect101, m101.nextRefresh)
	require.Equal(t, expect101.UnixMilli(), m101.orderTs)
	require.Equal(t, intervalSec*int64(time.Second), int64(m101.refreshInterval))

	m102 := got["102"]
	require.NotNil(t, m102)
	expect102 := calcNextExecTime(time.Unix(refreshStartSec, 0), intervalSec, time.Unix(lastRefreshSec, 0))
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
	require.Equal(t, intervalSec*int64(time.Second), int64(m102.refreshInterval))
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

func TestServerHelperRefreshMVDeletedWhenMetaNotFound(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextRefresh, deleted, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "100")
	require.NoError(t, err)
	require.True(t, deleted)
	require.True(t, nextRefresh.IsZero())
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		`SELECT TABLE_SCHEMA, MVIEW_NAME, UNIX_TIMESTAMP(REFRESH_START), REFRESH_INTERVAL FROM mysql.tidb_mviews WHERE MVIEW_ID = %?`,
	}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVSuccess(t *testing.T) {
	const (
		findMVSQL          = `SELECT TABLE_SCHEMA, MVIEW_NAME, UNIX_TIMESTAMP(REFRESH_START), REFRESH_INTERVAL FROM mysql.tidb_mviews WHERE MVIEW_ID = %?`
		refreshMVSQL       = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findLastRefreshSQL = `SELECT UNIX_TIMESTAMP(REFRESH_ENDTIME), REFRESH_FAILED_REASON FROM mysql.tidb_mview_refresh_hist WHERE MVIEW_ID = %? AND IS_NEWEST_REFRESH = 'YES'`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMVSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("test"),
			types.NewStringDatum("mv1"),
			types.NewIntDatum(0),
			types.NewIntDatum(60),
		}).ToRow(),
	}
	lastRefreshSec := time.Now().Unix()
	se.restrictedRows[findLastRefreshSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(lastRefreshSec),
			types.NewDatum(nil),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	nextRefresh, deleted, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "101")
	require.NoError(t, err)
	require.False(t, deleted)
	require.Equal(t, calcNextExecTime(time.Unix(0, 0), 60, time.Unix(lastRefreshSec, 0)), nextRefresh)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMVSQL,
		refreshMVSQL,
		findLastRefreshSQL,
	}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVFailedResult(t *testing.T) {
	const (
		findMVSQL          = `SELECT TABLE_SCHEMA, MVIEW_NAME, UNIX_TIMESTAMP(REFRESH_START), REFRESH_INTERVAL FROM mysql.tidb_mviews WHERE MVIEW_ID = %?`
		refreshMVSQL       = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findLastRefreshSQL = `SELECT UNIX_TIMESTAMP(REFRESH_ENDTIME), REFRESH_FAILED_REASON FROM mysql.tidb_mview_refresh_hist WHERE MVIEW_ID = %? AND IS_NEWEST_REFRESH = 'YES'`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMVSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("test"),
			types.NewStringDatum("mv1"),
			types.NewIntDatum(0),
			types.NewIntDatum(60),
		}).ToRow(),
	}
	se.restrictedRows[findLastRefreshSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(time.Now().Unix()),
			types.NewStringDatum("refresh timeout"),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	nextRefresh, deleted, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "101")
	require.ErrorContains(t, err, "mview refresh failed")
	require.ErrorContains(t, err, "refresh timeout")
	require.False(t, deleted)
	require.True(t, nextRefresh.IsZero())
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
	require.Equal(t, []string{
		findMVSQL,
		refreshMVSQL,
		findLastRefreshSQL,
	}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogDeletedWhenMetaNotFound(t *testing.T) {
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextPurge, deleted, err := (&serverHelper{}).PurgeMVLog(context.Background(), pool, "200")
	require.NoError(t, err)
	require.True(t, deleted)
	require.True(t, nextPurge.IsZero())
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		`SELECT TABLE_SCHEMA, MLOG_NAME, RELATED_MV, UNIX_TIMESTAMP(PURGE_START), PURGE_INTERVAL, PURGE_METHOD FROM mysql.tidb_mlogs WHERE MLOG_ID = %?`,
	}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogSuccess(t *testing.T) {
	const (
		findMLogSQL    = `SELECT TABLE_SCHEMA, MLOG_NAME, RELATED_MV, UNIX_TIMESTAMP(PURGE_START), PURGE_INTERVAL, PURGE_METHOD FROM mysql.tidb_mlogs WHERE MLOG_ID = %?`
		lockPurgeSQL   = `SELECT 1 FROM mysql.tidb_mlog_purge WHERE MLOG_ID = %? FOR UPDATE`
		deleteMLogSQL  = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = %?, LAST_PURGE_ROWS = %?, LAST_PURGE_DURATION = %? WHERE MLOG_ID = %?`
		clearNewestSQL = `UPDATE mysql.tidb_mlog_purge_hist SET IS_NEWEST_PURGE = 'NO' WHERE MLOG_ID = %? AND IS_NEWEST_PURGE = 'YES'`
		insertHistSQL  = `INSERT INTO mysql.tidb_mlog_purge_hist (MLOG_ID, MLOG_NAME, IS_NEWEST_PURGE, PURGE_METHOD, PURGE_TIME, PURGE_ENDTIME, PURGE_ROWS, PURGE_STATUS) VALUES (%?, %?, 'YES', %?, %?, %?, %?, %?)`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMLogSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewStringDatum("test"),
			types.NewStringDatum("mlog1"),
			types.NewDatum(nil),
			types.NewIntDatum(0),
			types.NewIntDatum(60),
			types.NewStringDatum("TIME_WINDOW"),
		}).ToRow(),
	}
	se.restrictedRows[lockPurgeSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(1),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	nextPurge, deleted, err := (&serverHelper{}).PurgeMVLog(context.Background(), pool, "201")
	require.NoError(t, err)
	require.False(t, deleted)
	require.True(t, nextPurge.After(time.Now().Add(-2*time.Second)))
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMLogSQL,
		lockPurgeSQL,
		deleteMLogSQL,
		updatePurgeSQL,
		clearNewestSQL,
		insertHistSQL,
	}, se.executedRestrictedSQL)
}
