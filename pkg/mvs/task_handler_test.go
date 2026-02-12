package mvs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	mysql "github.com/pingcap/tidb/pkg/parser/mysql"
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
	restrictedSQLModes    []mysql.SQLMode
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
	s.restrictedSQLModes = append(s.restrictedSQLModes, s.GetSessionVars().SQLMode)
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

	metricsMu              sync.Mutex
	taskDurationCounts     map[string]int
	metaFetchDurationCount map[string]int
	runEventCounts         map[string]int
}

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID string) (nextRefresh time.Time, err error) {
	m.lastRefreshID = mvID
	return m.refreshNext, m.refreshErr
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

func (*mockMVServiceHelper) reportMetrics(*MVService) {}

func (m *mockMVServiceHelper) observeTaskDuration(taskType, result string, duration time.Duration) {
	if duration < 0 {
		return
	}
	m.metricsMu.Lock()
	if m.taskDurationCounts == nil {
		m.taskDurationCounts = make(map[string]int)
	}
	m.taskDurationCounts[taskType+"/"+result]++
	m.metricsMu.Unlock()
}

func (m *mockMVServiceHelper) observeFetchDuration(fetchType, result string, duration time.Duration) {
	if duration < 0 {
		return
	}
	m.metricsMu.Lock()
	if m.metaFetchDurationCount == nil {
		m.metaFetchDurationCount = make(map[string]int)
	}
	m.metaFetchDurationCount[fetchType+"/"+result]++
	m.metricsMu.Unlock()
}

func (m *mockMVServiceHelper) taskDurationCount(taskType, result string) int {
	m.metricsMu.Lock()
	defer m.metricsMu.Unlock()
	return m.taskDurationCounts[taskType+"/"+result]
}

func (m *mockMVServiceHelper) fetchDurationCount(fetchType, result string) int {
	m.metricsMu.Lock()
	defer m.metricsMu.Unlock()
	return m.metaFetchDurationCount[fetchType+"/"+result]
}

func (m *mockMVServiceHelper) observeRunEvent(eventType string) {
	if eventType == "" {
		return
	}
	m.metricsMu.Lock()
	if m.runEventCounts == nil {
		m.runEventCounts = make(map[string]int)
	}
	m.runEventCounts[eventType]++
	m.metricsMu.Unlock()
}

func (m *mockMVServiceHelper) runEventCount(eventType string) int {
	m.metricsMu.Lock()
	defer m.metricsMu.Unlock()
	return m.runEventCounts[eventType]
}

func TestMVServiceDefaultTaskHandler(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		refreshErr: ErrMVRefreshHandlerNotRegistered,
		purgeErr:   ErrMVLogPurgeHandlerNotRegistered,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	_, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, "mv-1")
	require.ErrorIs(t, err, ErrMVRefreshHandlerNotRegistered)

	_, err = svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-1")
	require.ErrorIs(t, err, ErrMVLogPurgeHandlerNotRegistered)
}

func TestMVServiceUseInjectedTaskHandler(t *testing.T) {
	installMockTimeForTest(t)
	nextRefresh := mvsNow().Add(time.Minute).Round(0)
	nextPurge := mvsNow().Add(2 * time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshNext: nextRefresh,
		purgeNext:   nextPurge,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	gotNextRefresh, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, "mv-2")
	require.NoError(t, err)
	require.True(t, nextRefresh.Equal(gotNextRefresh))
	require.Equal(t, "mv-2", helper.lastRefreshID)

	gotNextPurge, err := svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, "mlog-2")
	require.NoError(t, err)
	require.True(t, nextPurge.Equal(gotNextPurge))
	require.Equal(t, "mlog-2", helper.lastPurgeID)
}

func TestMVServiceNotifyDDLChangeTriggersFetch(t *testing.T) {
	installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	helper := &mockMVServiceHelper{
		fetchLogs:  map[string]*mvLog{},
		fetchViews: map[string]*mv{},
	}
	svc := NewMVService(ctx, mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.lastRefresh.Store(mvsNow().UnixMilli())

	done := make(chan struct{})
	go func() {
		svc.Run()
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	require.Equal(t, int32(0), helper.fetchLogsCalls.Load())
	require.Equal(t, int32(0), helper.fetchViewCalls.Load())

	svc.NotifyDDLChange()
	require.Eventually(t, func() bool {
		return helper.fetchLogsCalls.Load() > 0 && helper.fetchViewCalls.Load() > 0
	}, time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return helper.runEventCount(mvRunEventFetchByDDL) > 0 &&
			helper.runEventCount(mvRunEventFetchMLogOK) > 0 &&
			helper.runEventCount(mvRunEventFetchMViewsOK) > 0
	}, time.Second, 20*time.Millisecond)
}

func TestMVServiceFetchAllMVMetaReportsDuration(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchLogs:  map[string]*mvLog{},
		fetchViews: map[string]*mv{},
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	require.NoError(t, svc.fetchAllMVMeta())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchDurationTypeMLogPurge, mvFetchDurationResultOK))
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchDurationTypeMViews, mvFetchDurationResultOK))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchDurationTypeMLogPurge, mvFetchDurationResultErr))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchDurationTypeMViews, mvFetchDurationResultErr))
}

func TestMVServiceFetchAllMVMetaAvoidsPartialApplyOnFetchError(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchLogs: map[string]*mvLog{
			"new-mlog": {
				ID:        "new-mlog",
				nextPurge: mvsNow().Add(time.Minute),
				orderTs:   mvsNow().Add(time.Minute).UnixMilli(),
			},
		},
		fetchViewsErr: errors.New("fetch views failed"),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	oldMLog := &mvLog{
		ID:        "old-mlog",
		nextPurge: mvsNow().Add(2 * time.Minute),
	}
	oldMLog.orderTs = oldMLog.nextPurge.UnixMilli()
	require.NoError(t, svc.buildMLogPurgeTasks(map[string]*mvLog{oldMLog.ID: oldMLog}))

	oldMV := &mv{
		ID:          "old-mv",
		nextRefresh: mvsNow().Add(3 * time.Minute),
	}
	oldMV.orderTs = oldMV.nextRefresh.UnixMilli()
	require.NoError(t, svc.buildMVRefreshTasks(map[string]*mv{oldMV.ID: oldMV}))

	err := svc.fetchAllMVMeta()
	require.Error(t, err)
	require.Equal(t, int64(0), svc.lastRefresh.Load())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchDurationTypeMLogPurge, mvFetchDurationResultOK))
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchDurationTypeMViews, mvFetchDurationResultErr))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchDurationTypeMLogPurge, mvFetchDurationResultErr))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchDurationTypeMViews, mvFetchDurationResultOK))
	require.Equal(t, 1, helper.runEventCount(mvRunEventFetchMLogOK))
	require.Equal(t, 1, helper.runEventCount(mvRunEventFetchMViewsError))
	require.Equal(t, 0, helper.runEventCount(mvRunEventFetchMLogError))
	require.Equal(t, 0, helper.runEventCount(mvRunEventFetchMViewsOK))

	svc.mvLogPurgeMu.Lock()
	_, hasOldMLog := svc.mvLogPurgeMu.pending["old-mlog"]
	_, hasNewMLog := svc.mvLogPurgeMu.pending["new-mlog"]
	svc.mvLogPurgeMu.Unlock()
	require.True(t, hasOldMLog)
	require.False(t, hasNewMLog)

	svc.mvRefreshMu.Lock()
	_, hasOldMV := svc.mvRefreshMu.pending["old-mv"]
	svc.mvRefreshMu.Unlock()
	require.True(t, hasOldMV)
}

func TestMVServicePurgeMVLogRemoveOnDeleted(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	l := &mvLog{
		ID:        "mlog-remove-1",
		nextPurge: mvsNow(),
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
	installMockTimeForTest(t)
	nextPurge := mvsNow().Add(time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		purgeNext: nextPurge,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	l := &mvLog{
		ID:        "mlog-reschedule-1",
		nextPurge: mvsNow().Add(-time.Minute).Round(0),
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

func TestMVServiceRefreshMVRemoveOnZeroNextRefresh(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-remove-1",
		nextRefresh: mvsNow(),
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
	installMockTimeForTest(t)
	nextRefresh := mvsNow().Add(time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshNext: nextRefresh,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-reschedule-1",
		nextRefresh: mvsNow().Add(-time.Minute).Round(0),
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

func TestMVServiceTaskExecutionReportsDuration(t *testing.T) {
	installMockTimeForTest(t)
	nextRefresh := mvsNow().Add(time.Minute).Round(0)
	nextPurge := mvsNow().Add(2 * time.Minute).Round(0)
	helper := &mockMVServiceHelper{
		refreshNext: nextRefresh,
		purgeNext:   nextPurge,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-duration-1",
		nextRefresh: mvsNow().Add(-time.Minute).Round(0),
	}
	l := &mvLog{
		ID:        "mlog-duration-1",
		nextPurge: mvsNow().Add(-time.Minute).Round(0),
	}
	require.NoError(t, svc.buildMVRefreshTasks(map[string]*mv{m.ID: m}))
	require.NoError(t, svc.buildMLogPurgeTasks(map[string]*mvLog{l.ID: l}))

	svc.refreshMV([]*mv{m})
	svc.purgeMVLog([]*mvLog{l})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvTaskDurationResultOK))
	require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypePurge, mvTaskDurationResultOK))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvTaskDurationResultErr))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvTaskDurationResultErr))
}

func TestMVServiceTaskExecutionReportsDurationFailed(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		refreshErr: errors.New("refresh failed"),
		purgeErr:   errors.New("purge failed"),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	defer svc.executor.Close()

	m := &mv{
		ID:          "mv-duration-failed-1",
		nextRefresh: mvsNow().Add(-time.Minute).Round(0),
	}
	l := &mvLog{
		ID:        "mlog-duration-failed-1",
		nextPurge: mvsNow().Add(-time.Minute).Round(0),
	}
	require.NoError(t, svc.buildMVRefreshTasks(map[string]*mv{m.ID: m}))
	require.NoError(t, svc.buildMLogPurgeTasks(map[string]*mvLog{l.ID: l}))

	svc.refreshMV([]*mv{m})
	svc.purgeMVLog([]*mvLog{l})

	require.Eventually(t, func() bool {
		return svc.executor.metrics.completedCount.Load() == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvTaskDurationResultErr))
	require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypePurge, mvTaskDurationResultErr))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvTaskDurationResultOK))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvTaskDurationResultOK))
}

func TestRegisterMVSBootstrapAndDDLHandler(t *testing.T) {
	installMockTimeForTest(t)
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
	require.Equal(t, notifier.MVServiceHandlerID, gotHandlerID)
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
	installMockTimeForTest(t)
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
	installMockTimeForTest(t)
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local)
	now := time.Date(2026, 1, 2, 3, 4, 22, 0, time.Local)
	require.Equal(t, time.Date(2026, 1, 2, 3, 4, 25, 0, time.Local), calcNextExecTime(start, 10, now))
}

func TestCalcNextScheduleTimeZeroInterval(t *testing.T) {
	installMockTimeForTest(t)
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local)
	now := time.Date(2026, 1, 2, 3, 4, 22, 0, time.Local)
	require.Equal(t, now, calcNextExecTime(start, 0, now))
	require.Equal(t, now, calcNextExecTime(start, -1, now))
}

func TestServerHelperFetchAllTiDBMLogPurge(t *testing.T) {
	installMockTimeForTest(t)
	const fetchMLogPurgeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`

	nextPurgeSec1 := int64(600)
	nextPurgeSec2 := int64(720)

	se := newRecordingSessionContext()
	se.restrictedRows[fetchMLogPurgeSQL] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec1),
			types.NewStringDatum("201"),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewStringDatum("202"),
		}).ToRow(),
		// invalid row with empty ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewStringDatum(""),
		}).ToRow(),
		// invalid row with NULL NEXT_TIME should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewStringDatum("203"),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serverHelper{}).fetchAllTiDBMLogPurge(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{fetchMLogPurgeSQL}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	l201 := got["201"]
	require.NotNil(t, l201)
	expect201 := time.Unix(nextPurgeSec1, 0)
	require.Equal(t, expect201, l201.nextPurge)
	require.Equal(t, expect201.UnixMilli(), l201.orderTs)
	require.Equal(t, int64(0), int64(l201.purgeInterval))

	l202 := got["202"]
	require.NotNil(t, l202)
	expect202 := time.Unix(nextPurgeSec2, 0)
	require.Equal(t, expect202, l202.nextPurge)
	require.Equal(t, expect202.UnixMilli(), l202.orderTs)
	require.Equal(t, int64(0), int64(l202.purgeInterval))
}

func TestServerHelperFetchAllTiDBMViews(t *testing.T) {
	installMockTimeForTest(t)
	const fetchMViewsSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`

	nextRefreshSec1 := int64(900)
	nextRefreshSec2 := int64(1200)

	se := newRecordingSessionContext()
	se.restrictedRows[fetchMViewsSQL] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec1),
			types.NewStringDatum("101"),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewStringDatum("102"),
		}).ToRow(),
		// invalid row with empty ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewStringDatum(""),
		}).ToRow(),
		// invalid row with NULL NEXT_TIME should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewStringDatum("103"),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serverHelper{}).fetchAllTiDBMViews(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{fetchMViewsSQL}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	m101 := got["101"]
	require.NotNil(t, m101)
	expect101 := time.Unix(nextRefreshSec1, 0)
	require.Equal(t, expect101, m101.nextRefresh)
	require.Equal(t, expect101.UnixMilli(), m101.orderTs)
	require.Equal(t, int64(0), int64(m101.refreshInterval))

	m102 := got["102"]
	require.NotNil(t, m102)
	expect102 := time.Unix(nextRefreshSec2, 0)
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
	require.Equal(t, int64(0), int64(m102.refreshInterval))
}

func TestWithRCRestrictedTxnCommit(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()

	err := withRCRestrictedTxn(context.Background(), se, func(_ context.Context, _ sessionctx.Context) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
}

func TestWithRCRestrictedTxnRollbackOnError(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	injectedErr := errors.New("injected error")

	err := withRCRestrictedTxn(context.Background(), se, func(_ context.Context, _ sessionctx.Context) error {
		return injectedErr
	})
	require.ErrorIs(t, err, injectedErr)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
}

func TestWithRCRestrictedTxnRollbackOnPanic(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()

	require.PanicsWithValue(t, "panic in txn body", func() {
		_ = withRCRestrictedTxn(context.Background(), se, func(_ context.Context, _ sessionctx.Context) error {
			panic("panic in txn body")
		})
	})
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
}

func TestServerHelperRefreshMVDeletedWhenMetaNotFound(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "100")
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVSuccess(t *testing.T) {
	installMockTimeForTest(t)
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findNextTimeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	)

	se := newRecordingSessionContext()
	originalSQLMode := mysql.ModeStrictTransTables | mysql.ModeOnlyFullGroupBy
	se.GetSessionVars().SQLMode = originalSQLMode
	expectedNextRefresh := mvsNow().Add(time.Minute).Round(0)
	se.restrictedRows[findNextTimeSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(expectedNextRefresh.Unix()),
		}).ToRow(),
	}
	mvTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "101")
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		refreshMVSQL,
		findNextTimeSQL,
	}, se.executedRestrictedSQL)
	require.Equal(t, []mysql.SQLMode{0, 0}, se.restrictedSQLModes)
	require.Equal(t, originalSQLMode, se.GetSessionVars().SQLMode)
}

func TestServerHelperRefreshMVDeletedWhenNextTimeNotFound(t *testing.T) {
	installMockTimeForTest(t)
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findNextTimeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	)

	se := newRecordingSessionContext()
	mvTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})
	se.restrictedRows[findNextTimeSQL] = nil
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serverHelper{}).RefreshMV(context.Background(), pool, "101")
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		refreshMVSQL,
		findNextTimeSQL,
	}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogDeletedWhenMetaNotFound(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serverHelper{}).PurgeMVLog(context.Background(), pool, "200")
	require.NoError(t, err)
	require.True(t, nextPurge.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogSuccess(t *testing.T) {
	installMockTimeForTest(t)
	const (
		findMinRefreshReadTSO   = `SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%?,%?)`
		lockPurgeInfoSQL        = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
		deleteMLogSQL           = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		updatePurgeInfoSQL      = `UPDATE mysql.tidb_mlog_purge_info SET PURGE_TIME = %?, PURGE_ENDTIME = %?, PURGE_ROWS = %?, PURGE_STATUS = 'SUCCESS', PURGE_JOB_ID = '', NEXT_TIME = %? WHERE MLOG_ID = %?`
		clearNewestPurgeHistSQL = `UPDATE mysql.tidb_mlog_purge_hist SET IS_NEWEST_PURGE = 'NO' WHERE MLOG_ID = %? AND IS_NEWEST_PURGE = 'YES'`
		insertPurgeHistSQL      = `INSERT INTO mysql.tidb_mlog_purge_hist (MLOG_ID, MLOG_NAME, IS_NEWEST_PURGE, PURGE_METHOD, PURGE_TIME, PURGE_ENDTIME, PURGE_ROWS, PURGE_STATUS) VALUES (%?, %?, 'YES', %?, %?, %?, %?, %?)`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMinRefreshReadTSO] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(100),
		}).ToRow(),
	}
	se.restrictedRows[lockPurgeInfoSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(mvsNow().Add(-time.Minute).Unix()),
		}).ToRow(),
	}
	mvInfo := &meta.MaterializedViewBaseInfo{
		MLogID:   201,
		MViewIDs: []int64{101, 102},
	}
	mvTable := &meta.TableInfo{
		ID:                   101,
		Name:                 pmodel.NewCIStr("mv_base"),
		State:                meta.StatePublic,
		MaterializedViewBase: mvInfo,
	}
	mvlogTable := &meta.TableInfo{
		ID:   201,
		Name: pmodel.NewCIStr("mlog1"),
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			BaseTableID:    101,
			PurgeMethod:    "TIME_WINDOW",
			PurgeStartWith: "'2026-01-02 03:04:05'",
			PurgeNext:      "60",
		},
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable, mvlogTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})

	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serverHelper{}).PurgeMVLog(context.Background(), pool, "201")
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.True(t, nextPurge.After(mvsNow().Add(-2*time.Second)))
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMinRefreshReadTSO,
		lockPurgeInfoSQL,
		deleteMLogSQL,
		clearNewestPurgeHistSQL,
		insertPurgeHistSQL,
		updatePurgeInfoSQL,
	}, se.executedRestrictedSQL)
}

func TestPurgeMVLogSuccess(t *testing.T) {
	installMockTimeForTest(t)
	const (
		findMinRefreshReadTSOSQL = `SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%?,%?)`
		lockPurgeSQL             = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
		deleteMLogSQL            = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		updatePurgeSQL           = `UPDATE mysql.tidb_mlog_purge_info SET PURGE_TIME = %?, PURGE_ENDTIME = %?, PURGE_ROWS = %?, PURGE_STATUS = 'SUCCESS', PURGE_JOB_ID = '', NEXT_TIME = %? WHERE MLOG_ID = %?`
		clearNewestSQL           = `UPDATE mysql.tidb_mlog_purge_hist SET IS_NEWEST_PURGE = 'NO' WHERE MLOG_ID = %? AND IS_NEWEST_PURGE = 'YES'`
		insertHistSQL            = `INSERT INTO mysql.tidb_mlog_purge_hist (MLOG_ID, MLOG_NAME, IS_NEWEST_PURGE, PURGE_METHOD, PURGE_TIME, PURGE_ENDTIME, PURGE_ROWS, PURGE_STATUS) VALUES (%?, %?, 'YES', %?, %?, %?, %?, %?)`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMinRefreshReadTSOSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(100),
		}).ToRow(),
	}
	se.restrictedRows[lockPurgeSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(mvsNow().Add(time.Minute).Unix()),
		}).ToRow(),
	}
	mvInfo := &meta.MaterializedViewBaseInfo{
		MLogID:   201,
		MViewIDs: []int64{101, 102},
	}
	mvTable := &meta.TableInfo{
		ID:                   101,
		Name:                 pmodel.NewCIStr("mv_base"),
		State:                meta.StatePublic,
		MaterializedViewBase: mvInfo,
	}
	mvlogTable := &meta.TableInfo{
		ID:   201,
		Name: pmodel.NewCIStr("mlog1"),
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			PurgeMethod:    "TIME_WINDOW",
			PurgeStartWith: "'2026-01-02 03:04:05'",
			PurgeNext:      "60",
		},
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable, mvlogTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})

	loc := time.Local
	if vars := se.GetSessionVars(); vars != nil && vars.Location() != nil {
		loc = vars.Location()
	}
	expectedNextPurge, parseErr := time.ParseInLocation("2006-01-02 15:04:05", "2026-01-02 03:04:05", loc)
	require.NoError(t, parseErr)
	infoSchema := se.GetDomainInfoSchema().(infoschema.InfoSchema)
	baseTable, err := infoSchema.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_base"))
	require.NoError(t, err)
	nextPurge, err := PurgeMVLog(context.Background(), se, baseTable, false)
	require.NoError(t, err)
	require.Equal(t, expectedNextPurge.Unix(), nextPurge.Unix())
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMinRefreshReadTSOSQL,
		lockPurgeSQL,
		deleteMLogSQL,
		clearNewestSQL,
		insertHistSQL,
		updatePurgeSQL,
	}, se.executedRestrictedSQL)
}

func TestPurgeMVLogSkipWhenNotForceAndNextTimeNull(t *testing.T) {
	installMockTimeForTest(t)
	const (
		findMinRefreshReadTSOSQL = `SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%?,%?)`
		lockPurgeSQL             = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMinRefreshReadTSOSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(100),
		}).ToRow(),
	}
	se.restrictedRows[lockPurgeSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
		}).ToRow(),
	}
	mvInfo := &meta.MaterializedViewBaseInfo{
		MLogID:   201,
		MViewIDs: []int64{101, 102},
	}
	mvTable := &meta.TableInfo{
		ID:                   101,
		Name:                 pmodel.NewCIStr("mv_base"),
		State:                meta.StatePublic,
		MaterializedViewBase: mvInfo,
	}
	mvlogTable := &meta.TableInfo{
		ID:   201,
		Name: pmodel.NewCIStr("mlog1"),
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			PurgeMethod:    "TIME_WINDOW",
			PurgeStartWith: "'2026-01-02 03:04:05'",
			PurgeNext:      "60",
		},
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable, mvlogTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})

	infoSchema := se.GetDomainInfoSchema().(infoschema.InfoSchema)
	baseTable, err := infoSchema.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_base"))
	require.NoError(t, err)
	nextPurge, err := PurgeMVLog(context.Background(), se, baseTable, true)
	require.NoError(t, err)
	require.True(t, nextPurge.IsZero())
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMinRefreshReadTSOSQL,
		lockPurgeSQL,
	}, se.executedRestrictedSQL)
}

func TestPurgeMVLogSkipWhenNotForceAndNextTimeFuture(t *testing.T) {
	installMockTimeForTest(t)
	const (
		findMinRefreshReadTSOSQL = `SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%?,%?)`
		lockPurgeSQL             = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
	)

	se := newRecordingSessionContext()
	se.restrictedRows[findMinRefreshReadTSOSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(100),
		}).ToRow(),
	}
	se.restrictedRows[lockPurgeSQL] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(mvsNow().Add(time.Minute).Unix()),
		}).ToRow(),
	}
	mvInfo := &meta.MaterializedViewBaseInfo{
		MLogID:   201,
		MViewIDs: []int64{101, 102},
	}
	mvTable := &meta.TableInfo{
		ID:                   101,
		Name:                 pmodel.NewCIStr("mv_base"),
		State:                meta.StatePublic,
		MaterializedViewBase: mvInfo,
	}
	mvlogTable := &meta.TableInfo{
		ID:   201,
		Name: pmodel.NewCIStr("mlog1"),
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			PurgeMethod:    "TIME_WINDOW",
			PurgeStartWith: "'2026-01-02 03:04:05'",
			PurgeNext:      "60",
		},
		State: meta.StatePublic,
	}
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema([]*meta.TableInfo{mvTable, mvlogTable})
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})

	expectNextPurge := mvsUnix(mvsNow().Add(time.Minute).Unix(), 0)
	infoSchema := se.GetDomainInfoSchema().(infoschema.InfoSchema)
	baseTable, err := infoSchema.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_base"))
	require.NoError(t, err)
	nextPurge, err := PurgeMVLog(context.Background(), se, baseTable, true)
	require.NoError(t, err)
	require.Equal(t, expectNextPurge, nextPurge)
	require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
	require.Equal(t, []string{
		findMinRefreshReadTSOSQL,
		lockPurgeSQL,
	}, se.executedRestrictedSQL)
}
