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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

const (
	testSQLFetchMVLogPurge               = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	testSQLFetchMVRefresh                = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	testSQLRefreshMV                     = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
	testSQLFindMVNextTime                = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	testSQLPurgeMVLog                    = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
	testSQLFindPurgeNextTime             = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_TIME IS NOT NULL`
	testSQLDeleteMVRefreshHistBeforeTSO  = `DELETE FROM mysql.tidb_mview_refresh_hist WHERE REFRESH_JOB_ID < %?`
	testSQLDeleteMVLogPurgeHistBeforeTSO = `DELETE FROM mysql.tidb_mlog_purge_hist WHERE PURGE_JOB_ID < %?`
)

var (
	testExpectedPurgeMVLogSQL = []string{
		testSQLPurgeMVLog,
		testSQLFindPurgeNextTime,
	}
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
	executedRestrictedArg [][]any
	restrictedRows        map[string][]chunk.Row
	restrictedErrs        map[string]error
}

type mockCurrentVersionStore struct {
	*mock.Store
	version kv.Version
	err     error
}

func (s *mockCurrentVersionStore) CurrentVersion(string) (kv.Version, error) {
	if s.err != nil {
		return kv.Version{}, s.err
	}
	return s.version, nil
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

func (s *recordingSessionContext) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	s.executedRestrictedSQL = append(s.executedRestrictedSQL, sql)
	argsCopy := make([]any, len(args))
	copy(argsCopy, args)
	s.executedRestrictedArg = append(s.executedRestrictedArg, argsCopy)
	if err, ok := s.restrictedErrs[sql]; ok {
		return nil, nil, err
	}
	if rows, ok := s.restrictedRows[sql]; ok {
		return rows, nil, nil
	}
	return nil, nil, nil
}

func (*recordingSessionContext) Close() {}

func withMockInfoSchema(t *testing.T, tables ...*meta.TableInfo) {
	t.Helper()
	oldMockInfoSchema := mock.MockInfoschema
	mock.MockInfoschema = func(_ []*meta.TableInfo) infoschemacontext.MetaOnlyInfoSchema {
		return infoschema.MockInfoSchema(tables)
	}
	t.Cleanup(func() {
		mock.MockInfoschema = oldMockInfoSchema
	})
}

func buildMockMVBaseAndMVLogTables(baseTableID, mLogID int64, mViewIDs ...int64) (baseTable *meta.TableInfo, mlogTable *meta.TableInfo) {
	baseInfo := &meta.MaterializedViewBaseInfo{
		MLogID:   mLogID,
		MViewIDs: mViewIDs,
	}
	baseTable = &meta.TableInfo{
		ID:                   baseTableID,
		Name:                 pmodel.NewCIStr("mv_base"),
		State:                meta.StatePublic,
		MaterializedViewBase: baseInfo,
	}
	mlogTable = &meta.TableInfo{
		ID:   mLogID,
		Name: pmodel.NewCIStr("mlog1"),
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			BaseTableID:    baseTableID,
			PurgeMethod:    "TIME_WINDOW",
			PurgeStartWith: "'2026-01-02 03:04:05'",
			PurgeNext:      "60",
		},
		State: meta.StatePublic,
	}
	return baseTable, mlogTable
}

func waitExecutorFinishedCount(t *testing.T, svc *MVService, expected int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return svc.executor.metrics.counters.finishedCount.Load() == expected
	}, time.Second, 10*time.Millisecond)
}

func setupPurgeMVLogMetaForTest(t *testing.T, se *recordingSessionContext, nextTimeRows []chunk.Row) {
	t.Helper()
	se.restrictedRows[testSQLFindPurgeNextTime] = nextTimeRows

	mvTable, mvlogTable := buildMockMVBaseAndMVLogTables(101, 201, 101, 102)
	withMockInfoSchema(t, mvTable, mvlogTable)
}

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
	refreshNext             time.Time
	purgeNext               time.Time
	refreshErr              error
	purgeErr                error
	currentTSO              uint64
	currentTSOErr           error
	historyGCErr            error
	fetchLogs               map[int64]*mvLog
	fetchViews              map[int64]*mv
	fetchLogsErr            error
	fetchViewsErr           error
	fetchLogsCalls          atomic.Int32
	fetchViewCalls          atomic.Int32
	historyGCCalls          atomic.Int32
	lastHistoryGCCurrentTSO atomic.Uint64
	lastHistoryGCRetention  atomic.Int64

	lastRefreshID int64
	lastPurgeID   int64

	metricsMu              sync.Mutex
	taskDurationCounts     map[string]int
	metaFetchDurationCount map[string]int
	runEventCounts         map[string]int
}

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	m.lastRefreshID = mvID
	return m.refreshNext, m.refreshErr
}

func (m *mockMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	m.lastPurgeID = mvLogID
	return m.purgeNext, m.purgeErr
}

func (m *mockMVServiceHelper) fetchAllTiDBMVLogPurge(context.Context, basic.SessionPool) (map[int64]*mvLog, error) {
	m.fetchLogsCalls.Add(1)
	if m.fetchLogsErr != nil {
		return nil, m.fetchLogsErr
	}
	return m.fetchLogs, nil
}

func (m *mockMVServiceHelper) fetchAllTiDBMVRefresh(context.Context, basic.SessionPool) (map[int64]*mv, error) {
	m.fetchViewCalls.Add(1)
	if m.fetchViewsErr != nil {
		return nil, m.fetchViewsErr
	}
	return m.fetchViews, nil
}

func (m *mockMVServiceHelper) GetCurrentTSO(context.Context, basic.SessionPool) (uint64, error) {
	if m.currentTSOErr != nil {
		return 0, m.currentTSOErr
	}
	if m.currentTSO == 0 {
		return 1, nil
	}
	return m.currentTSO, nil
}

func (m *mockMVServiceHelper) PurgeMVHistoryBeforeTSO(_ context.Context, _ basic.SessionPool, currentTSO uint64, retention time.Duration) error {
	m.historyGCCalls.Add(1)
	m.lastHistoryGCCurrentTSO.Store(currentTSO)
	m.lastHistoryGCRetention.Store(int64(retention))
	return m.historyGCErr
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

	_, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, 1)
	require.ErrorIs(t, err, ErrMVRefreshHandlerNotRegistered)

	_, err = svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, 1)
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

	gotNextRefresh, err := svc.mh.RefreshMV(context.Background(), mockSessionPool{}, 2)
	require.NoError(t, err)
	require.True(t, nextRefresh.Equal(gotNextRefresh))
	require.Equal(t, int64(2), helper.lastRefreshID)

	gotNextPurge, err := svc.mh.PurgeMVLog(context.Background(), mockSessionPool{}, 2)
	require.NoError(t, err)
	require.True(t, nextPurge.Equal(gotNextPurge))
	require.Equal(t, int64(2), helper.lastPurgeID)
}

func TestMVServiceNotifyDDLChangeTriggersFetch(t *testing.T) {
	installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	helper := &mockMVServiceHelper{
		fetchLogs:  map[int64]*mvLog{},
		fetchViews: map[int64]*mv{},
	}
	svc := NewMVService(ctx, mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())

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
			helper.runEventCount(mvRunEventFetchMViewOK) > 0
	}, time.Second, 20*time.Millisecond)
}

func TestMVServiceMaintenanceTimerTriggersHistoryGC(t *testing.T) {
	module := installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	helper := &mockMVServiceHelper{
		currentTSO: 1 << 40,
		fetchLogs:  map[int64]*mvLog{},
		fetchViews: map[int64]*mv{},
	}
	cfg := DefaultMVServiceConfig()
	cfg.BasicInterval = time.Second
	cfg.FetchInterval = 24 * time.Hour
	svc := NewMVService(ctx, mockSessionPool{}, helper, cfg)
	svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())
	svc.lastHistoryGCMillis.Store(mvsNow().UnixMilli())

	done := make(chan struct{})
	go func() {
		svc.Run()
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	require.Equal(t, int32(0), helper.historyGCCalls.Load())
	require.Eventually(t, func() bool {
		return helper.runEventCount(mvRunEventServerRefreshOK) > 0
	}, time.Second, 10*time.Millisecond)

	module.Advance(defaultMVHistoryGCInterval + cfg.BasicInterval)
	require.Eventually(t, func() bool {
		return helper.historyGCCalls.Load() > 0
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, helper.currentTSO, helper.lastHistoryGCCurrentTSO.Load())
	require.Equal(t, int64(defaultMVHistoryGCRetention), helper.lastHistoryGCRetention.Load())
	require.Eventually(t, func() bool {
		return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess) > 0
	}, time.Second, 10*time.Millisecond)
}

func TestMVServiceMaybeGCMVHistorySkipsWhenNotOwner(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		currentTSO: 1 << 40,
	}
	cfg := DefaultMVServiceConfig()
	cfg.ServerConsistentHashReplicas = 1
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
	svc.lastHistoryGCMillis.Store(mvsNow().Add(-defaultMVHistoryGCInterval).UnixMilli())

	svc.sch.mu.Lock()
	svc.sch.ID = "nodeA"
	svc.sch.chash.hashFunc = mustHash(map[string]uint32{
		"nodeA#0":           10,
		"nodeB#0":           30,
		mvHistoryGCOwnerKey: 20,
	})
	svc.sch.servers = map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
	}
	svc.sch.chash.Rebuild(svc.sch.servers)
	svc.sch.mu.Unlock()

	svc.maybeGCOperationHistory(mvsNow())
	require.Equal(t, int32(0), helper.historyGCCalls.Load())
	require.Equal(t, 0, helper.runEventCount(mvRunEventHistoryGCGetTSOErr))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed))
}

func TestMVServiceMaybeGCMVHistoryReportsMetrics(t *testing.T) {
	installMockTimeForTest(t)

	setupOwner := func(svc *MVService) {
		svc.lastHistoryGCMillis.Store(mvsNow().Add(-defaultMVHistoryGCInterval).UnixMilli())
		svc.sch.mu.Lock()
		svc.sch.ID = "nodeA"
		svc.sch.chash.hashFunc = mustHash(map[string]uint32{
			"nodeA#0":           10,
			"nodeB#0":           30,
			mvHistoryGCOwnerKey: 5,
		})
		svc.sch.servers = map[string]serverInfo{
			"nodeA": {ID: "nodeA"},
			"nodeB": {ID: "nodeB"},
		}
		svc.sch.chash.Rebuild(svc.sch.servers)
		svc.sch.mu.Unlock()
	}

	t.Run("success", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSO: 1 << 40}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setupOwner(svc)

		svc.maybeGCOperationHistory(mvsNow())
		require.Equal(t, int32(1), helper.historyGCCalls.Load())
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess))
	})

	t.Run("get_tso_error", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSOErr: errors.New("get tso failed")}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setupOwner(svc)

		svc.maybeGCOperationHistory(mvsNow())
		require.Equal(t, int32(0), helper.historyGCCalls.Load())
		require.Equal(t, 1, helper.runEventCount(mvRunEventHistoryGCGetTSOErr))
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed))
	})

	t.Run("purge_error", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			currentTSO:   1 << 40,
			historyGCErr: errors.New("purge failed"),
		}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setupOwner(svc)

		svc.maybeGCOperationHistory(mvsNow())
		require.Equal(t, int32(1), helper.historyGCCalls.Load())
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed))
	})
}

func TestMVServiceMarkFetchFailureRetryCadence(t *testing.T) {
	installMockTimeForTest(t)
	now := mvsNow()

	t.Run("periodic_failure_uses_fetch_interval", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		svc.markFetchFailure(now, false)
		require.Equal(t, now.Add(cfg.FetchInterval), svc.nextFetchTime(now))
	})

	t.Run("ddl_failure_uses_basic_interval", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		svc.markFetchFailure(now, true)
		require.Equal(t, now.Add(cfg.BasicInterval), svc.nextFetchTime(now))
		require.True(t, svc.nextFetchTime(now).Before(now.Add(cfg.FetchInterval)))
	})

	t.Run("ddl_failure_clamped_by_fetch_interval", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.BasicInterval = 40 * time.Second
		cfg.FetchInterval = 10 * time.Second
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		svc.markFetchFailure(now, true)
		require.Equal(t, now.Add(cfg.FetchInterval), svc.nextFetchTime(now))
	})
}

func TestMVServiceFetchAllMVMetaReportsDuration(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchLogs:  map[int64]*mvLog{},
		fetchViews: map[int64]*mv{},
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	require.NoError(t, svc.fetchAllMVMeta())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultSuccess))
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultSuccess))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultFailed))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultFailed))
}

func TestMVServiceFetchAllMVMetaAvoidsPartialApplyOnFetchError(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchLogs: map[int64]*mvLog{
			201: {
				ID:        201,
				nextPurge: mvsNow().Add(time.Minute),
				orderTs:   mvsNow().Add(time.Minute).UnixMilli(),
			},
		},
		fetchViewsErr: errors.New("fetch views failed"),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	oldMLog := &mvLog{
		ID:        101,
		nextPurge: mvsNow().Add(2 * time.Minute),
	}
	oldMLog.orderTs = oldMLog.nextPurge.UnixMilli()
	svc.buildMVLogPurgeTasks(map[int64]*mvLog{oldMLog.ID: oldMLog})

	oldMV := &mv{
		ID:          102,
		nextRefresh: mvsNow().Add(3 * time.Minute),
	}
	oldMV.orderTs = oldMV.nextRefresh.UnixMilli()
	svc.buildMVRefreshTasks(map[int64]*mv{oldMV.ID: oldMV})

	err := svc.fetchAllMVMeta()
	require.Error(t, err)
	require.Equal(t, int64(0), svc.lastMetaFetchMillis.Load())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultSuccess))
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultFailed))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultFailed))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultSuccess))
	require.Equal(t, 1, helper.runEventCount(mvRunEventFetchMLogOK))
	require.Equal(t, 1, helper.runEventCount(mvRunEventFetchMViewErr))
	require.Equal(t, 0, helper.runEventCount(mvRunEventFetchMLogErr))
	require.Equal(t, 0, helper.runEventCount(mvRunEventFetchMViewOK))

	svc.mvLogPurgeMu.Lock()
	_, hasOldMLog := svc.mvLogPurgeMu.pending[101]
	_, hasNewMLog := svc.mvLogPurgeMu.pending[201]
	svc.mvLogPurgeMu.Unlock()
	require.True(t, hasOldMLog)
	require.False(t, hasNewMLog)

	svc.mvRefreshMu.Lock()
	_, hasOldMV := svc.mvRefreshMu.pending[102]
	svc.mvRefreshMu.Unlock()
	require.True(t, hasOldMV)
}

func TestMVServicePurgeMVLogTaskResult(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("remove_on_zero_next_purge", func(t *testing.T) {
		helper := &mockMVServiceHelper{}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
		svc.executor.Run()
		defer svc.executor.Close()

		l := &mvLog{
			ID:        301,
			nextPurge: mvsNow(),
		}
		svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
		svc.purgeMVLog([]*mvLog{l})

		waitExecutorFinishedCount(t, svc, 1)
		require.Equal(t, int64(0), svc.executor.metrics.counters.failedCount.Load())
		require.Eventually(t, func() bool {
			svc.mvLogPurgeMu.Lock()
			_, ok := svc.mvLogPurgeMu.pending[l.ID]
			svc.mvLogPurgeMu.Unlock()
			return !ok
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, int64(0), l.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.mvLogCount.Load())
	})

	t.Run("reschedule_on_non_zero_next_purge", func(t *testing.T) {
		nextPurge := mvsNow().Add(time.Minute).Round(0)
		helper := &mockMVServiceHelper{
			purgeNext: nextPurge,
		}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
		svc.executor.Run()
		defer svc.executor.Close()

		l := &mvLog{
			ID:        302,
			nextPurge: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
		svc.purgeMVLog([]*mvLog{l})

		waitExecutorFinishedCount(t, svc, 1)
		require.Equal(t, int64(0), svc.executor.metrics.counters.failedCount.Load())

		svc.mvLogPurgeMu.Lock()
		item, ok := svc.mvLogPurgeMu.pending[l.ID]
		require.True(t, ok)
		require.True(t, item.Value.nextPurge.Equal(nextPurge))
		require.Equal(t, nextPurge.UnixMilli(), item.Value.orderTs)
		svc.mvLogPurgeMu.Unlock()
	})
}

func TestMVServiceRefreshMVTaskResult(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("remove_on_zero_next_refresh", func(t *testing.T) {
		helper := &mockMVServiceHelper{}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
		svc.executor.Run()
		defer svc.executor.Close()

		m := &mv{
			ID:          401,
			nextRefresh: mvsNow(),
		}
		svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
		svc.refreshMV([]*mv{m})

		waitExecutorFinishedCount(t, svc, 1)
		require.Equal(t, int64(0), svc.executor.metrics.counters.failedCount.Load())
		require.Eventually(t, func() bool {
			svc.mvRefreshMu.Lock()
			_, ok := svc.mvRefreshMu.pending[m.ID]
			svc.mvRefreshMu.Unlock()
			return !ok
		}, time.Second, 10*time.Millisecond)
		require.Equal(t, int64(0), m.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.mvCount.Load())
	})

	t.Run("reschedule_on_non_zero_next_refresh", func(t *testing.T) {
		nextRefresh := mvsNow().Add(time.Minute).Round(0)
		helper := &mockMVServiceHelper{
			refreshNext: nextRefresh,
		}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
		svc.executor.Run()
		defer svc.executor.Close()

		m := &mv{
			ID:          402,
			nextRefresh: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
		svc.refreshMV([]*mv{m})

		waitExecutorFinishedCount(t, svc, 1)
		require.Equal(t, int64(0), svc.executor.metrics.counters.failedCount.Load())

		svc.mvRefreshMu.Lock()
		item, ok := svc.mvRefreshMu.pending[m.ID]
		require.True(t, ok)
		require.True(t, item.Value.nextRefresh.Equal(nextRefresh))
		require.Equal(t, nextRefresh.UnixMilli(), item.Value.orderTs)
		svc.mvRefreshMu.Unlock()
	})
}

func TestMVServiceExecuteRefreshTaskSkipWhenDeleted(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		refreshNext: mvsNow().Add(time.Minute).Round(0),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	m := &mv{
		ID:          403,
		nextRefresh: mvsNow().Add(-time.Minute).Round(0),
	}
	svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
	svc.buildMVRefreshTasks(map[int64]*mv{})

	err := svc.executeRefreshTask(m)
	require.NoError(t, err)
	require.Equal(t, int64(0), helper.lastRefreshID)
	require.Equal(t, int64(0), svc.metrics.runningMVRefreshCount.Load())
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultFailed))
}

func TestMVServiceExecutePurgeTaskSkipWhenDeleted(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		purgeNext: mvsNow().Add(time.Minute).Round(0),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	l := &mvLog{
		ID:        303,
		nextPurge: mvsNow().Add(-time.Minute).Round(0),
	}
	svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
	svc.buildMVLogPurgeTasks(map[int64]*mvLog{})

	err := svc.executePurgeTask(l)
	require.NoError(t, err)
	require.Equal(t, int64(0), helper.lastPurgeID)
	require.Equal(t, int64(0), svc.metrics.runningMVLogPurgeCount.Load())
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultFailed))
}

func TestRegisterMVServiceBootstrapAndDDLHandler(t *testing.T) {
	installMockTimeForTest(t)
	called := atomic.Int32{}
	var (
		gotHandlerID notifier.HandlerID
		gotHandler   notifier.SchemaChangeHandler
	)
	svc := RegisterMVService(context.Background(), func(id notifier.HandlerID, handler notifier.SchemaChangeHandler) {
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
	require.Equal(t, TaskBackpressureConfig{
		CPUThreshold: defaultBackpressureCPUThreshold,
		MemThreshold: defaultBackpressureMemThreshold,
		Delay:        defaultTaskBackpressureDelay,
	}, svc.GetTaskBackpressureConfig())
	require.NotNil(t, svc.executor.backpressure.Load())

	createTable := notifier.NewCreateTableEvent(&meta.TableInfo{ID: 1})
	require.NoError(t, gotHandler(context.Background(), nil, createTable))
	require.Equal(t, int32(0), called.Load())

	createMVLogTable := notifier.NewCreateTableEvent(&meta.TableInfo{
		ID: 2,
		MaterializedViewLog: &meta.MaterializedViewLogInfo{
			BaseTableID: 1,
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, createMVLogTable))
	require.Equal(t, int32(1), called.Load())

	createMVTable := notifier.NewCreateTableEvent(&meta.TableInfo{
		ID: 3,
		MaterializedView: &meta.MaterializedViewInfo{
			BaseTableIDs: []int64{1},
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, createMVTable))
	require.Equal(t, int32(2), called.Load())

	mvLogEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvLogEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedViewLog))))
	require.NoError(t, gotHandler(context.Background(), nil, mvLogEvent))
	require.Equal(t, int32(3), called.Load())

	mvEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedView))))
	require.NoError(t, gotHandler(context.Background(), nil, mvEvent))
	require.Equal(t, int32(4), called.Load())
}

func TestServerHelperFetchAllTiDBMLogPurge(t *testing.T) {
	installMockTimeForTest(t)
	nextPurgeSec1 := int64(600)
	nextPurgeSec2 := int64(720)

	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVLogPurge] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec1),
			types.NewIntDatum(201),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewIntDatum(202),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextPurgeSec2),
			types.NewIntDatum(0),
		}).ToRow(),
		// Invalid row with NULL NEXT_TIME should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(203),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).fetchAllTiDBMVLogPurge(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVLogPurge}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	l201 := got[int64(201)]
	require.NotNil(t, l201)
	expect201 := time.Unix(nextPurgeSec1, 0)
	require.Equal(t, expect201, l201.nextPurge)
	require.Equal(t, expect201.UnixMilli(), l201.orderTs)
	require.Equal(t, int64(0), int64(l201.purgeInterval))

	l202 := got[int64(202)]
	require.NotNil(t, l202)
	expect202 := time.Unix(nextPurgeSec2, 0)
	require.Equal(t, expect202, l202.nextPurge)
	require.Equal(t, expect202.UnixMilli(), l202.orderTs)
	require.Equal(t, int64(0), int64(l202.purgeInterval))
}

func TestServerHelperFetchAllTiDBMVRefresh(t *testing.T) {
	installMockTimeForTest(t)
	nextRefreshSec1 := int64(900)
	nextRefreshSec2 := int64(1200)

	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVRefresh] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec1),
			types.NewIntDatum(101),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(102),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(0),
		}).ToRow(),
		// Invalid row with NULL NEXT_TIME should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(103),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).fetchAllTiDBMVRefresh(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVRefresh}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	m101 := got[int64(101)]
	require.NotNil(t, m101)
	expect101 := time.Unix(nextRefreshSec1, 0)
	require.Equal(t, expect101, m101.nextRefresh)
	require.Equal(t, expect101.UnixMilli(), m101.orderTs)
	require.Equal(t, int64(0), int64(m101.refreshInterval))

	m102 := got[int64(102)]
	require.NotNil(t, m102)
	expect102 := time.Unix(nextRefreshSec2, 0)
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
	require.Equal(t, int64(0), int64(m102.refreshInterval))
}

func TestServerHelperGetCurrentTSO(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	se.Store = &mockCurrentVersionStore{
		Store:   &mock.Store{},
		version: kv.NewVersion(123456),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).GetCurrentTSO(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, uint64(123456), got)
}

func TestServerHelperGetCurrentTSOInvalidVersion(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	se.Store = &mockCurrentVersionStore{
		Store:   &mock.Store{},
		version: kv.NewVersion(0),
	}
	pool := recordingSessionPool{se: se}

	_, err := (&serviceHelper{}).GetCurrentTSO(context.Background(), pool)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid version")
}

func TestServerHelperPurgeMVHistoryBeforeTSO(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}
	currentTSO := uint64(987654321)
	retention := time.Duration(0)

	err := (&serviceHelper{}).PurgeMVHistoryBeforeTSO(context.Background(), pool, currentTSO, retention)
	require.NoError(t, err)
	require.Equal(t, []string{
		testSQLDeleteMVRefreshHistBeforeTSO,
		testSQLDeleteMVLogPurgeHistBeforeTSO,
	}, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 2)
	require.Equal(t, []any{currentTSO}, se.executedRestrictedArg[0])
	require.Equal(t, []any{currentTSO}, se.executedRestrictedArg[1])
}

func TestServerHelperRefreshMVDeletedWhenMetaNotFound(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 100)
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVSuccess(t *testing.T) {
	installMockTimeForTest(t)

	se := newRecordingSessionContext()
	expectedNextRefresh := mvsNow().Add(time.Minute).Round(0)
	se.restrictedRows[testSQLFindMVNextTime] = []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(expectedNextRefresh.Unix()),
		}).ToRow(),
	}
	mvTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mvTable)
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVDeletedWhenNextTimeNotFound(t *testing.T) {
	installMockTimeForTest(t)

	se := newRecordingSessionContext()
	mvTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mvTable)
	se.restrictedRows[testSQLFindMVNextTime] = nil
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.True(t, nextRefresh.IsZero())
	require.Empty(t, se.executedSQL)
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogDeletedWhenMetaNotFound(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 200)
	require.NoError(t, err)
	require.True(t, nextPurge.IsZero())
	require.Empty(t, se.executedSQL)
	require.Empty(t, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogSuccess(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	nextTimeRows := []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(mvsNow().Add(time.Minute).Unix()),
		}).ToRow(),
	}
	setupPurgeMVLogMetaForTest(t, se, nextTimeRows)

	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.True(t, nextPurge.After(mvsNow().Add(-2*time.Second)))
	require.Empty(t, se.executedSQL)
	require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
	require.Len(t, se.executedRestrictedArg, 2)
	require.Len(t, se.executedRestrictedArg[0], 2)
	require.Equal(t, "mv_base", se.executedRestrictedArg[0][1])
}

func TestPurgeMVLogSkipWhenAutoPurge(t *testing.T) {
	installMockTimeForTest(t)
	testCases := []struct {
		name     string
		nextRows func(now time.Time) []chunk.Row
		expected func(now time.Time) time.Time
	}{
		{
			name: "next_time_null",
			nextRows: func(_ time.Time) []chunk.Row {
				return []chunk.Row{
					chunk.MutRowFromDatums([]types.Datum{
						types.NewDatum(nil),
					}).ToRow(),
				}
			},
			expected: func(_ time.Time) time.Time {
				return time.Time{}
			},
		},
		{
			name: "next_time_future",
			nextRows: func(now time.Time) []chunk.Row {
				return []chunk.Row{
					chunk.MutRowFromDatums([]types.Datum{
						types.NewIntDatum(now.Add(time.Minute).Unix()),
					}).ToRow(),
				}
			},
			expected: func(now time.Time) time.Time {
				return mvsUnix(now.Add(time.Minute).Unix(), 0)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := mvsNow()
			se := newRecordingSessionContext()
			setupPurgeMVLogMetaForTest(t, se, tc.nextRows(now))
			pool := recordingSessionPool{se: se}

			nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
			require.NoError(t, err)
			require.Equal(t, tc.expected(now), nextPurge)
			require.Empty(t, se.executedSQL)
			require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
			require.Len(t, se.executedRestrictedArg, 2)
			require.Len(t, se.executedRestrictedArg[0], 2)
			require.Equal(t, "mv_base", se.executedRestrictedArg[0][1])
		})
	}
}
