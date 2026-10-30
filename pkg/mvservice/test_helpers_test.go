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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

const (
	testEventuallyWait = time.Second
	testEventuallyTick = time.Millisecond
)

const (
	testSQLCountMVLogRows         = `SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*) FROM %n.%n`
	testSQLFetchMVLogAccumulation = `SELECT MLOG_ID FROM mysql.tidb_mlog_purge_info`
	testSQLFetchMVLogPurge        = `SELECT NEXT_PURGE_UNIX_SECONDS, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_PURGE_UNIX_SECONDS IS NOT NULL`
	testSQLFetchMVRefresh         = `SELECT NEXT_REFRESH_UNIX_SECONDS, MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE NEXT_REFRESH_UNIX_SECONDS IS NOT NULL`
	testSQLRefreshMV              = `REFRESH MATERIALIZED VIEW %n.%n FAST`
	testSQLFindMVNextTime         = `SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_REFRESH_UNIX_SECONDS IS NOT NULL`
	testSQLLockMVNextTime         = `SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT`
	testSQLUpdateMVNextTime       = `UPDATE mysql.tidb_mview_refresh_info SET NEXT_REFRESH_UNIX_SECONDS = %? WHERE MVIEW_ID = %?`
	testSQLPurgeMVLog             = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
	testSQLFindPurgeNextTime      = `SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_PURGE_UNIX_SECONDS IS NOT NULL`
	testSQLLockPurgeNextTime      = `SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT`
	testSQLUpdatePurgeNextTime    = `UPDATE mysql.tidb_mlog_purge_info SET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?`
)

var (
	testSQLMarkStaleMVRefreshHistOrphaned  = buildMarkRefreshHistoryRunningRowsOrphanedSQL()
	testSQLMarkStaleMVLogPurgeHistOrphaned = buildMarkLogPurgeHistoryRunningRowsOrphanedSQL()
	testSQLDeleteMVRefreshHistBeforeTSO    = buildDeleteRefreshHistoryByCutoffTSOSQL()
	testSQLDeleteMVLogPurgeHistBeforeTSO   = buildDeleteLogPurgeHistoryByCutoffTSOSQL()
	testSQLCountMVRefreshHist              = countRefreshHistorySQL
	testSQLCountMVLogPurgeHist             = countLogPurgeHistorySQL
	testExpectedPurgeMVLogSQL              = []string{
		testSQLPurgeMVLog,
		testSQLFindPurgeNextTime,
	}
)

type mockSessionPool struct{}

func (mockSessionPool) Get() (pools.Resource, error) { return nil, nil }

func (mockSessionPool) Put(pools.Resource) {}

func (mockSessionPool) Close() {}

type recordingSessionPool struct {
	se pools.Resource
}

func (p recordingSessionPool) Get() (pools.Resource, error) { return p.se, nil }

func (recordingSessionPool) Put(pools.Resource) {}

func (recordingSessionPool) Close() {}

type recordingSessionContext struct {
	*mock.Context
	executedSQL                             []string
	execErrs                                map[string]error
	executedRestrictedSQL                   []string
	executedRestrictedArg                   [][]any
	restrictedDistSQLScanConcurrency        []int
	restrictedMaintainQuota                 []int64
	restrictedMaintainIsolationReadEngines  []string
	restrictedMaxThreads                    []int64
	restrictedMaxBytesBeforeExternalJoin    []int64
	restrictedMaxBytesBeforeExternalAgg     []int64
	restrictedMaxBytesBeforeExternalSort    []int64
	restrictedMemQuotaQueryPerNode          []int64
	restrictedQuerySpillRatio               []float64
	restrictedStreamCount                   []int64
	restrictedBatchSize                     []uint64
	restrictedImportThreads                 []int
	restrictedImportDiskQuota               []string
	restrictedMLogPurgeBatchSize            []int
	restrictedMLogPurgeMinRate              []int
	restrictedMLogPurgeRateBudgetRatio      []float64
	restrictedMLogPurgeDeleteTiFlashThreads []int64
	restrictedRows                          map[string][]chunk.Row
	restrictedErrs                          map[string]error
	restrictedAffectedRows                  map[string][]uint64
	restrictedAffectedPos                   map[string]int
}

type faultyGlobalAccessor struct {
	*variable.MockGlobalAccessor
	getErrs   map[string]error
	getVals   map[string]string
	failAfter map[string]int
	getCalls  map[string]int
}

func (a *faultyGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	if a.getCalls == nil {
		a.getCalls = make(map[string]int)
	}
	a.getCalls[name]++
	if okAfter, ok := a.failAfter[name]; ok && a.getCalls[name] > okAfter {
		if err, ok := a.getErrs[name]; ok {
			return "", err
		}
		return "", errors.New("mock global read failure")
	}
	if err, ok := a.getErrs[name]; ok {
		if _, delayed := a.failAfter[name]; !delayed {
			return "", err
		}
	}
	if val, ok := a.getVals[name]; ok {
		return val, nil
	}
	return a.MockGlobalAccessor.GetGlobalSysVar(name)
}

func newRecordingSessionContext() *recordingSessionContext {
	return &recordingSessionContext{
		Context:                mock.NewContext(),
		execErrs:               make(map[string]error),
		restrictedRows:         make(map[string][]chunk.Row),
		restrictedErrs:         make(map[string]error),
		restrictedAffectedRows: make(map[string][]uint64),
		restrictedAffectedPos:  make(map[string]int),
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
	s.restrictedDistSQLScanConcurrency = append(s.restrictedDistSQLScanConcurrency, s.GetSessionVars().DistSQLScanConcurrency())
	s.restrictedMaintainQuota = append(s.restrictedMaintainQuota, s.GetSessionVars().MViewMaintainMemQuota)
	s.restrictedMaintainIsolationReadEngines = append(s.restrictedMaintainIsolationReadEngines, s.GetSessionVars().MViewMaintainIsolationReadEngines)
	s.restrictedMaxThreads = append(s.restrictedMaxThreads, s.GetSessionVars().TiFlashMaxThreads)
	s.restrictedMaxBytesBeforeExternalJoin = append(s.restrictedMaxBytesBeforeExternalJoin, s.GetSessionVars().TiFlashMaxBytesBeforeExternalJoin)
	s.restrictedMaxBytesBeforeExternalAgg = append(s.restrictedMaxBytesBeforeExternalAgg, s.GetSessionVars().TiFlashMaxBytesBeforeExternalGroupBy)
	s.restrictedMaxBytesBeforeExternalSort = append(s.restrictedMaxBytesBeforeExternalSort, s.GetSessionVars().TiFlashMaxBytesBeforeExternalSort)
	s.restrictedMemQuotaQueryPerNode = append(s.restrictedMemQuotaQueryPerNode, s.GetSessionVars().TiFlashMaxQueryMemoryPerNode)
	s.restrictedQuerySpillRatio = append(s.restrictedQuerySpillRatio, s.GetSessionVars().TiFlashQuerySpillRatio)
	s.restrictedStreamCount = append(s.restrictedStreamCount, s.GetSessionVars().TiFlashFineGrainedShuffleStreamCount)
	s.restrictedBatchSize = append(s.restrictedBatchSize, s.GetSessionVars().TiFlashFineGrainedShuffleBatchSize)
	s.restrictedImportThreads = append(s.restrictedImportThreads, s.GetSessionVars().MViewMaintainImportThreads)
	s.restrictedImportDiskQuota = append(s.restrictedImportDiskQuota, s.GetSessionVars().MViewMaintainImportDiskQuota)
	s.restrictedMLogPurgeBatchSize = append(s.restrictedMLogPurgeBatchSize, s.GetSessionVars().MLogPurgeBatchSize)
	s.restrictedMLogPurgeMinRate = append(s.restrictedMLogPurgeMinRate, s.GetSessionVars().MLogPurgeMinRate)
	s.restrictedMLogPurgeRateBudgetRatio = append(s.restrictedMLogPurgeRateBudgetRatio, s.GetSessionVars().MLogPurgeRateBudgetRatio)
	s.restrictedMLogPurgeDeleteTiFlashThreads = append(s.restrictedMLogPurgeDeleteTiFlashThreads, s.GetSessionVars().MLogPurgeDeleteTiFlashThreads)
	if seq, ok := s.restrictedAffectedRows[sql]; ok {
		pos := s.restrictedAffectedPos[sql]
		if pos < len(seq) {
			s.GetSessionVars().StmtCtx.SetAffectedRows(seq[pos])
			s.restrictedAffectedPos[sql] = pos + 1
		} else {
			s.GetSessionVars().StmtCtx.SetAffectedRows(0)
		}
	} else {
		s.GetSessionVars().StmtCtx.SetAffectedRows(0)
	}
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

func setupPurgeMVLogMetaForTest(t *testing.T, se *recordingSessionContext, nextTimeRows []chunk.Row) {
	t.Helper()
	se.restrictedRows[testSQLFindPurgeNextTime] = nextTimeRows

	mviewTable, mvlogTable := buildMockMVBaseAndMVLogTables(101, 201, 101, 102)
	withMockInfoSchema(t, mviewTable, mvlogTable)
}

func runMVServiceForTest(t *testing.T, svc *MVService, cancel context.CancelFunc) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		svc.Run()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(testEventuallyWait):
			t.Fatal("mv service did not stop in time")
		}
	})
}

func newRunningMVServiceForTest(t *testing.T, helper Helper) *MVService {
	t.Helper()
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.runTaskExecutors()
	t.Cleanup(func() {
		svc.closeTaskExecutors()
	})
	return svc
}

type mockMVServiceHelper struct {
	refreshNext                       time.Time
	purgeNext                         time.Time
	refreshErr                        error
	purgeErr                          error
	refreshPanic                      bool
	purgePanic                        bool
	currentTSO                        uint64
	currentTSOErr                     error
	historyGCErr                      error
	historyGCPanic                    bool
	fetchLogs                         map[int64]*mlogPurgeTask
	fetchAccumulationTasks            map[int64]*mlogAccumulationTask
	fetchAccumulationRowCounts        map[int64]uint64
	fetchViews                        map[int64]*mviewTask
	fetchLogsErr                      error
	fetchAccumulationTasksErr         error
	fetchAccumulationRowCountsErr     error
	fetchViewsErr                     error
	fetchLogsCalls                    atomic.Int32
	fetchAccumulationTaskCalls        atomic.Int32
	fetchAccumulationRowCountCalls    atomic.Int32
	fetchViewCalls                    atomic.Int32
	serverRefreshCalls                atomic.Int32
	historyGCCalls                    atomic.Int32
	refreshManualCancelBackoffApplied bool
	purgeManualCancelBackoffApplied   bool
	refreshManualCancelBackoffNext    time.Time
	purgeManualCancelBackoffNext      time.Time
	refreshManualCancelBackoffNextSet bool
	purgeManualCancelBackoffNextSet   bool
	refreshManualCancelBackoffErr     error
	purgeManualCancelBackoffErr       error
	refreshManualCancelBackoffCalls   atomic.Int32
	purgeManualCancelBackoffCalls     atomic.Int32
	lastHistoryGCCurrentTSO           atomic.Uint64
	lastMViewHistoryGCRetention       atomic.Int64
	lastMLogHistoryGCRetention        atomic.Int64
	syncRefreshAlertCalls             atomic.Int32
	cleanupStaleRefreshAlertCalls     atomic.Int32
	syncRefreshAlertErr               error
	cleanupStaleRefreshAlertErr       error
	lastMViewHistoryGCMaxRecords      atomic.Uint64
	lastMLogHistoryGCMaxRecords       atomic.Uint64

	lastRefreshID int64
	lastPurgeID   int64

	syncRefreshAlertMu              sync.Mutex
	lastRefreshAlertAt              time.Time
	lastRefreshAlertSync            []refreshAlertTask
	lastAccumulationRowCountTaskIDs []int64
	accumulationRowCountEntered     chan struct{}
	blockAccumulationRowCount       chan struct{}

	metricsMu          sync.Mutex
	taskDurationCounts map[string]int
	runEventCounts     map[string]int
}

type scriptedServerDiscoveryHelper struct {
	*mockMVServiceHelper

	mu           sync.Mutex
	selfID       string
	seq          []map[string]serverInfo
	seqIdx       int
	refreshErr   error
	refreshCalls atomic.Int32
}

func (*mockMVServiceHelper) serverFilter(serverInfo) bool { return true }

func (*mockMVServiceHelper) getServerInfo() (serverInfo, error) {
	return serverInfo{ID: "test-server"}, nil
}

func (m *mockMVServiceHelper) getAllServerInfo(context.Context) (map[string]serverInfo, error) {
	m.serverRefreshCalls.Add(1)
	return map[string]serverInfo{"test-server": {ID: "test-server"}}, nil
}

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mviewID int64) (nextRefresh time.Time, err error) {
	m.lastRefreshID = mviewID
	if m.refreshPanic {
		panic("mock refresh panic")
	}
	return m.refreshNext, m.refreshErr
}

func (m *mockMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mlogID int64) (nextPurge time.Time, err error) {
	m.lastPurgeID = mlogID
	if m.purgePanic {
		panic("mock purge panic")
	}
	return m.purgeNext, m.purgeErr
}

func (m *mockMVServiceHelper) TryBackoffRefreshManualCancel(_ context.Context, _ basic.SessionPool, _ int64, next time.Time) (bool, time.Time, error) {
	m.refreshManualCancelBackoffCalls.Add(1)
	if !m.refreshManualCancelBackoffNextSet && m.refreshManualCancelBackoffNext.IsZero() {
		m.refreshManualCancelBackoffNext = next
	}
	return m.refreshManualCancelBackoffApplied, m.refreshManualCancelBackoffNext, m.refreshManualCancelBackoffErr
}

func (m *mockMVServiceHelper) TryBackoffPurgeManualCancel(_ context.Context, _ basic.SessionPool, _ int64, next time.Time) (bool, time.Time, error) {
	m.purgeManualCancelBackoffCalls.Add(1)
	if !m.purgeManualCancelBackoffNextSet && m.purgeManualCancelBackoffNext.IsZero() {
		m.purgeManualCancelBackoffNext = next
	}
	return m.purgeManualCancelBackoffApplied, m.purgeManualCancelBackoffNext, m.purgeManualCancelBackoffErr
}

func (m *mockMVServiceHelper) LoadAllTiDBMVLogPurge(context.Context, basic.SessionPool) (map[int64]*mlogPurgeTask, error) {
	m.fetchLogsCalls.Add(1)
	if m.fetchLogsErr != nil {
		return nil, m.fetchLogsErr
	}
	return m.fetchLogs, nil
}

func (m *mockMVServiceHelper) LoadAllTiDBMVRefresh(context.Context, basic.SessionPool) (map[int64]*mviewTask, error) {
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

func (m *mockMVServiceHelper) taskDurationCount(taskType, result string) int {
	m.metricsMu.Lock()
	defer m.metricsMu.Unlock()
	return m.taskDurationCounts[taskType+"/"+result]
}

func (m *mockMVServiceHelper) fetchDurationCount(fetchType, result string) int {
	m.metricsMu.Lock()
	defer m.metricsMu.Unlock()
	return m.taskDurationCounts[fetchType+"/"+result]
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

func (h *scriptedServerDiscoveryHelper) serverFilter(serverInfo) bool {
	return true
}

func (h *scriptedServerDiscoveryHelper) getServerInfo() (serverInfo, error) {
	if h.selfID != "" {
		return serverInfo{ID: h.selfID}, nil
	}
	return serverInfo{ID: "test-server"}, nil
}

func (h *scriptedServerDiscoveryHelper) getAllServerInfo(context.Context) (map[string]serverInfo, error) {
	h.refreshCalls.Add(1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.refreshErr != nil {
		return nil, h.refreshErr
	}
	if len(h.seq) == 0 {
		return map[string]serverInfo{"test-server": {ID: "test-server"}}, nil
	}
	cur := h.seq[h.seqIdx]
	if h.seqIdx < len(h.seq)-1 {
		h.seqIdx++
	}
	return cloneServerInfoMap(cur), nil
}

func waitExecutorFinishedCount(t *testing.T, svc *MVService, expected int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return svc.combinedTaskExecutorMetrics().finishedCount == expected
	}, testEventuallyWait, testEventuallyTick)
}

func cloneServerInfoMap(in map[string]serverInfo) map[string]serverInfo {
	out := make(map[string]serverInfo, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func testSQLDeleteMVRefreshHistByCount(limit uint64) string {
	return buildDeleteRefreshHistoryByCountLimitSQL(limit)
}

func testSQLDeleteMVLogPurgeHistByCount(limit uint64) string {
	return buildDeleteLogPurgeHistoryByCountLimitSQL(limit)
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

func (m *mockMVServiceHelper) SyncMVRefreshAlertStates(_ context.Context, _ basic.SessionPool, updatedAt time.Time, states []refreshAlertTask) error {
	m.syncRefreshAlertCalls.Add(1)
	m.syncRefreshAlertMu.Lock()
	defer m.syncRefreshAlertMu.Unlock()
	m.lastRefreshAlertAt = updatedAt
	m.lastRefreshAlertSync = append(m.lastRefreshAlertSync[:0], states...)
	return m.syncRefreshAlertErr
}

func (m *mockMVServiceHelper) CleanupStaleMVRefreshAlerts(_ context.Context, _ basic.SessionPool) error {
	m.cleanupStaleRefreshAlertCalls.Add(1)
	return m.cleanupStaleRefreshAlertErr
}

func (m *mockMVServiceHelper) LoadAllTiDBMVLogAccumulationTasks(context.Context, basic.SessionPool) (map[int64]*mlogAccumulationTask, error) {
	m.fetchAccumulationTaskCalls.Add(1)
	if m.fetchAccumulationTasksErr != nil {
		return nil, m.fetchAccumulationTasksErr
	}
	return m.fetchAccumulationTasks, nil
}

func (m *mockMVServiceHelper) LoadTiDBMVLogAccumulationRowCounts(_ context.Context, _ basic.SessionPool, tasks map[int64]*mlogAccumulationTask) (map[int64]uint64, error) {
	m.fetchAccumulationRowCountCalls.Add(1)
	if m.fetchAccumulationRowCountsErr != nil {
		return nil, m.fetchAccumulationRowCountsErr
	}
	if m.accumulationRowCountEntered != nil {
		select {
		case m.accumulationRowCountEntered <- struct{}{}:
		default:
		}
	}
	if m.blockAccumulationRowCount != nil {
		<-m.blockAccumulationRowCount
	}
	ids := make([]int64, 0, len(tasks))
	ret := make(map[int64]uint64, len(tasks))
	for id := range tasks {
		ids = append(ids, id)
		if rowCount, ok := m.fetchAccumulationRowCounts[id]; ok {
			ret[id] = rowCount
		}
	}
	slices.Sort(ids)
	m.lastAccumulationRowCountTaskIDs = ids
	return ret, nil
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

func (m *mockMVServiceHelper) PurgeMVHistoryBeforeTSO(
	_ context.Context,
	_ basic.SessionPool,
	currentTSO uint64,
	mviewRefreshRetention time.Duration,
	mlogPurgeRetention time.Duration,
) error {
	if m.historyGCPanic {
		panic("mock history gc panic")
	}
	m.historyGCCalls.Add(1)
	m.lastHistoryGCCurrentTSO.Store(currentTSO)
	m.lastMViewHistoryGCRetention.Store(int64(mviewRefreshRetention))
	m.lastMLogHistoryGCRetention.Store(int64(mlogPurgeRetention))
	m.lastMViewHistoryGCMaxRecords.Store(defaultMVHistoryGCMaxRecords)
	m.lastMLogHistoryGCMaxRecords.Store(defaultMVHistoryGCMaxRecords)
	return m.historyGCErr
}

func setHistoryGCOwnerForTest(svc *MVService, ownerHash uint32) {
	setServiceOwnersForTest(svc, map[string]uint32{
		mvHistoryGCOwnerKey: ownerHash,
	})
	svc.nextHistoryGCAtMillis.Store(0)
}

func setRefreshAlertCleanupOwnerForTest(svc *MVService, ownerHash uint32) {
	setServiceOwnersForTest(svc, map[string]uint32{
		mvRefreshAlertCleanupOwnerKey: ownerHash,
	})
}

func addMVRefreshAlertTasksForTest(svc *MVService, tasks ...*mviewTask) {
	pending := make(map[int64]*mviewTask, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.orderTs == 0 && !task.nextRefresh.IsZero() {
			task.orderTs = task.nextRefresh.UnixMilli()
		}
		pending[task.ID] = task
	}
	svc.buildMViewRefreshAlertTasks(pending)
}

func addMVRefreshExecutionTasksForTest(svc *MVService, tasks ...*mviewTask) {
	svc.mviewRefreshMu.Lock()
	defer svc.mviewRefreshMu.Unlock()
	if svc.mviewRefreshMu.pending == nil {
		svc.mviewRefreshMu.pending = make(map[int64]mviewTaskItem, len(tasks))
	}
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.orderTs == 0 && !task.nextRefresh.IsZero() {
			task.orderTs = task.nextRefresh.UnixMilli()
		}
		svc.mviewRefreshMu.pending[task.ID] = svc.mviewRefreshMu.prio.Push(task)
	}
}

func setTaskOwnersForTest(svc *MVService, taskHashes map[int64]uint32) {
	overrides := make(map[string]uint32, len(taskHashes))
	for id, hash := range taskHashes {
		overrides[string(int64KeyToBinaryBytes(id))] = hash
	}
	setServiceHashOverridesForTest(svc, overrides)
}

func setServiceOwnersForTest(svc *MVService, ownerHashes map[string]uint32) {
	setServiceHashOverridesForTest(svc, ownerHashes)
}

func setServiceHashOverridesForTest(svc *MVService, overrides map[string]uint32) {
	svc.sch.mu.Lock()
	svc.sch.ID = "nodeA"
	svc.sch.chash.replicas = 1
	mapping := map[string]uint32{
		"nodeA#0":                     10,
		"nodeB#0":                     30,
		mvHistoryGCOwnerKey:           10,
		mvRefreshAlertCleanupOwnerKey: 10,
	}
	for key, hash := range overrides {
		mapping[key] = hash
	}
	svc.sch.chash.hashFunc = mustHash(mapping)
	svc.sch.rebuildLocked(map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
	})
	svc.sch.mu.Unlock()
}

func setThreeNodeRefreshAlertOwnersForTest(svc *MVService, selfID string) {
	svc.sch.mu.Lock()
	svc.sch.ID = selfID
	svc.sch.chash.replicas = 1
	svc.sch.chash.hashFunc = mustHash(map[string]uint32{
		"nodeA#0":                     10,
		"nodeB#0":                     20,
		"nodeC#0":                     30,
		mvRefreshAlertCleanupOwnerKey: 25,
	})
	svc.sch.rebuildLocked(map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
		"nodeC": {ID: "nodeC"},
	})
	svc.sch.mu.Unlock()
}
