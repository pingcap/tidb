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
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/types"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	testSQLFetchMVLogPurge     = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	testSQLFetchMVRefresh      = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	testSQLRefreshMV           = `REFRESH MATERIALIZED VIEW %n.%n FAST`
	testSQLFindMVNextTime      = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	testSQLLockMVNextTime      = `SELECT NEXT_TIME FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT`
	testSQLUpdateMVNextTime    = `UPDATE mysql.tidb_mview_refresh_info SET NEXT_TIME = %? WHERE MVIEW_ID = %?`
	testSQLPurgeMVLog          = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
	testSQLFindPurgeNextTime   = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_TIME IS NOT NULL`
	testSQLLockPurgeNextTime   = `SELECT NEXT_TIME FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT`
	testSQLUpdatePurgeNextTime = `UPDATE mysql.tidb_mlog_purge_info SET NEXT_TIME = %? WHERE MLOG_ID = %?`
)

var (
	testSQLDeleteMVRefreshHistBeforeTSO = fmt.Sprintf(
		`DELETE FROM mysql.tidb_mview_refresh_hist WHERE REFRESH_JOB_ID < %%? ORDER BY REFRESH_JOB_ID LIMIT %d`,
		historyGCDeleteBatchSize,
	)
	testSQLDeleteMVLogPurgeHistBeforeTSO = fmt.Sprintf(
		`DELETE FROM mysql.tidb_mlog_purge_hist WHERE PURGE_JOB_ID < %%? ORDER BY PURGE_JOB_ID LIMIT %d`,
		historyGCDeleteBatchSize,
	)
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
	executedSQL             []string
	execErrs                map[string]error
	executedRestrictedSQL   []string
	executedRestrictedArg   [][]any
	restrictedMaintainQuota []int64
	restrictedMaxThreads    []int64
	restrictedStreamCount   []int64
	restrictedBatchSize     []uint64
	restrictedRows          map[string][]chunk.Row
	restrictedErrs          map[string]error
	restrictedAffectedRows  map[string][]uint64
	restrictedAffectedPos   map[string]int
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
	s.restrictedMaintainQuota = append(s.restrictedMaintainQuota, s.GetSessionVars().MVMaintainMemQuota)
	s.restrictedMaxThreads = append(s.restrictedMaxThreads, s.GetSessionVars().TiFlashMaxThreads)
	s.restrictedStreamCount = append(s.restrictedStreamCount, s.GetSessionVars().TiFlashFineGrainedShuffleStreamCount)
	s.restrictedBatchSize = append(s.restrictedBatchSize, s.GetSessionVars().TiFlashFineGrainedShuffleBatchSize)
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

func TestLatestServerInfosByInstanceKeepsNewestDDLID(t *testing.T) {
	allServers := map[string]*infosync.ServerInfo{
		"old-node": {
			StaticServerInfo: infosync.StaticServerInfo{
				ID:             "old-node",
				IP:             "127.0.0.1",
				Port:           4000,
				StartTimestamp: 100,
			},
		},
		"new-node": {
			StaticServerInfo: infosync.StaticServerInfo{
				ID:             "new-node",
				IP:             "127.0.0.1",
				Port:           4000,
				StartTimestamp: 200,
			},
		},
		"peer-node": {
			StaticServerInfo: infosync.StaticServerInfo{
				ID:             "peer-node",
				IP:             "127.0.0.2",
				Port:           4000,
				StartTimestamp: 150,
			},
		},
	}

	got := latestServerInfosByInstance(allServers)

	require.Len(t, got, 2)
	require.NotContains(t, got, "old-node")
	require.Equal(t, serverInfo{
		ID:             "new-node",
		IP:             "127.0.0.1",
		Port:           4000,
		StartTimestamp: 200,
	}, got["new-node"])
	require.Equal(t, serverInfo{
		ID:             "peer-node",
		IP:             "127.0.0.2",
		Port:           4000,
		StartTimestamp: 150,
	}, got["peer-node"])
}

func waitExecutorFinishedCount(t *testing.T, svc *MVService, expected int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return svc.executor.metrics.counters.finishedCount.Load() == expected
	}, testEventuallyWait, testEventuallyTick)
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
	fetchLogs                         map[int64]*mvLog
	fetchViews                        map[int64]*mv
	fetchLogsErr                      error
	fetchViewsErr                     error
	fetchLogsCalls                    atomic.Int32
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

	lastRefreshID int64
	lastPurgeID   int64

	metricsMu          sync.Mutex
	taskDurationCounts map[string]int
	runEventCounts     map[string]int
}

func (*mockMVServiceHelper) serverFilter(serverInfo) bool { return true }

func (*mockMVServiceHelper) getServerInfo() (serverInfo, error) {
	return serverInfo{ID: "test-server"}, nil
}

func (m *mockMVServiceHelper) getAllServerInfo(context.Context) (map[string]serverInfo, error) {
	m.serverRefreshCalls.Add(1)
	return map[string]serverInfo{"test-server": {ID: "test-server"}}, nil
}

func (m *mockMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	m.lastRefreshID = mvID
	if m.refreshPanic {
		panic("mock refresh panic")
	}
	return m.refreshNext, m.refreshErr
}

func (m *mockMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	m.lastPurgeID = mvLogID
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

func (m *mockMVServiceHelper) loadAllTiDBMVLogPurge(context.Context, basic.SessionPool) (map[int64]*mvLog, error) {
	m.fetchLogsCalls.Add(1)
	if m.fetchLogsErr != nil {
		return nil, m.fetchLogsErr
	}
	return m.fetchLogs, nil
}

func (m *mockMVServiceHelper) loadAllTiDBMVRefresh(context.Context, basic.SessionPool) (map[int64]*mv, error) {
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

type scriptedServerDiscoveryHelper struct {
	*mockMVServiceHelper

	mu           sync.Mutex
	selfID       string
	seq          []map[string]serverInfo
	seqIdx       int
	refreshErr   error
	refreshCalls atomic.Int32
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

func cloneServerInfoMap(in map[string]serverInfo) map[string]serverInfo {
	out := make(map[string]serverInfo, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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

	helper := &mockMVServiceHelper{
		fetchLogs:  map[int64]*mvLog{},
		fetchViews: map[int64]*mv{},
	}
	svc := NewMVService(ctx, mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())
	runMVServiceForTest(t, svc, cancel)

	require.Equal(t, int32(0), helper.fetchLogsCalls.Load())
	require.Equal(t, int32(0), helper.fetchViewCalls.Load())

	svc.NotifyDDLChange()
	require.Eventually(t, func() bool {
		return helper.fetchLogsCalls.Load() > 0 && helper.fetchViewCalls.Load() > 0
	}, testEventuallyWait, testEventuallyTick)
	require.Eventually(t, func() bool {
		return helper.runEventCount(mvRunEventFetchByDDL) > 0
	}, testEventuallyWait, testEventuallyTick)
}

func TestMVServiceServerRefreshWithoutPeriodicFetch(t *testing.T) {
	newServerRefreshServiceForTest := func(
		t *testing.T,
		configure func(*mockMVServiceHelper) *scriptedServerDiscoveryHelper,
	) (*MockTimeModule, Config, *mockMVServiceHelper, *scriptedServerDiscoveryHelper) {
		t.Helper()

		module := installMockTimeForTest(t)
		ctx, cancel := context.WithCancel(context.Background())
		baseHelper := &mockMVServiceHelper{
			fetchLogs:  map[int64]*mvLog{},
			fetchViews: map[int64]*mv{},
		}
		cfg := DefaultMVServiceConfig()
		cfg.FetchInterval = 24 * time.Hour
		cfg.ServerRefreshInterval = time.Second

		discovery := configure(baseHelper)
		svc := NewMVService(ctx, mockSessionPool{}, discovery, cfg)
		svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())
		runMVServiceForTest(t, svc, cancel)
		return module, cfg, baseHelper, discovery
	}

	waitInitialServerRefresh := func(t *testing.T, discovery *scriptedServerDiscoveryHelper) {
		t.Helper()
		require.Eventually(t, func() bool {
			return discovery.refreshCalls.Load() > 0
		}, testEventuallyWait, testEventuallyTick)
	}

	testCases := []struct {
		name                  string
		configure             func(*mockMVServiceHelper) *scriptedServerDiscoveryHelper
		advanceSecondRefresh  bool
		expectServerChanged   bool
		expectServerRefeshErr bool
		expectMetaFetch       bool
	}{
		{
			name: "server_changed_triggers_fetch",
			configure: func(baseHelper *mockMVServiceHelper) *scriptedServerDiscoveryHelper {
				return &scriptedServerDiscoveryHelper{
					mockMVServiceHelper: baseHelper,
					selfID:              "test-server",
					seq: []map[string]serverInfo{
						{
							"test-server": {ID: "test-server"},
						},
						{
							"test-server": {ID: "test-server"},
							"node-2":      {ID: "node-2"},
						},
					},
				}
			},
			advanceSecondRefresh:  true,
			expectServerChanged:   true,
			expectServerRefeshErr: false,
			expectMetaFetch:       true,
		},
		{
			name: "server_refresh_error_does_not_trigger_fetch",
			configure: func(baseHelper *mockMVServiceHelper) *scriptedServerDiscoveryHelper {
				return &scriptedServerDiscoveryHelper{
					mockMVServiceHelper: baseHelper,
					selfID:              "test-server",
					refreshErr:          errors.New("refresh failed"),
				}
			},
			expectServerChanged:   false,
			expectServerRefeshErr: true,
			expectMetaFetch:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			module, cfg, baseHelper, discovery := newServerRefreshServiceForTest(t, tc.configure)
			waitInitialServerRefresh(t, discovery)

			require.Equal(t, 0, baseHelper.runEventCount(mvRunEventServerChanged))
			require.Equal(t, int32(0), baseHelper.fetchLogsCalls.Load())
			require.Equal(t, int32(0), baseHelper.fetchViewCalls.Load())

			if tc.advanceSecondRefresh {
				module.Advance(cfg.ServerRefreshInterval)
				require.Eventually(t, func() bool {
					return discovery.refreshCalls.Load() > 1
				}, testEventuallyWait, testEventuallyTick)
			}

			if tc.expectServerChanged {
				require.Eventually(t, func() bool {
					return baseHelper.runEventCount(mvRunEventServerChanged) > 0
				}, testEventuallyWait, testEventuallyTick)
			} else {
				require.Equal(t, 0, baseHelper.runEventCount(mvRunEventServerChanged))
			}

			if tc.expectServerRefeshErr {
				require.Eventually(t, func() bool {
					return baseHelper.runEventCount(mvRunEventServerRefreshError) > 0
				}, testEventuallyWait, testEventuallyTick)
			} else {
				require.Equal(t, 0, baseHelper.runEventCount(mvRunEventServerRefreshError))
			}

			if tc.expectMetaFetch {
				require.Eventually(t, func() bool {
					return baseHelper.fetchLogsCalls.Load() > 0 && baseHelper.fetchViewCalls.Load() > 0
				}, testEventuallyWait, testEventuallyTick)
			} else {
				require.Equal(t, int32(0), baseHelper.fetchLogsCalls.Load())
				require.Equal(t, int32(0), baseHelper.fetchViewCalls.Load())
			}

			require.Equal(t, 0, baseHelper.runEventCount(mvRunEventFetchByDDL))
			require.Equal(t, 0, baseHelper.runEventCount(mvRunEventFetchByInterval))
		})
	}
}

func TestMVServiceMaintenanceTimerTriggersHistoryGC(t *testing.T) {
	module := installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())

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
	svc.nextHistoryGCAtMillis.Store(mvsNow().Add(defaultMVHistoryGCInterval).UnixMilli())
	runMVServiceForTest(t, svc, cancel)

	require.Equal(t, int32(0), helper.historyGCCalls.Load())
	require.Eventually(t, func() bool {
		return helper.serverRefreshCalls.Load() > 0
	}, testEventuallyWait, testEventuallyTick)

	module.Advance(defaultMVHistoryGCInterval + cfg.BasicInterval)
	require.Eventually(t, func() bool {
		return helper.historyGCCalls.Load() > 0
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, helper.currentTSO, helper.lastHistoryGCCurrentTSO.Load())
	require.Equal(t, int64(defaultMVHistoryGCRetention), helper.lastMViewHistoryGCRetention.Load())
	require.Equal(t, int64(defaultMVHistoryGCRetention), helper.lastMLogHistoryGCRetention.Load())
	require.Eventually(t, func() bool {
		return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess) > 0
	}, testEventuallyWait, testEventuallyTick)
}

func TestMVServiceMaybeGCMVHistorySkipsWhenNotOwner(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		currentTSO: 1 << 40,
	}
	cfg := DefaultMVServiceConfig()
	cfg.ServerConsistentHashReplicas = 1
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
	svc.nextHistoryGCAtMillis.Store(0)
	svc.historyGCRetryCount.Store(3)
	setHistoryGCOwnerForTest(svc, 20)

	svc.maybeGCOperationHistory(mvsNow())
	require.Equal(t, int32(0), helper.historyGCCalls.Load())
	require.Equal(t, 0, helper.runEventCount(mvRunEventHistoryGCGetTSOErr))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess))
	require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed))
	require.Equal(t, int64(0), svc.historyGCRetryCount.Load())
}

func TestMVServiceMaybeGCMVHistoryReportsMetrics(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("success", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSO: 1 << 40}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		svc.maybeGCOperationHistory(mvsNow())
		require.Eventually(t, func() bool {
			return helper.historyGCCalls.Load() == 1
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultSuccess) > 0
		}, testEventuallyWait, testEventuallyTick)
	})

	t.Run("get_tso_error", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSOErr: errors.New("get tso failed")}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		svc.maybeGCOperationHistory(mvsNow())
		require.Equal(t, int32(0), helper.historyGCCalls.Load())
		require.Eventually(t, func() bool {
			return helper.runEventCount(mvRunEventHistoryGCGetTSOErr) > 0
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed) > 0
		}, testEventuallyWait, testEventuallyTick)
	})

	t.Run("purge_error", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			currentTSO:   1 << 40,
			historyGCErr: errors.New("purge failed"),
		}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		svc.maybeGCOperationHistory(mvsNow())
		require.Eventually(t, func() bool {
			return helper.historyGCCalls.Load() == 1
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed) > 0
		}, testEventuallyWait, testEventuallyTick)
	})

	t.Run("get_tso_error_retries_quickly", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSOErr: errors.New("get tso failed")}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		cfg.BasicInterval = 100 * time.Millisecond
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		startAt := mvsNow()
		svc.maybeGCOperationHistory(startAt)
		require.Equal(t, int32(0), helper.historyGCCalls.Load())
		require.Eventually(t, func() bool {
			return helper.runEventCount(mvRunEventHistoryGCGetTSOErr) > 0
		}, testEventuallyWait, testEventuallyTick)

		helper.currentTSOErr = nil
		svc.maybeGCOperationHistory(startAt.Add(cfg.BasicInterval))
		require.Eventually(t, func() bool {
			return helper.historyGCCalls.Load() == 1
		}, testEventuallyWait, testEventuallyTick)
	})

	t.Run("error_waits_for_basic_interval_retry", func(t *testing.T) {
		helper := &mockMVServiceHelper{currentTSOErr: context.Canceled}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		cfg.BasicInterval = 100 * time.Millisecond
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		startAt := mvsNow()
		svc.maybeGCOperationHistory(startAt)
		require.Equal(t, int32(0), helper.historyGCCalls.Load())
		require.Eventually(t, func() bool {
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed) > 0
		}, testEventuallyWait, testEventuallyTick)

		helper.currentTSOErr = nil
		svc.maybeGCOperationHistory(startAt.Add(cfg.BasicInterval / 2))
		require.Equal(t, int32(0), helper.historyGCCalls.Load())

		svc.maybeGCOperationHistory(startAt.Add(cfg.BasicInterval))
		require.Eventually(t, func() bool {
			return helper.historyGCCalls.Load() == 1
		}, testEventuallyWait, testEventuallyTick)
	})

	t.Run("panic_in_history_gc_is_recovered", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			currentTSO:     1 << 40,
			historyGCPanic: true,
		}
		cfg := DefaultMVServiceConfig()
		cfg.ServerConsistentHashReplicas = 1
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
		setHistoryGCOwnerForTest(svc, 5)

		svc.maybeGCOperationHistory(mvsNow())
		require.Eventually(t, func() bool {
			return helper.runEventCount(mvRunEventRecoveredPanic) > 0
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed) > 0
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return !svc.historyGCRunning.Load()
		}, testEventuallyWait, testEventuallyTick)
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

func TestMVServiceTaskResult(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("purge", func(t *testing.T) {
		t.Run("remove_on_zero_next", func(t *testing.T) {
			helper := &mockMVServiceHelper{}
			svc := newRunningMVServiceForTest(t, helper)

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
			}, testEventuallyWait, testEventuallyTick)
			require.Equal(t, int64(0), l.retryCount.Load())
			require.Equal(t, int64(0), svc.metrics.mvLogCount.Load())
		})

		t.Run("reschedule_on_non_zero_next", func(t *testing.T) {
			nextPurge := mvsNow().Add(time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				purgeNext: nextPurge,
			}
			svc := newRunningMVServiceForTest(t, helper)

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

		t.Run("manual_cancel_applied_backoff", func(t *testing.T) {
			expectedNext := mvsNow().Add(manualCancelBackoffDelay + time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				purgeErr:                        errMVTaskCanceledManually,
				purgeManualCancelBackoffApplied: true,
				purgeManualCancelBackoffNext:    expectedNext,
			}
			svc := newRunningMVServiceForTest(t, helper)

			l := &mvLog{
				ID:        305,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
			svc.purgeMVLog([]*mvLog{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())

			svc.mvLogPurgeMu.Lock()
			item, ok := svc.mvLogPurgeMu.pending[l.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextPurge.Equal(expectedNext))
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
			svc.mvLogPurgeMu.Unlock()
		})

		t.Run("manual_cancel_applied_clear_schedule", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				purgeErr:                        errMVTaskCanceledManually,
				purgeManualCancelBackoffApplied: true,
				purgeManualCancelBackoffNextSet: true,
			}
			svc := newRunningMVServiceForTest(t, helper)

			l := &mvLog{
				ID:        307,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
			svc.purgeMVLog([]*mvLog{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())
			require.Eventually(t, func() bool {
				svc.mvLogPurgeMu.Lock()
				_, ok := svc.mvLogPurgeMu.pending[l.ID]
				svc.mvLogPurgeMu.Unlock()
				return !ok
			}, testEventuallyWait, testEventuallyTick)
		})

		t.Run("manual_cancel_without_persist_forces_refetch", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				purgeErr: errMVTaskCanceledManually,
			}
			svc := newRunningMVServiceForTest(t, helper)
			svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())

			l := &mvLog{
				ID:        306,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})
			svc.purgeMVLog([]*mvLog{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())
			require.Equal(t, int64(0), svc.lastMetaFetchMillis.Load())

			svc.mvLogPurgeMu.Lock()
			_, ok := svc.mvLogPurgeMu.pending[l.ID]
			svc.mvLogPurgeMu.Unlock()
			require.False(t, ok)
		})
	})

	t.Run("refresh", func(t *testing.T) {
		t.Run("remove_on_zero_next", func(t *testing.T) {
			helper := &mockMVServiceHelper{}
			svc := newRunningMVServiceForTest(t, helper)

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
			}, testEventuallyWait, testEventuallyTick)
			require.Equal(t, int64(0), m.retryCount.Load())
			require.Equal(t, int64(0), svc.metrics.mvCount.Load())
		})

		t.Run("reschedule_on_non_zero_next", func(t *testing.T) {
			nextRefresh := mvsNow().Add(time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				refreshNext: nextRefresh,
			}
			svc := newRunningMVServiceForTest(t, helper)

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

		t.Run("manual_cancel_applied_backoff", func(t *testing.T) {
			expectedNext := mvsNow().Add(manualCancelBackoffDelay + time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				refreshErr:                        errMVTaskCanceledManually,
				refreshManualCancelBackoffApplied: true,
				refreshManualCancelBackoffNext:    expectedNext,
			}
			svc := newRunningMVServiceForTest(t, helper)

			m := &mv{
				ID:          403,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
			svc.refreshMV([]*mv{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())

			svc.mvRefreshMu.Lock()
			item, ok := svc.mvRefreshMu.pending[m.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextRefresh.Equal(expectedNext))
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
			svc.mvRefreshMu.Unlock()
		})

		t.Run("manual_cancel_applied_clear_schedule", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				refreshErr:                        errMVTaskCanceledManually,
				refreshManualCancelBackoffApplied: true,
				refreshManualCancelBackoffNextSet: true,
			}
			svc := newRunningMVServiceForTest(t, helper)

			m := &mv{
				ID:          405,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
			svc.refreshMV([]*mv{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())
			require.Eventually(t, func() bool {
				svc.mvRefreshMu.Lock()
				_, ok := svc.mvRefreshMu.pending[m.ID]
				svc.mvRefreshMu.Unlock()
				return !ok
			}, testEventuallyWait, testEventuallyTick)
		})

		t.Run("manual_cancel_without_persist_forces_refetch", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				refreshErr: errMVTaskCanceledManually,
			}
			svc := newRunningMVServiceForTest(t, helper)
			svc.lastMetaFetchMillis.Store(mvsNow().UnixMilli())

			m := &mv{
				ID:          404,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})
			svc.refreshMV([]*mv{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())
			require.Equal(t, int64(0), svc.lastMetaFetchMillis.Load())

			svc.mvRefreshMu.Lock()
			_, ok := svc.mvRefreshMu.pending[m.ID]
			svc.mvRefreshMu.Unlock()
			require.False(t, ok)
		})
	})
}

func TestMVServiceExecuteTaskSkipWhenDeleted(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("refresh", func(t *testing.T) {
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
	})

	t.Run("refresh_panic_reschedules", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			refreshPanic: true,
		}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

		m := &mv{
			ID:          404,
			nextRefresh: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMVRefreshTasks(map[int64]*mv{m.ID: m})

		err := svc.executeRefreshTask(m)
		require.Error(t, err)
		require.Contains(t, err.Error(), "refresh MV task panicked")
		require.Equal(t, int64(1), m.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.runningMVRefreshCount.Load())
		require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultFailed))

		svc.mvRefreshMu.Lock()
		item, ok := svc.mvRefreshMu.pending[m.ID]
		require.True(t, ok)
		require.Equal(t, m, item.Value)
		require.NotEqual(t, int64(maxNextScheduleTs), item.Value.orderTs)
		svc.mvRefreshMu.Unlock()
	})

	t.Run("purge", func(t *testing.T) {
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
	})

	t.Run("purge_panic_reschedules", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			purgePanic: true,
		}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

		l := &mvLog{
			ID:        304,
			nextPurge: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMVLogPurgeTasks(map[int64]*mvLog{l.ID: l})

		err := svc.executePurgeTask(l)
		require.Error(t, err)
		require.Contains(t, err.Error(), "purge MV log task panicked")
		require.Equal(t, int64(1), l.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.runningMVLogPurgeCount.Load())
		require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultFailed))

		svc.mvLogPurgeMu.Lock()
		item, ok := svc.mvLogPurgeMu.pending[l.ID]
		require.True(t, ok)
		require.Equal(t, l, item.Value)
		require.NotEqual(t, int64(maxNextScheduleTs), item.Value.orderTs)
		svc.mvLogPurgeMu.Unlock()
	})
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
	require.Equal(t, int32(2), called.Load())

	mvEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, mvEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionCreateMaterializedView))))
	require.NoError(t, gotHandler(context.Background(), nil, mvEvent))
	require.Equal(t, int32(2), called.Load())

	alterMVRefreshEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, alterMVRefreshEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionAlterMaterializedViewRefresh))))
	require.NoError(t, gotHandler(context.Background(), nil, alterMVRefreshEvent))
	require.Equal(t, int32(3), called.Load())

	alterMLogPurgeEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, alterMLogPurgeEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionAlterMaterializedViewLogPurge))))
	require.NoError(t, gotHandler(context.Background(), nil, alterMLogPurgeEvent))
	require.Equal(t, int32(4), called.Load())

	outOfPlaceCutoverEvent := &notifier.SchemaChangeEvent{}
	require.NoError(t, outOfPlaceCutoverEvent.UnmarshalJSON([]byte(fmt.Sprintf(`{"type":%d}`, meta.ActionMViewRefreshOutOfPlaceCutover))))
	require.NoError(t, gotHandler(context.Background(), nil, outOfPlaceCutoverEvent))
	require.Equal(t, int32(5), called.Load())

	dropTableEvent := notifier.NewDropTableEvent(&meta.TableInfo{ID: 4})
	require.NoError(t, gotHandler(context.Background(), nil, dropTableEvent))
	require.Equal(t, int32(5), called.Load())

	dropMVTableEvent := notifier.NewDropTableEvent(&meta.TableInfo{
		ID: 5,
		MaterializedView: &meta.MaterializedViewInfo{
			BaseTableIDs: []int64{1},
		},
	})
	require.NoError(t, gotHandler(context.Background(), nil, dropMVTableEvent))
	require.Equal(t, int32(6), called.Load())
}

func TestServerHelperLoadAllTiDBMLogPurge(t *testing.T) {
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

	got, err := (&serviceHelper{}).loadAllTiDBMVLogPurge(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVLogPurge}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	l201 := got[int64(201)]
	require.NotNil(t, l201)
	expect201 := time.Unix(nextPurgeSec1, 0)
	require.Equal(t, expect201, l201.nextPurge)
	require.Equal(t, expect201.UnixMilli(), l201.orderTs)

	l202 := got[int64(202)]
	require.NotNil(t, l202)
	expect202 := time.Unix(nextPurgeSec2, 0)
	require.Equal(t, expect202, l202.nextPurge)
	require.Equal(t, expect202.UnixMilli(), l202.orderTs)
}

func TestServerHelperLoadAllTiDBMVRefresh(t *testing.T) {
	installMockTimeForTest(t)
	nextRefreshSec1 := int64(900)
	nextRefreshSec2 := int64(1200)

	se := newRecordingSessionContext()
	se.restrictedRows[testSQLFetchMVRefresh] = []chunk.Row{
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec1),
			types.NewIntDatum(101),
			types.NewIntDatum(123456789),
		}).ToRow(),
		// Valid row.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(102),
			types.NewIntDatum(223456789),
		}).ToRow(),
		// Invalid row with non-positive ID should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(nextRefreshSec2),
			types.NewIntDatum(0),
			types.NewIntDatum(323456789),
		}).ToRow(),
		// Invalid row with NULL NEXT_TIME should be ignored.
		chunk.MutRowFromDatums([]types.Datum{
			types.NewDatum(nil),
			types.NewIntDatum(103),
			types.NewIntDatum(423456789),
		}).ToRow(),
	}
	pool := recordingSessionPool{se: se}

	got, err := (&serviceHelper{}).loadAllTiDBMVRefresh(context.Background(), pool)
	require.NoError(t, err)
	require.Equal(t, []string{testSQLFetchMVRefresh}, se.executedRestrictedSQL)
	require.Len(t, got, 2)

	m101 := got[int64(101)]
	require.NotNil(t, m101)
	expect101 := time.Unix(nextRefreshSec1, 0)
	require.Equal(t, expect101, m101.nextRefresh)
	require.Equal(t, expect101.UnixMilli(), m101.orderTs)
	require.Equal(t, uint64(123456789), m101.lastSuccessReadTSO)
	require.Equal(t, mvsUnixMilli(oracle.ExtractPhysical(123456789)), m101.lastSuccessTime)

	m102 := got[int64(102)]
	require.NotNil(t, m102)
	expect102 := time.Unix(nextRefreshSec2, 0)
	require.Equal(t, expect102, m102.nextRefresh)
	require.Equal(t, expect102.UnixMilli(), m102.orderTs)
	require.Equal(t, uint64(223456789), m102.lastSuccessReadTSO)
	require.Equal(t, mvsUnixMilli(oracle.ExtractPhysical(223456789)), m102.lastSuccessTime)
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

	buildExpectedSQL := func(refreshExec, purgeExec int) []string {
		sqls := make([]string, 0, refreshExec+purgeExec)
		for i := 0; i < refreshExec; i++ {
			sqls = append(sqls, testSQLDeleteMVRefreshHistBeforeTSO)
		}
		for i := 0; i < purgeExec; i++ {
			sqls = append(sqls, testSQLDeleteMVLogPurgeHistBeforeTSO)
		}
		return sqls
	}

	testCases := []struct {
		name                  string
		currentTSO            uint64
		mviewRetention        time.Duration
		mlogRetention         time.Duration
		refreshAffectedRows   []uint64
		purgeAffectedRows     []uint64
		expectRefreshExec     int
		expectPurgeExec       int
		assertArgsAreEqualTSO bool
		assertArgsNotEqual    bool
	}{
		{
			name:                  "zero_retention",
			currentTSO:            987654321,
			mviewRetention:        0,
			mlogRetention:         0,
			expectRefreshExec:     1,
			expectPurgeExec:       1,
			assertArgsAreEqualTSO: true,
		},
		{
			name:               "separate_retention",
			currentTSO:         uint64((10 * time.Hour / time.Millisecond) << 18),
			mviewRetention:     2 * time.Hour,
			mlogRetention:      4 * time.Hour,
			expectRefreshExec:  1,
			expectPurgeExec:    1,
			assertArgsNotEqual: true,
		},
		{
			name:           "batch_delete",
			currentTSO:     987654321,
			mviewRetention: 0,
			mlogRetention:  0,
			refreshAffectedRows: []uint64{
				uint64(historyGCDeleteBatchSize),
				uint64(historyGCDeleteBatchSize),
				1,
			},
			purgeAffectedRows: []uint64{0},
			expectRefreshExec: 3,
			expectPurgeExec:   1,
		},
		{
			name:              "batch_delete_no_max_batches",
			currentTSO:        987654321,
			mviewRetention:    0,
			mlogRetention:     0,
			expectRefreshExec: 26,
			expectPurgeExec:   1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			se := newRecordingSessionContext()
			if tc.name == "batch_delete_no_max_batches" {
				const extraBatches = 25
				refreshAffected := make([]uint64, extraBatches)
				for i := range refreshAffected {
					refreshAffected[i] = uint64(historyGCDeleteBatchSize)
				}
				se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = append(refreshAffected, 1)
				se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = []uint64{0}
			} else {
				if len(tc.refreshAffectedRows) > 0 {
					se.restrictedAffectedRows[testSQLDeleteMVRefreshHistBeforeTSO] = tc.refreshAffectedRows
				}
				if len(tc.purgeAffectedRows) > 0 {
					se.restrictedAffectedRows[testSQLDeleteMVLogPurgeHistBeforeTSO] = tc.purgeAffectedRows
				}
			}
			pool := recordingSessionPool{se: se}

			err := (&serviceHelper{}).PurgeMVHistoryBeforeTSO(
				context.Background(),
				pool,
				tc.currentTSO,
				tc.mviewRetention,
				tc.mlogRetention,
			)
			require.NoError(t, err)
			require.Equal(t, buildExpectedSQL(tc.expectRefreshExec, tc.expectPurgeExec), se.executedRestrictedSQL)
			require.Len(t, se.executedRestrictedArg, tc.expectRefreshExec+tc.expectPurgeExec)

			if tc.assertArgsAreEqualTSO {
				require.Equal(t, []any{tc.currentTSO}, se.executedRestrictedArg[0])
				require.Equal(t, []any{tc.currentTSO}, se.executedRestrictedArg[tc.expectRefreshExec])
			}
			if tc.assertArgsNotEqual {
				require.NotEqual(t, se.executedRestrictedArg[0][0], se.executedRestrictedArg[tc.expectRefreshExec][0])
			}
		})
	}
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

func TestServerHelperRefreshMVManualCancelNormalized(t *testing.T) {
	installMockTimeForTest(t)

	se := newRecordingSessionContext()
	se.restrictedErrs[testSQLRefreshMV] = errors.New("materialized view task canceled manually")
	mvTable := &meta.TableInfo{
		ID:    101,
		Name:  pmodel.NewCIStr("mv1"),
		State: meta.StatePublic,
	}
	withMockInfoSchema(t, mvTable)
	pool := recordingSessionPool{se: se}

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.ErrorIs(t, err, errMVTaskCanceledManually)
	require.True(t, nextRefresh.IsZero())
	require.Equal(t, []string{testSQLRefreshMV}, se.executedRestrictedSQL)
}

func TestServerHelperRefreshMVUsesGlobalRefreshSessionVars(t *testing.T) {
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

	vars := se.GetSessionVars()
	mockGlobalAccessor := variable.NewMockGlobalAccessor4Tests()
	mockGlobalAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = mockGlobalAccessor
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBMVMaintainMemQuota, "536870912"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBMaxTiFlashThreads, "8"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiFlashFineGrainedShuffleStreamCount, "16"))
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiFlashFineGrainedShuffleBatchSize, "4096"))
	require.NoError(t, vars.SetSystemVar(variable.TiDBMVMaintainMemQuota, "268435456"))
	require.NoError(t, vars.SetSystemVar(variable.TiDBMaxTiFlashThreads, "2"))
	require.NoError(t, vars.SetSystemVar(variable.TiFlashFineGrainedShuffleStreamCount, "4"))
	require.NoError(t, vars.SetSystemVar(variable.TiFlashFineGrainedShuffleBatchSize, "1024"))

	nextRefresh, err := (&serviceHelper{}).RefreshMV(context.Background(), pool, 101)
	require.NoError(t, err)
	require.Equal(t, expectedNextRefresh.Unix(), nextRefresh.Unix())
	require.Equal(t, []string{
		testSQLRefreshMV,
		testSQLFindMVNextTime,
	}, se.executedRestrictedSQL)
	require.Equal(t, []int64{536870912, 536870912}, se.restrictedMaintainQuota)
	require.Equal(t, []int64{8, 8}, se.restrictedMaxThreads)
	require.Equal(t, []int64{16, 16}, se.restrictedStreamCount)
	require.Equal(t, []uint64{4096, 4096}, se.restrictedBatchSize)
	require.Equal(t, int64(268435456), vars.MVMaintainMemQuota)
	require.Equal(t, int64(2), vars.TiFlashMaxThreads)
	require.Equal(t, int64(4), vars.TiFlashFineGrainedShuffleStreamCount)
	require.Equal(t, uint64(1024), vars.TiFlashFineGrainedShuffleBatchSize)
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

func TestServerHelperPurgeMVLogManualCancelNormalized(t *testing.T) {
	installMockTimeForTest(t)

	se := newRecordingSessionContext()
	setupPurgeMVLogMetaForTest(t, se, nil)
	se.restrictedErrs[testSQLPurgeMVLog] = errors.New("materialized view task canceled manually")
	pool := recordingSessionPool{se: se}

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.ErrorIs(t, err, errMVTaskCanceledManually)
	require.True(t, nextPurge.IsZero())
	require.Equal(t, []string{testSQLPurgeMVLog}, se.executedRestrictedSQL)
}

func TestServerHelperPurgeMVLogUsesGlobalMaintainMemQuota(t *testing.T) {
	installMockTimeForTest(t)
	se := newRecordingSessionContext()
	nextTimeRows := []chunk.Row{
		chunk.MutRowFromDatums([]types.Datum{
			types.NewIntDatum(mvsNow().Add(time.Minute).Unix()),
		}).ToRow(),
	}
	setupPurgeMVLogMetaForTest(t, se, nextTimeRows)

	pool := recordingSessionPool{se: se}
	vars := se.GetSessionVars()
	mockGlobalAccessor := variable.NewMockGlobalAccessor4Tests()
	mockGlobalAccessor.SessionVars = vars
	vars.GlobalVarsAccessor = mockGlobalAccessor
	require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBMVMaintainMemQuota, "536870912"))
	require.NoError(t, vars.SetSystemVar(variable.TiDBMVMaintainMemQuota, "268435456"))

	nextPurge, err := (&serviceHelper{}).PurgeMVLog(context.Background(), pool, 201)
	require.NoError(t, err)
	require.False(t, nextPurge.IsZero())
	require.Equal(t, testExpectedPurgeMVLogSQL, se.executedRestrictedSQL)
	require.Equal(t, []int64{536870912, 536870912}, se.restrictedMaintainQuota)
	require.Equal(t, int64(268435456), vars.MVMaintainMemQuota)
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

func TestServerHelperTryBackoffRefreshManualCancel(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("applied", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		expectedNext := mvsNow().UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			expectedNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				next := mvsNow().UTC().Add(time.Minute).Round(0)
				return &next, false, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(expectedNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(time.Time)
		require.True(t, ok)
		require.True(t, gotNext.Equal(expectedNext))
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("keep_later_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		currentNext := mvsNow().UTC().Add(manualCancelBackoffDelay + time.Minute).Round(0)
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := mvsNow().UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return &currentNext, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(currentNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(time.Time)
		require.True(t, ok)
		require.True(t, gotNext.Equal(currentNext))
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("no_next_schedule_clears_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := mvsNow().UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		require.Nil(t, se.executedRestrictedArg[1][0])
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("resolver_error_falls_back_to_cooldown", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockMVNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := mvsNow().UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockMVNextTime,
			testSQLUpdateMVNextTime,
			101,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, false, errors.New("mock resolver error")
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(cooldownNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime, testSQLUpdateMVNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(101)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(time.Time)
		require.True(t, ok)
		require.True(t, gotNext.Equal(cooldownNext))
		require.Equal(t, int64(101), se.executedRestrictedArg[1][1])
	})

	t.Run("lock_conflict", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedErrs[testSQLLockMVNextTime] = storeerr.ErrLockAcquireFailAndNoWaitSet
		pool := recordingSessionPool{se: se}

		applied, appliedNext, err := (&serviceHelper{}).TryBackoffRefreshManualCancel(context.Background(), pool, 101, mvsNow().UTC().Add(manualCancelBackoffDelay))
		require.NoError(t, err)
		require.False(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "ROLLBACK"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockMVNextTime}, se.executedRestrictedSQL)
	})
}

func TestServerHelperTryBackoffPurgeManualCancel(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("applied", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		expectedNext := mvsNow().UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			expectedNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				next := mvsNow().UTC().Add(time.Minute).Round(0)
				return &next, false, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(expectedNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(time.Time)
		require.True(t, ok)
		require.True(t, gotNext.Equal(expectedNext))
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})

	t.Run("keep_later_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		currentNext := mvsNow().UTC().Add(manualCancelBackoffDelay + time.Minute).Round(0)
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := mvsNow().UTC().Add(manualCancelBackoffDelay).Round(0)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return &currentNext, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.Equal(currentNext))
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		gotNext, ok := se.executedRestrictedArg[1][0].(time.Time)
		require.True(t, ok)
		require.True(t, gotNext.Equal(currentNext))
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})

	t.Run("no_next_schedule_clears_next_time", func(t *testing.T) {
		se := newRecordingSessionContext()
		se.restrictedRows[testSQLLockPurgeNextTime] = []chunk.Row{
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow(),
		}
		pool := recordingSessionPool{se: se}
		cooldownNext := mvsNow().UTC().Add(manualCancelBackoffDelay)

		applied, appliedNext, err := tryBackoffMVTaskManualCancel(
			context.Background(),
			pool,
			testSQLLockPurgeNextTime,
			testSQLUpdatePurgeNextTime,
			201,
			cooldownNext,
			func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error) {
				return nil, true, nil
			},
		)
		require.NoError(t, err)
		require.True(t, applied)
		require.True(t, appliedNext.IsZero())
		require.Equal(t, []string{"BEGIN PESSIMISTIC", "COMMIT"}, se.executedSQL)
		require.Equal(t, []string{testSQLLockPurgeNextTime, testSQLUpdatePurgeNextTime}, se.executedRestrictedSQL)
		require.Equal(t, []any{int64(201)}, se.executedRestrictedArg[0])
		require.Len(t, se.executedRestrictedArg[1], 2)
		require.Nil(t, se.executedRestrictedArg[1][0])
		require.Equal(t, int64(201), se.executedRestrictedArg[1][1])
	})
}
