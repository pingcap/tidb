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

//go:build intest

package mvservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

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
		fetchLogs:  map[int64]*mlogPurgeTask{},
		fetchViews: map[int64]*mviewTask{},
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
			fetchLogs:  map[int64]*mlogPurgeTask{},
			fetchViews: map[int64]*mviewTask{},
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
		fetchLogs:  map[int64]*mlogPurgeTask{},
		fetchViews: map[int64]*mviewTask{},
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
		fetchLogs: map[int64]*mlogPurgeTask{
			201: {
				ID:        201,
				nextPurge: mvsNow().Add(time.Minute),
				orderTs:   mvsNow().Add(time.Minute).UnixMilli(),
			},
		},
		fetchViewsErr: errors.New("fetch views failed"),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

	oldMLog := &mlogPurgeTask{
		ID:        101,
		nextPurge: mvsNow().Add(2 * time.Minute),
	}
	oldMLog.orderTs = oldMLog.nextPurge.UnixMilli()
	svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{oldMLog.ID: oldMLog})

	oldMV := &mviewTask{
		ID:          102,
		nextRefresh: mvsNow().Add(3 * time.Minute),
	}
	oldMV.orderTs = oldMV.nextRefresh.UnixMilli()
	svc.buildMViewRefreshTasks(map[int64]*mviewTask{oldMV.ID: oldMV})

	err := svc.fetchAllMVMeta()
	require.Error(t, err)
	require.Equal(t, int64(0), svc.lastMetaFetchMillis.Load())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultSuccess))
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultFailed))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMLogPurge, mvDurationResultFailed))
	require.Equal(t, 0, helper.fetchDurationCount(mvFetchTypeMViewRefresh, mvDurationResultSuccess))

	svc.mlogPurgeMu.Lock()
	_, hasOldMLog := svc.mlogPurgeMu.pending[101]
	_, hasNewMLog := svc.mlogPurgeMu.pending[201]
	svc.mlogPurgeMu.Unlock()
	require.True(t, hasOldMLog)
	require.False(t, hasNewMLog)

	svc.mviewRefreshMu.Lock()
	_, hasOldMV := svc.mviewRefreshMu.pending[102]
	svc.mviewRefreshMu.Unlock()
	require.True(t, hasOldMV)
}

func TestMVServiceTaskResult(t *testing.T) {
	installMockTimeForTest(t)

	t.Run("purge", func(t *testing.T) {
		t.Run("remove_on_zero_next", func(t *testing.T) {
			helper := &mockMVServiceHelper{}
			svc := newRunningMVServiceForTest(t, helper)

			l := &mlogPurgeTask{
				ID:        301,
				nextPurge: mvsNow(),
			}
			svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
			svc.purgeMLogs([]*mlogPurgeTask{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int64(0), svc.purgeExecutor.metrics.counters.failedCount.Load())
			require.Eventually(t, func() bool {
				svc.mlogPurgeMu.Lock()
				_, ok := svc.mlogPurgeMu.pending[l.ID]
				svc.mlogPurgeMu.Unlock()
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

			l := &mlogPurgeTask{
				ID:        302,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
			svc.purgeMLogs([]*mlogPurgeTask{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int64(0), svc.purgeExecutor.metrics.counters.failedCount.Load())

			svc.mlogPurgeMu.Lock()
			item, ok := svc.mlogPurgeMu.pending[l.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextPurge.Equal(nextPurge))
			require.Equal(t, nextPurge.UnixMilli(), item.Value.orderTs)
			svc.mlogPurgeMu.Unlock()
		})

		t.Run("manual_cancel_applied_backoff", func(t *testing.T) {
			expectedNext := mvsNow().Add(manualCancelBackoffDelay + time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				purgeErr:                        errMVTaskCanceledManually,
				purgeManualCancelBackoffApplied: true,
				purgeManualCancelBackoffNext:    expectedNext,
			}
			svc := newRunningMVServiceForTest(t, helper)

			l := &mlogPurgeTask{
				ID:        305,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
			svc.purgeMLogs([]*mlogPurgeTask{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())

			svc.mlogPurgeMu.Lock()
			item, ok := svc.mlogPurgeMu.pending[l.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextPurge.Equal(expectedNext))
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
			svc.mlogPurgeMu.Unlock()
		})

		t.Run("manual_cancel_applied_clear_schedule", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				purgeErr:                        errMVTaskCanceledManually,
				purgeManualCancelBackoffApplied: true,
				purgeManualCancelBackoffNextSet: true,
			}
			svc := newRunningMVServiceForTest(t, helper)

			l := &mlogPurgeTask{
				ID:        307,
				nextPurge: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
			svc.purgeMLogs([]*mlogPurgeTask{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())
			require.Eventually(t, func() bool {
				svc.mlogPurgeMu.Lock()
				_, ok := svc.mlogPurgeMu.pending[l.ID]
				svc.mlogPurgeMu.Unlock()
				return !ok
			}, testEventuallyWait, testEventuallyTick)
		})

		t.Run("manual_cancel_without_persist_uses_local_backoff", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				purgeErr: errMVTaskCanceledManually,
			}
			svc := newRunningMVServiceForTest(t, helper)
			now := mvsNow()
			svc.lastMetaFetchMillis.Store(now.UnixMilli())
			expectedNext := now.Add(manualCancelBackoffDelay)

			l := &mlogPurgeTask{
				ID:        306,
				nextPurge: now.Add(-time.Minute).Round(0),
			}
			svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
			svc.purgeMLogs([]*mlogPurgeTask{l})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.purgeManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), l.retryCount.Load())
			require.Equal(t, now.UnixMilli(), svc.lastMetaFetchMillis.Load())

			svc.mlogPurgeMu.Lock()
			item, ok := svc.mlogPurgeMu.pending[l.ID]
			svc.mlogPurgeMu.Unlock()
			require.True(t, ok)
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
		})
	})

	t.Run("refresh", func(t *testing.T) {
		t.Run("remove_on_zero_next", func(t *testing.T) {
			helper := &mockMVServiceHelper{}
			svc := newRunningMVServiceForTest(t, helper)

			m := &mviewTask{
				ID:          401,
				nextRefresh: mvsNow(),
			}
			svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
			svc.refreshMViews([]*mviewTask{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int64(0), svc.refreshExecutor.metrics.counters.failedCount.Load())
			require.Eventually(t, func() bool {
				svc.mviewRefreshMu.Lock()
				_, ok := svc.mviewRefreshMu.pending[m.ID]
				svc.mviewRefreshMu.Unlock()
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

			m := &mviewTask{
				ID:          402,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
			svc.refreshMViews([]*mviewTask{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int64(0), svc.refreshExecutor.metrics.counters.failedCount.Load())

			svc.mviewRefreshMu.Lock()
			item, ok := svc.mviewRefreshMu.pending[m.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextRefresh.Equal(nextRefresh))
			require.Equal(t, nextRefresh.UnixMilli(), item.Value.orderTs)
			svc.mviewRefreshMu.Unlock()
		})

		t.Run("manual_cancel_applied_backoff", func(t *testing.T) {
			expectedNext := mvsNow().Add(manualCancelBackoffDelay + time.Minute).Round(0)
			helper := &mockMVServiceHelper{
				refreshErr:                        errMVTaskCanceledManually,
				refreshManualCancelBackoffApplied: true,
				refreshManualCancelBackoffNext:    expectedNext,
			}
			svc := newRunningMVServiceForTest(t, helper)

			m := &mviewTask{
				ID:          403,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
			svc.refreshMViews([]*mviewTask{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())

			svc.mviewRefreshMu.Lock()
			item, ok := svc.mviewRefreshMu.pending[m.ID]
			require.True(t, ok)
			require.True(t, item.Value.nextRefresh.Equal(expectedNext))
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
			svc.mviewRefreshMu.Unlock()
		})

		t.Run("manual_cancel_applied_clear_schedule", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				refreshErr:                        errMVTaskCanceledManually,
				refreshManualCancelBackoffApplied: true,
				refreshManualCancelBackoffNextSet: true,
			}
			svc := newRunningMVServiceForTest(t, helper)

			m := &mviewTask{
				ID:          405,
				nextRefresh: mvsNow().Add(-time.Minute).Round(0),
			}
			svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
			svc.refreshMViews([]*mviewTask{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())
			require.Eventually(t, func() bool {
				svc.mviewRefreshMu.Lock()
				_, ok := svc.mviewRefreshMu.pending[m.ID]
				svc.mviewRefreshMu.Unlock()
				return !ok
			}, testEventuallyWait, testEventuallyTick)
		})

		t.Run("manual_cancel_without_persist_uses_local_backoff", func(t *testing.T) {
			helper := &mockMVServiceHelper{
				refreshErr: errMVTaskCanceledManually,
			}
			svc := newRunningMVServiceForTest(t, helper)
			now := mvsNow()
			svc.lastMetaFetchMillis.Store(now.UnixMilli())
			expectedNext := now.Add(manualCancelBackoffDelay)

			m := &mviewTask{
				ID:          404,
				nextRefresh: now.Add(-time.Minute).Round(0),
			}
			svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
			svc.refreshMViews([]*mviewTask{m})

			waitExecutorFinishedCount(t, svc, 1)
			require.Equal(t, int32(1), helper.refreshManualCancelBackoffCalls.Load())
			require.Equal(t, int64(0), m.retryCount.Load())
			require.Equal(t, now.UnixMilli(), svc.lastMetaFetchMillis.Load())

			svc.mviewRefreshMu.Lock()
			item, ok := svc.mviewRefreshMu.pending[m.ID]
			svc.mviewRefreshMu.Unlock()
			require.True(t, ok)
			require.Equal(t, expectedNext.UnixMilli(), item.Value.orderTs)
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

		m := &mviewTask{
			ID:          403,
			nextRefresh: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
		svc.buildMViewRefreshTasks(map[int64]*mviewTask{})

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

		m := &mviewTask{
			ID:          404,
			nextRefresh: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})

		err := svc.executeRefreshTask(m)
		require.Error(t, err)
		require.Contains(t, err.Error(), "refresh MV task panicked")
		require.Equal(t, int64(1), m.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.runningMVRefreshCount.Load())
		require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultFailed))

		svc.mviewRefreshMu.Lock()
		item, ok := svc.mviewRefreshMu.pending[m.ID]
		require.True(t, ok)
		require.Equal(t, m, item.Value)
		require.NotEqual(t, int64(maxNextScheduleTs), item.Value.orderTs)
		svc.mviewRefreshMu.Unlock()
	})

	t.Run("purge", func(t *testing.T) {
		helper := &mockMVServiceHelper{
			purgeNext: mvsNow().Add(time.Minute).Round(0),
		}
		svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())

		l := &mlogPurgeTask{
			ID:        303,
			nextPurge: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})
		svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{})

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

		l := &mlogPurgeTask{
			ID:        304,
			nextPurge: mvsNow().Add(-time.Minute).Round(0),
		}
		svc.buildMLogPurgeTasks(map[int64]*mlogPurgeTask{l.ID: l})

		err := svc.executePurgeTask(l)
		require.Error(t, err)
		require.Contains(t, err.Error(), "purge MV log task panicked")
		require.Equal(t, int64(1), l.retryCount.Load())
		require.Equal(t, int64(0), svc.metrics.runningMVLogPurgeCount.Load())
		require.Equal(t, 0, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
		require.Equal(t, 1, helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultFailed))

		svc.mlogPurgeMu.Lock()
		item, ok := svc.mlogPurgeMu.pending[l.ID]
		require.True(t, ok)
		require.Equal(t, l, item.Value)
		require.NotEqual(t, int64(maxNextScheduleTs), item.Value.orderTs)
		svc.mlogPurgeMu.Unlock()
	})
}

func TestMVServiceMaintenanceTimerTriggersHistoryGC(t *testing.T) {
	module := installMockTimeForTest(t)
	ctx, cancel := context.WithCancel(context.Background())

	helper := &mockMVServiceHelper{
		currentTSO: 1 << 40,
		fetchLogs:  map[int64]*mlogPurgeTask{},
		fetchViews: map[int64]*mviewTask{},
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
	require.Equal(t, defaultMVHistoryGCMaxRecords, helper.lastMViewHistoryGCMaxRecords.Load())
	require.Equal(t, defaultMVHistoryGCMaxRecords, helper.lastMLogHistoryGCMaxRecords.Load())
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
	require.Equal(t, 0, helper.runEventCount(mvRunEventGetTSOErr))
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
			return helper.runEventCount(mvRunEventGetTSOErr) > 0
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
			return helper.runEventCount(mvRunEventGetTSOErr) > 0
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
			return helper.taskDurationCount(mvTaskDurationTypeHistoryGC, mvDurationResultFailed) > 0
		}, testEventuallyWait, testEventuallyTick)
		require.Eventually(t, func() bool {
			return !svc.historyGCRunning.Load()
		}, testEventuallyWait, testEventuallyTick)
	})
}

func TestServerHelperMLogAnalyzeControls(t *testing.T) {
	oldEnabled := vardef.RunAutoAnalyze.Load()
	t.Cleanup(func() { vardef.RunAutoAnalyze.Store(oldEnabled) })
	restoreTime := InstallMockTimeModuleForTest(NewMockTimeModule(time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC)))
	t.Cleanup(restoreTime)
	for _, tt := range []struct {
		name    string
		enabled bool
		start   string
		end     string
		allowed bool
	}{
		{name: "disabled", start: "00:00 +0000", end: "23:59 +0000"},
		{name: "outside window", enabled: true, start: "13:00 +0000", end: "14:00 +0000"},
		{name: "inside window", enabled: true, start: "11:00 +0000", end: "13:00 +0000", allowed: true},
		{name: "overnight inside", enabled: true, start: "11:00 +0000", end: "01:00 +0000", allowed: true},
		{name: "overnight outside", enabled: true, start: "23:00 +0000", end: "01:00 +0000"},
		{name: "offset inside", enabled: true, start: "19:00 +0800", end: "21:00 +0800", allowed: true},
		{name: "invalid window", enabled: true, start: "invalid", end: "13:00 +0000"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			vardef.RunAutoAnalyze.Store(tt.enabled)
			for _, phase := range []string{"scan", "execute"} {
				t.Run(phase, func(t *testing.T) {
					helper, se := newMLogAnalyzeHelperForTest(t)
					se.restrictedRows[testSQLAutoAnalyzeParameters] = []chunk.Row{
						chunk.MutRowFromValues(vardef.TiDBAutoAnalyzeStartTime, tt.start).ToRow(),
						chunk.MutRowFromValues(vardef.TiDBAutoAnalyzeEndTime, tt.end).ToRow(),
					}
					pool := recordingSessionPool{se: se}
					if phase == "scan" {
						tasks, err := helper.LoadAllTiDBMVLogAnalyzeTasks(context.Background(), pool)
						require.NoError(t, err)
						require.Equal(t, tt.allowed, len(tasks) == 1)
					} else {
						require.NoError(t, helper.AnalyzeMVLog(context.Background(), pool, 201))
						require.Equal(t, tt.allowed, containsAnalyzeSQL(se.executedRestrictedSQL))
					}
				})
			}
		})
	}
}

func TestMVServiceMLogAnalyzeScanSubmitsTasks(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchAnalyzeTasks: map[int64]*mlogAnalyzeTask{
			101: {schemaName: "test", mlogName: "$mlog$t"},
		},
	}
	cfg := DefaultMVServiceConfig()
	cfg.ServerConsistentHashReplicas = 1
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)
	setTaskOwnersForTest(svc, map[int64]uint32{101: 5})
	svc.mlogAnalyzeExecutor.Run()
	t.Cleanup(func() {
		svc.mlogAnalyzeExecutor.Close()
	})

	now := mvsNow()
	svc.maybeScanMLogAnalyze(now)
	require.Eventually(t, func() bool {
		return helper.analyzeMLogCalls.Load() == 1 && svc.metrics.mvLogAnalyzeTaskCount.Load() == 0
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int64(101), helper.lastAnalyzeID.Load())
	require.Equal(t, int64(0), svc.metrics.mvLogAnalyzeTaskCount.Load())
	require.Equal(t, now.Add(mlogAnalyzeScanInterval).UnixMilli(), svc.nextMLogAnalyzeScanMillis.Load())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMLogAnalyze, mvDurationResultSuccess))

	svc.maybeScanMLogAnalyze(now.Add(mlogAnalyzeScanInterval - time.Second))
	require.Equal(t, int32(1), helper.fetchAnalyzeTaskCalls.Load())
}

func TestMVServiceMLogAnalyzeScanFailureWaitsUntilNextInterval(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{
		fetchAnalyzeTasksErr: errors.New("mock mlog analyze scan failed"),
	}
	cfg := DefaultMVServiceConfig()
	cfg.ServerConsistentHashReplicas = 1
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, cfg)

	now := mvsNow()
	svc.maybeScanMLogAnalyze(now)
	require.Eventually(t, func() bool {
		return !svc.mlogAnalyzeScanRunning.Load()
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int32(1), helper.fetchAnalyzeTaskCalls.Load())
	require.Equal(t, int32(0), helper.analyzeMLogCalls.Load())
	require.Equal(t, now.Add(mlogAnalyzeScanInterval).UnixMilli(), svc.nextMLogAnalyzeScanMillis.Load())
	require.Equal(t, 1, helper.fetchDurationCount(mvFetchTypeMLogAnalyze, mvDurationResultFailed))

	svc.maybeScanMLogAnalyze(now.Add(mlogAnalyzeScanInterval - time.Second))
	require.Equal(t, int32(1), helper.fetchAnalyzeTaskCalls.Load())
}

func TestMVServiceMLogAnalyzeTaskDedupAndNoImmediateRetry(t *testing.T) {
	installMockTimeForTest(t)
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	helper := &mockMVServiceHelper{
		analyzeMLogErr:     errors.New("analyze failed"),
		analyzeMLogEntered: entered,
		blockAnalyzeMLog:   block,
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.mlogAnalyzeExecutor.Run()
	t.Cleanup(func() {
		close(block)
		svc.mlogAnalyzeExecutor.Close()
	})

	svc.analyzeMVLog([]int64{101, 101})
	require.Eventually(t, func() bool {
		return helper.analyzeMLogCalls.Load() == 1
	}, testEventuallyWait, testEventuallyTick)
	<-entered
	require.Equal(t, int64(1), svc.metrics.mvLogAnalyzeTaskCount.Load())
	require.Equal(t, int64(0), svc.mlogAnalyzeExecutor.metrics.counters.failedCount.Load())

	block <- struct{}{}
	require.Eventually(t, func() bool {
		return svc.metrics.mvLogAnalyzeTaskCount.Load() == 0
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int32(1), helper.analyzeMLogCalls.Load())
	require.Equal(t, int64(1), svc.mlogAnalyzeExecutor.metrics.counters.failedCount.Load())
}

func TestMVServiceMLogAnalyzeSubmitRejectedCleansDedupState(t *testing.T) {
	installMockTimeForTest(t)
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	require.True(t, svc.mlogAnalyzeExecutor.Close())

	svc.analyzeMVLog([]int64{101})

	require.Equal(t, int64(0), svc.metrics.mvLogAnalyzeTaskCount.Load())
	require.Equal(t, int32(0), helper.analyzeMLogCalls.Load())
	require.Equal(t, int64(1), svc.mlogAnalyzeExecutor.metrics.counters.rejectedCount.Load())
}
