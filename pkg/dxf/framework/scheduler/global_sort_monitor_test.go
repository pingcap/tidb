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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	frameworkmock "github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type globalSortMonitorWalkEntry struct {
	path string
	size int64
}

type globalSortMonitorStorage struct {
	storeapi.Storage
	entries    []globalSortMonitorWalkEntry
	walkErr    error
	walkFn     func(context.Context) error
	closeCount int
}

func (s *globalSortMonitorStorage) WalkDir(
	ctx context.Context,
	_ *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	if s.walkFn != nil {
		if err := s.walkFn(ctx); err != nil {
			return err
		}
	}
	if s.walkErr != nil {
		return s.walkErr
	}
	for _, entry := range s.entries {
		if err := fn(entry.path, entry.size); err != nil {
			return err
		}
	}
	return nil
}

func (s *globalSortMonitorStorage) Close() {
	s.closeCount++
}

func newGlobalSortMonitorTestManager(t *testing.T) (*Manager, *frameworkmock.MockTaskManager, *observer.ObservedLogs) {
	t.Helper()
	taskMgr := frameworkmock.NewMockTaskManager(gomock.NewController(t))
	mgr := NewManager(context.Background(), nil, taskMgr, "test", proto.NodeResourceForTest)
	core, logs := observer.New(zap.DebugLevel)
	mgr.logger = zap.New(core)
	return mgr, taskMgr, logs
}

func requireGlobalSortGauge(t *testing.T, expected float64) {
	t.Helper()
	require.Equal(t, expected, testutil.ToFloat64(metrics.GlobalSortResidualDataSize))
}

func requireNoGlobalSortCredentials(t *testing.T, logs *observer.ObservedLogs, values ...string) {
	t.Helper()
	for _, entry := range logs.All() {
		logged := fmt.Sprintf("%s %v", entry.Message, entry.ContextMap())
		for _, value := range values {
			require.NotContains(t, logged, value)
		}
	}
}

func requireNoGlobalSortCandidate(t *testing.T, logs *observer.ObservedLogs, candidate string) {
	t.Helper()
	for _, entry := range logs.All() {
		require.NotContains(t, fmt.Sprintf("%s %v", entry.Message, entry.ContextMap()), candidate)
	}
}

func TestManagerMonitorGlobalSort(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	t.Run("first gate has tasks", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "s3://bucket/prefix"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return([]*proto.TaskBase{{ID: 1}}, nil)

		mgr.monitorGlobalSort()

		require.Zero(t, factoryCalls)
		requireGlobalSortGauge(t, 0)
		require.Empty(t, logs.All())
	})

	t.Run("first gate query error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "s3://bucket/prefix"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, errors.New("first gate failed"))

		mgr.monitorGlobalSort()

		require.Zero(t, factoryCalls)
		requireGlobalSortGauge(t, 37)
		require.Len(t, logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		require.Contains(t, logs.All()[0].ContextMap()["error"], "first gate failed")
	})

	t.Run("empty URI is a successful zero scan", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return "" }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSort()

		require.Zero(t, factoryCalls)
		requireGlobalSortGauge(t, 0)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		successLogs := logs.FilterMessage("global sort residual monitor success").All()
		require.Len(t, successLogs, 1)
		fields := successLogs[0].ContextMap()
		require.Equal(t, "", fields["storage-uri"])
		require.EqualValues(t, 0, fields["residual-size-bytes"])
		require.EqualValues(t, 0, fields["residual-object-count"])
		require.Equal(t, []any{}, fields["sample-prefixes"])
		require.Equal(t, false, fields["sample-prefixes-omitted"])
	})

	t.Run("invalid URI uses the default factory", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const uri = "s3:///missing-bucket?access-key=invalid-ak&secret-access-key=invalid-sk&session-token=invalid-token"
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return uri }
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSort()

		requireGlobalSortGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortCredentials(t, logs, "invalid-ak", "invalid-sk", "invalid-token")
	})

	t.Run("injected store creation error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const (
			accessKey    = "create-ak-fragment"
			secretKey    = "create+sk-fragment"
			sessionToken = "create-token-fragment"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=create%2Bsk-fragment&SeSsIoN_ToKeN=" + sessionToken
		)
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, fmt.Errorf("injected creation leaked fragments %s %s %s", accessKey, secretKey, sessionToken)
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSort()

		requireGlobalSortGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortCredentials(t, logs, accessKey, secretKey, sessionToken, "create%2Bsk-fragment")
	})

	t.Run("malformed storage URI is never logged", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const (
			secret = "malformed-secret"
			uri    = "s3://bucket/%zz?secret-access-key=" + secret
		)
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, errors.New("malformed factory failure")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSort()

		requireGlobalSortGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.Equal(t, "<invalid>", warnings[0].ContextMap()["storage-uri"])
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortCredentials(t, logs, uri, secret)
	})

	t.Run("configured empty store", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortMonitorStorage{Storage: objstore.NewMemStorage()}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 0)
		require.Len(t, logs.FilterMessage("global sort residual monitor success").All(), 1)
	})

	t.Run("configured nonempty store", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		const (
			accessKey    = "success-ak"
			secretKey    = "success+sk"
			sessionToken = "success-token"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=success%2Bsk&SeSsIoN_ToKeN=" + sessionToken
		)
		entries := make([]globalSortMonitorWalkEntry, 0, 11)
		for i := 10; i >= 0; i-- {
			entries = append(entries, globalSortMonitorWalkEntry{
				path: fmt.Sprintf("prefix-%02d/file", i),
				size: int64(i + 1),
			})
		}
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 66)
		successLogs := logs.FilterMessage("global sort residual monitor success").All()
		require.Len(t, successLogs, 1)
		fields := successLogs[0].ContextMap()
		require.Contains(t, fields["storage-uri"], "xxxxxx")
		require.EqualValues(t, 66, fields["residual-size-bytes"])
		require.EqualValues(t, 11, fields["residual-object-count"])
		require.Equal(t, []any{
			"prefix-00/", "prefix-01/", "prefix-02/", "prefix-03/", "prefix-04/",
			"prefix-05/", "prefix-06/", "prefix-07/", "prefix-08/", "prefix-09/",
		}, fields["sample-prefixes"])
		require.Equal(t, true, fields["sample-prefixes-omitted"])
		requireNoGlobalSortCredentials(t, logs, accessKey, secretKey, sessionToken, "success%2Bsk")
	})

	t.Run("configured Azure credentials are redacted", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		const (
			accountKey    = "azure-account"
			sasToken      = "azure+sas"
			encryptionKey = "azure-encryption"
			uri           = "azure://container/prefix?AcCoUnT_KeY=" + accountKey + "&SaS-ToKeN=azure%2Bsas&EnCrYpTiOn-KeY=" + encryptionKey
		)
		store := &globalSortMonitorStorage{Storage: objstore.NewMemStorage()}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 0)
		successLogs := logs.FilterMessage("global sort residual monitor success").All()
		require.Len(t, successLogs, 1)
		require.Contains(t, successLogs[0].ContextMap()["storage-uri"], "xxxxxx")
		requireNoGlobalSortCredentials(t, logs,
			accountKey, sasToken, encryptionKey, "azure%2Bsas")
	})

	t.Run("task appears at the second gate", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const candidate = "candidate-prefix/"
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortMonitorWalkEntry{{path: candidate + "file", size: 41}},
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return([]*proto.TaskBase{{ID: 1}}, nil),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 0)
		require.Empty(t, logs.FilterMessage("global sort residual monitor success").All())
		discardLogs := logs.FilterMessage("global sort residual monitor discarded scan because tasks appeared").All()
		require.Len(t, discardLogs, 1)
		require.Len(t, logs.FilterLevelExact(zap.InfoLevel).All(), 1)
		fields := discardLogs[0].ContextMap()
		require.Equal(t, "memstore:///residual", fields["storage-uri"])
		require.EqualValues(t, 1, fields["task-count"])
		for _, field := range []string{
			"residual-size-bytes",
			"residual-object-count",
			"sample-prefixes",
			"sample-prefixes-omitted",
		} {
			require.NotContains(t, fields, field)
		}
		requireNoGlobalSortCandidate(t, logs, candidate)
	})

	t.Run("second gate query error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortMonitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, errors.New("second gate failed")),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 37)
		require.Len(t, logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		requireNoGlobalSortCandidate(t, logs, "candidate-prefix/")
	})

	for _, testCase := range []struct {
		name    string
		uri     string
		entries []globalSortMonitorWalkEntry
		walkErr error
		secrets []string
	}{
		{
			name: "walk error",
			uri:  "azure://container/prefix?account-key=walk-account&sas-token=walk%2Bsas&encryption-key=walk-encryption",
			walkErr: errors.New(
				"walk leaked fragments walk-account walk+sas walk-encryption",
			),
			secrets: []string{"walk-account", "walk+sas", "walk%2Bsas", "walk-encryption"},
		},
		{
			name: "size overflow",
			uri:  "memstore:///residual",
			entries: []globalSortMonitorWalkEntry{
				{path: "candidate-a/file", size: math.MaxInt64},
				{path: "candidate-b/file", size: 1},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
			metrics.GlobalSortResidualDataSize.Set(37)
			store := &globalSortMonitorStorage{
				Storage: objstore.NewMemStorage(),
				entries: testCase.entries,
				walkErr: testCase.walkErr,
			}
			mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
				return testCase.uri
			}
			mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
				return store, nil
			}
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

			mgr.monitorGlobalSort()

			require.Equal(t, 1, store.closeCount)
			requireGlobalSortGauge(t, 37)
			warnings := logs.FilterLevelExact(zap.WarnLevel).All()
			require.Len(t, warnings, 1)
			require.NotContains(t, warnings[0].ContextMap(), "error")
			requireNoGlobalSortCredentials(t, logs, testCase.secrets...)
			requireNoGlobalSortCandidate(t, logs, "candidate-")
		})
	}

	t.Run("canceled before first read", func(t *testing.T) {
		mgr, _, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		mgr.Cancel()

		mgr.monitorGlobalSort()

		requireGlobalSortGauge(t, 37)
		require.Empty(t, logs.All())
	})

	t.Run("walk returns cancellation", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortMonitorStorage{Storage: objstore.NewMemStorage()}
		store.walkFn = func(context.Context) error {
			mgr.Cancel()
			return context.Canceled
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortCandidate(t, logs, "candidate-")
	})

	t.Run("cancellation during second read", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortMonitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).DoAndReturn(
				func(context.Context) ([]*proto.TaskBase, error) {
					mgr.Cancel()
					return nil, context.Canceled
				},
			),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortCandidate(t, logs, "candidate-prefix/")
	})

	t.Run("canceled immediately before publication", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortMonitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).DoAndReturn(
				func(context.Context) ([]*proto.TaskBase, error) {
					mgr.Cancel()
					return nil, nil
				},
			),
		)

		mgr.monitorGlobalSort()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortCandidate(t, logs, "candidate-prefix/")
	})
}
