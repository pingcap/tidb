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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	frameworkmock "github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type globalSortResidualMonitorStorage struct {
	storeapi.Storage
	entries    []globalSortResidualWalkEntry
	walkErr    error
	walkFn     func(context.Context) error
	closeCount int
}

func (s *globalSortResidualMonitorStorage) WalkDir(
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

func (s *globalSortResidualMonitorStorage) Close() {
	s.closeCount++
}

func newGlobalSortResidualMonitorTestManager(t *testing.T) (*Manager, *frameworkmock.MockTaskManager, *observer.ObservedLogs) {
	t.Helper()
	taskMgr := frameworkmock.NewMockTaskManager(gomock.NewController(t))
	mgr := NewManager(context.Background(), nil, taskMgr, "test", proto.NodeResourceForTest)
	core, logs := observer.New(zap.DebugLevel)
	mgr.logger = zap.New(core)
	return mgr, taskMgr, logs
}

func requireGlobalSortResidualGauge(t *testing.T, expected float64) {
	t.Helper()
	require.Equal(t, expected, testutil.ToFloat64(metrics.GlobalSortResidualDataSize))
}

func requireNoGlobalSortResidualCredentials(t *testing.T, logs *observer.ObservedLogs, values ...string) {
	t.Helper()
	for _, entry := range logs.All() {
		logged := fmt.Sprintf("%s %v", entry.Message, entry.ContextMap())
		for _, value := range values {
			require.NotContains(t, logged, value)
		}
	}
}

func requireNoGlobalSortResidualCandidate(t *testing.T, logs *observer.ObservedLogs, candidate string) {
	t.Helper()
	for _, entry := range logs.All() {
		require.NotContains(t, fmt.Sprintf("%s %v", entry.Message, entry.ContextMap()), candidate)
	}
}

func TestManagerMonitorGlobalSortResidual(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	t.Run("first gate has tasks", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "s3://bucket/prefix"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return([]*proto.TaskBase{{ID: 1}}, nil)

		mgr.monitorGlobalSortResidual()

		require.Zero(t, factoryCalls)
		requireGlobalSortResidualGauge(t, 0)
		require.Empty(t, logs.All())
	})

	t.Run("first gate query error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "s3://bucket/prefix"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, errors.New("first gate failed"))

		mgr.monitorGlobalSortResidual()

		require.Zero(t, factoryCalls)
		requireGlobalSortResidualGauge(t, 37)
		require.Len(t, logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		require.Contains(t, logs.All()[0].ContextMap()["error"], "first gate failed")
	})

	t.Run("empty URI is a successful zero scan", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		factoryCalls := 0
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return "" }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSortResidual()

		require.Zero(t, factoryCalls)
		requireGlobalSortResidualGauge(t, 0)
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
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const uri = "s3:///missing-bucket?access-key=invalid-ak&secret-access-key=invalid-sk&session-token=invalid-token"
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return uri }
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSortResidual()

		requireGlobalSortResidualGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortResidualCredentials(t, logs, "invalid-ak", "invalid-sk", "invalid-token")
	})

	t.Run("injected store creation error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const (
			accessKey    = "create-ak-fragment"
			secretKey    = "create+sk-fragment"
			sessionToken = "create-token-fragment"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=create%2Bsk-fragment&SeSsIoN_ToKeN=" + sessionToken
		)
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, fmt.Errorf("injected creation leaked fragments %s %s %s", accessKey, secretKey, sessionToken)
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSortResidual()

		requireGlobalSortResidualGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortResidualCredentials(t, logs, accessKey, secretKey, sessionToken, "create%2Bsk-fragment")
	})

	t.Run("malformed storage URI is never logged", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const (
			secret = "malformed-secret"
			uri    = "s3://bucket/%zz?secret-access-key=" + secret
		)
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, errors.New("malformed factory failure")
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSortResidual()

		requireGlobalSortResidualGauge(t, 37)
		warnings := logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.Equal(t, "<invalid>", warnings[0].ContextMap()["storage-uri"])
		require.NotContains(t, warnings[0].ContextMap(), "error")
		requireNoGlobalSortResidualCredentials(t, logs, uri, secret)
	})

	t.Run("configured empty store", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortResidualMonitorStorage{Storage: objstore.NewMemStorage()}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 0)
		require.Len(t, logs.FilterMessage("global sort residual monitor success").All(), 1)
	})

	t.Run("configured nonempty store", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		const (
			accessKey    = "success-ak"
			secretKey    = "success+sk"
			sessionToken = "success-token"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=success%2Bsk&SeSsIoN_ToKeN=" + sessionToken
		)
		entries := make([]globalSortResidualWalkEntry, 0, 11)
		for i := 10; i >= 0; i-- {
			entries = append(entries, globalSortResidualWalkEntry{
				path: fmt.Sprintf("prefix-%02d/file", i),
				size: int64(i + 1),
			})
		}
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 66)
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
		requireNoGlobalSortResidualCredentials(t, logs, accessKey, secretKey, sessionToken, "success%2Bsk")
	})

	t.Run("configured Azure credentials are redacted", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		const (
			accountKey    = "azure-account"
			sasToken      = "azure+sas"
			encryptionKey = "azure-encryption"
			uri           = "azure://container/prefix?AcCoUnT_KeY=" + accountKey + "&SaS-ToKeN=azure%2Bsas&EnCrYpTiOn-KeY=" + encryptionKey
		)
		store := &globalSortResidualMonitorStorage{Storage: objstore.NewMemStorage()}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string { return uri }
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
		)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 0)
		successLogs := logs.FilterMessage("global sort residual monitor success").All()
		require.Len(t, successLogs, 1)
		require.Contains(t, successLogs[0].ContextMap()["storage-uri"], "xxxxxx")
		requireNoGlobalSortResidualCredentials(t, logs,
			accountKey, sasToken, encryptionKey, "azure%2Bsas")
	})

	t.Run("task appears at the second gate", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		const candidate = "candidate-prefix/"
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{{path: candidate + "file", size: 41}},
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return([]*proto.TaskBase{{ID: 1}}, nil),
		)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 0)
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
		requireNoGlobalSortResidualCandidate(t, logs, candidate)
	})

	t.Run("second gate query error", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		gomock.InOrder(
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil),
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, errors.New("second gate failed")),
		)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 37)
		require.Len(t, logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		requireNoGlobalSortResidualCandidate(t, logs, "candidate-prefix/")
	})

	for _, testCase := range []struct {
		name    string
		uri     string
		entries []globalSortResidualWalkEntry
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
			entries: []globalSortResidualWalkEntry{
				{path: "candidate-a/file", size: math.MaxInt64},
				{path: "candidate-b/file", size: 1},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
			metrics.GlobalSortResidualDataSize.Set(37)
			store := &globalSortResidualMonitorStorage{
				Storage: objstore.NewMemStorage(),
				entries: testCase.entries,
				walkErr: testCase.walkErr,
			}
			mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
				return testCase.uri
			}
			mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
				return store, nil
			}
			taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

			mgr.monitorGlobalSortResidual()

			require.Equal(t, 1, store.closeCount)
			requireGlobalSortResidualGauge(t, 37)
			warnings := logs.FilterLevelExact(zap.WarnLevel).All()
			require.Len(t, warnings, 1)
			require.NotContains(t, warnings[0].ContextMap(), "error")
			requireNoGlobalSortResidualCredentials(t, logs, testCase.secrets...)
			requireNoGlobalSortResidualCandidate(t, logs, "candidate-")
		})
	}

	t.Run("canceled before first read", func(t *testing.T) {
		mgr, _, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		mgr.Cancel()

		mgr.monitorGlobalSortResidual()

		requireGlobalSortResidualGauge(t, 37)
		require.Empty(t, logs.All())
	})

	t.Run("walk returns cancellation", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortResidualMonitorStorage{Storage: objstore.NewMemStorage()}
		store.walkFn = func(context.Context) error {
			mgr.Cancel()
			return context.Canceled
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortResidualCandidate(t, logs, "candidate-")
	})

	t.Run("cancellation during second read", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
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

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortResidualCandidate(t, logs, "candidate-prefix/")
	})

	t.Run("canceled immediately before publication", func(t *testing.T) {
		mgr, taskMgr, logs := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(37)
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []globalSortResidualWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
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

		mgr.monitorGlobalSortResidual()

		require.Equal(t, 1, store.closeCount)
		requireGlobalSortResidualGauge(t, 37)
		require.Empty(t, logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoGlobalSortResidualCandidate(t, logs, "candidate-prefix/")
	})
}

func TestGlobalSortResidualMonitorRequestNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	t.Run("single flight stops with manager", func(t *testing.T) {
		mgr, taskMgr, _ := newGlobalSortResidualMonitorTestManager(t)
		walkStarted := make(chan struct{})
		releaseWalk := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-releaseWalk:
			default:
				close(releaseWalk)
			}
		})
		var factoryCalls atomic.Int32
		var walkCalls atomic.Int32
		store := &globalSortResidualMonitorStorage{
			Storage: objstore.NewMemStorage(),
			walkFn: func(ctx context.Context) error {
				walkCalls.Add(1)
				close(walkStarted)
				<-releaseWalk
				return ctx.Err()
			},
		}
		mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
			return "memstore:///residual"
		}
		mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls.Add(1)
			return store, nil
		}
		taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil)

		requestReturned := make(chan struct{})
		go func() {
			mgr.requestGlobalSortResidualMonitor()
			close(requestReturned)
		}()
		select {
		case <-walkStarted:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "global sort residual scan did not start")
		}
		select {
		case <-requestReturned:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "global sort residual monitor request did not return asynchronously")
		}
		mgr.requestGlobalSortResidualMonitor()

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		metrics.GlobalSortResidualDataSize.Set(37)
		stopDone := make(chan struct{})
		go func() {
			mgr.Stop()
			close(stopDone)
		}()
		select {
		case <-mgr.ctx.Done():
		case <-time.After(5 * time.Second):
			require.FailNow(t, "manager was not canceled")
		}
		select {
		case <-stopDone:
			require.FailNow(t, "manager stopped before the residual scan exited")
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseWalk)
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "manager did not stop after the residual scan exited")
		}

		require.Equal(t, int32(1), factoryCalls.Load())
		require.Equal(t, int32(1), walkCalls.Load())
		requireGlobalSortResidualGauge(t, 0)
	})

	t.Run("stop clears gauge without a request", func(t *testing.T) {
		mgr, _, _ := newGlobalSortResidualMonitorTestManager(t)
		metrics.GlobalSortResidualDataSize.Set(73)

		mgr.Stop()

		requireGlobalSortResidualGauge(t, 0)
	})
}

func TestGlobalSortResidualMonitorRequestStopRaceNextGen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}
	t.Cleanup(func() {
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	mgr, taskMgr, _ := newGlobalSortResidualMonitorTestManager(t)
	mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
		return ""
	}
	taskMgr.EXPECT().GetAllTasks(gomock.Any()).Return(nil, nil).AnyTimes()
	admissionEntered := make(chan struct{}, 1)
	releaseAdmission := make(chan struct{})
	workerStarted := make(chan struct{}, 1)
	releaseWorker := make(chan struct{})
	var releaseAdmissionOnce sync.Once
	var releaseWorkerOnce sync.Once
	t.Cleanup(func() {
		releaseAdmissionOnce.Do(func() { close(releaseAdmission) })
		releaseWorkerOnce.Do(func() { close(releaseWorker) })
	})

	var admissionCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/beforeGlobalSortResidualMonitorRun",
		func() {
			admissionCalls.Add(1)
			select {
			case admissionEntered <- struct{}{}:
			default:
			}
			<-releaseAdmission
		},
	)
	var workerCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/globalSortResidualMonitorWorker",
		func() {
			workerCalls.Add(1)
			select {
			case workerStarted <- struct{}{}:
			default:
			}
			<-releaseWorker
		},
	)

	requestDone := make(chan struct{})
	go func() {
		mgr.requestGlobalSortResidualMonitor()
		close(requestDone)
	}()
	select {
	case <-admissionEntered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "request did not reach monitor worker admission")
	}

	metrics.GlobalSortResidualDataSize.Set(37)
	stopDone := make(chan struct{})
	go func() {
		mgr.Stop()
		close(stopDone)
	}()
	select {
	case <-mgr.ctx.Done():
	case <-time.After(5 * time.Second):
		require.FailNow(t, "manager was not canceled")
	}
	select {
	case <-stopDone:
		require.FailNow(t, "manager stopped before the admitted worker was registered")
	case <-time.After(100 * time.Millisecond):
	}

	requestDuringStopDone := make(chan struct{})
	go func() {
		mgr.requestGlobalSortResidualMonitor()
		close(requestDuringStopDone)
	}()
	releaseAdmissionOnce.Do(func() { close(releaseAdmission) })
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "admitted request did not return")
	}
	select {
	case <-workerStarted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "admitted monitor worker did not start")
	}
	select {
	case <-requestDuringStopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "request during Stop did not return")
	}
	select {
	case <-stopDone:
		require.FailNow(t, "manager stopped before the admitted monitor worker exited")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())

	releaseWorkerOnce.Do(func() { close(releaseWorker) })
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "manager did not stop after the admitted monitor worker exited")
	}
	requireGlobalSortResidualGauge(t, 0)

	mgr.requestGlobalSortResidualMonitor()
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Equal(t, int32(1), workerCalls.Load())
}

func TestCleanupLoopRequestsGlobalSortResidualMonitor(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("global sort residual monitoring is only enabled in NextGen")
	}

	originalCleanUpInterval := DefaultCleanUpInterval
	DefaultCleanUpInterval = 500 * time.Millisecond
	t.Cleanup(func() {
		DefaultCleanUpInterval = originalCleanUpInterval
		metrics.GlobalSortResidualDataSize.Set(0)
	})

	mgr, taskMgr, _ := newGlobalSortResidualMonitorTestManager(t)
	cleanupStarted := []chan struct{}{
		make(chan struct{}, 1),
		make(chan struct{}, 1),
		make(chan struct{}, 1),
	}
	releaseCleanup := []chan struct{}{
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
	}
	var releaseCleanupOnce [3]sync.Once
	releaseCleanupCall := func(index int) {
		releaseCleanupOnce[index].Do(func() { close(releaseCleanup[index]) })
	}
	var cleanupCalls atomic.Int32
	unexpectedCleanupCall := make(chan int32, 1)
	taskMgr.EXPECT().GetCleanupTasks(gomock.Any()).DoAndReturn(
		func(context.Context) ([]*proto.Task, error) {
			callNumber := cleanupCalls.Add(1)
			call := int(callNumber) - 1
			if call >= len(cleanupStarted) {
				select {
				case unexpectedCleanupCall <- callNumber:
				default:
				}
				return nil, nil
			}
			cleanupStarted[call] <- struct{}{}
			<-releaseCleanup[call]
			return nil, nil
		},
	).AnyTimes()

	monitorStarted := make(chan struct{}, 1)
	var getAllTasksCalls atomic.Int32
	taskMgr.EXPECT().GetAllTasks(gomock.Any()).DoAndReturn(
		func(context.Context) ([]*proto.TaskBase, error) {
			call := getAllTasksCalls.Add(1)
			if call == 1 {
				monitorStarted <- struct{}{}
			}
			return nil, nil
		},
	).Times(2)

	scanStarted := make(chan struct{}, 1)
	store := &globalSortResidualMonitorStorage{
		Storage: objstore.NewMemStorage(),
		walkFn: func(context.Context) error {
			scanStarted <- struct{}{}
			return nil
		},
	}
	var resolverCalls atomic.Int32
	mgr.globalSortStorageURIResolver = func(context.Context, kv.Storage) string {
		resolverCalls.Add(1)
		return "memstore:///residual"
	}
	var factoryCalls atomic.Int32
	mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
		factoryCalls.Add(1)
		return store, nil
	}

	requestStarted := make(chan int32, 3)
	releaseStartupRequest := make(chan struct{})
	var releaseStartupRequestOnce sync.Once
	releaseStartupMonitorRequest := func() {
		releaseStartupRequestOnce.Do(func() { close(releaseStartupRequest) })
	}
	var requestCalls atomic.Int32
	testfailpoint.EnableCall(t,
		"github.com/pingcap/tidb/pkg/dxf/framework/scheduler/beforeGlobalSortResidualMonitorRun",
		func() {
			call := requestCalls.Add(1)
			requestStarted <- call
			switch call {
			case 1:
				<-releaseStartupRequest
			case 2:
				mgr.Cancel()
			}
		},
	)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		mgr.cleanTaskLoop()
	}()
	t.Cleanup(func() {
		for i := range releaseCleanup {
			releaseCleanupCall(i)
		}
		releaseStartupMonitorRequest()
		mgr.Cancel()
		select {
		case <-loopDone:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "cleanup task loop did not stop")
		}
		mgr.wg.Wait()
	})

	select {
	case <-cleanupStarted[0]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup cleanup did not start")
	}
	mgr.finishCh <- struct{}{}
	require.Zero(t, requestCalls.Load())
	releaseCleanupCall(0)
	select {
	case call := <-requestStarted:
		require.Equal(t, int32(1), call)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup cleanup did not request the residual monitor")
	}
	releaseStartupMonitorRequest()

	select {
	case <-monitorStarted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "startup residual monitor did not start")
	}
	select {
	case <-cleanupStarted[1]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "finish signal did not drain cleanup tasks")
	}
	mgr.wg.Wait()
	select {
	case <-scanStarted:
	default:
		require.FailNow(t, "startup residual monitor did not scan storage")
	}
	require.Equal(t, int32(2), getAllTasksCalls.Load())
	require.Equal(t, int32(1), resolverCalls.Load())
	require.Equal(t, int32(1), factoryCalls.Load())
	require.Equal(t, 1, store.closeCount)

	releaseCleanupCall(1)
	select {
	case call := <-requestStarted:
		require.FailNow(t, "finish signal requested the residual monitor", "request %d", call)
	case <-cleanupStarted[2]:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "periodic cleanup did not start")
	}
	require.Equal(t, int32(1), requestCalls.Load())
	require.Equal(t, int32(2), getAllTasksCalls.Load())

	releaseCleanupCall(2)
	select {
	case call := <-requestStarted:
		require.Equal(t, int32(2), call)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "periodic cleanup did not request the residual monitor")
	}
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "cleanup task loop did not stop")
	}
	mgr.wg.Wait()
	select {
	case call := <-unexpectedCleanupCall:
		require.FailNow(t, "cleanup task loop ran after the periodic monitor request", "call %d", call)
	default:
	}
	require.Equal(t, int32(3), cleanupCalls.Load())
	require.Equal(t, int32(2), requestCalls.Load())
	require.Equal(t, int32(2), getAllTasksCalls.Load())
	require.Equal(t, int32(1), resolverCalls.Load())
	require.Equal(t, int32(1), factoryCalls.Load())
	require.Equal(t, 1, store.closeCount)
}

func TestGlobalSortResidualMonitorRequestClassic(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("classic-only assertion")
	}

	mgr, _, _ := newGlobalSortResidualMonitorTestManager(t)
	var factoryCalls atomic.Int32
	mgr.globalSortStoreFactory = func(context.Context, string) (storeapi.Storage, error) {
		factoryCalls.Add(1)
		return nil, errors.New("unexpected factory call")
	}

	mgr.requestGlobalSortResidualMonitor()
	mgr.Stop()

	require.Zero(t, factoryCalls.Load())
}
