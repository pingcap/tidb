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

package orphandata

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type monitorWalkEntry struct {
	path string
	size int64
}

type monitorStorage struct {
	storeapi.Storage
	entries    []monitorWalkEntry
	walkErr    error
	walkFn     func(context.Context) error
	closeCount int
}

func (s *monitorStorage) WalkDir(
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

func (s *monitorStorage) Close() {
	s.closeCount++
}

type testMonitor struct {
	*Monitor
	logs *observer.ObservedLogs
}

type activeProducerCheckerFunc func(context.Context) (bool, error)

func (f activeProducerCheckerFunc) HasActiveProducers(ctx context.Context) (bool, error) {
	return f(ctx)
}

func staticURI(uri string) func() string {
	return func() string { return uri }
}

func newTestMonitor(t *testing.T, cfg Config) *testMonitor {
	t.Helper()
	if cfg.Logger == nil {
		core, logs := observer.New(zap.DebugLevel)
		cfg.Logger = zap.New(core)
		return &testMonitor{
			Monitor: NewMonitor(cfg),
			logs:    logs,
		}
	}
	return &testMonitor{
		Monitor: NewMonitor(cfg),
	}
}

func requireGauge(t *testing.T, expected float64) {
	t.Helper()
	require.Equal(t, expected, testutil.ToFloat64(metrics.GlobalSortOrphanDataSize))
}

func requireNoCredentials(t *testing.T, logs *observer.ObservedLogs, values ...string) {
	t.Helper()
	for _, entry := range logs.All() {
		require.NotContains(t, entry.ContextMap(), "storage-uri")
		logged := fmt.Sprintf("%s %v", entry.Message, entry.ContextMap())
		for _, value := range values {
			require.NotContains(t, logged, value)
		}
	}
}

func requireNoCandidate(t *testing.T, logs *observer.ObservedLogs, candidate string) {
	t.Helper()
	for _, entry := range logs.All() {
		require.NotContains(t, fmt.Sprintf("%s %v", entry.Message, entry.ContextMap()), candidate)
	}
}

func TestMonitor(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortOrphanDataSize.Set(0)
	})

	t.Run("config dependencies", func(t *testing.T) {
		checks := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				checks++
				return false, nil
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(_ context.Context, uri string) (storeapi.Storage, error) {
			require.Equal(t, "memstore:///orphandata", uri)
			return &monitorStorage{Storage: objstore.NewMemStorage()}, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 2, checks)
	})

	t.Run("reads the latest storage URI on every trigger", func(t *testing.T) {
		uris := []string{"memstore:///first", "memstore:///second"}
		index := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI: func() string {
				return uris[index]
			},
		})
		var scannedURIs []string
		m.storeFactory = func(_ context.Context, uri string) (storeapi.Storage, error) {
			scannedURIs = append(scannedURIs, uri)
			index++
			return &monitorStorage{Storage: objstore.NewMemStorage()}, nil
		}

		m.Trigger(context.Background())
		m.Trigger(context.Background())

		require.Equal(t, []string{"memstore:///first", "memstore:///second"}, scannedURIs)
	})

	t.Run("first gate has tasks", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		factoryCalls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return true, nil }),
			GetStorageURI:         staticURI("s3://bucket/prefix"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}

		m.Trigger(context.Background())

		require.Zero(t, factoryCalls)
		// the previous value is kept while producers are active.
		requireGauge(t, 37)
		require.Empty(t, m.logs.All())
	})

	t.Run("first gate query error", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		factoryCalls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, errors.New("first gate failed") }),
			GetStorageURI:         staticURI("s3://bucket/prefix"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}

		m.Trigger(context.Background())

		require.Zero(t, factoryCalls)
		requireGauge(t, 37)
		require.Len(t, m.logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		require.Contains(t, m.logs.All()[0].ContextMap()["error"], "first gate failed")
	})

	t.Run("empty URI does nothing", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		factoryCalls := 0
		calls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				calls++
				return false, nil
			}),
			GetStorageURI: staticURI(""),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return nil, errors.New("unexpected factory call")
		}

		m.Trigger(context.Background())

		require.Zero(t, factoryCalls)
		require.Zero(t, calls)
		requireGauge(t, 37)
		require.Empty(t, m.logs.All())
	})

	t.Run("invalid URI uses the default factory", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		const uri = "s3:///missing-bucket?access-key=invalid-ak&secret-access-key=invalid-sk&session-token=invalid-token"
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})

		m.Trigger(context.Background())

		requireGauge(t, 37)
		warnings := m.logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0].ContextMap(), "error")
	})

	t.Run("injected store creation error", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		const (
			accessKey    = "create-ak-fragment"
			secretKey    = "create+sk-fragment"
			sessionToken = "create-token-fragment"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=create%2Bsk-fragment&SeSsIoN_ToKeN=" + sessionToken
		)
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, fmt.Errorf("injected creation leaked fragments %s %s %s", accessKey, secretKey, sessionToken)
		}

		m.Trigger(context.Background())

		requireGauge(t, 37)
		warnings := m.logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0].ContextMap(), "error")
	})

	t.Run("malformed storage URI logs the error", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		const (
			secret = "malformed-secret"
			uri    = "s3://bucket/%zz?secret-access-key=" + secret
		)
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return nil, errors.New("malformed factory failure")
		}

		m.Trigger(context.Background())

		requireGauge(t, 37)
		warnings := m.logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.NotContains(t, warnings[0].ContextMap(), "storage-uri")
		require.Contains(t, warnings[0].ContextMap(), "error")
	})

	t.Run("configured empty store", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		store := &monitorStorage{Storage: objstore.NewMemStorage()}
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 0)
		require.Len(t, m.logs.FilterMessage("global sort orphan data monitor success").All(), 1)
	})

	t.Run("configured nonempty store", func(t *testing.T) {
		const (
			accessKey    = "success-ak"
			secretKey    = "success+sk"
			sessionToken = "success-token"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=success%2Bsk&SeSsIoN_ToKeN=" + sessionToken
		)
		entries := make([]monitorWalkEntry, 0, 11)
		for i := 10; i >= 0; i-- {
			entries = append(entries, monitorWalkEntry{
				path: fmt.Sprintf("prefix-%02d/file", i),
				size: int64(i + 1),
			})
		}
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 66)
		successLogs := m.logs.FilterMessage("global sort orphan data monitor success").All()
		require.Len(t, successLogs, 1)
		fields := successLogs[0].ContextMap()
		require.NotContains(t, fields, "storage-uri")
		require.EqualValues(t, 66, fields["size-bytes"])
		require.EqualValues(t, 11, fields["object-count"])
		require.Equal(t, []any{
			"prefix-10/file", "prefix-09/file", "prefix-08/file", "prefix-07/file", "prefix-06/file",
			"prefix-05/file", "prefix-04/file", "prefix-03/file", "prefix-02/file", "prefix-01/file",
		}, fields["sample-objects"])
		require.Equal(t, true, fields["sample-truncated"])
		requireNoCredentials(t, m.logs, accessKey, secretKey, sessionToken, "success%2Bsk")
	})

	t.Run("configured Azure URI is omitted", func(t *testing.T) {
		const (
			accountKey    = "azure-account"
			sasToken      = "azure+sas"
			encryptionKey = "azure-encryption"
			uri           = "azure://container/prefix?AcCoUnT_KeY=" + accountKey + "&SaS-ToKeN=azure%2Bsas&EnCrYpTiOn-KeY=" + encryptionKey
		)
		store := &monitorStorage{Storage: objstore.NewMemStorage()}
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 0)
		successLogs := m.logs.FilterMessage("global sort orphan data monitor success").All()
		require.Len(t, successLogs, 1)
		require.NotContains(t, successLogs[0].ContextMap(), "storage-uri")
		requireNoCredentials(t, m.logs, accountKey, sasToken, encryptionKey, "azure%2Bsas")
	})

	t.Run("task appears at the second gate", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		const candidate = "candidate-prefix/"
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []monitorWalkEntry{{path: candidate + "file", size: 41}},
		}
		calls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				calls++
				if calls == 1 {
					return false, nil
				}
				return true, nil
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		// the previous value is kept while producers are active.
		requireGauge(t, 37)
		require.Empty(t, m.logs.FilterMessage("global sort orphan data monitor success").All())
		discardLogs := m.logs.FilterMessage("global sort orphan data monitor discarded scan because tasks appeared").All()
		require.Len(t, discardLogs, 1)
		require.Len(t, m.logs.FilterLevelExact(zap.InfoLevel).All(), 1)
		fields := discardLogs[0].ContextMap()
		for _, field := range []string{
			"storage-uri",
			"task-count",
			"size-bytes",
			"object-count",
			"sample-objects",
			"sample-truncated",
		} {
			require.NotContains(t, fields, field)
		}
		requireNoCandidate(t, m.logs, candidate)
	})

	t.Run("second gate query error", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []monitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		calls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				calls++
				if calls == 1 {
					return false, nil
				}
				return false, errors.New("second gate failed")
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 37)
		require.Len(t, m.logs.FilterLevelExact(zap.WarnLevel).All(), 1)
		requireNoCandidate(t, m.logs, "candidate-prefix/")
	})

	t.Run("walk error", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		const uri = "azure://container/prefix?account-key=walk-account&sas-token=walk%2Bsas&encryption-key=walk-encryption"
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			walkErr: errors.New(
				"walk leaked fragments walk-account walk+sas walk-encryption",
			),
		}
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 37)
		warnings := m.logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.Contains(t, warnings[0].ContextMap(), "error")
		requireNoCandidate(t, m.logs, "candidate-")
	})

	t.Run("canceled before first read", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		checks := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(c context.Context) (bool, error) {
				checks++
				return false, c.Err()
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})

		m.Trigger(ctx)

		require.Equal(t, 1, checks)
		requireGauge(t, 37)
		require.Empty(t, m.logs.All())
	})

	t.Run("walk returns cancellation", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &monitorStorage{Storage: objstore.NewMemStorage()}
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI("memstore:///orphandata"),
		})
		store.walkFn = func(context.Context) error {
			cancel()
			return context.Canceled
		}
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(ctx)

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 37)
		require.Empty(t, m.logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoCandidate(t, m.logs, "candidate-")
	})

	t.Run("cancellation during second read", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []monitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		calls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				calls++
				if calls == 1 {
					return false, nil
				}
				cancel()
				return false, context.Canceled
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(ctx)

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 37)
		require.Empty(t, m.logs.FilterLevelExact(zap.WarnLevel).All())
		requireNoCandidate(t, m.logs, "candidate-prefix/")
	})

	t.Run("cancellation after the final gate still publishes", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &monitorStorage{
			Storage: objstore.NewMemStorage(),
			entries: []monitorWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		calls := 0
		m := newTestMonitor(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				calls++
				if calls == 1 {
					return false, nil
				}
				cancel()
				return false, nil
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(ctx)

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 41)
		require.Len(t, m.logs.FilterMessage("global sort orphan data monitor success").All(), 1)
		require.Empty(t, m.logs.FilterLevelExact(zap.WarnLevel).All())
	})
}
