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

	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictpath"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type cleanerWalkEntry struct {
	path string
	size int64
}

type cleanerStorage struct {
	storeapi.Storage
	entries     []cleanerWalkEntry
	walkErr     error
	walkFn      func(context.Context) error
	closeCount  int
	deleteCalls int
	deleteFn    func(context.Context, []string) error
}

func (s *cleanerStorage) WalkDir(
	ctx context.Context,
	_ *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	if s.walkFn != nil {
		if err := s.walkFn(ctx); err != nil {
			return err
		}
	}
	if s.entries == nil {
		if err := s.Storage.WalkDir(ctx, &storeapi.WalkOption{}, fn); err != nil {
			return err
		}
		return s.walkErr
	}
	for _, entry := range s.entries {
		if err := fn(entry.path, entry.size); err != nil {
			return err
		}
	}
	return s.walkErr
}

func (s *cleanerStorage) DeleteFiles(ctx context.Context, keys []string) error {
	s.deleteCalls++
	if s.deleteFn != nil {
		return s.deleteFn(ctx, keys)
	}
	// Synthetic listing entries are used by the existing statistics tests.
	if s.entries != nil {
		return nil
	}
	return s.Storage.DeleteFiles(ctx, keys)
}

func (s *cleanerStorage) Close() {
	s.closeCount++
}

type testCleaner struct {
	*Cleaner
	logs *observer.ObservedLogs
}

type activeProducerCheckerFunc func(context.Context) (bool, error)

func (f activeProducerCheckerFunc) HasActiveProducers(ctx context.Context) (bool, error) {
	return f(ctx)
}

func staticURI(uri string) func() string {
	return func() string { return uri }
}

func newTestCleaner(t *testing.T, cfg Config) *testCleaner {
	t.Helper()
	if cfg.Logger == nil {
		core, logs := observer.New(zap.DebugLevel)
		cfg.Logger = zap.New(core)
		return &testCleaner{
			Cleaner: NewCleaner(cfg),
			logs:    logs,
		}
	}
	return &testCleaner{
		Cleaner: NewCleaner(cfg),
	}
}

// orphanDataMetricName is the name of the metric the cleaner publishes to.
const orphanDataMetricName = "tidb_global_sort_orphan_data_size_bytes"

// requireGauge asserts the orphan data size is the expected value.
func requireGauge(t *testing.T, expected float64) {
	t.Helper()
	require.Equal(t, expected, testutil.ToFloat64(metrics.GlobalSortOrphanDataSize))
}

// requireGaugeAbsent asserts the orphan data size series is absent, i.e. it was
// reset rather than set to a value.
func requireGaugeAbsent(t *testing.T) {
	t.Helper()
	require.Zero(t, testutil.CollectAndCount(metrics.GlobalSortOrphanDataSize, orphanDataMetricName))
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

func TestCleaner(t *testing.T) {
	t.Cleanup(func() {
		metrics.GlobalSortOrphanDataSize.Reset()
	})

	t.Run("cleanup deletes only scanned keys and retains conflict data", func(t *testing.T) {
		ctx := context.Background()
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		const oldKey = "123/old-file"
		const newKey = "123/new-file"
		retainedKey := conflictpath.StoragePrefix + "9/3-uuid/0_1"
		require.NoError(t, store.WriteFile(ctx, oldKey, []byte("old")))
		require.NoError(t, store.WriteFile(ctx, retainedKey, []byte("retained")))
		checks := 0
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				checks++
				if checks == 2 {
					require.NoError(t, store.WriteFile(ctx, newKey, []byte("new")))
				}
				return false, nil
			}),
			GetStorageURI:    staticURI("memstore:///orphandata"),
			RetainedPrefixes: []string{conflictpath.StoragePrefix},
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) { return store, nil }

		m.Trigger(ctx)

		exists, err := store.FileExists(ctx, oldKey)
		require.NoError(t, err)
		require.False(t, exists)
		for _, key := range []string{newKey, retainedKey} {
			data, err := store.ReadFile(ctx, key)
			require.NoError(t, err)
			require.NotEmpty(t, data)
		}
		require.Equal(t, 1, store.deleteCalls)
		requireGauge(t, 3)
		m.Trigger(ctx)
		require.Equal(t, 2, store.deleteCalls)
		m.Trigger(ctx)
		require.Equal(t, 2, store.deleteCalls)
		requireGauge(t, 0)
		data, err := store.ReadFile(ctx, retainedKey)
		require.NoError(t, err)
		require.Equal(t, []byte("retained"), data)
	})

	t.Run("cleanup fails closed without a producer checker", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		factoryCalls := 0
		m := newTestCleaner(t, Config{GetStorageURI: staticURI("memstore:///orphandata")})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			factoryCalls++
			return &cleanerStorage{Storage: objstore.NewMemStorage()}, nil
		}

		m.Trigger(context.Background())

		require.Zero(t, factoryCalls)
		requireGauge(t, 37)
	})

	t.Run("cleanup bounds each sweep and measures the full scan", func(t *testing.T) {
		ctx := context.Background()
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		for i := range 1001 {
			require.NoError(t, store.WriteFile(ctx, fmt.Sprintf("123/file-%04d", i), []byte("x")))
		}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) { return store, nil }

		m.Trigger(ctx)

		remaining := 0
		require.NoError(t, store.WalkDir(ctx, &storeapi.WalkOption{}, func(string, int64) error { remaining++; return nil }))
		require.Equal(t, 1, remaining)
		require.Equal(t, 1, store.deleteCalls)
		requireGauge(t, 1001)
		summaryLogs := m.logs.FilterMessage("global sort orphan data cleaner finished").All()
		require.Len(t, summaryLogs, 1)
		require.EqualValues(t, 1001, summaryLogs[0].ContextMap()["object-count"])
		require.EqualValues(t, 1000, summaryLogs[0].ContextMap()["deletion-candidate-count"])
		m.Trigger(ctx)
		requireGauge(t, 1)
		require.Equal(t, 2, store.deleteCalls)
	})

	t.Run("cleanup retries remaining objects after a partial deletion failure", func(t *testing.T) {
		ctx := context.Background()
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		for _, key := range []string{"123/a", "123/b"} {
			require.NoError(t, store.WriteFile(ctx, key, []byte("data")))
		}
		const secret = "delete-secret-fragment"
		const uri = "s3://bucket/prefix?secret-access-key=" + secret
		store.deleteFn = func(ctx context.Context, keys []string) error {
			requireGauge(t, 8)
			require.Len(t, keys, 2)
			require.NoError(t, store.Storage.DeleteFile(ctx, keys[0]))
			return errors.New(uri)
		}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) { return store, nil }

		m.Trigger(ctx)

		requireGauge(t, 8)
		warnings := m.logs.FilterLevelExact(zap.WarnLevel).All()
		require.Len(t, warnings, 1)
		require.EqualValues(t, 2, warnings[0].ContextMap()["deletion-candidate-count"])
		summaryLogs := m.logs.FilterMessage("global sort orphan data cleaner finished").All()
		require.Len(t, summaryLogs, 1)
		require.EqualValues(t, 8, summaryLogs[0].ContextMap()["size-bytes"])
		require.EqualValues(t, 2, summaryLogs[0].ContextMap()["object-count"])
		require.Equal(t, zap.WarnLevel, m.logs.All()[0].Level)
		require.Equal(t, zap.InfoLevel, m.logs.All()[1].Level)
		requireNoCredentials(t, m.logs, uri, secret)
		store.deleteFn = nil
		m.Trigger(ctx)
		requireGauge(t, 4)
		for _, key := range []string{"123/a", "123/b"} {
			exists, err := store.FileExists(ctx, key)
			require.NoError(t, err)
			require.False(t, exists)
		}
		require.Equal(t, 2, store.deleteCalls)
	})

	t.Run("cleanup preserves objects when a gate or scan fails", func(t *testing.T) {
		for _, scenario := range []string{"first active", "first error", "second active", "second error", "walk error", "walk canceled", "first canceled without error"} {
			t.Run(scenario, func(t *testing.T) {
				metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				store := &cleanerStorage{Storage: objstore.NewMemStorage()}
				require.NoError(t, store.WriteFile(ctx, "123/file", []byte("data")))
				checks := 0
				factoryCalls := 0
				m := newTestCleaner(t, Config{
					ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
						checks++
						if (checks == 1 && scenario == "first active") || (checks == 2 && scenario == "second active") {
							return true, nil
						}
						if (checks == 1 && scenario == "first error") || (checks == 2 && scenario == "second error") {
							return false, errors.New("query failed")
						}
						if checks == 1 && scenario == "first canceled without error" {
							cancel()
						}
						return false, nil
					}),
					GetStorageURI: staticURI("memstore:///orphandata"),
				})
				if scenario == "walk error" {
					store.walkErr = errors.New("scan failed")
				}
				if scenario == "walk canceled" {
					store.walkFn = func(context.Context) error { cancel(); return context.Canceled }
				}
				m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
					factoryCalls++
					return store, nil
				}

				m.Trigger(ctx)

				exists, err := store.FileExists(context.Background(), "123/file")
				require.NoError(t, err)
				require.True(t, exists)
				require.Zero(t, store.deleteCalls)
				requireGauge(t, 37)
				if scenario == "first canceled without error" {
					require.Equal(t, 1, factoryCalls)
				}
			})
		}
	})

	t.Run("config dependencies", func(t *testing.T) {
		checks := 0
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) {
				checks++
				return false, nil
			}),
			GetStorageURI: staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(_ context.Context, uri string) (storeapi.Storage, error) {
			require.Equal(t, "memstore:///orphandata", uri)
			return &cleanerStorage{Storage: objstore.NewMemStorage()}, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 2, checks)
	})

	t.Run("reads the latest storage URI on every trigger", func(t *testing.T) {
		uris := []string{"memstore:///first", "memstore:///second"}
		index := 0
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI: func() string {
				return uris[index]
			},
		})
		var scannedURIs []string
		m.storeFactory = func(_ context.Context, uri string) (storeapi.Storage, error) {
			scannedURIs = append(scannedURIs, uri)
			index++
			return &cleanerStorage{Storage: objstore.NewMemStorage()}, nil
		}

		m.Trigger(context.Background())
		m.Trigger(context.Background())

		require.Equal(t, []string{"memstore:///first", "memstore:///second"}, scannedURIs)
	})

	t.Run("first gate has tasks", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		factoryCalls := 0
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		factoryCalls := 0
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		factoryCalls := 0
		calls := 0
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		const uri = "s3:///missing-bucket?access-key=invalid-ak&secret-access-key=invalid-sk&session-token=invalid-token"
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		const (
			accessKey    = "create-ak-fragment"
			secretKey    = "create+sk-fragment"
			sessionToken = "create-token-fragment"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=create%2Bsk-fragment&SeSsIoN_ToKeN=" + sessionToken
		)
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		const (
			secret = "malformed-secret"
			uri    = "s3://bucket/%zz?secret-access-key=" + secret
		)
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI("memstore:///orphandata"),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 0)
		require.Len(t, m.logs.FilterMessage("global sort orphan data cleaner finished").All(), 1)
	})

	t.Run("configured nonempty store", func(t *testing.T) {
		const (
			accessKey    = "success-ak"
			secretKey    = "success+sk"
			sessionToken = "success-token"
			uri          = "s3://bucket/prefix?AcCeSs_KeY=" + accessKey + "&SeCrEt_AcCeSs_KeY=success%2Bsk&SeSsIoN_ToKeN=" + sessionToken
		)
		entries := make([]cleanerWalkEntry, 0, 11)
		for i := 10; i >= 0; i-- {
			entries = append(entries, cleanerWalkEntry{
				path: fmt.Sprintf("prefix-%02d/file", i),
				size: int64(i + 1),
			})
		}
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			entries: entries,
		}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 66)
		summaryLogs := m.logs.FilterMessage("global sort orphan data cleaner finished").All()
		require.Len(t, summaryLogs, 1)
		fields := summaryLogs[0].ContextMap()
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

	t.Run("retained prefixes are not orphan data", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			entries: []cleanerWalkEntry{
				{path: "123/meta.json", size: 5},
				{path: conflictpath.StoragePrefix + "9/3-uuid/0_1", size: 41},
			},
		}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI("memstore:///orphandata"),
			RetainedPrefixes:      []string{conflictpath.StoragePrefix},
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		requireGauge(t, 5)
		summaryLogs := m.logs.FilterMessage("global sort orphan data cleaner finished").All()
		require.Len(t, summaryLogs, 1)
		require.EqualValues(t, 1, summaryLogs[0].ContextMap()["object-count"])
	})

	t.Run("configured Azure URI is omitted", func(t *testing.T) {
		const (
			accountKey    = "azure-account"
			sasToken      = "azure+sas"
			encryptionKey = "azure-encryption"
			uri           = "azure://container/prefix?AcCoUnT_KeY=" + accountKey + "&SaS-ToKeN=azure%2Bsas&EnCrYpTiOn-KeY=" + encryptionKey
		)
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		m := newTestCleaner(t, Config{
			ActiveProducerChecker: activeProducerCheckerFunc(func(context.Context) (bool, error) { return false, nil }),
			GetStorageURI:         staticURI(uri),
		})
		m.storeFactory = func(context.Context, string) (storeapi.Storage, error) {
			return store, nil
		}

		m.Trigger(context.Background())

		require.Equal(t, 1, store.closeCount)
		requireGauge(t, 0)
		summaryLogs := m.logs.FilterMessage("global sort orphan data cleaner finished").All()
		require.Len(t, summaryLogs, 1)
		require.NotContains(t, summaryLogs[0].ContextMap(), "storage-uri")
		requireNoCredentials(t, m.logs, accountKey, sasToken, encryptionKey, "azure%2Bsas")
	})

	t.Run("task appears at the second gate", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		const candidate = "candidate-prefix/"
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			entries: []cleanerWalkEntry{{path: candidate + "file", size: 41}},
		}
		calls := 0
		m := newTestCleaner(t, Config{
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
		require.Empty(t, m.logs.FilterMessage("global sort orphan data cleaner finished").All())
		discardLogs := m.logs.FilterMessage("global sort orphan data cleaner discarded scan because tasks appeared").All()
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			entries: []cleanerWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		calls := 0
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		const uri = "azure://container/prefix?account-key=walk-account&sas-token=walk%2Bsas&encryption-key=walk-encryption"
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			walkErr: errors.New(
				"walk leaked fragments walk-account walk+sas walk-encryption",
			),
		}
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		checks := 0
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		m := newTestCleaner(t, Config{
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
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &cleanerStorage{
			Storage: objstore.NewMemStorage(),
			entries: []cleanerWalkEntry{{path: "candidate-prefix/file", size: 41}},
		}
		calls := 0
		m := newTestCleaner(t, Config{
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

	t.Run("cancellation after the final gate is handled by storage", func(t *testing.T) {
		metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		store := &cleanerStorage{Storage: objstore.NewMemStorage()}
		require.NoError(t, store.WriteFile(ctx, "candidate-prefix/file", []byte("data")))
		calls := 0
		m := newTestCleaner(t, Config{
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
		requireGauge(t, 4)
		require.Equal(t, 1, store.deleteCalls)
		exists, err := store.FileExists(context.Background(), "candidate-prefix/file")
		require.NoError(t, err)
		require.True(t, exists)
		require.Len(t, m.logs.FilterMessage("global sort orphan data cleaner finished").All(), 1)
		require.Empty(t, m.logs.FilterLevelExact(zap.WarnLevel).All())
	})
}

func TestCleanerResetDeletesPublishedSize(t *testing.T) {
	t.Cleanup(func() { metrics.GlobalSortOrphanDataSize.Reset() })

	m := newTestCleaner(t, Config{})
	metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(37)
	requireGauge(t, 37)

	m.Reset()

	requireGaugeAbsent(t)
}
