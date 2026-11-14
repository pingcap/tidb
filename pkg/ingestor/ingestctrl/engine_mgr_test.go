// Copyright 2023 PingCAP, Inc.
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

package ingestctrl

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/mock/mocklocal"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func getBackendConfig(t *testing.T) BackendConfig {
	return BackendConfig{
		MemTableSize:                config.DefaultEngineMemCacheSize,
		MaxOpenFiles:                1000,
		DisableAutomaticCompactions: true,
		LocalStoreDir:               path.Join(t.TempDir(), "sorted-kv"),
		DupeDetectEnabled:           false,
		DuplicateDetectOpt:          common.DupDetectOpt{},
		WorkerConcurrency:           toAtomic(8),
		LocalWriterMemCacheSize:     config.DefaultLocalWriterMemCacheSize,
		CheckpointEnabled:           false,
	}
}

func TestEngineManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeHelper := mocklocal.NewMockStoreHelper(ctrl)
	ctx := context.Background()
	syncMapLen := func(m *sync.Map) int {
		count := 0
		m.Range(func(key, value any) bool {
			count++
			return true
		})
		return count
	}
	isEmptyDir := func(name string) bool {
		f, err := os.Open(name)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.Readdirnames(1) // Or f.Readdir(1)
		if err == io.EOF {
			return true
		}
		require.Fail(t, err.Error())
		return false
	}

	backendConfig := getBackendConfig(t)
	em, err := newEngineManager(backendConfig, storeHelper, log.L())
	require.NoError(t, err)
	storeHelper.EXPECT().GetTS(gomock.Any()).Return(int64(0), int64(0), nil)
	engine1ID := uuid.New()
	require.NoError(t, em.openEngine(ctx, &backend.EngineConfig{}, engine1ID))
	require.Equal(t, 1, syncMapLen(&em.engines))
	_, ok := em.engines.Load(engine1ID)
	require.True(t, ok)
	require.True(t, ctrl.Satisfied())
	require.Len(t, em.engineFileSizes(), 1)

	require.NoError(t, em.closeEngine(ctx, &backend.EngineConfig{}, engine1ID))
	require.Equal(t, 0, int(em.getImportedKVCount(engine1ID)))
	// close non-existent engine
	require.ErrorContains(t, em.closeEngine(ctx, &backend.EngineConfig{}, uuid.New()), "does not exist")

	// reset non-existent engine should work
	require.NoError(t, em.resetEngine(ctx, uuid.New(), false))
	storeHelper.EXPECT().GetTS(gomock.Any()).Return(int64(0), int64(0), nil)
	require.NoError(t, em.resetEngine(ctx, engine1ID, false))
	require.Equal(t, 1, syncMapLen(&em.engines))
	_, ok = em.engines.Load(engine1ID)
	require.True(t, ok)
	require.True(t, ctrl.Satisfied())

	require.NoError(t, em.cleanupEngine(ctx, uuid.New()))
	require.NoError(t, em.cleanupEngine(ctx, engine1ID))
	require.Equal(t, 0, syncMapLen(&em.engines))

	require.True(t, isEmptyDir(backendConfig.LocalStoreDir))
	em.close()

	t.Run("failed initialization releases DB", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			directoryErr bool
			metadataErr  bool
			recovery     bool
			existing     bool
		}{
			{name: "SST directory", directoryErr: true},
			{name: "metadata", metadataErr: true},
			{name: "timestamp"},
			{name: "recovery metadata", metadataErr: true, recovery: true},
			{name: "existing engine metadata", metadataErr: true, existing: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				storeHelper := mocklocal.NewMockStoreHelper(ctrl)
				cfg := getBackendConfig(t)
				cfg.CheckpointEnabled = true
				em, err := newEngineManager(cfg, storeHelper, log.L())
				require.NoError(t, err)
				t.Cleanup(em.close)
				engineID := uuid.New()
				sstDir := engineSSTDir(cfg.LocalStoreDir, engineID)
				if tc.directoryErr {
					// A file at the directory path makes Mkdir fail without relying on permissions.
					require.NoError(t, os.WriteFile(sstDir, []byte("checkpoint"), 0o600))
				} else {
					require.NoError(t, os.Mkdir(sstDir, 0o750))
					require.NoError(t, os.WriteFile(path.Join(sstDir, "checkpoint"), []byte("preserved"), 0o600))
				}

				db, err := em.openEngineDB(engineID, false)
				require.NoError(t, err)
				require.NoError(t, db.Set([]byte("checkpoint"), []byte("preserved"), pebble.NoSync))
				badMeta := []byte(`{"ts":7,"length":"bad"}`)
				if tc.metadataErr {
					require.NoError(t, db.Set(engineMetaKey, badMeta, pebble.NoSync))
				}
				require.NoError(t, db.Flush())
				require.NoError(t, db.Close())

				var existing *Engine
				if tc.existing {
					existing = &Engine{
						UUID:         engineID,
						logger:       log.L(),
						ctx:          ctx,
						cancel:       func() {},
						sstDir:       sstDir,
						sstMetasChan: make(chan metaOrFlush, 64),
					}
					existing.TS = 88
					existing.Length.Store(3)
					existing.TotalSize.Store(4)
					em.engines.Store(engineID, existing)
				}
				if !tc.directoryErr && !tc.metadataErr {
					storeHelper.EXPECT().GetTS(gomock.Any()).Return(int64(0), int64(0), errors.New("injected GetTS error"))
				}
				engineCfg := &backend.EngineConfig{KeepSortDir: true}
				if tc.recovery {
					err = em.closeEngine(ctx, engineCfg, engineID)
				} else {
					err = em.openEngine(ctx, engineCfg, engineID)
				}
				require.Error(t, err)
				if tc.directoryErr {
					require.ErrorIs(t, err, os.ErrExist)
				} else if tc.metadataErr {
					require.ErrorContains(t, err, "cannot unmarshal")
				} else {
					require.ErrorContains(t, err, "injected GetTS error")
				}
				registered, ok := em.engines.Load(engineID)
				if tc.existing {
					require.True(t, ok)
					require.Same(t, existing, registered)
					require.Nil(t, existing.getDB())
					require.Equal(t, uint64(88), existing.TS)
					require.EqualValues(t, 3, existing.Length.Load())
					require.EqualValues(t, 4, existing.TotalSize.Load())
				} else {
					require.False(t, ok, "failed initialization must not publish an engine")
				}

				// Reopening the same path proves failure cleanup released Pebble's file lock.
				db, err = em.openEngineDB(engineID, false)
				require.NoError(t, err)
				value, closer, err := db.Get([]byte("checkpoint"))
				require.NoError(t, err)
				require.Equal(t, []byte("preserved"), value)
				require.NoError(t, closer.Close())
				if tc.metadataErr {
					value, closer, err = db.Get(engineMetaKey)
					require.NoError(t, err)
					require.Equal(t, badMeta, value)
					require.NoError(t, closer.Close())
				}
				require.NoError(t, saveEngineMetaToDB(&engineMeta{TS: 99}, db))
				require.NoError(t, db.Flush())
				require.NoError(t, db.Close())
				if tc.directoryErr {
					value, err := os.ReadFile(sstDir)
					require.NoError(t, err)
					require.Equal(t, []byte("checkpoint"), value)
					require.NoError(t, os.Remove(sstDir))
				} else {
					value, err := os.ReadFile(path.Join(sstDir, "checkpoint"))
					require.NoError(t, err)
					require.Equal(t, []byte("preserved"), value)
				}
				if !tc.recovery {
					require.NoError(t, em.openEngine(ctx, engineCfg, engineID))
				}
				require.NoError(t, em.closeEngine(ctx, engineCfg, engineID))
				registered, ok = em.engines.Load(engineID)
				require.True(t, ok)
				require.Equal(t, uint64(99), registered.(*Engine).TS)
			})
		}
	})
}

func TestCleanupAllLocalEnginesLogsErrorOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeHelper := mocklocal.NewMockStoreHelper(ctrl)

	observedCore, observedLogs := observer.New(zapcore.DebugLevel)
	backendConfig := getBackendConfig(t)
	em, err := newEngineManager(backendConfig, storeHelper, log.Wrap(zap.New(observedCore)))
	require.NoError(t, err)

	ctx := context.Background()
	engineID := uuid.New()
	storeHelper.EXPECT().GetTS(gomock.Any()).Return(int64(0), int64(0), nil)
	require.NoError(t, em.openEngine(ctx, &backend.EngineConfig{}, engineID))

	engineVal, ok := em.engines.Load(engineID)
	require.True(t, ok)
	engineVal.(*Engine).ingestErr.Set(errors.New("injected finishWrite error"))

	em.cleanupAllLocalEngines(ctx)
	require.Empty(t, em.engineFileSizes())

	logEntries := observedLogs.FilterMessage("cleanup all local engines failed").All()
	require.Len(t, logEntries, 1)
	require.Contains(t, logEntries[0].ContextMap()["error"], "injected finishWrite error")

	em.close()
}
