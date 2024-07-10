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

package local

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/mock/mocklocal"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func getBackendConfig(t *testing.T) BackendConfig {
	return BackendConfig{
		MemTableSize:                config.DefaultEngineMemCacheSize,
		MaxOpenFiles:                1000,
		DisableAutomaticCompactions: true,
		LocalStoreDir:               path.Join(t.TempDir(), "sorted-kv"),
		DupeDetectEnabled:           false,
		DuplicateDetectOpt:          common.DupDetectOpt{},
		WorkerConcurrency:           8,
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
}

func TestGetExternalEngineKVStatistics(t *testing.T) {
	em := &engineManager{
		externalEngine: map[uuid.UUID]common.Engine{},
	}
	// non existent uuid
	size, count := em.getExternalEngineKVStatistics(uuid.New())
	require.Zero(t, size)
	require.Zero(t, count)
}
