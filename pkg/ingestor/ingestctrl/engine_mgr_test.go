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
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/mock/mocklocal"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	pderr "github.com/tikv/pd/client/errs"
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
	local := &Backend{
		engineMgr: em,
		pdCli:     &mockPdClient{tsErrors: []error{context.DeadlineExceeded}},
	}
	require.NoError(t, local.SetTSBeforeImportEngine(ctx, engine1ID, 0))
	require.Equal(t, int64(2), local.pdCli.(*mockPdClient).tsCalls.Load())
	persistentPDCli := &mockPdClient{tsErrors: make([]error, maxRetryTimes)}
	for i := range persistentPDCli.tsErrors {
		persistentPDCli.tsErrors[i] = context.DeadlineExceeded
	}
	local.pdCli = persistentPDCli
	err = local.SetTSBeforeImportEngine(ctx, engine1ID, 0)
	require.ErrorIs(t, err, errdef.ErrSetTSBeforeImport)
	require.Equal(t, int64(maxRetryTimes), persistentPDCli.tsCalls.Load())
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	local.pdCli = &mockPdClient{tsErrors: []error{context.DeadlineExceeded}}
	err = local.SetTSBeforeImportEngine(canceledCtx, engine1ID, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, errdef.ErrSetTSBeforeImport)
	nonRetryablePDCli := &mockPdClient{tsErrors: []error{errors.New("invalid TSO request")}}
	local.pdCli = nonRetryablePDCli
	err = local.SetTSBeforeImportEngine(ctx, engine1ID, 0)
	require.ErrorIs(t, err, errdef.ErrSetTSBeforeImport)
	require.ErrorContains(t, err, "after 1 attempts")
	require.Equal(t, int64(1), nonRetryablePDCli.tsCalls.Load())
	streamErr := pderr.ErrClientCreateTSOStream.FastGenByArgs(pderr.RetryTimeoutErr)
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "internal cancellation", err: context.Canceled},
		{name: "wrapped internal cancellation", err: fmt.Errorf("TSO stream: %w", context.Canceled)},
		{name: "stream creation timeout", err: streamErr},
		{name: "wrapped stream creation timeout", err: fmt.Errorf("TSO stream: %w", streamErr)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pdClient := &mockPdClient{tsErrors: []error{testCase.err}}
			local.pdCli = pdClient
			require.NoError(t, local.SetTSBeforeImportEngine(ctx, engine1ID, 0))
			require.Equal(t, int64(2), pdClient.tsCalls.Load())
		})
	}
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "persistent internal cancellation", err: context.Canceled},
		{name: "persistent stream creation timeout", err: streamErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pdClient := &mockPdClient{tsErrors: make([]error, maxRetryTimes)}
			for i := range pdClient.tsErrors {
				pdClient.tsErrors[i] = testCase.err
			}
			local.pdCli = pdClient
			err := local.SetTSBeforeImportEngine(ctx, engine1ID, 0)
			require.ErrorIs(t, err, errdef.ErrSetTSBeforeImport)
			require.NotErrorIs(t, err, context.Canceled)
			require.Equal(t, int64(maxRetryTimes), pdClient.tsCalls.Load())
		})
	}
	t.Run("expired caller deadline", func(t *testing.T) {
		deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		pdClient := &mockPdClient{tsErrors: []error{context.DeadlineExceeded}}
		local.pdCli = pdClient
		err := local.SetTSBeforeImportEngine(deadlineCtx, engine1ID, 0)
		require.ErrorIs(t, err, errdef.ErrSetTSBeforeImport)
		require.Zero(t, pdClient.tsCalls.Load())
	})
	t.Run("caller deadline during retry backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			deadlineCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			pdClient := &mockPdClient{tsErrors: []error{context.DeadlineExceeded}}
			local.pdCli = pdClient
			err := local.SetTSBeforeImportEngine(deadlineCtx, engine1ID, 0)
			require.ErrorIs(t, err, errdef.ErrSetTSBeforeImport)
			require.Equal(t, int64(1), pdClient.tsCalls.Load())
		})
	})
	t.Run("caller cancellation during retry backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			canceledCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
			pdClient := &mockPdClient{tsErrors: []error{context.DeadlineExceeded}}
			local.pdCli = pdClient
			err := local.SetTSBeforeImportEngine(canceledCtx, engine1ID, 0)
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, errdef.ErrSetTSBeforeImport)
			require.Equal(t, int64(1), pdClient.tsCalls.Load())
		})
	})
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
