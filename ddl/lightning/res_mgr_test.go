// Copyright 2021 PingCAP, Inc.
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

package lightning

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMemoryControl(t *testing.T) {
	GlobalEnv.SetMinQuota()
	InitGlobalLightningBackendEnv()
	BackCtxMgr.MemRoot.SetMaxMemoryQuota(int64(2 * _gb))
	require.Equal(t, true, GlobalEnv.IsInited)
	require.Equal(t, int64(0), BackCtxMgr.MemRoot.CurrentUsage())

	ctx := context.Background()
	// Init important variables
	sysVars := obtainImportantVariables()
	jobID := int64(1)
	cfg, err := generateLightningConfig(BackCtxMgr.MemRoot, jobID, false)
	require.NoError(t, err)
	BackCtxMgr.Store(jobID, newBackendContext(ctx, jobID, nil, cfg, sysVars, BackCtxMgr.MemRoot))

	// Run one add index with 8 workers, test memory consumption.
	requireMem := StructSizeBackendCtx
	BackCtxMgr.MemRoot.Consume(requireMem)
	requireMem = StructSizeEngineInfo
	BackCtxMgr.MemRoot.Consume(requireMem)
	engineKey := "enKey1"
	wCnt := BackCtxMgr.MemRoot.WorkerDegree(8, engineKey, jobID)
	require.Equal(t, 8, wCnt)
	bc, exist := BackCtxMgr.Load(jobID)
	require.Equal(t, true, exist)
	var uuid uuid.UUID
	eninfo := NewEngineInfo(1, engineKey, nil, bc, nil, "", uuid, 8, BackCtxMgr.MemRoot)
	bc.EngMgr.Store(engineKey, eninfo)
	// add 8 workers more
	wCnt = BackCtxMgr.MemRoot.WorkerDegree(8, engineKey, jobID)
	require.Equal(t, 8, wCnt)
	en, exist1 := bc.EngMgr.Load(engineKey)
	en.writerCount += wCnt
	require.Equal(t, true, exist1)
	require.Equal(t, 16, en.writerCount)
	// Add 8 workers more
	wCnt = BackCtxMgr.MemRoot.WorkerDegree(8, engineKey, jobID)
	require.Equal(t, 0, wCnt)

	type TestCase struct {
		name      string
		bcKey     int64
		enKey     string
		writerCnt int
	}
	tests := []TestCase{
		{"case2", 2, "enKey2", 8},
		{"case3", 3, "enKey3", 2},
		{"case4", 4, "enKey4", 1},
		{"case5", 5, "enKey5", 0},
	}
	for _, test := range tests {
		// Run second add index with 16 worker, memory consumption
		jobID = test.bcKey
		requireMem = StructSizeBackendCtx
		BackCtxMgr.MemRoot.Consume(requireMem)
		requireMem = StructSizeEngineInfo
		BackCtxMgr.MemRoot.Consume(requireMem)
		engineKey = test.enKey
		BackCtxMgr.Store(jobID, newBackendContext(ctx, jobID, nil, cfg, sysVars, BackCtxMgr.MemRoot))
		wCnt = BackCtxMgr.MemRoot.WorkerDegree(16, engineKey, jobID)
		require.Equal(t, test.writerCnt, wCnt)
	}

	for _, test := range tests {
		BackCtxMgr.MemRoot.ReleaseWithTag(test.enKey)
		BackCtxMgr.MemRoot.Release(StructSizeEngineInfo)
		BackCtxMgr.MemRoot.Release(StructSizeBackendCtx)
	}

	BackCtxMgr.MemRoot.ReleaseWithTag("enKey1")
	BackCtxMgr.MemRoot.Release(StructSizeEngineInfo)
	BackCtxMgr.MemRoot.Release(StructSizeBackendCtx)
	require.Equal(t, int64(0), BackCtxMgr.MemRoot.CurrentUsage())
}

func TestStructSize(t *testing.T) {
	require.Greater(t, StructSizeBackendCtx, int64(0))
	require.Greater(t, StructSizeEngineInfo, int64(0))
	require.Greater(t, StructSizeWorkerCtx, int64(0))
}
