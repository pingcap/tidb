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
	GlobalEnv.LitMemRoot.maxLimit = int64(2 * _gb)
	require.Equal(t, true, GlobalEnv.IsInited)
	require.Equal(t, int64(0), GlobalEnv.LitMemRoot.currUsage)

	ctx := context.Background()
	// Init important variables
	sysVars := obtainImportantVariables()
	bcKey := "bcKey1"
	cfg, err := generateLightningConfig(bcKey, false)
	require.NoError(t, err)
	GlobalEnv.LitMemRoot.backendCache[bcKey] = newBackendContext(ctx, bcKey, nil, cfg, sysVars)

	// Run one add index with 8 workers, test memory consumption.
	requireMem := StructSizeBackendCtx
	GlobalEnv.LitMemRoot.currUsage += requireMem
	requireMem = StructSizeEngineInfo
	GlobalEnv.LitMemRoot.currUsage += requireMem
	engineKey := "enKey1"
	wCnt := GlobalEnv.LitMemRoot.workerDegree(8, engineKey, bcKey)
	require.Equal(t, 8, wCnt)
	bc, exist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, false)
	require.Equal(t, true, exist)
	var uuid uuid.UUID
	eninfo := NewEngineInfo(1, engineKey, nil, bc, nil, "", uuid, 8)
	bc.engineCache[engineKey] = eninfo
	// add 8 workers more
	wCnt = GlobalEnv.LitMemRoot.workerDegree(8, engineKey, bcKey)
	require.Equal(t, 8, wCnt)
	en, exist1 := bc.engineCache[engineKey]
	en.writerCount += wCnt
	require.Equal(t, true, exist1)
	require.Equal(t, 16, en.writerCount)
	// Add 8 workers more
	wCnt = GlobalEnv.LitMemRoot.workerDegree(8, engineKey, bcKey)
	require.Equal(t, 0, wCnt)

	type TestCase struct {
		name      string
		bcKey     string
		enKey     string
		writerCnt int
	}
	tests := []TestCase{
		{"case2", "bcKey2", "enKey2", 8},
		{"case3", "bcKey3", "enKey3", 2},
		{"case4", "bcKey4", "enKey4", 1},
		{"case5", "bcKey5", "enKey5", 0},
	}
	for _, test := range tests {
		// Run second add index with 16 worker, memory consumption
		bcKey = test.bcKey
		requireMem = StructSizeBackendCtx
		GlobalEnv.LitMemRoot.currUsage += requireMem
		requireMem = StructSizeEngineInfo
		GlobalEnv.LitMemRoot.currUsage += requireMem
		engineKey = test.enKey
		GlobalEnv.LitMemRoot.backendCache[bcKey] = newBackendContext(ctx, bcKey, nil, cfg, sysVars)
		wCnt = GlobalEnv.LitMemRoot.workerDegree(16, engineKey, bcKey)
		require.Equal(t, test.writerCnt, wCnt)
	}

	for _, test := range tests {
		GlobalEnv.LitMemRoot.currUsage -= GlobalEnv.LitMemRoot.structSize[test.enKey]
		GlobalEnv.LitMemRoot.currUsage -= StructSizeEngineInfo
		GlobalEnv.LitMemRoot.currUsage -= StructSizeBackendCtx
	}

	GlobalEnv.LitMemRoot.currUsage -= GlobalEnv.LitMemRoot.structSize["enKey1"]
	GlobalEnv.LitMemRoot.currUsage -= StructSizeEngineInfo
	GlobalEnv.LitMemRoot.currUsage -= StructSizeBackendCtx
	require.Equal(t, int64(0), GlobalEnv.LitMemRoot.currUsage)
}

func TestStructSize(t *testing.T) {
	require.Greater(t, StructSizeBackendCtx, int64(0))
	require.Greater(t, StructSizeEngineInfo, int64(0))
	require.Greater(t, StructSizeWorkerCtx, int64(0))
}
