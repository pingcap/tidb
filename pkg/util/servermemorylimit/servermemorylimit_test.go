// Copyright 2022 PingCAP, Inc.
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

package servermemorylimit

import (
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestMemoryUsageOpsHistory(t *testing.T) {
	info := sessmgr.ProcessInfo{}
	genInfo := func(i int) {
		info.ID = uint64(i)
		info.DB = strconv.Itoa(2 * i)
		info.User = strconv.Itoa(3 * i)
		info.Host = strconv.Itoa(4 * i)
		info.Digest = strconv.Itoa(5 * i)
		info.Info = strconv.Itoa(6 * i)
	}

	for i := range 3 {
		genInfo(i)
		GlobalMemoryOpsHistoryManager.recordOne(&info, time.Now(), uint64(i), uint64(2*i))
	}

	checkResult := func(datums []types.Datum, i int) {
		require.Equal(t, datums[1].GetString(), "SessionKill")
		require.Equal(t, datums[2].GetInt64(), int64(i))
		require.Equal(t, datums[3].GetInt64(), int64(2*i))
		require.Equal(t, datums[4].GetInt64(), int64(i))
		require.Equal(t, datums[7].GetString(), strconv.Itoa(4*i))
		require.Equal(t, datums[8].GetString(), strconv.Itoa(2*i))
		require.Equal(t, datums[9].GetString(), strconv.Itoa(3*i))
		require.Equal(t, datums[10].GetString(), strconv.Itoa(5*i))
		require.Equal(t, datums[11].GetString(), strconv.Itoa(6*i))
	}

	rows := GlobalMemoryOpsHistoryManager.GetRows()
	require.Equal(t, 3, len(rows))
	for i := range 3 {
		checkResult(rows[i], i)
	}
	// Test evict
	for i := 3; i < 53; i++ {
		genInfo(i)
		GlobalMemoryOpsHistoryManager.recordOne(&info, time.Now(), uint64(i), uint64(2*i))
	}
	rows = GlobalMemoryOpsHistoryManager.GetRows()
	require.Equal(t, 50, len(rows))
	for i := 3; i < 53; i++ {
		checkResult(rows[i-3], i)
	}
	require.Equal(t, GlobalMemoryOpsHistoryManager.offsets, 3)
}

func TestServerlessMemoryScalerConfig(t *testing.T) {
	prevServerMemoryLimit := memory.ServerMemoryLimit.Load()
	prevServerMemoryLimitOriginText := memory.ServerMemoryLimitOriginText.Load()
	prevPercentage := gctuner.GlobalMemoryLimitTuner.GetPercentage()
	prevGoMemLimit := debug.SetMemoryLimit(-1)
	defer func() {
		memory.ServerMemoryLimit.Store(prevServerMemoryLimit)
		memory.ServerMemoryLimitOriginText.Store(prevServerMemoryLimitOriginText)
		gctuner.GlobalMemoryLimitTuner.SetPercentage(prevPercentage)
		debug.SetMemoryLimit(prevGoMemLimit)
	}()
	if kerneltype.IsNextGen() {
		prevDeployMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(prevDeployMode))
		})
	}
	gctuner.GlobalMemoryLimitTuner.SetPercentage(0.7)

	t.Run("non-starter mode does not read env memory limits", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			require.NoError(t, deploymode.Set(deploymode.Premium))
		}
		memory.ServerMemoryLimit.Store(1234)
		t.Setenv(serverlessEnvMinMemoryLimit, "1073741824")
		t.Setenv(serverlessEnvMaxMemoryLimit, "2147483648")

		scaler, ok := newServerlessMemoryScaler(make(chan struct{}))
		require.False(t, ok)
		require.Nil(t, scaler)
		require.Equal(t, uint64(1234), memory.ServerMemoryLimit.Load())
	})

	t.Run("starter mode initializes memory limit without scaler loop", func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip("Starter deploy mode requires nextgen kernel")
		}
		require.NoError(t, deploymode.Set(deploymode.Starter))
		t.Setenv(serverlessEnvNamespace, "")
		t.Setenv(serverlessEnvPodName, "")
		t.Setenv(serverlessEnvNodeIP, "")
		t.Setenv(serverlessEnvMinMemoryLimit, "1073741824")
		t.Setenv(serverlessEnvMaxMemoryLimit, "2147483648")
		t.Setenv(serverlessEnvDisableMemoryShrink, "true")
		t.Setenv(serverlessEnvMemoryShrinkCooldown, "5m")

		scaler, ok := newServerlessMemoryScaler(make(chan struct{}))
		require.False(t, ok)
		require.Nil(t, scaler)
		require.Equal(t, uint64(1073741824), memory.ServerMemoryLimit.Load())
		require.Equal(t, "1073741824", memory.ServerMemoryLimitOriginText.Load())
	})

	t.Run("starter mode starts scaler loop when pod env is present", func(t *testing.T) {
		if !kerneltype.IsNextGen() {
			t.Skip("Starter deploy mode requires nextgen kernel")
		}
		require.NoError(t, deploymode.Set(deploymode.Starter))
		t.Setenv(serverlessEnvNamespace, "ns")
		t.Setenv(serverlessEnvPodName, "pod")
		t.Setenv(serverlessEnvNodeIP, "127.0.0.1")
		t.Setenv(serverlessEnvMinMemoryLimit, "1073741824")
		t.Setenv(serverlessEnvMaxMemoryLimit, "2147483648")
		t.Setenv(serverlessEnvDisableMemoryShrink, "false")
		t.Setenv(serverlessEnvMemoryShrinkCooldown, "30s")

		scaler, ok := newServerlessMemoryScaler(make(chan struct{}))
		require.True(t, ok)
		require.NotNil(t, scaler)
		defer scaler.stop()
		require.Equal(t, "ns", scaler.namespace)
		require.Equal(t, "pod", scaler.podName)
		require.Equal(t, "127.0.0.1", scaler.nodeIP)
		require.Equal(t, uint64(1073741824), scaler.minMemoryLimit)
		require.Equal(t, uint64(2147483648), scaler.maxMemoryLimit)
		require.False(t, scaler.disableMemoryShrink)
		require.Equal(t, 30*time.Second, scaler.memoryShrinkCooldown)
	})

	t.Run("non-positive shrink cooldown disables shrink", func(t *testing.T) {
		t.Setenv(serverlessEnvDisableMemoryShrink, "false")
		t.Setenv(serverlessEnvMemoryShrinkCooldown, "0s")

		disableMemoryShrink, memoryShrinkCooldown := loadServerlessMemoryShrinkConfig()
		require.True(t, disableMemoryShrink)
		require.Equal(t, defaultServerlessMemoryShrinkCooldown, memoryShrinkCooldown)
	})
}
