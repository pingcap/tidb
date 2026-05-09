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

package servermemorylimit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const (
	serverlessEnvNamespace            = "NAMESPACE"
	serverlessEnvPodName              = "POD_NAME"
	serverlessEnvNodeIP               = "NODE_IP"
	serverlessEnvMinMemoryLimit       = "MIN_MEMORY_LIMIT"
	serverlessEnvMaxMemoryLimit       = "MAX_MEMORY_LIMIT"
	serverlessEnvDisableMemoryShrink  = "DISABLE_MEMORY_SHRINK"
	serverlessEnvMemoryShrinkCooldown = "MEMORY_SHRINK_COOLDOWN"

	defaultServerlessMemoryShrinkCooldown = time.Minute
	maxServerlessMemoryScale              = 2 * 1024 * 1024 * 1024 // 2GB
)

// serverlessMemoryScaler is used to scale memory limit of a pod in serverless mode.
type serverlessMemoryScaler struct {
	exitCh chan struct{}
	stopCh chan struct{}

	namespace            string
	podName              string
	nodeIP               string
	minMemoryLimit       uint64
	maxMemoryLimit       uint64
	disableMemoryShrink  bool
	memoryShrinkCooldown time.Duration

	stopOnce sync.Once
}

// serverlessMemoryPod stores memory usage information of a pod.
type serverlessMemoryPod struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Limit     int64  `json:"limit,omitempty"`
	MinLimit  int64  `json:"min_limit,omitempty"`
	MaxLimit  int64  `json:"max_limit,omitempty"`
	Used      int64  `json:"used,omitempty"`
}

// serverlessMemoryScaleRequest is the request to scale a pod's memory limit.
type serverlessMemoryScaleRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Limit     int64  `json:"limit,omitempty"`
}

func (smqh *Handle) updateServerlessMemoryScaler() {
	smqh.serverlessMu.Lock()
	defer smqh.serverlessMu.Unlock()

	if !deploymode.IsStarter() {
		smqh.serverlessStartAttempted = false
		smqh.stopServerlessMemoryScalerLocked()
		return
	}
	if smqh.serverlessScaler != nil || smqh.serverlessStartAttempted {
		return
	}

	smqh.serverlessStartAttempted = true
	scaler, ok := newServerlessMemoryScaler(smqh.exitCh)
	if !ok {
		return
	}
	smqh.serverlessScaler = scaler
	go scaler.run()
}

func (smqh *Handle) stopServerlessMemoryScaler() {
	smqh.serverlessMu.Lock()
	defer smqh.serverlessMu.Unlock()
	smqh.stopServerlessMemoryScalerLocked()
}

func (smqh *Handle) stopServerlessMemoryScalerLocked() {
	if smqh.serverlessScaler == nil {
		return
	}
	smqh.serverlessScaler.stop()
	smqh.serverlessScaler = nil
}

func newServerlessMemoryScaler(exitCh chan struct{}) (*serverlessMemoryScaler, bool) {
	if !deploymode.IsStarter() {
		return nil, false
	}

	namespace := os.Getenv(serverlessEnvNamespace)
	podName := os.Getenv(serverlessEnvPodName)
	nodeIP := os.Getenv(serverlessEnvNodeIP)
	minMemoryLimitStr := os.Getenv(serverlessEnvMinMemoryLimit)
	maxMemoryLimitStr := os.Getenv(serverlessEnvMaxMemoryLimit)

	if minMemoryLimitStr == "" || maxMemoryLimitStr == "" {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT or MAX_MEMORY_LIMIT not set, skip init memory limits")
		return nil, false
	}

	minMemoryLimit, err := strconv.ParseUint(minMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MIN_MEMORY_LIMIT is bad format, skip init memory limits",
			zap.String("value", minMemoryLimitStr),
			zap.Error(err))
		return nil, false
	}

	maxMemoryLimit, err := strconv.ParseUint(maxMemoryLimitStr, 10, 64)
	if err != nil {
		logutil.BgLogger().Info("env MAX_MEMORY_LIMIT is bad format, skip init memory limits",
			zap.String("value", maxMemoryLimitStr),
			zap.Error(err))
		return nil, false
	}

	disableMemoryShrink, memoryShrinkCooldown := loadServerlessMemoryShrinkConfig()
	memory.ServerMemoryLimitOriginText.Store(minMemoryLimitStr)
	memory.ServerMemoryLimit.Store(minMemoryLimit)
	gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()

	if namespace == "" || podName == "" || nodeIP == "" {
		logutil.BgLogger().Info("env NAMESPACE, POD_NAME, NODE_IP not set, skip start memory scaler loop",
			zap.Bool("disable_memory_shrink", disableMemoryShrink),
			zap.Duration("memory_shrink_cooldown", memoryShrinkCooldown),
		)
		return nil, false
	}

	scaler := &serverlessMemoryScaler{
		exitCh:               exitCh,
		stopCh:               make(chan struct{}),
		namespace:            namespace,
		podName:              podName,
		nodeIP:               nodeIP,
		minMemoryLimit:       minMemoryLimit,
		maxMemoryLimit:       maxMemoryLimit,
		disableMemoryShrink:  disableMemoryShrink,
		memoryShrinkCooldown: memoryShrinkCooldown,
	}
	logutil.BgLogger().Info("start serverless memory scaler success",
		zap.Bool("disable_memory_shrink", disableMemoryShrink),
		zap.Duration("memory_shrink_cooldown", memoryShrinkCooldown),
	)
	return scaler, true
}

func loadServerlessMemoryShrinkConfig() (bool, time.Duration) {
	disableMemoryShrink := false
	memoryShrinkCooldown := defaultServerlessMemoryShrinkCooldown

	disableMemoryShrinkStr := os.Getenv(serverlessEnvDisableMemoryShrink)
	if disableMemoryShrinkStr != "" {
		if v, err := strconv.ParseBool(disableMemoryShrinkStr); err != nil {
			logutil.BgLogger().Warn("env DISABLE_MEMORY_SHRINK is bad format, ignore it",
				zap.String("value", disableMemoryShrinkStr),
				zap.Error(err),
			)
		} else {
			disableMemoryShrink = v
		}
	}

	memoryShrinkCooldownStr := os.Getenv(serverlessEnvMemoryShrinkCooldown)
	if memoryShrinkCooldownStr != "" {
		if d, err := time.ParseDuration(memoryShrinkCooldownStr); err != nil {
			logutil.BgLogger().Warn("env MEMORY_SHRINK_COOLDOWN is bad format, ignore it",
				zap.String("value", memoryShrinkCooldownStr),
				zap.Error(err),
			)
		} else if d <= 0 {
			disableMemoryShrink = true
		} else {
			memoryShrinkCooldown = d
		}
	}

	return disableMemoryShrink, memoryShrinkCooldown
}

func (ms *serverlessMemoryScaler) stop() {
	ms.stopOnce.Do(func() {
		close(ms.stopCh)
	})
}

func (ms *serverlessMemoryScaler) report(stat *runtime.MemStats, limit uint64) {
	pod := &serverlessMemoryPod{
		Name:      ms.podName,
		Namespace: ms.namespace,
		Limit:     int64(limit),
		MinLimit:  int64(ms.minMemoryLimit),
		MaxLimit:  int64(ms.maxMemoryLimit),
		Used:      int64(stat.HeapInuse),
	}
	data, _ := json.Marshal(pod)
	client := &http.Client{Timeout: time.Second}
	res, err := client.Post("http://"+ms.nodeIP+":4040/report", "application/json", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Warn("report memory usage failed", zap.Error(err))
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Warn("report memory usage failed", zap.Int("status", res.StatusCode))
	}
}

func (ms *serverlessMemoryScaler) scaleMemory(to uint64) bool {
	logutil.BgLogger().Info("begin to scale memory", zap.Uint64("new_limit", to))
	req := &serverlessMemoryScaleRequest{
		Namespace: ms.namespace,
		Name:      ms.podName,
		Limit:     int64(to),
	}
	data, _ := json.Marshal(req)
	client := &http.Client{Timeout: time.Second}
	res, err := client.Post("http://"+ms.nodeIP+":4040/scale", "application/json", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Warn("scale memory failed", zap.Error(err))
		return false
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Warn("scale memory failed", zap.Int("status", res.StatusCode))
		return false
	}
	logutil.BgLogger().Info("scale memory success", zap.Uint64("limit", to))
	memory.ServerMemoryLimit.Store(to)
	gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
	return true
}

func (ms *serverlessMemoryScaler) run() {
	if ms.minMemoryLimit == 0 || ms.maxMemoryLimit == 0 {
		return
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	lastReport, lastIncrease, lastIncreaseFailed := time.Now(), time.Now(), time.Now()
	for {
		select {
		case <-ticker.C:
			stats := memory.ForceReadMemStats()
			limit := memory.ServerMemoryLimit.Load()

			if time.Since(lastReport) > time.Second {
				ms.report(stats, limit)
				lastReport = time.Now()
			}
			if limit != ms.maxMemoryLimit && stats.HeapInuse > limit*8/10 && time.Since(lastIncreaseFailed) > time.Second {
				if limit >= maxServerlessMemoryScale {
					limit += maxServerlessMemoryScale
				} else {
					limit *= 2
				}
				if limit > ms.maxMemoryLimit {
					limit = ms.maxMemoryLimit
				}
				ok := ms.scaleMemory(limit)
				if ok {
					lastIncrease = time.Now()
				} else {
					lastIncreaseFailed = time.Now()
				}
			} else if !ms.disableMemoryShrink &&
				limit != ms.minMemoryLimit &&
				time.Since(lastIncrease) > ms.memoryShrinkCooldown &&
				stats.Sys-stats.HeapReleased < limit/2 {
				limit = limit * 2 / 3
				if limit < ms.minMemoryLimit {
					limit = ms.minMemoryLimit
				}
				ms.scaleMemory(limit)
			}
		case <-ms.stopCh:
			return
		case <-ms.exitCh:
			return
		}
	}
}
