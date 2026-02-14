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

package mvs

import (
	"context"
	"fmt"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MVServiceConfig is the config for constructing MVService.
type MVServiceConfig struct {
	TaskMaxConcurrency int
	TaskTimeout        time.Duration

	FetchInterval         time.Duration
	BasicInterval         time.Duration
	ServerRefreshInterval time.Duration

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	ServerConsistentHashReplicas int

	TaskBackpressure TaskBackpressureConfig
}

// DefaultMVServiceConfig returns the default MV service config.
func DefaultMVServiceConfig() MVServiceConfig {
	return MVServiceConfig{
		TaskMaxConcurrency:           defaultMVTaskMaxConcurrency,
		TaskTimeout:                  defaultMVTaskTimeout,
		FetchInterval:                defaultMVFetchInterval,
		BasicInterval:                defaultMVBasicInterval,
		ServerRefreshInterval:        defaultTiDBServerRefreshInterval,
		RetryBaseDelay:               defaultMVTaskRetryBase,
		RetryMaxDelay:                defaultMVTaskRetryMax,
		ServerConsistentHashReplicas: defaultServerConsistentHashReplicas,
	}
}

// normalizeMVServiceConfig clamps invalid values to safe defaults.
func normalizeMVServiceConfig(cfg MVServiceConfig) MVServiceConfig {
	def := DefaultMVServiceConfig()
	if cfg.TaskMaxConcurrency <= 0 {
		cfg.TaskMaxConcurrency = def.TaskMaxConcurrency
	}
	if cfg.TaskTimeout < 0 {
		cfg.TaskTimeout = 0
	}
	if cfg.FetchInterval <= 0 {
		cfg.FetchInterval = def.FetchInterval
	}
	if cfg.BasicInterval <= 0 {
		cfg.BasicInterval = def.BasicInterval
	}
	if cfg.ServerRefreshInterval <= 0 {
		cfg.ServerRefreshInterval = def.ServerRefreshInterval
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = def.RetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = def.RetryMaxDelay
	}
	if cfg.ServerConsistentHashReplicas <= 0 {
		cfg.ServerConsistentHashReplicas = def.ServerConsistentHashReplicas
	}
	return cfg
}

// NewMVService creates a new MVService with the given helper and config.
func NewMVService(ctx context.Context, se basic.SessionPool, helper MVServiceHelper, cfg MVServiceConfig) *MVService {
	if helper == nil || se == nil {
		panic("invalid arguments")
	}
	cfg = normalizeMVServiceConfig(cfg)
	mgr := &MVService{
		sysSessionPool: se,
		sch:            NewServerConsistentHash(ctx, cfg.ServerConsistentHashReplicas, helper),
		executor:       NewTaskExecutor(cfg.TaskMaxConcurrency, cfg.TaskTimeout),

		notifier: NewNotifier(),
		ctx:      ctx,
		mh:       helper,

		fetchInterval:         cfg.FetchInterval,
		basicInterval:         cfg.BasicInterval,
		serverRefreshInterval: cfg.ServerRefreshInterval,
	}
	def := DefaultMVServiceConfig()
	if err := mgr.SetRetryDelayConfig(cfg.RetryBaseDelay, cfg.RetryMaxDelay); err != nil {
		logutil.BgLogger().Warn("invalid MV service retry config, fallback to defaults",
			zap.Error(err),
			zap.Duration("retry_base", cfg.RetryBaseDelay),
			zap.Duration("retry_max", cfg.RetryMaxDelay))
		_ = mgr.SetRetryDelayConfig(def.RetryBaseDelay, def.RetryMaxDelay)
	}
	if err := mgr.SetTaskBackpressureConfig(cfg.TaskBackpressure); err != nil {
		logutil.BgLogger().Warn("invalid MV service backpressure config, disable backpressure",
			zap.Error(err),
			zap.Float64("cpu_threshold", cfg.TaskBackpressure.CPUThreshold),
			zap.Float64("mem_threshold", cfg.TaskBackpressure.MemThreshold),
			zap.Duration("delay", cfg.TaskBackpressure.Delay))
		_ = mgr.SetTaskBackpressureConfig(TaskBackpressureConfig{})
	}
	return mgr
}

// SetTaskExecConfig sets the execution config for MV tasks.
func (t *MVService) SetTaskExecConfig(maxConcurrency int, timeout time.Duration) {
	t.executor.UpdateConfig(maxConcurrency, timeout)
}

// GetTaskExecConfig returns the current execution config for MV tasks.
func (t *MVService) GetTaskExecConfig() (maxConcurrency int, timeout time.Duration) {
	return t.executor.GetConfig()
}

// SetRetryDelayConfig sets retry delay config.
func (t *MVService) SetRetryDelayConfig(baseDelay, maxDelay time.Duration) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if baseDelay <= 0 || maxDelay <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if baseDelay > maxDelay {
		return fmt.Errorf("retry base delay must be less than or equal to max delay")
	}
	t.retryBaseDelayNanos.Store(int64(baseDelay))
	t.retryMaxDelayNanos.Store(int64(maxDelay))
	return nil
}

// GetRetryDelayConfig returns retry delay config.
func (t *MVService) GetRetryDelayConfig() (baseDelay, maxDelay time.Duration) {
	if t == nil {
		return defaultMVTaskRetryBase, defaultMVTaskRetryMax
	}
	baseDelay = time.Duration(t.retryBaseDelayNanos.Load())
	maxDelay = time.Duration(t.retryMaxDelayNanos.Load())
	if baseDelay <= 0 {
		baseDelay = defaultMVTaskRetryBase
	}
	if maxDelay <= 0 {
		maxDelay = defaultMVTaskRetryMax
	}
	if baseDelay > maxDelay {
		maxDelay = baseDelay
	}
	return baseDelay, maxDelay
}

// SetTaskBackpressureConfig sets task backpressure config.
func (t *MVService) SetTaskBackpressureConfig(cfg TaskBackpressureConfig) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if cfg.CPUThreshold < 0 || cfg.CPUThreshold > 1 {
		return fmt.Errorf("cpu threshold out of range")
	}
	if cfg.MemThreshold < 0 || cfg.MemThreshold > 1 {
		return fmt.Errorf("memory threshold out of range")
	}
	if cfg.Delay < 0 {
		return fmt.Errorf("backpressure delay must be non-negative")
	}
	if cfg.CPUThreshold > 0 || cfg.MemThreshold > 0 {
		t.SetTaskBackpressureController(NewCPUMemBackpressureController(cfg.CPUThreshold, cfg.MemThreshold, cfg.Delay))
	} else {
		t.SetTaskBackpressureController(nil)
	}
	t.backpressureMu.Lock()
	t.backpressureCfg = cfg
	t.backpressureMu.Unlock()
	return nil
}

// GetTaskBackpressureConfig returns task backpressure config.
func (t *MVService) GetTaskBackpressureConfig() TaskBackpressureConfig {
	if t == nil {
		return TaskBackpressureConfig{}
	}
	t.backpressureMu.RLock()
	cfg := t.backpressureCfg
	t.backpressureMu.RUnlock()
	return cfg
}

// SetTaskBackpressureController sets the task backpressure controller.
func (t *MVService) SetTaskBackpressureController(controller TaskBackpressureController) {
	t.executor.SetBackpressureController(controller)
}

// calcRetryDelay computes exponential backoff with an upper bound.
func calcRetryDelay(retryCount int64, baseDelay, maxDelay time.Duration) time.Duration {
	if retryCount <= 0 {
		return baseDelay
	}
	delay := baseDelay
	for i := int64(1); i < retryCount && delay < maxDelay; i++ {
		delay *= 2
		if delay >= maxDelay {
			delay = maxDelay
			break
		}
	}
	return delay
}

// retryDelay computes the current retry delay using runtime config.
func (t *MVService) retryDelay(retryCount int64) time.Duration {
	baseDelay, maxDelay := t.GetRetryDelayConfig()
	return calcRetryDelay(retryCount, baseDelay, maxDelay)
}
