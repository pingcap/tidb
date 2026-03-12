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

package mvservice

import (
	"context"
	"fmt"
	"runtime"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
)

// Config is the config for constructing MVService.
type Config struct {
	TaskMaxConcurrency int
	TaskTimeout        time.Duration

	FetchInterval         time.Duration
	BasicInterval         time.Duration
	ServerRefreshInterval time.Duration

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	MViewRefreshHistRetention time.Duration
	MLogPurgeHistRetention    time.Duration

	ServerConsistentHashReplicas int

	TaskBackpressure TaskBackpressureConfig
}

// DefaultMVServiceConfig returns the default MV service config.
func DefaultMVServiceConfig() Config {
	return Config{
		TaskMaxConcurrency:           defaultMVTaskMaxConcurrency(),
		TaskTimeout:                  defaultMVTaskTimeout,
		FetchInterval:                defaultMVFetchInterval,
		BasicInterval:                defaultMVBasicInterval,
		ServerRefreshInterval:        defaultServerRefreshInterval,
		RetryBaseDelay:               defaultMVTaskRetryBase,
		RetryMaxDelay:                defaultMVTaskRetryMax,
		MViewRefreshHistRetention:    defaultMVHistoryGCRetention,
		MLogPurgeHistRetention:       defaultMVHistoryGCRetention,
		ServerConsistentHashReplicas: defaultCHReplicas,
	}
}

func defaultMVTaskMaxConcurrency() int {
	if maxProcs := runtime.GOMAXPROCS(0); maxProcs > 0 {
		return maxProcs
	}
	return 1
}

// normalizeMVServiceConfig clamps invalid values to safe defaults.
func normalizeMVServiceConfig(cfg Config) Config {
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
	if cfg.MViewRefreshHistRetention <= 0 {
		cfg.MViewRefreshHistRetention = def.MViewRefreshHistRetention
	}
	if cfg.MLogPurgeHistRetention <= 0 {
		cfg.MLogPurgeHistRetention = def.MLogPurgeHistRetention
	}
	if cfg.ServerConsistentHashReplicas <= 0 {
		cfg.ServerConsistentHashReplicas = def.ServerConsistentHashReplicas
	}
	return cfg
}

// NewMVService creates a new MVService with the given helper and config.
func NewMVService(ctx context.Context, se basic.SessionPool, helper Helper, cfg Config) *MVService {
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

		basicInterval:         cfg.BasicInterval,
		serverRefreshInterval: cfg.ServerRefreshInterval,
	}
	if err := mgr.setFetchInterval(cfg.FetchInterval); err != nil {
		panic(fmt.Sprintf("invalid MV service fetch interval config: fetch_interval=%s err=%v",
			cfg.FetchInterval, err))
	}
	if err := mgr.SetMViewRefreshHistRetention(cfg.MViewRefreshHistRetention); err != nil {
		panic(fmt.Sprintf("invalid MV service mview refresh history retention config: retention=%s err=%v",
			cfg.MViewRefreshHistRetention, err))
	}
	if err := mgr.SetMLogPurgeHistRetention(cfg.MLogPurgeHistRetention); err != nil {
		panic(fmt.Sprintf("invalid MV service mlog purge history retention config: retention=%s err=%v",
			cfg.MLogPurgeHistRetention, err))
	}
	if err := mgr.setRetryDelayConfig(cfg.RetryBaseDelay, cfg.RetryMaxDelay); err != nil {
		panic(fmt.Sprintf("invalid MV service retry config: base=%s max=%s err=%v",
			cfg.RetryBaseDelay, cfg.RetryMaxDelay, err))
	}
	if err := mgr.SetTaskBackpressureConfig(cfg.TaskBackpressure); err != nil {
		panic(fmt.Sprintf("invalid MV service backpressure config: cpu_threshold=%v mem_threshold=%v delay=%s err=%v",
			cfg.TaskBackpressure.CPUThreshold, cfg.TaskBackpressure.MemThreshold, cfg.TaskBackpressure.Delay, err))
	}
	mgr.nextHistoryGCAtMillis.Store(mvsNow().Add(mgr.historyGCInterval()).UnixMilli())
	return mgr
}

// SetTaskMaxConcurrency sets max concurrency for MV tasks.
func (t *MVService) SetTaskMaxConcurrency(maxConcurrency int) {
	if maxConcurrency == 0 {
		maxConcurrency = defaultMVTaskMaxConcurrency()
	}
	t.executor.setMaxConcurrency(maxConcurrency)
}

// SetTaskTimeout sets timeout for MV tasks.
func (t *MVService) SetTaskTimeout(timeout time.Duration) {
	t.executor.setTimeout(timeout)
}

// setFetchInterval sets metadata fetch interval.
func (t *MVService) setFetchInterval(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("fetch interval must be positive")
	}
	if interval < time.Millisecond {
		return fmt.Errorf("fetch interval must be at least 1ms")
	}
	t.fetchIntervalMillis.Store(interval.Milliseconds())
	return nil
}

// fetchInterval returns metadata fetch interval.
func (t *MVService) fetchInterval() time.Duration {
	interval := time.Duration(t.fetchIntervalMillis.Load()) * time.Millisecond
	if interval <= 0 {
		return defaultMVFetchInterval
	}
	return interval
}

// setRetryDelayConfig sets retry delay config.
func (t *MVService) setRetryDelayConfig(baseDelay, maxDelay time.Duration) error {
	if baseDelay <= 0 || maxDelay <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if baseDelay < time.Millisecond || maxDelay < time.Millisecond {
		return fmt.Errorf("retry delay must be at least 1ms")
	}
	if baseDelay > maxDelay {
		return fmt.Errorf("retry base delay must be less than or equal to max delay")
	}
	t.retryBaseDelayMillis.Store(baseDelay.Milliseconds())
	t.retryMaxDelayMillis.Store(maxDelay.Milliseconds())
	return nil
}

// retryDelayConfig returns retry delay config.
func (t *MVService) retryDelayConfig() (baseDelay, maxDelay time.Duration) {
	baseDelay = time.Duration(t.retryBaseDelayMillis.Load()) * time.Millisecond
	maxDelay = time.Duration(t.retryMaxDelayMillis.Load()) * time.Millisecond
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

// historyGCInterval returns history GC interval.
func (t *MVService) historyGCInterval() time.Duration {
	mviewRefreshRetention, mlogPurgeRetention := t.historyGCRetentionConfig()
	return deriveHistoryGCInterval(mviewRefreshRetention, mlogPurgeRetention)
}

// SetMViewRefreshHistRetention sets retention for mysql.tidb_mview_refresh_hist.
func (t *MVService) SetMViewRefreshHistRetention(mviewRefreshRetention time.Duration) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if mviewRefreshRetention <= 0 {
		return fmt.Errorf("history gc retention must be positive")
	}
	if mviewRefreshRetention < time.Millisecond {
		return fmt.Errorf("history gc retention must be at least 1ms")
	}
	t.mviewRefreshHistRetentionMillis.Store(mviewRefreshRetention.Milliseconds())
	t.rescheduleHistoryGCEarlier(mvsNow(), t.historyGCInterval())
	return nil
}

// SetMLogPurgeHistRetention sets retention for mysql.tidb_mlog_purge_hist.
func (t *MVService) SetMLogPurgeHistRetention(mlogPurgeRetention time.Duration) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if mlogPurgeRetention <= 0 {
		return fmt.Errorf("history gc retention must be positive")
	}
	if mlogPurgeRetention < time.Millisecond {
		return fmt.Errorf("history gc retention must be at least 1ms")
	}
	t.mlogPurgeHistRetentionMillis.Store(mlogPurgeRetention.Milliseconds())
	t.rescheduleHistoryGCEarlier(mvsNow(), t.historyGCInterval())
	return nil
}

// historyGCRetentionConfig returns separate history retention for mview refresh and mlog purge.
func (t *MVService) historyGCRetentionConfig() (mviewRefreshRetention, mlogPurgeRetention time.Duration) {
	mviewRefreshRetention = time.Duration(t.mviewRefreshHistRetentionMillis.Load()) * time.Millisecond
	mlogPurgeRetention = time.Duration(t.mlogPurgeHistRetentionMillis.Load()) * time.Millisecond
	if mviewRefreshRetention <= 0 {
		mviewRefreshRetention = defaultMVHistoryGCRetention
	}
	if mlogPurgeRetention <= 0 {
		mlogPurgeRetention = defaultMVHistoryGCRetention
	}
	return mviewRefreshRetention, mlogPurgeRetention
}

func deriveHistoryGCInterval(mviewRefreshRetention, mlogPurgeRetention time.Duration) time.Duration {
	intervalBase := min(mviewRefreshRetention, mlogPurgeRetention)
	interval := intervalBase / 100
	if interval < time.Second {
		return time.Second
	}
	if interval > time.Hour {
		return time.Hour
	}
	return interval
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
	baseDelay, maxDelay := t.retryDelayConfig()
	return calcRetryDelay(retryCount, baseDelay, maxDelay)
}
