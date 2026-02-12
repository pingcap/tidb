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
		executor:       NewTaskExecutor(ctx, cfg.TaskMaxConcurrency, cfg.TaskTimeout),

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
			zap.Bool("enabled", cfg.TaskBackpressure.Enabled),
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
func (t *MVService) SetRetryDelayConfig(base, max time.Duration) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if base <= 0 || max <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if base > max {
		return fmt.Errorf("retry base delay must be less than or equal to max delay")
	}
	t.retryBaseDelayNanos.Store(int64(base))
	t.retryMaxDelayNanos.Store(int64(max))
	return nil
}

// GetRetryDelayConfig returns retry delay config.
func (t *MVService) GetRetryDelayConfig() (base, max time.Duration) {
	if t == nil {
		return defaultMVTaskRetryBase, defaultMVTaskRetryMax
	}
	base = time.Duration(t.retryBaseDelayNanos.Load())
	max = time.Duration(t.retryMaxDelayNanos.Load())
	if base <= 0 {
		base = defaultMVTaskRetryBase
	}
	if max <= 0 {
		max = defaultMVTaskRetryMax
	}
	if base > max {
		max = base
	}
	return base, max
}

// SetTaskBackpressureConfig sets task backpressure config.
func (t *MVService) SetTaskBackpressureConfig(cfg TaskBackpressureConfig) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if cfg.Enabled {
		if cfg.CPUThreshold < 0 || cfg.CPUThreshold > 1 {
			return fmt.Errorf("cpu threshold out of range")
		}
		if cfg.MemThreshold < 0 || cfg.MemThreshold > 1 {
			return fmt.Errorf("memory threshold out of range")
		}
		if cfg.CPUThreshold <= 0 && cfg.MemThreshold <= 0 {
			return fmt.Errorf("at least one threshold must be positive when backpressure is enabled")
		}
		if cfg.Delay < 0 {
			return fmt.Errorf("backpressure delay must be non-negative")
		}
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

func calcRetryDelay(retryCount int64, base, max time.Duration) time.Duration {
	if retryCount <= 0 {
		return base
	}
	delay := base
	for i := int64(1); i < retryCount && delay < max; i++ {
		delay *= 2
		if delay >= max {
			delay = max
			break
		}
	}
	return delay
}

func (t *MVService) retryDelay(retryCount int64) time.Duration {
	base, max := t.GetRetryDelayConfig()
	return calcRetryDelay(retryCount, base, max)
}
