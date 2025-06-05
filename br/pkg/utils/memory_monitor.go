// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	DefaultProfilesDir = "/tmp/profiles"
	// default memory usage alarm ratio (80%)
	defaultMemoryUsageAlarmRatio = 0.8
	// default number of alarm records to keep
	defaultMemoryUsageAlarmKeepRecordNum = 3
)

// BRConfigProvider implements memoryusagealarm.ConfigProvider for BR
type BRConfigProvider struct {
	ratio   *atomic.Float64
	keepNum *atomic.Int64
	logDir  string
}

func (p *BRConfigProvider) GetMemoryUsageAlarmRatio() float64 {
	return p.ratio.Load()
}

func (p *BRConfigProvider) GetMemoryUsageAlarmKeepRecordNum() int64 {
	return p.keepNum.Load()
}

func (p *BRConfigProvider) GetLogDir() string {
	if p.logDir == "" {
		return DefaultProfilesDir
	}
	return p.logDir
}

func (p *BRConfigProvider) GetComponentName() string {
	return "br"
}

// RunMemoryMonitor starts monitoring memory usage and dumps profiles when thresholds are exceeded
func RunMemoryMonitor(ctx context.Context, dumpDir string, memoryLimit uint64) error {
	// just in case
	if dumpDir == "" {
		dumpDir = DefaultProfilesDir
	}

	// Set memory limit if specified
	if memoryLimit > 0 {
		memory.ServerMemoryLimit.Store(memoryLimit)
	}

	log.Info("Memory monitor starting",
		zap.String("dump_dir", dumpDir),
		zap.Bool("using_temp_dir", dumpDir == os.TempDir()),
		zap.Float64("memory_usage_alarm_ratio", defaultMemoryUsageAlarmRatio),
		zap.Uint64("memory_limit_mb", memoryLimit/1024/1024))

	// Initialize BR config provider with default values
	provider := &BRConfigProvider{
		ratio:   atomic.NewFloat64(defaultMemoryUsageAlarmRatio),
		keepNum: atomic.NewInt64(defaultMemoryUsageAlarmKeepRecordNum),
		logDir:  dumpDir,
	}

	exitCh := make(chan struct{})
	handle := memoryusagealarm.NewMemoryUsageAlarmHandle(exitCh, provider)
	// BR doesn't need session manager so setting to nil
	handle.SetSessionManager(nil)

	go func() {
		go handle.Run()
		<-ctx.Done()
		close(exitCh)
	}()

	return nil
}
