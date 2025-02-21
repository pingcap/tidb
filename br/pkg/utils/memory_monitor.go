// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"os"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	MonitorInterval    = 1 * time.Second
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
	if dumpDir == "" {
		dumpDir = DefaultProfilesDir
	}

	log.Info("Memory monitor will store profiles in",
		zap.String("dump_dir", dumpDir),
		zap.Bool("using_temp_dir", dumpDir == os.TempDir()))

	// Set memory limit if specified
	if memoryLimit > 0 {
		memory.ServerMemoryLimit.Store(memoryLimit)
	}

	// Initialize BR config provider with default values
	provider := &BRConfigProvider{
		ratio:   atomic.NewFloat64(defaultMemoryUsageAlarmRatio),
		keepNum: atomic.NewInt64(defaultMemoryUsageAlarmKeepRecordNum),
		logDir:  dumpDir,
	}

	exitCh := make(chan struct{})
	handle := memoryusagealarm.NewMemoryUsageAlarmHandle(exitCh, provider)

	go func() {
		go handle.Run()
		<-ctx.Done()
		close(exitCh)
	}()

	return nil
}
