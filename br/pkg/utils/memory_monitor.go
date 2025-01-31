// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	MemoryWarningThreshold = 0.8
	MemoryDumpThreshold    = 0.9

	MonitorInterval     = 1 * time.Second
	DefaultDumpInterval = 5 * time.Minute

	DumpDirPerm = 0755

	HeapDumpFilePattern = "br_heap_%s_%d.pprof"
	TimeFormat          = "20060102_150405"

	DefaultHeapDumpDir = "/tmp/heapdump"

	MaxHeapDumps = 3
)

// MemoryMonitorConfig holds configuration for memory monitoring
type MemoryMonitorConfig struct {
	// DumpDir is where heap dumps will be written
	DumpDir string
	// MemoryLimit is the memory limit in bytes (if known)
	MemoryLimit uint64
	// MinDumpInterval is the minimum time between heap dumps
	MinDumpInterval time.Duration
}

// cleanupOldHeapDumps removes old heap dumps if there are more than MaxHeapDumps
func cleanupOldHeapDumps(dumpDir string) {
	files, err := filepath.Glob(filepath.Join(dumpDir, "br_heap_*.pprof"))
	if err != nil {
		log.Warn("Failed to list heap dumps for cleanup", zap.Error(err))
		return
	}

	if len(files) <= MaxHeapDumps {
		return
	}

	// sort by filename which contains timestamp
	sort.Strings(files)

	// remove older files (keeping the last MaxHeapDumps files)
	for i := 0; i < len(files)-MaxHeapDumps; i++ {
		if err := os.Remove(files[i]); err != nil {
			log.Warn("Failed to remove old heap dump",
				zap.String("file", files[i]),
				zap.Error(err))
			continue
		}
		log.Info("Removed old heap dump", zap.String("file", files[i]))
	}
}

// StartMemoryMonitor starts monitoring memory usage and dumps heap when thresholds are exceeded
func StartMemoryMonitor(ctx context.Context, cfg MemoryMonitorConfig) error {
	if cfg.DumpDir == "" {
		cfg.DumpDir = DefaultHeapDumpDir
		if err := os.MkdirAll(cfg.DumpDir, DumpDirPerm); err != nil {
			log.Warn("Failed to create default heap dump directory, falling back to temp dir",
				zap.String("default_dir", cfg.DumpDir),
				zap.Error(err))
			cfg.DumpDir = os.TempDir()
		}
	}
	if cfg.MinDumpInterval == 0 {
		cfg.MinDumpInterval = DefaultDumpInterval
	}

	// Ensure dump directory exists
	if err := os.MkdirAll(cfg.DumpDir, DumpDirPerm); err != nil {
		return fmt.Errorf("failed to create dump directory: %v", err)
	}

	// Clean up any existing old heap dumps on startup
	cleanupOldHeapDumps(cfg.DumpDir)

	log.Info("Memory monitor will store heap dumps in",
		zap.String("dump_dir", cfg.DumpDir),
		zap.Bool("using_temp_dir", cfg.DumpDir == os.TempDir()),
		zap.Int("max_dumps", MaxHeapDumps))

	var lastDump time.Time
	var memStats runtime.MemStats

	go func() {
		ticker := time.NewTicker(MonitorInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runtime.ReadMemStats(&memStats)

				var memoryUsage float64
				if cfg.MemoryLimit > 0 {
					memoryUsage = float64(memStats.Alloc) / float64(cfg.MemoryLimit)
				} else {
					// if no limit specified, use system memory as reference
					memoryUsage = float64(memStats.Alloc) / float64(memStats.Sys)
				}

				if memoryUsage >= MemoryWarningThreshold {
					log.Warn("High memory usage detected",
						zap.Float64("usage_percentage", memoryUsage*100),
						zap.Uint64("alloc_bytes", memStats.Alloc),
						zap.Uint64("sys_bytes", memStats.Sys))
				}

				// dump heap at dump threshold if enough time has passed since last dump
				if memoryUsage >= MemoryDumpThreshold && time.Since(lastDump) >= cfg.MinDumpInterval {
					dumpPath := filepath.Join(cfg.DumpDir,
						fmt.Sprintf(HeapDumpFilePattern,
							time.Now().Format(TimeFormat),
							memStats.Alloc/units.MiB))

					if err := dumpHeap(dumpPath); err != nil {
						log.Error("Failed to dump heap",
							zap.Error(err),
							zap.String("path", dumpPath))
						continue
					}

					log.Info("Heap dump created",
						zap.String("path", dumpPath),
						zap.Float64("usage_percentage", memoryUsage*100),
						zap.Uint64("alloc_mb", memStats.Alloc/units.MiB))

					lastDump = time.Now()

					// clean up old dumps after creating a new one
					cleanupOldHeapDumps(cfg.DumpDir)
				}
			}
		}
	}()

	return nil
}

// dumpHeap creates a heap profile at the specified path
func dumpHeap(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create heap dump file: %v", err)
	}
	defer f.Close()

	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("failed to write heap profile: %v", err)
	}

	return nil
}
