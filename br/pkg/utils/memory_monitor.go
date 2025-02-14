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

	TimeFormat         = "20060102_150405"
	DefaultProfilesDir = "/tmp/profiles"
	// MaxDumpBatches is the number of dump directories to keep, where each directory contains multiple profile types
	MaxDumpBatches     = 3
	ProfileFilePattern = "%s.pprof"
	DumpDirPattern     = "br_dump_%s_%d"
	ProfileGlobPattern = "br_*"

	// profile types
	ProfileHeap      = "heap"
	ProfileGoroutine = "goroutine"

	// debug levels
	GoroutineDebugLevel = 2 // includes stack traces, status, and creation location
)

type profileItem struct {
	Name  string
	Debug int
}

var defaultProfiles = []profileItem{
	{Name: ProfileHeap},
	{Name: ProfileGoroutine, Debug: GoroutineDebugLevel},
}

// MemoryMonitorConfig holds configuration for memory monitoring
type MemoryMonitorConfig struct {
	// DumpDir is where profile dumps will be written
	DumpDir string
	// MemoryLimit is the memory limit in bytes (if known)
	MemoryLimit uint64
	// MinDumpInterval is the minimum time between profile dumps
	MinDumpInterval time.Duration
}

// cleanupOldProfileDumps removes old profile dump directories if there are more than MaxDumpBatches
func cleanupOldProfileDumps(dumpDir string) {
	files, err := filepath.Glob(filepath.Join(dumpDir, ProfileGlobPattern))
	if err != nil {
		log.Warn("Failed to list profile dumps for cleanup", zap.Error(err))
		return
	}

	if len(files) <= MaxDumpBatches {
		return
	}

	// sort by filename which contains timestamp
	sort.Strings(files)

	// remove older directories (keeping the last MaxDumpBatches directories)
	for i := 0; i < len(files)-MaxDumpBatches; i++ {
		if err := os.RemoveAll(files[i]); err != nil {
			log.Warn("Failed to remove old profile dump directory",
				zap.String("dir", files[i]),
				zap.Error(err))
			continue
		}
		log.Info("Removed old profile dump directory", zap.String("dir", files[i]))
	}
}

// StartMemoryMonitor starts monitoring memory usage and dumps profiles when thresholds are exceeded
func StartMemoryMonitor(ctx context.Context, cfg MemoryMonitorConfig) error {
	if cfg.DumpDir == "" {
		cfg.DumpDir = DefaultProfilesDir
		if err := os.MkdirAll(cfg.DumpDir, DumpDirPerm); err != nil {
			log.Warn("Failed to create default profiles directory, falling back to temp dir",
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

	// Clean up any existing old profile dumps on startup
	cleanupOldProfileDumps(cfg.DumpDir)

	log.Info("Memory monitor will store profiles in",
		zap.String("dump_dir", cfg.DumpDir),
		zap.Bool("using_temp_dir", cfg.DumpDir == os.TempDir()),
		zap.Int("max_dump_batches", MaxDumpBatches))

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
						zap.Float64("usage_percentage", float64(int(memoryUsage*10000))/100),
						zap.Uint64("alloc_bytes", memStats.Alloc),
						zap.Uint64("sys_bytes", memStats.Sys))
				}

				// dump profiles at dump threshold if enough time has passed since last dump
				if memoryUsage >= MemoryDumpThreshold && time.Since(lastDump) >= cfg.MinDumpInterval {
					dumpTime := time.Now().Format(TimeFormat)
					dumpDir := filepath.Join(cfg.DumpDir, fmt.Sprintf(DumpDirPattern, dumpTime, memStats.Alloc/units.MiB))

					if err := os.MkdirAll(dumpDir, DumpDirPerm); err != nil {
						log.Error("Failed to create dump directory",
							zap.Error(err),
							zap.String("path", dumpDir))
						continue
					}

					if err := dumpProfiles(dumpDir); err != nil {
						log.Error("Failed to dump profiles",
							zap.Error(err),
							zap.String("path", dumpDir))
						continue
					}

					log.Info("Memory profiles created",
						zap.String("path", dumpDir),
						zap.Float64("usage_percentage", float64(int(memoryUsage*10000))/100),
						zap.Uint64("alloc_mb", memStats.Alloc/units.MiB))

					lastDump = time.Now()

					// clean up old dumps after creating a new one
					cleanupOldProfileDumps(cfg.DumpDir)
				}
			}
		}
	}()

	return nil
}

// dumpProfiles creates all configured profile types at the specified directory
func dumpProfiles(dumpDir string) error {
	for _, item := range defaultProfiles {
		if err := writeProfile(item, dumpDir); err != nil {
			return fmt.Errorf("failed to write %s profile: %v", item.Name, err)
		}
	}
	return nil
}

// writeProfile writes a single profile type to the specified directory
func writeProfile(item profileItem, dumpDir string) error {
	fileName := filepath.Join(dumpDir, fmt.Sprintf(ProfileFilePattern, item.Name))
	f, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create profile file: %v", err)
	}
	defer f.Close()

	p := pprof.Lookup(item.Name)
	if p == nil {
		return fmt.Errorf("profile %s not found", item.Name)
	}

	return p.WriteTo(f, item.Debug)
}
