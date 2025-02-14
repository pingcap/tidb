// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testTimeGap      = time.Minute
	testMemoryLimit  = 100 * 1024 * 1024
	smallMemoryLimit = 1024
	testWaitTime     = 100 * time.Millisecond
	testDumpContent  = "test profile dump %d"
	testFilePerm     = 0644
	readOnlyDirPerm  = 0555
)

// createTestProfileDumps creates test dump directories, each containing multiple profile types
func createTestProfileDumps(t *testing.T, dir string, batchCount int, timeGap time.Duration) []string {
	var dumpDirs []string
	baseTime := time.Now().Add(-timeGap * time.Duration(batchCount))

	for i := 0; i < batchCount; i++ {
		dumpTime := baseTime.Add(timeGap * time.Duration(i)).Format(TimeFormat)
		dumpDir := filepath.Join(dir, fmt.Sprintf(DumpDirPattern, dumpTime, 100+i))
		err := os.MkdirAll(dumpDir, DumpDirPerm)
		require.NoError(t, err)

		// create all profile types in this batch
		for _, profile := range defaultProfiles {
			path := filepath.Join(dumpDir, fmt.Sprintf(ProfileFilePattern, profile.Name))
			err := os.WriteFile(path, []byte(fmt.Sprintf(testDumpContent, i)), testFilePerm)
			require.NoError(t, err)
		}
		dumpDirs = append(dumpDirs, dumpDir)
	}
	return dumpDirs
}

func TestCleanupOldProfileDumps(t *testing.T) {
	t.Run("cleanup works correctly", func(t *testing.T) {
		dir := t.TempDir()
		dumpDirs := createTestProfileDumps(t, dir, MaxDumpBatches+2, testTimeGap)
		sort.Strings(dumpDirs)

		// verify all test directories and their profiles were created
		for _, dumpDir := range dumpDirs {
			_, err := os.Stat(dumpDir)
			require.NoError(t, err)
			// verify all profile types exist in this batch
			for _, profile := range defaultProfiles {
				_, err := os.Stat(filepath.Join(dumpDir, fmt.Sprintf(ProfileFilePattern, profile.Name)))
				require.NoError(t, err)
			}
		}

		cleanupOldProfileDumps(dir)

		// verify newest dump directories and their profiles exist
		for i := len(dumpDirs) - MaxDumpBatches; i < len(dumpDirs); i++ {
			_, err := os.Stat(dumpDirs[i])
			require.NoError(t, err)
			// verify all profile types still exist in this batch
			for _, profile := range defaultProfiles {
				_, err := os.Stat(filepath.Join(dumpDirs[i], fmt.Sprintf(ProfileFilePattern, profile.Name)))
				require.NoError(t, err)
			}
		}

		// verify oldest dump directories are gone
		for i := 0; i < len(dumpDirs)-MaxDumpBatches; i++ {
			_, err := os.Stat(dumpDirs[i])
			require.True(t, os.IsNotExist(err))
		}
	})

	t.Run("no cleanup needed when batches <= MaxDumpBatches", func(t *testing.T) {
		dir := t.TempDir()
		dumpDirs := createTestProfileDumps(t, dir, MaxDumpBatches, testTimeGap)
		cleanupOldProfileDumps(dir)

		for _, dumpDir := range dumpDirs {
			_, err := os.Stat(dumpDir)
			require.NoError(t, err)
			// verify all profile types still exist in this batch
			for _, profile := range defaultProfiles {
				_, err := os.Stat(filepath.Join(dumpDir, fmt.Sprintf(ProfileFilePattern, profile.Name)))
				require.NoError(t, err)
			}
		}
	})

	t.Run("handles non-existent directory", func(t *testing.T) {
		dir := t.TempDir()
		nonExistentDir := filepath.Join(dir, "non-existent")
		cleanupOldProfileDumps(nonExistentDir)
	})

	t.Run("handles empty directory", func(t *testing.T) {
		dir := t.TempDir()
		emptyDir := filepath.Join(dir, "empty")
		err := os.MkdirAll(emptyDir, DumpDirPerm)
		require.NoError(t, err)
		cleanupOldProfileDumps(emptyDir)
	})
}

func TestStartMemoryMonitor(t *testing.T) {
	dir := t.TempDir()

	t.Run("creates dump directory if not exists", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		dumpDir := filepath.Join(dir, "new-dir")
		cfg := MemoryMonitorConfig{
			DumpDir:     dumpDir,
			MemoryLimit: testMemoryLimit,
		}

		err := StartMemoryMonitor(ctx, cfg)
		require.NoError(t, err)

		stat, err := os.Stat(dumpDir)
		require.NoError(t, err)
		require.True(t, stat.IsDir())
	})

	t.Run("uses temp dir as fallback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		readOnlyDir := filepath.Join(dir, "readonly")
		err := os.MkdirAll(readOnlyDir, readOnlyDirPerm)
		require.NoError(t, err)

		cfg := MemoryMonitorConfig{
			DumpDir:     readOnlyDir,
			MemoryLimit: testMemoryLimit,
		}

		err = StartMemoryMonitor(ctx, cfg)
		require.NoError(t, err)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		cfg := MemoryMonitorConfig{
			DumpDir:     dir,
			MemoryLimit: smallMemoryLimit,
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		err := StartMemoryMonitor(ctx, cfg)
		require.NoError(t, err)

		time.Sleep(testWaitTime)
		cancel()

		time.Sleep(2 * MonitorInterval)
		dumpDirs, err := filepath.Glob(filepath.Join(dir, ProfileGlobPattern))
		require.NoError(t, err)
		batchCount := len(dumpDirs)

		time.Sleep(2 * MonitorInterval)
		dumpDirs, err = filepath.Glob(filepath.Join(dir, ProfileGlobPattern))
		require.NoError(t, err)
		require.Equal(t, batchCount, len(dumpDirs))
	})

	t.Run("enforces minimum dump interval", func(t *testing.T) {
		cfg := MemoryMonitorConfig{
			DumpDir:         dir,
			MemoryLimit:     smallMemoryLimit,
			MinDumpInterval: time.Second,
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		err := StartMemoryMonitor(ctx, cfg)
		require.NoError(t, err)

		time.Sleep(testWaitTime)
		dumpDirs, err := filepath.Glob(filepath.Join(dir, ProfileGlobPattern))
		require.NoError(t, err)
		initialBatchCount := len(dumpDirs)

		time.Sleep(500 * time.Millisecond)
		dumpDirs, err = filepath.Glob(filepath.Join(dir, ProfileGlobPattern))
		require.NoError(t, err)
		require.Equal(t, initialBatchCount, len(dumpDirs))
	})
}
