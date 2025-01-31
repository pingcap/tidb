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
	testDumpContent  = "test heap dump %d"
	testFilePerm     = 0644
	readOnlyDirPerm  = 0555
)

func createTestHeapDumps(t *testing.T, dir string, count int, timeGap time.Duration) []string {
	var files []string
	baseTime := time.Now().Add(-timeGap * time.Duration(count))

	for i := 0; i < count; i++ {
		fileName := fmt.Sprintf("br_heap_%s_%d.pprof",
			baseTime.Add(timeGap*time.Duration(i)).Format(TimeFormat),
			100+i)
		path := filepath.Join(dir, fileName)

		err := os.WriteFile(path, []byte(fmt.Sprintf(testDumpContent, i)), testFilePerm)
		require.NoError(t, err)
		files = append(files, path)
	}
	return files
}

func TestCleanupOldHeapDumps(t *testing.T) {
	t.Run("cleanup works correctly", func(t *testing.T) {
		dir := t.TempDir()
		files := createTestHeapDumps(t, dir, MaxHeapDumps+2, testTimeGap)
		sort.Strings(files)

		// verify all test files were created
		for _, f := range files {
			_, err := os.Stat(f)
			require.NoError(t, err)
		}

		cleanupOldHeapDumps(dir)

		// verify newest files exist
		for i := len(files) - MaxHeapDumps; i < len(files); i++ {
			_, err := os.Stat(files[i])
			require.NoError(t, err)
		}

		// verify oldest files are gone
		for i := 0; i < len(files)-MaxHeapDumps; i++ {
			_, err := os.Stat(files[i])
			require.True(t, os.IsNotExist(err))
		}
	})

	t.Run("no cleanup needed when files <= MaxHeapDumps", func(t *testing.T) {
		dir := t.TempDir()
		files := createTestHeapDumps(t, dir, MaxHeapDumps, testTimeGap)
		cleanupOldHeapDumps(dir)

		for _, f := range files {
			_, err := os.Stat(f)
			require.NoError(t, err)
		}
	})

	t.Run("handles non-existent directory", func(t *testing.T) {
		dir := t.TempDir()
		nonExistentDir := filepath.Join(dir, "non-existent")
		cleanupOldHeapDumps(nonExistentDir)
	})

	t.Run("handles empty directory", func(t *testing.T) {
		dir := t.TempDir()
		emptyDir := filepath.Join(dir, "empty")
		err := os.MkdirAll(emptyDir, DumpDirPerm)
		require.NoError(t, err)
		cleanupOldHeapDumps(emptyDir)
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
		files, err := filepath.Glob(filepath.Join(dir, "br_heap_*.pprof"))
		require.NoError(t, err)
		fileCount := len(files)

		time.Sleep(2 * MonitorInterval)
		files, err = filepath.Glob(filepath.Join(dir, "br_heap_*.pprof"))
		require.NoError(t, err)
		require.Equal(t, fileCount, len(files))
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
		files, err := filepath.Glob(filepath.Join(dir, "br_heap_*.pprof"))
		require.NoError(t, err)
		initialCount := len(files)

		time.Sleep(500 * time.Millisecond)
		files, err = filepath.Glob(filepath.Join(dir, "br_heap_*.pprof"))
		require.NoError(t, err)
		require.Equal(t, initialCount, len(files))
	})
}
