// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestRunMemoryMonitor(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	testCases := []struct {
		name        string
		dumpDir     string
		memoryLimit uint64
	}{
		{
			name:        "with custom values",
			dumpDir:     tmpDir,
			memoryLimit: 1024 * 1024 * 1024, // 1GB
		},
		{
			name:        "with default values",
			dumpDir:     "",
			memoryLimit: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset memory limit before test
			memory.ServerMemoryLimit.Store(0)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := RunMemoryMonitor(ctx, tc.dumpDir, tc.memoryLimit)
			require.NoError(t, err)

			// Verify memory limit is set correctly
			if tc.memoryLimit > 0 {
				require.Equal(t, tc.memoryLimit, memory.ServerMemoryLimit.Load())
			}

			// Verify dump directory
			expectedDir := tc.dumpDir
			if expectedDir == "" {
				expectedDir = DefaultProfilesDir
			}

			// Give monitor a moment to start
			time.Sleep(100 * time.Millisecond)

			// Verify oom_record directory is created
			oomDir := filepath.Join(expectedDir, "oom_record")
			if tc.dumpDir != "" { // Only check if custom dir provided
				_, err = os.Stat(oomDir)
				require.NoError(t, err)
			}

			// Test graceful shutdown
			cancel()
			time.Sleep(100 * time.Millisecond) // Give monitor time to stop
		})
	}
}

func TestBRConfigProvider(t *testing.T) {
	provider := &BRConfigProvider{
		ratio:   atomic.NewFloat64(0.8),
		keepNum: atomic.NewInt64(3),
		logDir:  "/custom/dir",
	}

	// Test GetMemoryUsageAlarmRatio
	require.Equal(t, 0.8, provider.GetMemoryUsageAlarmRatio())

	// Test GetMemoryUsageAlarmKeepRecordNum
	require.Equal(t, int64(3), provider.GetMemoryUsageAlarmKeepRecordNum())

	// Test GetLogDir
	require.Equal(t, "/custom/dir", provider.GetLogDir())

	// Test GetLogDir with default
	provider.logDir = ""
	require.Equal(t, DefaultProfilesDir, provider.GetLogDir())

	// Test GetComponentName
	require.Equal(t, "br", provider.GetComponentName())
}
