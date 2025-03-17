// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

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
