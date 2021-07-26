// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeDiskName(t *testing.T) {
	t.Parallel()
	require.Equal(t, "sdb", normalizeDiskName("/dev/sdb"))
	require.Equal(t, "sda", normalizeDiskName("sda"))
}

func TestIsNormalizedDiskNameAllowed(t *testing.T) {
	t.Parallel()
	require.True(t, isNormalizedDiskNameAllowed("disk1s4"))
	require.True(t, isNormalizedDiskNameAllowed("rootfs"))
	require.True(t, isNormalizedDiskNameAllowed("sda"))
	require.True(t, isNormalizedDiskNameAllowed("sda1"))
	require.True(t, isNormalizedDiskNameAllowed("sdb"))
	require.True(t, isNormalizedDiskNameAllowed("sdb3"))
	require.True(t, isNormalizedDiskNameAllowed("sdc"))
	require.True(t, isNormalizedDiskNameAllowed("nvme0"))
	require.True(t, isNormalizedDiskNameAllowed("nvme0n1"))
	require.True(t, isNormalizedDiskNameAllowed("nvme0n1p0"))
	require.True(t, isNormalizedDiskNameAllowed("md127"))
	require.True(t, isNormalizedDiskNameAllowed("mdisk1s4"))
}

func TestIsNormalizedDiskNameNotAllowed(t *testing.T) {
	t.Parallel()
	require.False(t, isNormalizedDiskNameAllowed("foo"))
	require.False(t, isNormalizedDiskNameAllowed("/rootfs"))
	require.False(t, isNormalizedDiskNameAllowed("asmdisk01p1"))
}

func TestNormalizeFieldName(t *testing.T) {
	t.Parallel()
	require.Equal(t, "deviceName", normalizeFieldName("deviceName"))
	require.Equal(t, "deviceName", normalizeFieldName("device-name"))
	require.Equal(t, "deviceName", normalizeFieldName("device_name"))
	require.Equal(t, "l1CacheSize", normalizeFieldName("l1-cache-size"))
	require.Equal(t, "freePercent", normalizeFieldName("free-percent"))
}
