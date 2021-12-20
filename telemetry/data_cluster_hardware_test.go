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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeDiskName(t *testing.T) {
	tests := []struct {
		diskName string
		expected string
	}{
		{"/dev/sdb", "sdb"},
		{"sda", "sda"},
	}

	for _, test := range tests {
		t.Run(test.diskName, func(t *testing.T) {
			require.Equal(t, test.expected, normalizeDiskName(test.diskName))
		})
	}
}

func TestIsNormalizedDiskNameAllowed(t *testing.T) {
	tests := []struct {
		diskName string
	}{
		{"disk1s4"},
		{"rootfs"},
		{"sda"},
		{"sda1"},
		{"sdb"},
		{"sdb3"},
		{"sdc"},
		{"nvme0"},
		{"nvme0n1"},
		{"nvme0n1p0"},
		{"md127"},
		{"mdisk1s4"},
	}

	for _, test := range tests {
		t.Run(test.diskName, func(t *testing.T) {
			require.True(t, isNormalizedDiskNameAllowed(test.diskName))
		})
	}
}

func TestIsNormalizedDiskNameNotAllowed(t *testing.T) {
	tests := []struct {
		diskName string
	}{
		{"foo"},
		{"/rootfs"},
		{"asmdisk01p1"},
	}

	for _, test := range tests {
		t.Run(test.diskName, func(t *testing.T) {
			require.False(t, isNormalizedDiskNameAllowed(test.diskName))
		})
	}
}

func TestNormalizeFieldName(t *testing.T) {
	tests := []struct {
		fileName string
		expected string
	}{
		{"deviceName", "deviceName"},
		{"device-name", "deviceName"},
		{"device_name", "deviceName"},
		{"l1-cache-size", "l1CacheSize"},
		{"free-percent", "freePercent"},
	}

	for _, test := range tests {
		t.Run(test.fileName, func(t *testing.T) {
			require.Equal(t, test.expected, normalizeFieldName(test.fileName))
		})
	}
}
