// Copyright 2025 PingCAP, Inc.
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

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeByteSizeUnit(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// IEC binary units should be normalized to SI units
		{"24MiB", "24MB"},
		{"100GiB", "100GB"},
		{"256KiB", "256KB"},
		{"1TiB", "1TB"},
		{"10PiB", "10PB"},
		{"2EiB", "2EB"},
		// Decimal values should also be normalized
		{"24.5MiB", "24.5MB"},
		{"1.5GiB", "1.5GB"},
		// SI units should remain unchanged
		{"24MB", "24MB"},
		{"100GB", "100GB"},
		{"256KB", "256KB"},
		// Non-byte values should remain unchanged
		{"hello", "hello"},
		{"true", "true"},
		{"12345", "12345"},
		{"", ""},
		{"/path/to/file", "/path/to/file"},
		// Invalid formats should remain unchanged
		{"24MiBx", "24MiBx"},
		{"MiB", "MiB"},
		{"24 MiB", "24 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeByteSizeUnit(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
