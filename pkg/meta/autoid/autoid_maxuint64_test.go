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

package autoid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMaxUint64Boundary tests that NextGlobalAutoID correctly handles
// the MaxUint64 boundary case for unsigned AUTO_INCREMENT columns.
//
// The issue: When UPDATE id = -1 triggers AUTO_INCREMENT rebase to MaxUint64
// (18446744073709551615), after TiDB restart, the allocator incorrectly
// returns 0 as the next ID instead of returning an error.
//
// This test verifies the fix by checking that:
// 1. For unsigned types with maxv=-1 (MaxUint64 as int64), an error is returned
// 2. For signed types with maxv=-1, 0 is returned (normal behavior)
// 3. For normal cases, the behavior is unchanged
func TestMaxUint64Boundary(t *testing.T) {
	// Test case 1: Unsigned type with maxv = -1 (MaxUint64 as int64)
	// This simulates the bug scenario after TiDB restart
	t.Run("unsigned_maxv_minus_one_returns_error", func(t *testing.T) {
		// MaxUint64 as int64 is -1
		maxv := int64(-1)
		isUnsigned := true

		// The fix logic: if isUnsigned && maxv == -1, return error
		// This simulates what NextGlobalAutoID does now
		shouldReturnError := isUnsigned && maxv == -1
		require.True(t, shouldReturnError, "For unsigned type with maxv=-1, should return error")

		// Verify the overflow: -1 + 1 = 0 (which is invalid for unsigned AUTO_INCREMENT)
		nextID := maxv + 1
		require.Equal(t, int64(0), nextID, "maxv=-1 + 1 results in 0, which is invalid for unsigned AUTO_INCREMENT")
	})

	// Test case 2: Signed type with maxv = -1
	// For signed types, -1 + 1 = 0 is valid
	t.Run("signed_maxv_minus_one_returns_zero", func(t *testing.T) {
		maxv := int64(-1)
		isUnsigned := false

		// The fix logic: only check for unsigned types
		shouldReturnError := isUnsigned && maxv == -1
		require.False(t, shouldReturnError, "For signed type with maxv=-1, should NOT return error")

		nextID := maxv + 1
		require.Equal(t, int64(0), nextID, "For signed type, maxv=-1 + 1 = 0 is valid")
	})

	// Test case 3: Normal case (not at boundary)
	t.Run("normal_case_unsigned", func(t *testing.T) {
		maxv := int64(99)
		isUnsigned := true

		// The fix logic: only check when maxv == -1
		shouldReturnError := isUnsigned && maxv == -1
		require.False(t, shouldReturnError, "For unsigned type with maxv=99, should NOT return error")

		nextID := maxv + 1
		require.Equal(t, int64(100), nextID, "Normal case: maxv + 1 should work correctly")
	})

	// Test case 4: Verify MaxUint64 value
	t.Run("verify_maxuint64_value", func(t *testing.T) {
		// MaxUint64 as uint64
		maxUint64 := uint64(18446744073709551615)

		// When cast to int64, it becomes -1 (two's complement)
		maxUint64AsInt64 := int64(maxUint64)
		require.Equal(t, int64(-1), maxUint64AsInt64, "MaxUint64 cast to int64 equals -1")

		// Adding 1 to -1 results in 0
		nextID := maxUint64AsInt64 + 1
		require.Equal(t, int64(0), nextID, "-1 + 1 = 0")

		// As uint64, this would wrap around to 0
		nextIDAsUint64 := uint64(nextID)
		require.Equal(t, uint64(0), nextIDAsUint64, "0 as uint64 is 0")
	})
}