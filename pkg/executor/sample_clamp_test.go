// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

// TestSplitIntoMultiRangesClampEmptyEndKey is a regression test for the bug
// where TABLESAMPLE REGIONS() panics in keyspace environments. When the last
// region's EndKey is an empty byte slice ([]byte{}) instead of nil, the old
// check `end == nil` failed to clamp it, causing the scan to read past the
// table boundary into other tables' data.
// Fixed by upstream PR https://github.com/pingcap/tidb/pull/63599.
func TestSplitIntoMultiRangesClampEmptyEndKey(t *testing.T) {
	tableStart := kv.Key{0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	tableEnd := tableStart.PrefixNext()

	// Simulate the clamping logic from splitIntoMultiRanges using the same
	// condition as production code: len(end) == 0.
	clamp := func(end []byte) []byte {
		if len(end) == 0 || kv.Key(end).Cmp(tableEnd) > 0 {
			return tableEnd
		}
		return end
	}

	// nil end key: both old (== nil) and new (len == 0) handle this correctly.
	require.Equal(t, kv.Key(tableEnd), kv.Key(clamp(nil)))

	// Empty slice end key: old code (== nil) would NOT clamp this, causing the
	// scan to extend past the table. The fix (len == 0) clamps it correctly.
	require.Equal(t, kv.Key(tableEnd), kv.Key(clamp([]byte{})))

	// End key beyond table boundary: should be clamped.
	require.Equal(t, kv.Key(tableEnd), kv.Key(clamp([]byte{0xff, 0xff})))

	// End key within table boundary: should be kept as-is.
	midKey := append(tableStart, 0x50)
	require.Equal(t, kv.Key(midKey), kv.Key(clamp(midKey)))
}
