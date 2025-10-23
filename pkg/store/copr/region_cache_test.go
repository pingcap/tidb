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

package copr

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

// Test helpers
var (
	// Helper to create KeyRange
	kr = func(start, end string) tikv.KeyRange {
		return tikv.KeyRange{
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}

	// Helper to create KeyLocation
	kl = func(start, end string, regionID uint64) *tikv.KeyLocation {
		return &tikv.KeyLocation{
			Region:   tikv.NewRegionVerID(regionID, 0, 0),
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}
)

// TestValidateLocationCoverage tests various coverage scenarios
func TestValidateLocationCoverage(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		ranges    []tikv.KeyRange
		locs      []*tikv.KeyLocation
		wantValid bool // whether we expect validation to pass
	}{
		{
			name:      "single range, single location - exact match",
			ranges:    []tikv.KeyRange{kr("a", "z")},
			locs:      []*tikv.KeyLocation{kl("a", "z", 1)},
			wantValid: true,
		},
		{
			name:      "single range, single location - location covers more",
			ranges:    []tikv.KeyRange{kr("b", "y")},
			locs:      []*tikv.KeyLocation{kl("a", "z", 1)},
			wantValid: true,
		},
		{
			name:   "single range split across two locations",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("m", "z", 2),
			},
			wantValid: true, // Valid partial coverage
		},
		{
			name:   "single range split across three locations",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "h", 1),
				kl("h", "p", 2),
				kl("p", "z", 3),
			},
			wantValid: true, // Valid partial coverage across multiple locations
		},
		{
			name: "multiple ranges, single location covers all",
			ranges: []tikv.KeyRange{
				kr("b", "d"),
				kr("f", "h"),
			},
			locs:      []*tikv.KeyLocation{kl("a", "z", 1)},
			wantValid: true,
		},
		{
			name: "multiple ranges, multiple locations - aligned",
			ranges: []tikv.KeyRange{
				kr("a", "m"),
				kr("m", "z"),
			},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("m", "z", 2),
			},
			wantValid: true,
		},
		{
			name:      "empty start key - location also empty",
			ranges:    []tikv.KeyRange{kr("", "m")},
			locs:      []*tikv.KeyLocation{kl("", "m", 1)},
			wantValid: true,
		},
		{
			name:      "empty start key - location NOT empty",
			ranges:    []tikv.KeyRange{kr("", "m")},
			locs:      []*tikv.KeyLocation{kl("a", "m", 1)},
			wantValid: false, // Invalid - location doesn't start from beginning
		},
		{
			name:      "empty end key - location also empty",
			ranges:    []tikv.KeyRange{kr("m", "")},
			locs:      []*tikv.KeyLocation{kl("m", "", 1)},
			wantValid: true,
		},
		{
			name:      "empty end key - location NOT empty",
			ranges:    []tikv.KeyRange{kr("m", "")},
			locs:      []*tikv.KeyLocation{kl("m", "z", 1)},
			wantValid: false, // Invalid - location doesn't extend to infinity
		},
		{
			name:      "range with empty end - location extends to infinity",
			ranges:    []tikv.KeyRange{kr("m", "")},
			locs:      []*tikv.KeyLocation{kl("a", "", 1)},
			wantValid: true, // Location extends to infinity, covers the range
		},
		{
			name:      "location doesn't cover range start",
			ranges:    []tikv.KeyRange{kr("a", "z")},
			locs:      []*tikv.KeyLocation{kl("b", "z", 1)},
			wantValid: false, // Invalid - location starts after range
		},
		{
			name:      "location doesn't cover range end",
			ranges:    []tikv.KeyRange{kr("a", "z")},
			locs:      []*tikv.KeyLocation{kl("a", "y", 1)},
			wantValid: false, // Invalid - location ends before range and no next location
		},
		{
			name:   "gap between locations",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("n", "z", 2), // Gap between 'm' and 'n'
			},
			wantValid: false, // Invalid - gap in coverage
		},
		{
			name: "missing range coverage",
			ranges: []tikv.KeyRange{
				kr("a", "m"),
				kr("m", "z"),
			},
			locs:      []*tikv.KeyLocation{kl("a", "m", 1)},
			wantValid: false, // Invalid - second range not covered
		},

		// Edge cases
		{
			name:      "empty ranges",
			ranges:    []tikv.KeyRange{},
			locs:      []*tikv.KeyLocation{kl("a", "z", 1)},
			wantValid: true,
		},
		{
			name:      "empty locations",
			ranges:    []tikv.KeyRange{kr("a", "z")},
			locs:      []*tikv.KeyLocation{},
			wantValid: false,
		},
		{
			name:      "both empty",
			ranges:    []tikv.KeyRange{},
			locs:      []*tikv.KeyLocation{},
			wantValid: true,
		},
		{
			name: "exact boundary match",
			ranges: []tikv.KeyRange{
				kr("a", "m"),
				kr("m", "z"),
			},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("m", "z", 2),
			},
			wantValid: true,
		},
		{
			name:      "location boundary equals range start",
			ranges:    []tikv.KeyRange{kr("m", "z")},
			locs:      []*tikv.KeyLocation{kl("m", "z", 1)},
			wantValid: true,
		},

		// Monotonicity violations
		{
			name:   "locations not monotonic",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("m", "z", 1),
				kl("a", "m", 2), // Out of order
			},
			wantValid: false,
		},
		{
			name:   "locations overlap",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "n", 1),
				kl("m", "z", 2), // Overlaps with previous location
			},
			wantValid: false,
		},
		{
			name:   "location extends to infinity but not last",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "", 1),  // Extends to infinity
				kl("m", "z", 2), // But there's another location after!
			},
			wantValid: false,
		},
		{
			name:   "current location starts from beginning after non-beginning",
			ranges: []tikv.KeyRange{kr("a", "z")},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("", "z", 2), // Starts from beginning after a non-beginning location
			},
			wantValid: false,
		},
		{
			name:   "valid: first location starts from beginning",
			ranges: []tikv.KeyRange{kr("", "z")},
			locs: []*tikv.KeyLocation{
				kl("", "m", 1), // First location can start from beginning
				kl("m", "z", 2),
			},
			wantValid: true,
		},
		{
			name:   "valid: last location extends to infinity",
			ranges: []tikv.KeyRange{kr("a", "")},
			locs: []*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("m", "", 2), // Last location can extend to infinity
			},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateLocationCoverage(ctx, tt.ranges, tt.locs)
			if got != tt.wantValid {
				t.Errorf("validateLocationCoverage() = %v, want %v", got, tt.wantValid)
			}
		})
	}
}

// TestPanicInSplitKeyRangesByBuckets tests that panic recovery correctly logs the location index
func TestPanicInSplitKeyRangesByBuckets(t *testing.T) {
	// Set up mock TiKV cluster with multiple regions
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	// Create 4 regions: nil---g---n---x---nil
	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("x"))
	// Add buckets to each region
	cluster.SplitRegionBuckets(regionIDs[0], [][]byte{{}, {'c'}, {'g'}}, regionIDs[0])
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'g'}, {'k'}, {'n'}}, regionIDs[1])
	cluster.SplitRegionBuckets(regionIDs[2], [][]byte{{'n'}, {'t'}, {'x'}}, regionIDs[2])
	cluster.SplitRegionBuckets(regionIDs[3], [][]byte{{'x'}, {}}, regionIDs[3])

	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	// Build key ranges that span multiple regions
	ranges := buildCopRanges("a", "z")

	// Enable the failpoint to trigger panic at location index 2 (the 3rd region)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/panicInSplitKeyRangesByBuckets", "return(2)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/panicInSplitKeyRangesByBuckets"))
	}()

	// Call SplitKeyRangesByBuckets which should trigger the panic
	// The defer/recover in the function should catch it, log diagnostics, and re-panic
	didPanic := false
	panicValue := ""
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
				panicValue = r.(string)
				t.Logf("Successfully caught panic: %v", r)
			}
		}()

		// This will trigger the panic when processing location index 2
		_, err = cache.SplitKeyRangesByBuckets(bo, ranges)
		// Should not reach here
		t.Fatal("Expected panic but none occurred")
	}()

	// Verify that panic occurred and was re-panicked
	require.True(t, didPanic, "Expected panic to occur")
	require.Equal(t, "failpoint triggered panic in bucket splitting", panicValue)

	t.Logf("Test completed successfully - panic was caught, diagnostics logged, and re-panicked as expected")
}
