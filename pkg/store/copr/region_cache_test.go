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

	"github.com/tikv/client-go/v2/tikv"
)

// TestValidateLocationCoverage tests various coverage scenarios
func TestValidateLocationCoverage(t *testing.T) {
	ctx := context.Background()

	// Helper to create KeyRange
	kr := func(start, end string) tikv.KeyRange {
		return tikv.KeyRange{
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}

	// Helper to create KeyLocation
	kl := func(start, end string, regionID uint64) *tikv.KeyLocation {
		return &tikv.KeyLocation{
			Region:   tikv.NewRegionVerID(regionID, 0, 0),
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}

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

// TestValidateLocationCoverageEdgeCases tests additional edge cases
func TestValidateLocationCoverageEdgeCases(t *testing.T) {
	ctx := context.Background()

	kr := func(start, end string) tikv.KeyRange {
		return tikv.KeyRange{
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}

	kl := func(start, end string, regionID uint64) *tikv.KeyLocation {
		return &tikv.KeyLocation{
			Region:   tikv.NewRegionVerID(regionID, 0, 0),
			StartKey: []byte(start),
			EndKey:   []byte(end),
		}
	}

	t.Run("empty ranges", func(t *testing.T) {
		got := validateLocationCoverage(ctx, []tikv.KeyRange{}, []*tikv.KeyLocation{kl("a", "z", 1)})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for empty ranges")
		}
	})

	t.Run("empty locations", func(t *testing.T) {
		got := validateLocationCoverage(ctx, []tikv.KeyRange{kr("a", "z")}, []*tikv.KeyLocation{})
		if got {
			t.Errorf("validateLocationCoverage() = true, want false for missing coverage")
		}
	})

	t.Run("both empty", func(t *testing.T) {
		got := validateLocationCoverage(ctx, []tikv.KeyRange{}, []*tikv.KeyLocation{})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for both empty")
		}
	})

	t.Run("exact boundary match", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "m"), kr("m", "z")},
			[]*tikv.KeyLocation{kl("a", "m", 1), kl("m", "z", 2)})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for exact boundary match")
		}
	})

	t.Run("location boundary equals range start", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("m", "z")},
			[]*tikv.KeyLocation{kl("m", "z", 1)})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for matching boundaries")
		}
	})

	t.Run("locations not monotonic", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "z")},
			[]*tikv.KeyLocation{
				kl("m", "z", 1),
				kl("a", "m", 2), // Out of order
			})
		if got {
			t.Errorf("validateLocationCoverage() = true, want false for non-monotonic locations")
		}
	})

	t.Run("locations overlap", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "z")},
			[]*tikv.KeyLocation{
				kl("a", "n", 1),
				kl("m", "z", 2), // Overlaps with previous location
			})
		if got {
			t.Errorf("validateLocationCoverage() = true, want false for overlapping locations")
		}
	})

	t.Run("location extends to infinity but not last", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "z")},
			[]*tikv.KeyLocation{
				kl("a", "", 1),  // Extends to infinity
				kl("m", "z", 2), // But there's another location after!
			})
		if got {
			t.Errorf("validateLocationCoverage() = true, want false for infinity location not being last")
		}
	})

	t.Run("current location starts from beginning after non-beginning", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "z")},
			[]*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("", "z", 2), // Starts from beginning after a non-beginning location
			})
		if got {
			t.Errorf("validateLocationCoverage() = true, want false for going back to beginning")
		}
	})

	t.Run("valid: first location starts from beginning", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("", "z")},
			[]*tikv.KeyLocation{
				kl("", "m", 1), // First location can start from beginning
				kl("m", "z", 2),
			})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for valid beginning location")
		}
	})

	t.Run("valid: last location extends to infinity", func(t *testing.T) {
		got := validateLocationCoverage(ctx,
			[]tikv.KeyRange{kr("a", "")},
			[]*tikv.KeyLocation{
				kl("a", "m", 1),
				kl("m", "", 2), // Last location can extend to infinity
			})
		if !got {
			t.Errorf("validateLocationCoverage() = false, want true for valid infinity location")
		}
	})
}
