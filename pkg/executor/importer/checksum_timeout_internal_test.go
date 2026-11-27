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

package importer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestAffinityChecksumReadTimeoutMs(t *testing.T) {
	orig := fetchMaxAffinityMergeRegionSizeMiB
	defer func() { fetchMaxAffinityMergeRegionSizeMiB = orig }()

	// Fetch failure falls back to the PD default (256 MiB): the PD split gate
	// is (256 + 10) * 4 MiB, at 4 MiB/s that is 266s.
	fetchMaxAffinityMergeRegionSizeMiB = func(context.Context) (uint64, bool) { return 0, false }
	require.Equal(t, uint64(266000), AffinityChecksumReadTimeoutMs(context.Background()))

	// A raised PD configuration scales the budget: (1024 + 10) * 4 MiB at
	// 4 MiB/s -> 1034s.
	fetchMaxAffinityMergeRegionSizeMiB = func(context.Context) (uint64, bool) { return 1024, true }
	require.Equal(t, uint64(1034000), AffinityChecksumReadTimeoutMs(context.Background()))

	// The derived budget never goes below the global copr-req-timeout: with
	// the floor raised above any derivable value, the floor wins.
	origTimeout := config.GetGlobalConfig().TiKVClient.CoprReqTimeout
	config.GetGlobalConfig().TiKVClient.CoprReqTimeout = 300 * time.Second
	defer func() { config.GetGlobalConfig().TiKVClient.CoprReqTimeout = origTimeout }()
	fetchMaxAffinityMergeRegionSizeMiB = func(context.Context) (uint64, bool) { return 0, false }
	require.Equal(t, uint64(300000), AffinityChecksumReadTimeoutMs(context.Background()))

	// A zero PD value is normalized to the default instead of collapsing the
	// budget to the floor.
	config.GetGlobalConfig().TiKVClient.CoprReqTimeout = origTimeout
	fetchMaxAffinityMergeRegionSizeMiB = func(context.Context) (uint64, bool) { return 0, true }
	require.Equal(t, uint64(266000), AffinityChecksumReadTimeoutMs(context.Background()))

	// A value beyond the accepted bound bypassing the fetch hook (or from a
	// misbehaving source) is clamped instead of overflowing the arithmetic.
	fetchMaxAffinityMergeRegionSizeMiB = func(context.Context) (uint64, bool) {
		return maxAffinityMergeRegionSizeMiBUpperBound + 1, true
	}
	require.Equal(t, uint64(maxAffinityMergeRegionSizeMiBUpperBound+affinityRegionSizeBufferMB)*1000,
		AffinityChecksumReadTimeoutMs(context.Background()))
}

func TestMaxAffinityMergeRegionSizeFromAny(t *testing.T) {
	cases := []struct {
		raw  any
		want uint64
		ok   bool
	}{
		{float64(256), 256, true},
		{float64(256.5), 0, false}, // non-integer
		{float64(-1), 0, false},    // negative
		{uint64(512), 512, true},
		{int64(384), 384, true},
		{int64(-384), 0, false},
		{json.Number("768"), 768, true},
		{json.Number("bad"), 0, false},
		{"128", 128, true},
		{"nope", 0, false},
		{nil, 0, false},
	}
	for _, c := range cases {
		got, ok := maxAffinityMergeRegionSizeFromAny(c.raw)
		require.Equal(t, c.ok, ok, "raw=%v", c.raw)
		if c.ok {
			require.Equal(t, c.want, got, "raw=%v", c.raw)
		}
	}
}
