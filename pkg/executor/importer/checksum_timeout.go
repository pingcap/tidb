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
	"math"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// defaultMaxAffinityMergeRegionSizeMiB mirrors PD's
	// schedule.max-affinity-merge-region-size default. It is the fallback when
	// the live PD configuration cannot be fetched.
	defaultMaxAffinityMergeRegionSizeMiB = 256
	// maxAffinityMergeRegionSizeMiBUpperBound rejects absurd PD values so the
	// derived-deadline arithmetic cannot overflow.
	maxAffinityMergeRegionSizeMiBUpperBound = 1 << 32
	// affinityRegionAutoSplitFactor is how many times
	// schedule.max-affinity-merge-region-size an affinity region may grow
	// before TiKV is allowed to split it. PD's split gate is
	// (config MiB + affinityRegionSizeBufferMB) * factor
	// (pkg/schedule/affinity/split.go), and passing the gate only makes the
	// split allowed, so a region can reach just below that bound.
	affinityRegionAutoSplitFactor = 4
	// affinityRegionSizeBufferMB mirrors PD's split-gate buffer (MiB).
	affinityRegionSizeBufferMB = 10
	// affinityChecksumScanRateMiBPerSec is a conservative lower bound on the
	// effective scan rate of one checksum coprocessor request. The reported
	// livelock case scanned at tens of MB/s, so this leaves an order of
	// magnitude of headroom for slow disks and load.
	affinityChecksumScanRateMiBPerSec = 4
)

// fetchMaxAffinityMergeRegionSizeMiB reads PD's
// schedule.max-affinity-merge-region-size (MiB). The variable indirection
// keeps timeout tests hermetic without a PD instance.
var fetchMaxAffinityMergeRegionSizeMiB = func(ctx context.Context) (uint64, bool) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cfg, err := infosync.GetPDScheduleConfig(ctx)
	if err != nil {
		return 0, false
	}
	return maxAffinityMergeRegionSizeFromAny(cfg["max-affinity-merge-region-size"])
}

// maxAffinityMergeRegionSizeFromAny parses the PD schedule value, which may
// arrive as any JSON number representation depending on the encoding path.
func maxAffinityMergeRegionSizeFromAny(raw any) (uint64, bool) {
	switch v := raw.(type) {
	case float64:
		if v >= 0 && v <= maxAffinityMergeRegionSizeMiBUpperBound && v == math.Trunc(v) {
			return uint64(v), true
		}
	case uint64:
		return v, true
	case int64:
		if v >= 0 {
			return uint64(v), true
		}
	case json.Number:
		if u, err := strconv.ParseUint(string(v), 10, 64); err == nil {
			return u, true
		}
	case string:
		if u, err := strconv.ParseUint(v, 10, 64); err == nil {
			return u, true
		}
	}
	return 0, false
}

// AffinityChecksumReadTimeoutMs derives a per-request coprocessor read
// deadline (ms) for checksum requests on affinity tables, as a budget
// estimate. Checksum requests cannot page, so one region is one RPC: the
// budget must cover scanning the largest region affinity keeps — the PD
// split gate is (max-affinity-merge-region-size + 10) * 4 MiB — at a
// conservative scan rate, or the request is cancelled and rebuilt forever.
// It never goes below the global copr-req-timeout. Both the ADMIN CHECKSUM
// executor and the NextGen IMPORT INTO checksum manager use it.
func AffinityChecksumReadTimeoutMs(ctx context.Context) uint64 {
	sizeMiB, ok := fetchMaxAffinityMergeRegionSizeMiB(ctx)
	switch {
	case !ok:
		logutil.Logger(ctx).Warn(
			"cannot read PD schedule.max-affinity-merge-region-size, use the default for the checksum read deadline",
			zap.Uint64("maxAffinityMergeRegionSizeMiB", defaultMaxAffinityMergeRegionSizeMiB))
		sizeMiB = defaultMaxAffinityMergeRegionSizeMiB
	case sizeMiB == 0:
		logutil.Logger(ctx).Info(
			"PD schedule.max-affinity-merge-region-size is 0, use the default for the checksum read deadline",
			zap.Uint64("maxAffinityMergeRegionSizeMiB", defaultMaxAffinityMergeRegionSizeMiB))
		sizeMiB = defaultMaxAffinityMergeRegionSizeMiB
	}
	if sizeMiB > maxAffinityMergeRegionSizeMiBUpperBound {
		sizeMiB = maxAffinityMergeRegionSizeMiBUpperBound
	}
	// MiB at 4 MiB/s: one MiB costs 1/4s, so seconds == sizeMiB * factor / rate.
	seconds := (sizeMiB + affinityRegionSizeBufferMB) * affinityRegionAutoSplitFactor / affinityChecksumScanRateMiBPerSec
	ms := seconds * 1000
	if floor := uint64(config.GetGlobalConfig().TiKVClient.CoprReqTimeout / time.Millisecond); ms < floor {
		ms = floor
	}
	return ms
}
