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

package reporter

import (
	"sync"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	ruBaseBucketSeconds   uint64 = 15
	ruReportWindowSeconds uint64 = 60

	// Online stage (1~14s in a collecting bucket): allow larger pre-TopN headroom.
	ruOpenPreTopNUsers       = maxPreTopNUsers
	ruOpenPreTopNSQLsPerUser = maxPreTopNSQLsPerUser

	// Compacted 15s buckets: compact to 200x200.
	ruCompactedTopNUsers       = maxTopUsers
	ruCompactedTopNSQLsPerUser = maxTopSQLsPerUser

	// Final 60s report points: compact to 100x100.
	ruReportTopNUsers       = 100
	ruReportTopNSQLsPerUser = 100

	// Merge stage may combine up to 4 compacted buckets, so keep enough pre-cap before final 100x100 filtering.
	ruReportMergePreTopNUsers       = int(ruReportWindowSeconds/ruBaseBucketSeconds) * ruCompactedTopNUsers
	ruReportMergePreTopNSQLsPerUser = int(ruReportWindowSeconds/ruBaseBucketSeconds) * ruCompactedTopNSQLsPerUser

	// The final output can contain up to 4 report points (15s granularity),
	// each with 100x100 top entries.
	ruReportOutputMaxUsers       = int(ruReportWindowSeconds/ruBaseBucketSeconds) * ruReportTopNUsers
	ruReportOutputMaxSQLsPerUser = int(ruReportWindowSeconds/ruBaseBucketSeconds) * ruReportTopNSQLsPerUser
)

type ruPointBucket struct {
	startTs             uint64
	collecting          *ruCollecting // non-nil = actively collecting; nil = compacted
	compactedCollecting *ruCollecting // read-only snapshot, valid only when collecting == nil
}

// ruWindowAggregator keeps online 15s buckets:
//   - collecting bucket aggregates 1s batches with 400x400 pre-cap
//   - compacted bucket keeps 200x200 compacted records
//   - report stage (every 60s) merges compacted buckets and applies 100x100 filtering
type ruWindowAggregator struct {
	mu                sync.Mutex
	buckets           map[uint64]*ruPointBucket // 15s startTs -> bucket
	lastReportedEndTs uint64
}

func newRUWindowAggregator() *ruWindowAggregator {
	return &ruWindowAggregator{
		buckets: make(map[uint64]*ruPointBucket),
	}
}

func alignToInterval(ts, interval uint64) uint64 {
	if interval == 0 {
		return ts
	}
	return ts - ts%interval
}

func (a *ruWindowAggregator) addBatchToBucket(ts uint64, increments stmtstats.RUIncrementMap) {
	if len(increments) == 0 {
		return
	}
	bucketStart := alignToInterval(ts, ruBaseBucketSeconds)

	a.mu.Lock()
	defer a.mu.Unlock()

	// Late data for already reported windows is ignored.
	if a.lastReportedEndTs > 0 && bucketStart < a.lastReportedEndTs {
		return
	}

	a.rotateBucketsBefore(bucketStart)

	bucket, ok := a.buckets[bucketStart]
	if !ok {
		bucket = &ruPointBucket{
			startTs:    bucketStart,
			collecting: newRUCollectingWithCaps(ruOpenPreTopNUsers, ruOpenPreTopNSQLsPerUser),
		}
		a.buckets[bucketStart] = bucket
	}
	if bucket.collecting == nil {
		// Out-of-order data hitting an already compacted bucket.
		return
	}

	// Collapse all points in this 15s bucket to the bucket start timestamp.
	bucket.collecting.addBatch(bucketStart, increments)
}

// takeReportRecords attempts to emit one aligned closed 60s window for nowTs.
// It does not catch up multiple missed windows: if called late, only the latest
// complete window is emitted and older windows are dropped.
// granularitySec must be 15/30/60 from state.GetTopRUItemInterval (normalized at SetTopRUItemInterval).
func (a *ruWindowAggregator) takeReportRecords(nowTs, granularitySec uint64, keyspaceName []byte) []tipb.TopRURecord {
	windowEnd := alignToInterval(nowTs, ruReportWindowSeconds)
	if windowEnd < ruReportWindowSeconds {
		return nil
	}

	// Phase 1: Take buckets under lock (fast path).
	takenBuckets := a.takeBucketsForWindow(windowEnd)
	if takenBuckets == nil {
		return nil
	}

	// Phase 2: Build report records outside lock (slow path: merge + TopN + proto).
	windowStart := windowEnd - ruReportWindowSeconds
	return buildReportRecords(takenBuckets, windowStart, windowEnd, granularitySec, keyspaceName)
}

// takeBucketsForWindow extracts buckets for the given window under lock.
// Returns nil if the window has already been reported or is not ready.
// The returned map is owned by the caller; original buckets are removed from aggregator.
func (a *ruWindowAggregator) takeBucketsForWindow(windowEnd uint64) map[uint64]*ruPointBucket {
	a.mu.Lock()
	defer a.mu.Unlock()

	if windowEnd <= a.lastReportedEndTs {
		return nil
	}

	// Rotate all buckets that are no longer writable for this report boundary.
	a.rotateBucketsBefore(windowEnd)

	windowStart := windowEnd - ruReportWindowSeconds

	// Take buckets for this window (move ownership to caller).
	takenBuckets := make(map[uint64]*ruPointBucket, int(ruReportWindowSeconds/ruBaseBucketSeconds))
	for ts := windowStart; ts < windowEnd; ts += ruBaseBucketSeconds {
		if bucket, ok := a.buckets[ts]; ok {
			takenBuckets[ts] = bucket
			delete(a.buckets, ts)
		}
	}
	a.lastReportedEndTs = windowEnd

	// Defensive cleanup for stale buckets.
	for ts := range a.buckets {
		if ts < windowStart {
			delete(a.buckets, ts)
		}
	}

	return takenBuckets
}

func (a *ruWindowAggregator) rotateBucketsBefore(boundaryStart uint64) {
	for _, bucket := range a.buckets {
		if bucket.collecting == nil {
			continue
		}

		if bucket.startTs+ruBaseBucketSeconds <= boundaryStart {
			// Compact to internal snapshot (avoids proto conversion).
			bucket.compactedCollecting = bucket.collecting.compactWithLimits(ruCompactedTopNUsers, ruCompactedTopNSQLsPerUser)
			bucket.collecting = nil
		}
	}
}

// buildReportRecords merges taken buckets and produces final proto records.
// This function does NOT require any lock; it operates on owned bucket data.
func buildReportRecords(buckets map[uint64]*ruPointBucket, windowStart, windowEnd, granularitySec uint64, keyspaceName []byte) []tipb.TopRURecord {
	mergedOutput := newRUCollectingWithCaps(ruReportMergePreTopNUsers, ruReportMergePreTopNSQLsPerUser)
	for groupStart := windowStart; groupStart < windowEnd; groupStart += granularitySec {
		groupCollecting := newRUCollectingWithCaps(ruReportMergePreTopNUsers, ruReportMergePreTopNSQLsPerUser)
		for ts := groupStart; ts < groupStart+granularitySec; ts += ruBaseBucketSeconds {
			bucket, ok := buckets[ts]
			if !ok || bucket.compactedCollecting == nil {
				continue
			}
			// Merge internal structure directly (no proto conversion).
			groupCollecting.mergeFrom(bucket.compactedCollecting, groupStart, true)
		}
		// Apply TopN and merge into output (still internal).
		groupCompacted := groupCollecting.compactWithLimits(ruReportTopNUsers, ruReportTopNSQLsPerUser)
		mergedOutput.mergeFrom(groupCompacted, 0, false)
	}
	// Final proto conversion only at output.
	return mergedOutput.getReportRecordsWithLimits(keyspaceName, ruReportOutputMaxUsers, ruReportOutputMaxSQLsPerUser)
}
