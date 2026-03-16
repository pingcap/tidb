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

	// Final 60s report points: compact to 100x100.
	ruReportTopNUsers       = 100
	ruReportTopNSQLsPerUser = 100
)

type ruPointBucket struct {
	startTs             uint64
	collecting          *ruCollecting // non-nil = actively collecting; nil = compacted
	compactedCollecting *ruCollecting // read-only snapshot, valid only when collecting == nil
}

// ruWindowAggregator keeps online 15s buckets for TopRU reporting.
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
			collecting: newRUCollectingWithCaps(maxPreTopNUsers, maxPreTopNSQLsPerUser),
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

// takeReportRecords emits one aligned closed 60s window for nowTs.
// If called late, older windows are dropped.
// itemInterval must be 15, 30, or 60.
func (a *ruWindowAggregator) takeReportRecords(nowTs, itemInterval uint64, keyspaceName []byte) []tipb.TopRURecord {
	windowEnd := alignToInterval(nowTs, ruReportWindowSeconds)
	if windowEnd < ruReportWindowSeconds {
		return nil
	}

	// Step 1: take buckets under lock.
	takenBuckets := a.takeBucketsForWindow(windowEnd)
	if takenBuckets == nil {
		return nil
	}

	// Step 2: build report records outside lock.
	windowStart := windowEnd - ruReportWindowSeconds
	return buildReportRecords(takenBuckets, windowStart, windowEnd, itemInterval, keyspaceName)
}

// takeBucketsForWindow extracts buckets for the given window under lock.
// It returns nil if the window has already been reported or is not ready.
func (a *ruWindowAggregator) takeBucketsForWindow(windowEnd uint64) map[uint64]*ruPointBucket {
	a.mu.Lock()
	defer a.mu.Unlock()

	if windowEnd <= a.lastReportedEndTs {
		return nil
	}

	// Rotate buckets that are no longer writable for this report boundary.
	a.rotateBucketsBefore(windowEnd)

	windowStart := windowEnd - ruReportWindowSeconds

	// Take buckets for this window.
	takenBuckets := make(map[uint64]*ruPointBucket, int(ruReportWindowSeconds/ruBaseBucketSeconds))
	for ts := windowStart; ts < windowEnd; ts += ruBaseBucketSeconds {
		if bucket, ok := a.buckets[ts]; ok {
			takenBuckets[ts] = bucket
			delete(a.buckets, ts)
		}
	}
	a.lastReportedEndTs = windowEnd

	// Clean stale buckets.
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
			// Compact to an internal snapshot.
			bucket.compactedCollecting = bucket.collecting.compactWithLimits(maxTopUsers, maxTopSQLsPerUser)
			bucket.collecting = nil
		}
	}
}

// buildReportRecords merges taken buckets and produces final proto records.
// It does not require a lock.
func buildReportRecords(buckets map[uint64]*ruPointBucket, windowStart, windowEnd, itemInterval uint64, keyspaceName []byte) []tipb.TopRURecord {
	singleBucket := windowEnd-windowStart <= itemInterval

	bucketsPerInterval := int((itemInterval + ruBaseBucketSeconds - 1) / ruBaseBucketSeconds)
	intervalPreCapUsers := bucketsPerInterval * maxTopUsers
	intervalPreCapSQLsPerUser := bucketsPerInterval * maxTopSQLsPerUser

	intervalsPerWindow := int((windowEnd - windowStart + itemInterval - 1) / itemInterval)
	mergedPreCapUsers := intervalsPerWindow * ruReportTopNUsers
	mergedPreCapSQLsPerUser := intervalsPerWindow * ruReportTopNSQLsPerUser
	var mergedOutput *ruCollecting
	if !singleBucket {
		mergedOutput = newRUCollectingWithCaps(mergedPreCapUsers, mergedPreCapSQLsPerUser)
	}

	for intervalStart := windowStart; intervalStart < windowEnd; intervalStart += itemInterval {
		intervalCollecting := newRUCollectingWithCaps(intervalPreCapUsers, intervalPreCapSQLsPerUser)
		for bucketStart := intervalStart; bucketStart < intervalStart+itemInterval; bucketStart += ruBaseBucketSeconds {
			bucket, ok := buckets[bucketStart]
			if !ok || bucket.compactedCollecting == nil {
				continue
			}
			// Merge internal structure directly.
			intervalCollecting.mergeFrom(bucket.compactedCollecting, intervalStart, true)
		}
		// Apply TopN and merge into output.
		intervalCompacted := intervalCollecting.compactWithLimits(ruReportTopNUsers, ruReportTopNSQLsPerUser)
		if singleBucket {
			if intervalCompacted == nil {
				return nil
			}
			return intervalCompacted.toTopRURecords(keyspaceName)
		}
		mergedOutput.mergeFrom(intervalCompacted, 0, false)
	}
	// Convert to proto at output.
	return mergedOutput.toTopRURecords(keyspaceName)
}
