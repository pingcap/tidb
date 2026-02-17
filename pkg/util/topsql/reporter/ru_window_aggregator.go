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

type ruBucketState uint8

const (
	ruBucketStateCollecting ruBucketState = iota
	ruBucketStateCompacted
)

type ruPointBucket struct {
	startTs    uint64
	state      ruBucketState
	collecting *ruCollecting
	records    []tipb.TopRURecord
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

func (a *ruWindowAggregator) addSecondBatch(ts uint64, increments stmtstats.RUIncrementMap) {
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

	a.rotateBucketsBeforeLocked(bucketStart)

	bucket, ok := a.buckets[bucketStart]
	if !ok {
		bucket = &ruPointBucket{
			startTs:    bucketStart,
			state:      ruBucketStateCollecting,
			collecting: newRUCollectingWithCaps(ruOpenPreTopNUsers, ruOpenPreTopNSQLsPerUser),
		}
		a.buckets[bucketStart] = bucket
	}
	if bucket.state != ruBucketStateCollecting || bucket.collecting == nil {
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

	a.mu.Lock()
	defer a.mu.Unlock()

	if windowEnd <= a.lastReportedEndTs {
		return nil
	}

	// Rotate all buckets that are no longer writable for this report boundary.
	a.rotateBucketsBeforeLocked(windowEnd)

	windowStart := windowEnd - ruReportWindowSeconds
	records := a.buildReportRecordsLocked(windowStart, windowEnd, granularitySec, keyspaceName)

	// Consume the current 60s window.
	for ts := windowStart; ts < windowEnd; ts += ruBaseBucketSeconds {
		delete(a.buckets, ts)
	}
	a.lastReportedEndTs = windowEnd

	// Defensive cleanup for stale buckets.
	for ts := range a.buckets {
		if ts < windowStart {
			delete(a.buckets, ts)
		}
	}

	return records
}

func (a *ruWindowAggregator) rotateBucketsBeforeLocked(boundaryStart uint64) {
	for _, bucket := range a.buckets {
		if bucket.state != ruBucketStateCollecting {
			continue
		}
		if bucket.startTs+ruBaseBucketSeconds <= boundaryStart {
			a.rotateBucketLocked(bucket)
		}
	}
}

func (a *ruWindowAggregator) rotateBucketLocked(bucket *ruPointBucket) {
	if bucket == nil || bucket.state != ruBucketStateCollecting {
		return
	}
	if bucket.collecting == nil {
		bucket.state = ruBucketStateCompacted
		return
	}
	bucket.records = bucket.collecting.getReportRecordsWithLimits(nil, ruCompactedTopNUsers, ruCompactedTopNSQLsPerUser)
	bucket.collecting = nil
	bucket.state = ruBucketStateCompacted
}

func (a *ruWindowAggregator) buildReportRecordsLocked(windowStart, windowEnd, granularitySec uint64, keyspaceName []byte) []tipb.TopRURecord {
	mergedOutput := newRUCollectingWithCaps(ruReportMergePreTopNUsers, ruReportMergePreTopNSQLsPerUser)
	for groupStart := windowStart; groupStart < windowEnd; groupStart += granularitySec {
		groupCollecting := newRUCollectingWithCaps(ruReportMergePreTopNUsers, ruReportMergePreTopNSQLsPerUser)
		for ts := groupStart; ts < groupStart+granularitySec; ts += ruBaseBucketSeconds {
			bucket, ok := a.buckets[ts]
			if !ok || len(bucket.records) == 0 {
				continue
			}
			mergeRURecordsIntoCollecting(groupCollecting, bucket.records, groupStart, true)
		}
		groupRecords := groupCollecting.getReportRecordsWithLimits(keyspaceName, ruReportTopNUsers, ruReportTopNSQLsPerUser)
		mergeRURecordsIntoCollecting(mergedOutput, groupRecords, 0, false)
	}
	return mergedOutput.getReportRecordsWithLimits(keyspaceName, ruReportOutputMaxUsers, ruReportOutputMaxSQLsPerUser)
}

func mergeRURecordsIntoCollecting(dst *ruCollecting, records []tipb.TopRURecord, timestamp uint64, rewriteTimestamp bool) {
	if dst == nil || len(records) == 0 {
		return
	}
	for _, rec := range records {
		for _, item := range rec.Items {
			ts := item.TimestampSec
			if rewriteTimestamp {
				ts = timestamp
			}
			dst.addRecordItem(ts, rec.User, rec.SqlDigest, rec.PlanDigest, &stmtstats.RUIncrement{
				TotalRU:      item.TotalRu,
				ExecCount:    item.ExecCount,
				ExecDuration: item.ExecDuration,
			})
		}
	}
}
