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

package ddl

import (
	"context"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// Concurrent proof/import reads share a process-wide budget across DDL jobs.
// The estimate covers the body, the declared decoded record views, and
// per-object proof scratch. It is not a hard RSS cap:
// completed global evidence, Lightning and GC retention are measured separately.
var spfreshBulkReads = spfreshBulkReadLimiter{changed: make(chan struct{})}

type spfreshBulkReadLimiter struct {
	mu      sync.Mutex
	used    uint64
	changed chan struct{}
}

// The embedding runtime's MemTotal cap includes its NUMA/cgroup allowance.
// Respect the SQL memory limit as well; host-wide free memory is not our quota.
func spfreshBulkMemoryLimit() uint64 {
	total, err := memory.MemTotal()
	if err != nil {
		return 0
	}
	if limit := memory.ServerMemoryLimit.Load(); limit != 0 {
		total = min(total, limit)
	}
	return total
}

func spfreshBulkReadMemoryBudget() uint64 {
	// Half for authenticated objects, a separate quarter for import admission,
	// and at least a quarter unassigned for SQL, retained evidence and GC.
	budget := spfreshBulkMemoryLimit() / 2
	metrics.SPFreshBulkReadBudgetBytes.Set(float64(budget))
	return budget
}

func spfreshBulkReadWeight(size int64, records uint64) uint64 {
	// The decoder allocates one exact record slice; keys/values borrow the
	// body. Reserve 12.5% body headroom and 2 MiB for bounded header/proof
	// scratch, rather than charging every payload byte as a minimum-size KV.
	const viewBytes = uint64(unsafe.Sizeof(pkdbspfreshbulk.SortedRunRecord{}))
	const scratchBytes = uint64(2 << 20)
	if size < 0 || records > math.MaxUint64/viewBytes {
		return math.MaxUint64
	}
	weight := records * viewBytes
	for _, bytes := range []uint64{uint64(size), uint64(size) / 8, scratchBytes} {
		if weight > math.MaxUint64-bytes {
			return math.MaxUint64
		}
		weight += bytes
	}
	return weight
}

// acquire rejects work that cannot fit, including an unavailable resource
// limit. Waiting cannot make such work safe. Existing work drains on quota
// reductions; no new work is admitted until the reservations fit again.
func (l *spfreshBulkReadLimiter) acquire(ctx context.Context, weight uint64, budget func() uint64) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limit := budget()
		if limit == 0 || weight > limit {
			return nil, errors.Errorf("SPFresh memory reservation %d exceeds budget %d; increase the effective memory quota or rebuild with smaller shards", weight, limit)
		}
		l.mu.Lock()
		if l.used <= limit && weight <= limit-l.used {
			l.used += weight
			l.mu.Unlock()
			return func() {
				l.mu.Lock()
				l.used -= weight
				close(l.changed)
				l.changed = make(chan struct{})
				l.mu.Unlock()
			}, nil
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// readSPFreshBudgetedPinnedSortedRun retains its reservation until the caller has
// finished decoding/consuming all views into the returned bytes. Release once.
func readSPFreshBudgetedPinnedSortedRun(ctx context.Context, store storage.ExternalStorage, prefix string, reference pkdbspfreshbulk.ManifestRef) ([]byte, func(), error) {
	key, err := exactSPFreshBulkObjectKey(prefix, reference.URI)
	if err != nil {
		return nil, nil, err
	}
	reader, err := pkdbspfreshbulk.OpenObject(ctx, store, key)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()
	// At most one bounded header per active proof worker sits outside the
	// object budget. Never acquire a second reservation while holding one.
	recordCount, err := reader.PrefetchSortedRunRecordCount(ctx)
	if err != nil {
		return nil, nil, err
	}
	started := time.Now()
	weight := spfreshBulkReadWeight(reader.Size(), recordCount)
	release, err := spfreshBulkReads.acquire(ctx, weight, spfreshBulkReadMemoryBudget)
	if err != nil {
		return nil, nil, err
	}
	metrics.SPFreshBulkReadBudgetWait.WithLabelValues("proof").Observe(time.Since(started).Seconds())
	metrics.SPFreshBulkReadInFlightBytes.Add(float64(weight))
	acquired := time.Now()
	finish := func() {
		metrics.SPFreshBulkReadInFlightBytes.Sub(float64(weight))
		metrics.SPFreshBulkReadLeaseDuration.Observe(time.Since(acquired).Seconds())
		release()
	}
	encoded, err := reader.ReadPinned(ctx, reference)
	if err != nil {
		finish()
		return nil, nil, err
	}
	return encoded, finish, nil
}
