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

package indexusage

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"
)

// snapshotBatchSize is an implementation candidate pending workload validation.
const snapshotBatchSize = 2048

// The full index ID inventory costs 16 bytes per entry. Reject oversized
// exports before allocating it or holding the collector lock for the scan.
const maxSnapshotIndexes = 1_000_000

type snapshotEntry struct {
	LastUsedAt       *time.Time `json:"last_used_at"`
	TableID          int64      `json:"table_id"`
	IndexID          int64      `json:"index_id"`
	QueryTotal       uint64     `json:"query_total"`
	KVReqTotal       uint64     `json:"kv_req_total"`
	RowAccessTotal   uint64     `json:"row_access_total"`
	PercentageAccess [7]uint64  `json:"percentage_access"`
}

// WriteSnapshot writes cumulative details followed by a footer, without resetting
// counters. Each batch is copied under the read lock and encoded after unlocking.
// Additions after key enumeration can wait until the next snapshot; deleted keys
// are skipped. The result deliberately does not claim a single-instant view.
func (c *Collector) WriteSnapshot(w io.Writer, snapshotTime time.Time) error {
	return c.writeSnapshot(w, snapshotTime, snapshotBatchSize, maxSnapshotIndexes)
}

func (c *Collector) writeSnapshot(w io.Writer, snapshotTime time.Time, batchSize, indexLimit int) error {
	c.RLock()
	count := len(c.indexUsage)
	if count > indexLimit {
		c.RUnlock()
		return fmt.Errorf("index usage snapshot has %d indexes, limit is %d", count, indexLimit)
	}
	keys := make([]GlobalIndexID, 0, count)
	for id := range c.indexUsage {
		keys = append(keys, id)
	}
	c.RUnlock()
	// Stable ordering makes producer artifacts reproducible; it is not an input
	// requirement for consumers and does not extend the collector lock duration.
	slices.SortFunc(keys, func(a, b GlobalIndexID) int {
		if a.TableID < b.TableID {
			return -1
		}
		if a.TableID > b.TableID {
			return 1
		}
		if a.IndexID < b.IndexID {
			return -1
		}
		if a.IndexID > b.IndexID {
			return 1
		}
		return 0
	})
	encoder := json.NewEncoder(w)
	rows := 0
	batch := make([]snapshotEntry, 0, batchSize)
	for offset := 0; offset < len(keys); offset += batchSize {
		batch = batch[:0]
		c.RLock()
		for _, id := range keys[offset:min(offset+batchSize, len(keys))] {
			if sample, ok := c.indexUsage[id]; ok {
				entry := snapshotEntry{TableID: id.TableID, IndexID: id.IndexID, QueryTotal: sample.QueryTotal,
					KVReqTotal: sample.KvReqTotal, RowAccessTotal: sample.RowAccessTotal, PercentageAccess: sample.PercentageAccess}
				if !sample.LastUsedAt.IsZero() {
					t := sample.LastUsedAt.UTC()
					entry.LastUsedAt = &t
				}
				batch = append(batch, entry)
			}
		}
		c.RUnlock()
		for _, entry := range batch {
			if err := encoder.Encode(entry); err != nil {
				return err
			}
			rows++
		}
	}
	return encoder.Encode(struct {
		SnapshotTime time.Time `json:"snapshot_time"`
		RecordType   string    `json:"record_type"`
		RowCount     int       `json:"row_count"`
	}{SnapshotTime: snapshotTime.UTC().Truncate(time.Microsecond), RecordType: "footer", RowCount: rows})
}
