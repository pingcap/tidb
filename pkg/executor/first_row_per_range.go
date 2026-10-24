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
	"bytes"

	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
)

// firstRowPerRangeStoreBatchSize is how many range tasks go to one store in a
// single coprocessor request when a reader reads the first row of each range.
// A larger tidb_store_batch_size takes precedence.
const firstRowPerRangeStoreBatchSize = 256

// setFirstRowPerRangeKeyRanges sets the key ranges of a reader that reads only
// the first row of each range. Each range expects one row, which lets the
// coprocessor batch the per-range tasks by store. Duplicate ranges are dropped,
// because each would return the same row again.
func setFirstRowPerRangeKeyRanges(builder *distsql.RequestBuilder, ranges []kv.KeyRange) {
	deduped := ranges[:0:0]
	for i, r := range ranges {
		if i > 0 && bytes.Equal(r.StartKey, ranges[i-1].StartKey) && bytes.Equal(r.EndKey, ranges[i-1].EndKey) {
			continue
		}
		deduped = append(deduped, r)
	}
	hints := make([]int, len(deduped))
	for i := range hints {
		hints[i] = 1
	}
	builder.SetKeyRangesWithHints(deduped, hints)
}

// adjustFirstRowPerRangeRequest makes every key range its own coprocessor task,
// so the pushed-down Limit 1 applies per range, and batches those tasks by
// store so they don't each cost a round trip. The tasks return rows in no
// particular order, so the request doesn't keep order or page.
func adjustFirstRowPerRangeRequest(kvReq *kv.Request, dctx *distsqlctx.DistSQLContext) {
	kvReq.RangesPerTask = 1
	kvReq.KeepOrder = false
	kvReq.Paging.Enable = false
	kvReq.StoreBatchSize = max(firstRowPerRangeStoreBatchSize, dctx.StoreBatchSize)
	// The Limit 1 below the scan makes the request builder drop to one worker
	// for a plain scan; that sizing assumes one task, not one task per range.
	kvReq.Concurrency = dctx.DistSQLConcurrency
}
