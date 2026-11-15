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
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/membuf"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lightningconfig "github.com/pingcap/tidb/pkg/lightning/config"
)

// The owner keeps one reference through ImportIngestEngine, including job
// generation and retries. Region jobs borrow additional references. Records
// already contain physical API V2 keys, exactly as the old local writer stored
// them; no keyspace prefix conversion, sorting or payload copying is needed.
type spfreshBulkImportEngine struct {
	id            string
	role          string
	records       []pkdbspfreshbulk.SortedRunRecord
	ts            uint64
	totalBytes    int64
	splitKeys     [][]byte
	refs          atomic.Int64
	importedBytes atomic.Int64
	importedCount atomic.Int64
}

var _ common.Engine = (*spfreshBulkImportEngine)(nil)
var _ common.IngestData = (*spfreshBulkImportEngine)(nil)

// batch is nonempty, authenticated and structurally validated by the caller.
func newSPFreshBulkImportEngine(batch spfreshBulkSortedRun, uri string, ts uint64) *spfreshBulkImportEngine {
	e := &spfreshBulkImportEngine{
		id: fmt.Sprintf("%s/%x", uri, batch.records[0].Key), role: batch.header.RunRole,
		records: batch.records, ts: ts,
	}
	e.refs.Store(1)
	var size, count int64
	for _, record := range e.records {
		if count == 0 {
			e.splitKeys = append(e.splitKeys, record.Key)
		}
		n := int64(len(record.Key)) + int64(len(record.Value))
		e.totalBytes += n
		size += n
		count++
		if size >= int64(lightningconfig.SplitRegionSize) || count >= int64(lightningconfig.SplitRegionKeys) {
			size, count = 0, 0
		}
	}
	last := e.records[len(e.records)-1].Key
	end := make([]byte, len(last)+1)
	copy(end, last) // append 0 for the exclusive upper bound of an index key.
	e.splitKeys = append(e.splitKeys, end)
	return e
}

func (e *spfreshBulkImportEngine) ID() string { return e.id }

func (e *spfreshBulkImportEngine) KVStatistics() (int64, int64) {
	return e.totalBytes, int64(len(e.records))
}

func (e *spfreshBulkImportEngine) ImportedStatistics() (int64, int64) {
	return e.importedBytes.Load(), e.importedCount.Load()
}

func (*spfreshBulkImportEngine) ConflictInfo() common.ConflictInfo { return common.ConflictInfo{} }

func (e *spfreshBulkImportEngine) GetKeyRange() ([]byte, []byte, error) {
	return e.splitKeys[0], e.splitKeys[len(e.splitKeys)-1], nil
}

func (e *spfreshBulkImportEngine) GetRegionSplitKeys() ([][]byte, error) {
	return e.splitKeys, nil
}

func (e *spfreshBulkImportEngine) LoadIngestData(ctx context.Context, out chan<- common.DataAndRanges) error {
	ranges := make([]common.Range, 0, len(e.splitKeys)-1)
	for i := 1; i < len(e.splitKeys); i++ {
		ranges = append(ranges, common.Range{Start: e.splitKeys[i-1], End: e.splitKeys[i]})
	}
	select {
	case out <- common.DataAndRanges{Data: e, SortedRanges: ranges}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *spfreshBulkImportEngine) bounds(lower, upper []byte) (int, int) {
	start, end := 0, len(e.records)
	if len(lower) != 0 {
		start = sort.Search(end, func(i int) bool { return bytes.Compare(e.records[i].Key, lower) >= 0 })
	}
	if len(upper) != 0 {
		end = start + sort.Search(end-start, func(i int) bool { return bytes.Compare(e.records[start+i].Key, upper) >= 0 })
	}
	return start, end
}

func (e *spfreshBulkImportEngine) GetFirstAndLastKey(lower, upper []byte) ([]byte, []byte, error) {
	start, end := e.bounds(lower, upper)
	if start == end {
		return nil, nil, nil
	}
	return e.records[start].Key, e.records[end-1].Key, nil
}

func (e *spfreshBulkImportEngine) NewIter(ctx context.Context, lower, upper []byte, _ *membuf.Pool) common.ForwardIter {
	start, end := e.bounds(lower, upper)
	return &spfreshBulkMemoryIter{records: e.records[start:end], pos: -1, err: ctx.Err()}
}

func (e *spfreshBulkImportEngine) GetTS() uint64 { return e.ts }
func (e *spfreshBulkImportEngine) IncRef()       { e.refs.Add(1) }
func (e *spfreshBulkImportEngine) DecRef() {
	if e.refs.Add(-1) == 0 {
		e.records = nil
		e.splitKeys = nil
	}
}

func (e *spfreshBulkImportEngine) Finish(size, count int64) {
	e.importedBytes.Add(size)
	e.importedCount.Add(count)
}

// Close releases the owner's reference, once all import jobs have drained.
func (e *spfreshBulkImportEngine) Close() error {
	e.DecRef()
	return nil
}

type spfreshBulkMemoryIter struct {
	records []pkdbspfreshbulk.SortedRunRecord
	pos     int
	err     error
}

func (i *spfreshBulkMemoryIter) First() bool { i.pos = 0; return i.Valid() }
func (i *spfreshBulkMemoryIter) Valid() bool {
	return i.err == nil && i.pos >= 0 && i.pos < len(i.records)
}
func (i *spfreshBulkMemoryIter) Next() bool    { i.pos++; return i.Valid() }
func (i *spfreshBulkMemoryIter) Key() []byte   { return i.records[i.pos].Key }
func (i *spfreshBulkMemoryIter) Value() []byte { return i.records[i.pos].Value }
func (i *spfreshBulkMemoryIter) Error() error  { return i.err }
func (i *spfreshBulkMemoryIter) Close() error  { i.records = nil; return nil }

// Views belong to the engine, not an RPC batch; retries may read them again.
func (*spfreshBulkMemoryIter) ReleaseBuf() {}
