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

package globalsort

import (
	"context"

	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"go.uber.org/atomic"
)

// MemoryEngine ingests KV pairs that are already sorted and still in memory.
// An import small enough to be encoded by a single subtask doesn't need to send
// its KVs through the object store and read them back, which is what the
// external engine does.
type MemoryEngine struct {
	kvs      []simplesst.KVPair
	ts       uint64
	startKey tidbkv.Key
	// endKey is exclusive, like the external engine's.
	endKey tidbkv.Key
	// totalSize is the real size of the KVs, only used for logging.
	totalSize  int64
	totalCount int64
	conflict   engineapi.ConflictInfo

	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

var _ engineapi.Engine = (*MemoryEngine)(nil)

// NewMemoryEngine builds an engine over kvs, which must be sorted by key.
func NewMemoryEngine(
	kvs []simplesst.KVPair,
	ts uint64,
	totalSize int64,
	conflict engineapi.ConflictInfo,
) *MemoryEngine {
	e := &MemoryEngine{
		kvs:             kvs,
		ts:              ts,
		totalSize:       totalSize,
		totalCount:      int64(len(kvs)),
		conflict:        conflict,
		importedKVSize:  atomic.NewInt64(0),
		importedKVCount: atomic.NewInt64(0),
	}
	if len(kvs) > 0 {
		e.startKey = tidbkv.Key(kvs[0].Key)
		e.endKey = tidbkv.Key(kvs[len(kvs)-1].Key).Next()
	}
	return e
}

// ID implements engineapi.Engine.
func (*MemoryEngine) ID() string {
	return "memory"
}

// LoadIngestData implements engineapi.Engine. All KVs are already in memory, so
// they go out as a single batch covering the whole key range.
func (e *MemoryEngine) LoadIngestData(ctx context.Context, outCh chan<- engineapi.DataAndRanges) error {
	if len(e.kvs) == 0 {
		return nil
	}
	data := &MemoryIngestData{
		kvs:             e.kvs,
		ts:              e.ts,
		released:        atomic.NewBool(false),
		refCnt:          atomic.NewInt64(0),
		importedKVSize:  e.importedKVSize,
		importedKVCount: e.importedKVCount,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case outCh <- engineapi.DataAndRanges{
		Data:         data,
		SortedRanges: []engineapi.Range{{Start: e.startKey, End: e.endKey}},
	}:
	}
	return nil
}

// KVStatistics implements engineapi.Engine.
//
// The size reported here is the same estimate the ingest step's planner puts in
// every range group (config.DefaultBatchSize), not the real one: ImportEngine
// uses it to decide whether the data needs more than one region, and reporting
// the real size would make this the only engine that skips that decision.
func (e *MemoryEngine) KVStatistics() (totalKVSize int64, totalKVCount int64) {
	if len(e.kvs) == 0 {
		return 0, 0
	}
	return int64(config.DefaultBatchSize), e.totalCount
}

// ImportedStatistics implements engineapi.Engine.
func (e *MemoryEngine) ImportedStatistics() (importedKVSize int64, importedKVCount int64) {
	return e.importedKVSize.Load(), e.importedKVCount.Load()
}

// ConflictInfo implements engineapi.Engine.
func (e *MemoryEngine) ConflictInfo() engineapi.ConflictInfo {
	return e.conflict
}

// GetKeyRange implements engineapi.Engine.
func (e *MemoryEngine) GetKeyRange() (startKey []byte, endKey []byte, err error) {
	return e.startKey, e.endKey, nil
}

// GetRegionSplitKeys implements engineapi.Engine. Only the boundaries are
// returned: an engine that fits in memory is never worth pre-splitting.
func (e *MemoryEngine) GetRegionSplitKeys() ([][]byte, error) {
	return [][]byte{e.startKey, e.endKey}, nil
}

// Close implements engineapi.Engine.
func (e *MemoryEngine) Close() error {
	e.kvs = nil
	return nil
}
