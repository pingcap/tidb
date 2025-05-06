// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engineapi

import (
	"context"
)

// Range contains a start key and an end key. The Range's key should not be
// encoded by duplicate detection.
type Range struct {
	Start []byte
	End   []byte // end is always exclusive except import_sstpb.SSTMeta
}

// DataAndRanges is a pair of IngestData and list of Range. Each Range will
// become a regionJob, and the regionJob will read data from Data field.
type DataAndRanges struct {
	Data         IngestData
	SortedRanges []Range
}

// Engine describes the common interface of local and external engine that
// local backend uses.
type Engine interface {
	// ID is the identifier of an engine.
	ID() string
	// LoadIngestData sends DataAndRanges to outCh.
	LoadIngestData(ctx context.Context, outCh chan<- DataAndRanges) error
	// KVStatistics returns the total kv size and total kv count.
	KVStatistics() (totalKVSize int64, totalKVCount int64)
	// ImportedStatistics returns the imported kv size and imported kv count.
	ImportedStatistics() (importedKVSize int64, importedKVCount int64)
	// GetKeyRange returns the key range [startKey, endKey) of the engine. If the
	// duplicate detection is enabled, the keys in engine are encoded by duplicate
	// detection but the returned keys should not be encoded.
	GetKeyRange() (startKey []byte, endKey []byte, err error)
	// GetRegionSplitKeys checks the KV distribution of the Engine and returns the
	// keys that can be used as region split keys. If the duplicate detection is
	// enabled, the keys stored in engine are encoded by duplicate detection but the
	// returned keys should not be encoded.
	//
	// Currently, the start/end key of this import should also be included in the
	// returned split keys.
	GetRegionSplitKeys() ([][]byte, error)
	Close() error
}

// ConflictInfo records the KV conflict information.
// to help describe how we do conflict resolution, we separate 'conflict KV' out
// from 'duplicate KV':
//   - 'duplicate KV' means the KV pairs that have the same key, including keys
//     come from PK/UK/non-UK.
//   - 'conflict KV' means the KV pairs that have the same key, and they can cause
//     conflict ROWS in the table, including keys come from PK/UK. non-UK keys may
//     became duplicate when PK keys are duplicated, but we don't need to consider
//     them when resolving conflicts.
type ConflictInfo struct {
	// Count is the recorded count of conflict KV pairs, either PK or UK.
	Count uint64 `json:"count,omitempty"`
	// Files is the list of files that contain conflict KV pairs.
	// it's in the same format as normal KV files.
	Files []string `json:"files,omitempty"`
}

// Merge merges the other ConflictInfo into this one.
func (c *ConflictInfo) Merge(other *ConflictInfo) {
	c.Count += other.Count
	c.Files = append(c.Files, other.Files...)
}

// OnDuplicateKey is the action when a duplicate key is found during global sort.
// Note: lightning also have similar concept call OnDup, they have different semantic.
// we put it here to avoid import cycle.
type OnDuplicateKey int

const (
	// OnDuplicateKeyIgnore means ignore the duplicate key.
	// this is the current behavior, we will keep it before we fully switch to
	// below 3 options.
	OnDuplicateKeyIgnore OnDuplicateKey = iota
	// OnDuplicateKeyRecord means record the duplicate keys to external store.
	// depends on the step, we might only record when number of duplicates are larger
	// than 2, since we only see the local sorted data, not the global sorted data,
	// such as during encoding and merge sorting.
	// we use this for PK and UK in import-into.
	OnDuplicateKeyRecord
	// OnDuplicateKeyRemove means remove the duplicate key silently.
	// we use this action for non-unique secondary indexes in import-into.
	OnDuplicateKeyRemove
	// OnDuplicateKeyError return an error when a duplicate key is found.
	// may use this for add unique index.
	OnDuplicateKeyError
)

// String implements fmt.Stringer interface.
func (o OnDuplicateKey) String() string {
	switch o {
	case OnDuplicateKeyIgnore:
		return "ignore"
	case OnDuplicateKeyRecord:
		return "record"
	case OnDuplicateKeyRemove:
		return "remove"
	case OnDuplicateKeyError:
		return "error"
	}
	return "unknown"
}
