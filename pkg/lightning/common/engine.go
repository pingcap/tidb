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

package common

import (
	"context"

	"github.com/pingcap/tidb/pkg/lightning/log"
)

// Range contains a start key and an end key.
type Range struct {
	Start []byte
	End   []byte // end is always exclusive except import_sstpb.SSTMeta
}

// Engine describes the common interface of local and external engine that
// local backend uses.
type Engine interface {
	// ID is the identifier of an engine.
	ID() string
	// LoadIngestData sends DataAndRange to outCh. Implementation may choose smaller
	// ranges than given regionRanges, and data is contained in its range.
	LoadIngestData(ctx context.Context, regionRanges []Range, outCh chan<- DataAndRange) error
	// KVStatistics returns the total kv size and total kv count.
	KVStatistics() (totalKVSize int64, totalKVCount int64)
	// ImportedStatistics returns the imported kv size and imported kv count.
	ImportedStatistics() (importedKVSize int64, importedKVCount int64)
	// GetKeyRange returns the key range [startKey, endKey) of the engine. If the
	// duplicate detection is enabled, the keys in engine are encoded by duplicate
	// detection but the returned keys should not be encoded.
	GetKeyRange() (startKey []byte, endKey []byte, err error)
	// SplitRanges splits the range [startKey, endKey) into multiple ranges. If the
	// duplicate detection is enabled, the keys in engine are encoded by duplicate
	// detection but the returned keys should not be encoded.
	SplitRanges(startKey, endKey []byte, sizeLimit, keysLimit int64, logger log.Logger) ([]Range, error)
	Close() error
}
