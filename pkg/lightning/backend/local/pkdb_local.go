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

package local

import (
	"context"
	"math"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func tableIDsForUKPrecheckConflict(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) []int64 {
	if tableInfo.Partition == nil || indexInfo.Global {
		return []int64{tableInfo.ID}
	}

	defs := tableInfo.Partition.Definitions
	tids := make([]int64, 0, len(defs))
	for _, def := range defs {
		tids = append(tids, def.ID)
	}
	return tids
}

// BuildIndexKeyRangesForUKPrecheck return unique indices's keyRanges on local engine
func (local *Backend) BuildIndexKeyRangesForUKPrecheck(indexEngineUUID uuid.UUID, tableInfo *model.TableInfo) ([]kv.KeyRange, error) {
	ranges := make([]kv.KeyRange, 0, len(tableInfo.Indices)+1)
	indexEngine := local.engineMgr.rLockEngine(indexEngineUUID)
	if indexEngine == nil {
		return nil, errors.Errorf("engine '%s' not found", indexEngineUUID)
	}
	defer indexEngine.rUnlock()

	for _, indexInfo := range tableInfo.Indices {
		if indexInfo.State != model.StatePublic || !indexInfo.Unique {
			continue
		}

		for _, tableID := range tableIDsForUKPrecheckConflict(tableInfo, indexInfo) {
			startKey, lastKey := tablecodec.GetTableIndexKeyRange(tableID, indexInfo.ID)
			startKey, lastKey = local.tikvCodec.EncodeRange(startKey, lastKey)
			startKey, lastKey, err := indexEngine.GetFirstAndLastKey(startKey, lastKey)
			if err != nil {
				return nil, err
			}
			if startKey == nil {
				continue
			}
			ranges = append(ranges, kv.KeyRange{StartKey: startKey, EndKey: nextKey(lastKey)})
		}
	}
	return ranges, nil
}

// BuildClusteredPKRangesForPrecheck return primary key's keyRanges on local engine
func (local *Backend) BuildClusteredPKRangesForPrecheck(dataEngineUUID uuid.UUID) (kv.KeyRange, error) {
	dataEngine := local.engineMgr.rLockEngine(dataEngineUUID)
	if dataEngine == nil {
		return kv.KeyRange{}, errors.Errorf("engine '%s' not found", dataEngineUUID)
	}
	defer dataEngine.rUnlock()
	startKey, endKey, err := dataEngine.GetKeyRange()
	if err != nil {
		return kv.KeyRange{}, err
	}
	if startKey != nil {
		return kv.KeyRange{StartKey: startKey, EndKey: endKey}, nil
	}
	return kv.KeyRange{}, nil
}

// PrecheckConflictForImportEngineRanges scans the specified key ranges of a
// closed local engine checks duplicate logical key on both historical data
// on tikv and local incremental data
func (local *Backend) PrecheckConflictForImportEngineRanges(
	ctx context.Context,
	engineUUID uuid.UUID,
	ranges []kv.KeyRange,
) error {
	return local.engineMgr.precheckConflictForImportEngineRanges(ctx, engineUUID, ranges)
}

func (local *Backend) getTiKVScanner(ctx context.Context, start, end []byte) (*txnsnapshot.Scanner, error) {
	if local.tikvCli == nil {
		return nil, errors.New("tikv client is not available")
	}
	snapshot := local.tikvCli.GetSnapshot(math.MaxUint64)
	snapshot.SetKeyOnly(true)
	iter, err := snapshot.Iter(start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scanner, ok := iter.(*txnsnapshot.Scanner)
	if !ok {
		iter.Close()
		return nil, errors.Errorf("unexpected tikv iterator type %T", iter)
	}
	return scanner, nil
}
