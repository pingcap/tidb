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

package drr

import (
	"context"
	"fmt"
	"path"
	"sync"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// FlushSim simulates log backup file generation for region flushes.
type FlushSim struct {
	mu      sync.Mutex
	pd      *PDSim
	storage storeapi.Storage
	seq     uint64
	records []FlushRecord
}

func NewFlushSim(pd *PDSim, storage storeapi.Storage) *FlushSim {
	return &FlushSim{pd: pd, storage: storage}
}

func formatTaggedMetaName(flushTS, storeID, minDefaultTS, minTS, maxTS, suffixToken uint64) string {
	return fmt.Sprintf(
		"%016x%016x-%c%016x%c%016x%c%016x%c%016x.meta",
		flushTS,
		storeID,
		backupmetas.NameMinBeginTSTag,
		minDefaultTS,
		backupmetas.NameMinTSTag,
		minTS,
		backupmetas.NameMaxTSTag,
		maxTS,
		regionIDTag,
		suffixToken,
	)
}

// FlushStore simulates one store flush and emits one metadata file with all regions on that store.
func (f *FlushSim) FlushStore(ctx context.Context, storeID uint64, targetCheckpoint uint64) (FlushRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	states, err := f.pd.RegionSnapshotsOnStore(storeID)
	if err != nil {
		return FlushRecord{}, err
	}
	if len(states) == 0 {
		return FlushRecord{}, fmt.Errorf("store %d has no regions to flush", storeID)
	}

	minTS := uint64(^uint64(0))
	logPaths := make([]string, 0, len(states))
	regionIDs := make([]uint64, 0, len(states))
	groups := make([]*backuppb.DataFileGroup, 0, len(states))

	f.seq++
	flushSeq := f.seq
	for _, state := range states {
		if targetCheckpoint <= state.Checkpoint {
			return FlushRecord{}, fmt.Errorf(
				"store %d region %d target checkpoint %d must be greater than current %d",
				storeID,
				state.ID,
				targetCheckpoint,
				state.Checkpoint,
			)
		}
		rMinTS := state.Checkpoint + 1
		if rMinTS < minTS {
			minTS = rMinTS
		}

		logPath := path.Join(
			"v1/log",
			fmt.Sprintf("store-%d", storeID),
			fmt.Sprintf("flush-%08d-region-%d.log", flushSeq, state.ID),
		)
		if err := f.storage.WriteFile(ctx, logPath, nil); err != nil {
			return FlushRecord{}, fmt.Errorf("write log file %s: %w", logPath, err)
		}

		logPaths = append(logPaths, logPath)
		regionIDs = append(regionIDs, state.ID)
		groups = append(groups, &backuppb.DataFileGroup{
			MinTs: rMinTS,
			MaxTs: targetCheckpoint,
			DataFilesInfo: []*backuppb.DataFileInfo{
				{
					Path:  logPath,
					MinTs: rMinTS,
					MaxTs: targetCheckpoint,
				},
			},
		})
	}

	metaPath := path.Join(
		stream.GetStreamBackupMetaPrefix(),
		formatTaggedMetaName(targetCheckpoint, storeID, minTS, minTS, targetCheckpoint, flushSeq),
	)
	metadata := &backuppb.Metadata{
		StoreId:    int64(storeID),
		MinTs:      minTS,
		MaxTs:      targetCheckpoint,
		FileGroups: groups,
	}
	payload, err := metadata.Marshal()
	if err != nil {
		return FlushRecord{}, fmt.Errorf("marshal backupmeta %s: %w", metaPath, err)
	}
	if err := f.storage.WriteFile(ctx, metaPath, payload); err != nil {
		return FlushRecord{}, fmt.Errorf("write backupmeta %s: %w", metaPath, err)
	}

	for _, state := range states {
		if _, err := f.pd.flushRegionByStore(ctx, storeID, state.ID, targetCheckpoint); err != nil {
			return FlushRecord{}, fmt.Errorf("flush region %d by store %d: %w", state.ID, storeID, err)
		}
	}

	record := FlushRecord{
		Sequence:     flushSeq,
		StoreID:      storeID,
		RegionIDs:    regionIDs,
		FlushTS:      targetCheckpoint,
		MinTS:        minTS,
		MaxTS:        targetCheckpoint,
		MetadataPath: metaPath,
		LogPaths:     logPaths,
	}
	f.records = append(f.records, record)
	return record.clone(), nil
}

// Records returns all flush records in creation order.
func (f *FlushSim) Records() []FlushRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]FlushRecord, 0, len(f.records))
	for _, r := range f.records {
		result = append(result, r.clone())
	}
	return result
}

// RecordsUpTo returns flush records with FlushTS <= tso.
func (f *FlushSim) RecordsUpTo(tso uint64) []FlushRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]FlushRecord, 0, len(f.records))
	for _, r := range f.records {
		if r.FlushTS <= tso {
			result = append(result, r.clone())
		}
	}
	return result
}
