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

func formatTaggedMetaName(flushTS, storeID, minDefaultTS, minTS, maxTS, regionID uint64) string {
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
		regionID,
	)
}

// FlushRegion simulates a region flush and emits one metadata file and one log file.
func (f *FlushSim) FlushRegion(ctx context.Context, regionID uint64, targetCheckpoint uint64) (FlushRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state, ok := f.pd.RegionSnapshot(regionID)
	if !ok {
		return FlushRecord{}, fmt.Errorf("region %d not found", regionID)
	}
	if targetCheckpoint <= state.Checkpoint {
		return FlushRecord{}, fmt.Errorf(
			"region %d target checkpoint %d must be greater than current %d",
			regionID,
			targetCheckpoint,
			state.Checkpoint,
		)
	}

	f.seq++
	minTS := state.Checkpoint + 1
	logPath := path.Join(
		"v1/log",
		fmt.Sprintf("store-%d", state.StoreID),
		fmt.Sprintf("region-%d-%08d.log", state.ID, f.seq),
	)
	if err := f.storage.WriteFile(ctx, logPath, nil); err != nil {
		return FlushRecord{}, fmt.Errorf("write log file %s: %w", logPath, err)
	}

	metaPath := path.Join(
		stream.GetStreamBackupMetaPrefix(),
		formatTaggedMetaName(targetCheckpoint, state.StoreID, minTS, minTS, targetCheckpoint, state.ID),
	)
	metadata := &backuppb.Metadata{
		StoreId: int64(state.StoreID),
		MinTs:   minTS,
		MaxTs:   targetCheckpoint,
		FileGroups: []*backuppb.DataFileGroup{
			{
				MinTs: minTS,
				MaxTs: targetCheckpoint,
				DataFilesInfo: []*backuppb.DataFileInfo{
					{
						Path:  logPath,
						MinTs: minTS,
						MaxTs: targetCheckpoint,
					},
				},
			},
		},
	}
	payload, err := metadata.Marshal()
	if err != nil {
		return FlushRecord{}, fmt.Errorf("marshal backupmeta %s: %w", metaPath, err)
	}
	if err := f.storage.WriteFile(ctx, metaPath, payload); err != nil {
		return FlushRecord{}, fmt.Errorf("write backupmeta %s: %w", metaPath, err)
	}

	if _, err := f.pd.setRegionCheckpoint(regionID, targetCheckpoint); err != nil {
		return FlushRecord{}, fmt.Errorf("set region %d checkpoint: %w", regionID, err)
	}

	record := FlushRecord{
		Sequence:     f.seq,
		RegionID:     state.ID,
		StoreID:      state.StoreID,
		FlushTS:      targetCheckpoint,
		MinTS:        minTS,
		MaxTS:        targetCheckpoint,
		MetadataPath: metaPath,
		LogPaths:     []string{logPath},
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
