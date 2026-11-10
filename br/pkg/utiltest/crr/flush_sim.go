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

package testutil

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// FlushSim simulates log backup file generation for region flushes.
type FlushSim struct {
	mu      sync.Mutex
	pd      *PDSim
	seed    int64
	storage storeapi.Storage
	seq     uint64
	records []FlushRecord
	stores  map[uint64]*sync.Mutex
}

type regionFiles struct {
	groups    []*backuppb.DataFileGroup
	logPaths  []string
	regionIDs []uint64
	minTS     uint64
	maxTS     uint64
}

func NewFlushSimWithTestContext(pd *PDSim, storage storeapi.Storage, tc *TestContext) *FlushSim {
	return &FlushSim{
		pd:      pd,
		seed:    tc.Seed(),
		storage: storage,
		stores:  make(map[uint64]*sync.Mutex),
	}
}

func formatTaggedMetaName(flushTS, storeID, minDefaultTS, minTS, maxTS, suffixToken uint64) string {
	return fmt.Sprintf(
		"%016X%016X-%c%016X%c%016X%c%016X%c%016X.meta",
		flushTS,
		storeID,
		backupmetas.NameMinBeginTsInDefaultCfTag,
		minDefaultTS,
		backupmetas.NameMinTSTag,
		minTS,
		backupmetas.NameMaxTSTag,
		maxTS,
		regionIDTag,
		suffixToken,
	)
}

func pickRegionTSRange(rng *deterministicRNG, globalCheckpoint, latestTS uint64) (uint64, uint64) {
	minTS := rng.Uint64InRange(globalCheckpoint, latestTS)
	maxTS := rng.Uint64InRange(globalCheckpoint, latestTS)
	if maxTS < minTS {
		minTS, maxTS = maxTS, minTS
	}
	return minTS, maxTS
}

func (f *FlushSim) buildRegionFiles(
	ctx context.Context,
	storeID uint64,
	flushSeq uint64,
	globalCheckpoint uint64,
	latestTS uint64,
	states []RegionState,
	rng *deterministicRNG,
) (regionFiles, error) {
	result := regionFiles{
		groups:    make([]*backuppb.DataFileGroup, 0, len(states)),
		logPaths:  make([]string, 0, len(states)),
		regionIDs: make([]uint64, 0, len(states)),
		minTS:     ^uint64(0),
	}

	for _, state := range states {
		rMinTS, rMaxTS := pickRegionTSRange(rng, globalCheckpoint, latestTS)
		if rMinTS < result.minTS {
			result.minTS = rMinTS
		}
		if rMaxTS > result.maxTS {
			result.maxTS = rMaxTS
		}

		logPath := path.Join(
			"v1/log",
			fmt.Sprintf("store-%d", storeID),
			fmt.Sprintf("flush-%08d-region-%d.log", flushSeq, state.ID),
		)
		if err := f.storage.WriteFile(ctx, logPath, nil); err != nil {
			return regionFiles{}, fmt.Errorf("write log file %s: %w", logPath, err)
		}

		result.logPaths = append(result.logPaths, logPath)
		result.regionIDs = append(result.regionIDs, state.ID)
		result.groups = append(result.groups, &backuppb.DataFileGroup{
			MinTs: rMinTS,
			MaxTs: rMaxTS,
			Path:  logPath,
			DataFilesInfo: []*backuppb.DataFileInfo{
				{
					MinTs: rMinTS,
					MaxTs: rMaxTS,
				},
			},
		})
	}
	return result, nil
}

func (f *FlushSim) lockStore(storeID uint64) func() {
	f.mu.Lock()
	storeMu, ok := f.stores[storeID]
	if !ok {
		storeMu = &sync.Mutex{}
		f.stores[storeID] = storeMu
	}
	f.mu.Unlock()

	storeMu.Lock()
	return storeMu.Unlock
}

func (f *FlushSim) nextFlushSequence() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	return f.seq
}

func (f *FlushSim) flushRNG(storeID, flushSeq uint64) *deterministicRNG {
	return newDeterministicRNG(f.seed, fmt.Sprintf("flush-sim-store-%d-flush-%d", storeID, flushSeq))
}

func (f *FlushSim) appendRecord(record FlushRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()

	insertAt := len(f.records)
	for insertAt > 0 && f.records[insertAt-1].Sequence > record.Sequence {
		insertAt--
	}
	f.records = append(f.records, FlushRecord{})
	copy(f.records[insertAt+1:], f.records[insertAt:])
	f.records[insertAt] = record
}

func (f *FlushSim) writeBackupMeta(
	ctx context.Context,
	storeID uint64,
	flushSeq uint64,
	flushTS uint64,
	checkpointTS uint64,
	files regionFiles,
) (string, error) {
	metaPath := path.Join(
		stream.GetStreamBackupMetaPrefix(),
		formatTaggedMetaName(flushTS, storeID, files.minTS, files.minTS, files.maxTS, flushSeq),
	)
	metadata := &backuppb.Metadata{
		StoreId:    int64(storeID),
		MinTs:      files.minTS,
		MaxTs:      checkpointTS,
		FileGroups: files.groups,
	}
	payload, err := metadata.Marshal()
	if err != nil {
		return "", fmt.Errorf("marshal backupmeta %s: %w", metaPath, err)
	}
	if err := f.storage.WriteFile(ctx, metaPath, payload); err != nil {
		return "", fmt.Errorf("write backupmeta %s: %w", metaPath, err)
	}
	return metaPath, nil
}

func (f *FlushSim) flushRegions(
	ctx context.Context,
	storeID uint64,
	checkpointTS uint64,
) error {
	if _, err := f.pd.flushStore(ctx, storeID, checkpointTS); err != nil {
		return fmt.Errorf("flush store %d: %w", storeID, err)
	}
	return nil
}

// FlushStore simulates one store flush and emits one metadata file with all regions on that store.
func (f *FlushSim) FlushStore(ctx context.Context, storeID uint64) (FlushRecord, error) {
	failpoint.InjectCall("begin-flush-store")

	unlockStore := f.lockStore(storeID)
	defer unlockStore()

	states, err := f.pd.RegionSnapshotsOnStore(storeID)
	if err != nil {
		return FlushRecord{}, err
	}
	if len(states) == 0 {
		return FlushRecord{}, fmt.Errorf("store %d has no regions to flush", storeID)
	}

	checkpointTS := f.pd.AllocTSO()
	flushTS := f.pd.AllocTSO()
	latestTS := checkpointTS
	globalCheckpoint := f.pd.GlobalCheckpoint()

	flushSeq := f.nextFlushSequence()
	files, err := f.buildRegionFiles(
		ctx,
		storeID,
		flushSeq,
		globalCheckpoint,
		latestTS,
		states,
		f.flushRNG(storeID, flushSeq),
	)
	if err != nil {
		return FlushRecord{}, err
	}

	failpoint.InjectCall("before-write-flush-meta")
	metaPath, err := f.writeBackupMeta(ctx, storeID, flushSeq, flushTS, checkpointTS, files)
	if err != nil {
		return FlushRecord{}, err
	}
	failpoint.InjectCall("after-write-flush-meta")

	if err := f.flushRegions(ctx, storeID, checkpointTS); err != nil {
		return FlushRecord{}, err
	}
	failpoint.InjectCall("after-flush-regions")

	record := FlushRecord{
		Sequence:     flushSeq,
		StoreID:      storeID,
		RegionIDs:    files.regionIDs,
		CheckpointTS: checkpointTS,
		FlushTS:      flushTS,
		MinTS:        files.minTS,
		MaxTS:        files.maxTS,
		MetadataPath: metaPath,
		LogPaths:     files.logPaths,
	}
	f.appendRecord(record)
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

// RecordsUpTo returns flush records with CheckpointTS <= tso.
func (f *FlushSim) RecordsUpTo(tso uint64) []FlushRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]FlushRecord, 0, len(f.records))
	for _, r := range f.records {
		if r.CheckpointTS <= tso {
			result = append(result, r.clone())
		}
	}
	return result
}
