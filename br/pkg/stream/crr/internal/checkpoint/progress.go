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

package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"golang.org/x/sync/errgroup"
)

type roundPlan struct {
	pendingPaths      map[string]struct{}
	maxFlushTSByStore map[uint64]uint64
}

func (c *Calculator) waitUpstreamCheckpointAdvance(ctx context.Context) (uint64, error) {
	for {
		checkpoint, err := c.pd.GetGlobalCheckpointForTask(ctx, c.taskName)
		if err != nil {
			return 0, fmt.Errorf("get global checkpoint for task %s: %w", c.taskName, err)
		}
		if checkpoint > c.lastCheckpoint {
			return checkpoint, nil
		}
		if err := sleepWithContext(ctx, c.pollInterval); err != nil {
			return 0, err
		}
	}
}

func (c *Calculator) loadAliveStores(ctx context.Context) (map[uint64]struct{}, error) {
	stores, err := c.pd.Stores(ctx)
	if err != nil {
		return nil, fmt.Errorf("load alive stores from pd: %w", err)
	}
	aliveStores := make(map[uint64]struct{}, len(stores))
	for _, store := range stores {
		if store.ID == 0 {
			continue
		}
		aliveStores[store.ID] = struct{}{}
	}
	return aliveStores, nil
}

func (c *Calculator) planRound(
	ctx context.Context,
	aliveStores map[uint64]struct{},
) (roundPlan, error) {
	plan := roundPlan{
		pendingPaths:      make(map[string]struct{}),
		maxFlushTSByStore: make(map[uint64]uint64),
	}

	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, egCtx := errgroup.WithContext(planCtx)
	eg.SetLimit(c.metaReadConcurrency)

	var planMu sync.Mutex
	var iterErr error
	for metaFile, err := range c.newMetaFileSeq(egCtx) {
		if err != nil {
			iterErr = err
			cancel()
			break
		}
		if metaFile.storeID != 0 {
			if _, ok := aliveStores[metaFile.storeID]; !ok {
				continue
			}
		}

		eg.Go(func() error {
			failpoint.InjectCall("before-read-meta", metaFile.path)

			metaBytes, err := c.upstream.ReadFile(egCtx, metaFile.path)
			if err != nil {
				return fmt.Errorf("read upstream backupmeta %s: %w", metaFile.path, err)
			}

			meta := &backuppb.Metadata{}
			if err := meta.Unmarshal(metaBytes); err != nil {
				return fmt.Errorf("unmarshal backupmeta %s: %w", metaFile.path, err)
			}

			storeID, err := resolveStoreID(metaFile.storeID, meta.GetStoreId(), metaFile.path)
			if err != nil {
				return err
			}
			if _, ok := aliveStores[storeID]; !ok {
				return nil
			}

			logPaths := extractDataFilePaths(meta)

			planMu.Lock()
			plan.pendingPaths[metaFile.path] = struct{}{}
			for _, logPath := range logPaths {
				plan.pendingPaths[logPath] = struct{}{}
			}
			if metaFile.flushTS > plan.maxFlushTSByStore[storeID] {
				plan.maxFlushTSByStore[storeID] = metaFile.flushTS
			}
			planMu.Unlock()

			failpoint.InjectCall("flush-meta", metaFile.path, storeID, metaFile.flushTS)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		if iterErr != nil && errors.Is(err, context.Canceled) {
			return roundPlan{}, iterErr
		}
		return roundPlan{}, err
	}
	if iterErr != nil {
		return roundPlan{}, iterErr
	}
	return plan, nil
}

func resolveStoreID(nameStoreID uint64, contentStoreID int64, metaPath string) (uint64, error) {
	if contentStoreID <= 0 {
		return 0, fmt.Errorf("backupmeta %s contains invalid store id %d", metaPath, contentStoreID)
	}
	storeID := uint64(contentStoreID)
	if nameStoreID != 0 && nameStoreID != storeID {
		return 0, fmt.Errorf(
			"backupmeta %s has mismatched store id between name (%d) and content (%d)",
			metaPath, nameStoreID, storeID,
		)
	}
	return storeID, nil
}

func extractDataFilePaths(meta *backuppb.Metadata) []string {
	paths := make([]string, 0, len(meta.Files))
	for _, file := range meta.Files {
		if file.Path != "" {
			paths = append(paths, file.Path)
		}
	}
	for _, group := range meta.FileGroups {
		for _, file := range group.DataFilesInfo {
			if file.Path != "" {
				paths = append(paths, file.Path)
			}
		}
	}
	return paths
}

func (c *Calculator) waitDownstreamSync(ctx context.Context, pendingPaths map[string]struct{}) error {
	for len(pendingPaths) > 0 {
		for filePath := range pendingPaths {
			exists, err := c.downstream.FileExists(ctx, filePath)
			if err != nil {
				return fmt.Errorf("check downstream file %s: %w", filePath, err)
			}
			if exists {
				delete(pendingPaths, filePath)
			}
		}
		if len(pendingPaths) == 0 {
			return nil
		}
		if err := sleepWithContext(ctx, c.pollInterval); err != nil {
			return err
		}
	}
	return nil
}

func (c *Calculator) advanceSyncedState(
	aliveStores map[uint64]struct{},
	maxFlushTSByStore map[uint64]uint64,
) {
	for storeID := range c.syncedByStore {
		if _, ok := aliveStores[storeID]; !ok {
			delete(c.syncedByStore, storeID)
		}
	}
	for storeID, flushTS := range maxFlushTSByStore {
		if flushTS > c.syncedByStore[storeID] {
			c.syncedByStore[storeID] = flushTS
		}
	}

	if len(aliveStores) == 0 {
		return
	}

	syncedCandidate := uint64(math.MaxUint64)
	consideredStores := 0
	for storeID := range aliveStores {
		storeSyncedTS, ok := c.syncedByStore[storeID]
		if !ok {
			continue
		}
		consideredStores++
		if storeSyncedTS < syncedCandidate {
			syncedCandidate = storeSyncedTS
		}
	}
	if consideredStores == 0 {
		return
	}
	if syncedCandidate > c.syncedTS {
		c.syncedTS = syncedCandidate
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
