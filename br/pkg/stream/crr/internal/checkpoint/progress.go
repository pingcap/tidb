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
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type roundPlan struct {
	pendingPaths      map[string]struct{}
	maxFlushTSByStore map[uint64]uint64
}

func (c *Calculator) pollUpstreamCheckpoint(ctx context.Context) (uint64, bool, error) {
	checkpoint, err := c.deps.PD.GetGlobalCheckpointForTask(ctx, c.cfg.TaskName)
	if err != nil {
		return 0, false, fmt.Errorf("get global checkpoint for task %s: %w", c.cfg.TaskName, err)
	}
	if checkpoint > c.state.lastCheckpoint {
		c.observe(CheckpointEvent{
			Type:               EventUpstreamAdvanced,
			TaskName:           c.cfg.TaskName,
			UpstreamCheckpoint: checkpoint,
		})
		return checkpoint, true, nil
	}
	c.observe(CheckpointEvent{
		Type:               EventWaitingUpstream,
		TaskName:           c.cfg.TaskName,
		LoopIteration:      1,
		UpstreamCheckpoint: checkpoint,
		SafeCheckpoint:     c.state.lastCheckpoint,
	})
	return checkpoint, false, nil
}

func (c *Calculator) loadAliveStores(ctx context.Context) (map[uint64]struct{}, error) {
	stores, err := c.deps.PD.Stores(ctx)
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
	upstreamCheckpoint uint64,
	aliveStores map[uint64]struct{},
) (roundPlan, error) {
	plan := roundPlan{
		pendingPaths:      make(map[string]struct{}),
		maxFlushTSByStore: make(map[uint64]uint64),
	}

	planCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, egCtx := errgroup.WithContext(planCtx)
	eg.SetLimit(c.cfg.MetaReadConcurrency)

	var planMu sync.Mutex
	var iterErr error
	for metaFile, err := range c.newMetaFileSeq(egCtx) {
		if err != nil {
			iterErr = err
			cancel()
			break
		}

		eg.Go(func() error {
			failpoint.InjectCall("before-read-meta", metaFile.path)

			loadedMeta, err := loadMetaFile(egCtx, c.deps.Upstream, metaFile)
			if err != nil {
				return err
			}

			planMu.Lock()
			plan.pendingPaths[loadedMeta.path] = struct{}{}
			for _, logPath := range loadedMeta.dataFilePaths {
				plan.pendingPaths[logPath] = struct{}{}
			}
			if _, ok := aliveStores[loadedMeta.storeID]; ok && loadedMeta.flushTS > plan.maxFlushTSByStore[loadedMeta.storeID] {
				plan.maxFlushTSByStore[loadedMeta.storeID] = loadedMeta.flushTS
			}
			planMu.Unlock()

			failpoint.InjectCall("flush-meta", loadedMeta.path, loadedMeta.storeID, loadedMeta.flushTS)
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

func (c *Calculator) waitDownstreamSync(ctx context.Context, pendingPaths map[string]struct{}) error {
	var loopIteration uint64
	for len(pendingPaths) > 0 {
		for filePath := range pendingPaths {
			exists, err := c.deps.Downstream.FileExists(ctx, filePath)
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
		loopIteration++
		c.observe(CheckpointEvent{
			Type:             EventWaitingDownstream,
			TaskName:         c.cfg.TaskName,
			LoopIteration:    loopIteration,
			PendingFileCount: len(pendingPaths),
		})
		if err := sleepWithContext(ctx, c.cfg.PollInterval); err != nil {
			return err
		}
	}
	return nil
}

func (c *Calculator) advanceSyncedState(
	aliveStores map[uint64]struct{},
	maxFlushTSByStore map[uint64]uint64,
) {
	for storeID := range c.state.syncedByStore {
		if _, ok := aliveStores[storeID]; !ok {
			delete(c.state.syncedByStore, storeID)
		}
	}
	for storeID, flushTS := range maxFlushTSByStore {
		if flushTS > c.state.syncedByStore[storeID] {
			c.state.syncedByStore[storeID] = flushTS
		}
	}

	if len(aliveStores) == 0 {
		return
	}

	syncedCandidate := uint64(math.MaxUint64)
	missingStores := make([]uint64, 0)
	for storeID := range aliveStores {
		storeSyncedTS, ok := c.state.syncedByStore[storeID]
		if !ok {
			missingStores = append(missingStores, storeID)
			continue
		}
		if storeSyncedTS < syncedCandidate {
			syncedCandidate = storeSyncedTS
		}
	}
	if len(missingStores) > 0 {
		c.warnUnsafeSyncedTS(missingStores, "alive store has no observed flush ts yet")
		return
	}
	if syncedCandidate < c.state.syncedTS {
		behindStores := make([]uint64, 0)
		for storeID, storeSyncedTS := range c.state.syncedByStore {
			if storeSyncedTS < c.state.syncedTS {
				behindStores = append(behindStores, storeID)
			}
		}
		c.warnUnsafeSyncedTS(behindStores, "alive store flush ts is behind current synced-ts")
		return
	}
	if syncedCandidate > c.state.syncedTS {
		c.state.syncedTS = syncedCandidate
	}
}

func (c *Calculator) warnUnsafeSyncedTS(storeIDs []uint64, reason string) {
	if c.state.syncedTS == 0 {
		return
	}
	if len(storeIDs) == 0 {
		return
	}
	log.Warn(
		"crr checkpoint calculator cannot safely advance synced-ts",
		zap.String("category", "crr checkpoint"),
		zap.String("task", c.cfg.TaskName),
		zap.Uint64s("store-ids", storeIDs),
		zap.Uint64("synced-ts", c.state.syncedTS),
		zap.String("reason", reason),
	)
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
