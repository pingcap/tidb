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

package crr

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/stream/backupmetas"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	metaSuffix          = ".meta"
	defaultPollInterval = 500 * time.Millisecond
)

// PDMetaReader defines the upstream metadata APIs used by checkpoint calculation.
type PDMetaReader interface {
	GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error)
	Stores(ctx context.Context) ([]streamhelper.Store, error)
}

// UpstreamStorageReader defines the upstream storage APIs used by checkpoint calculation.
type UpstreamStorageReader interface {
	WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error
	ReadFile(ctx context.Context, name string) ([]byte, error)
}

// DownstreamObjectChecker defines downstream checks allowed by checkpoint calculation.
//
// It intentionally only allows existence checks. The calculator must not read
// object contents from downstream storage.
type DownstreamObjectChecker interface {
	FileExists(ctx context.Context, name string) (bool, error)
}

// CheckpointCalculatorConfig controls checkpoint calculation behavior.
type CheckpointCalculatorConfig struct {
	TaskName     string
	PollInterval time.Duration
}

// CheckpointCalculator calculates a downstream-safe checkpoint for CRR.
//
// It is stateful and expected to be reused across rounds.
type CheckpointCalculator struct {
	pd         PDMetaReader
	upstream   UpstreamStorageReader
	downstream DownstreamObjectChecker

	taskName     string
	pollInterval time.Duration

	lastCheckpoint uint64
	syncedTS       uint64
	syncedByStore  map[uint64]uint64
}

// NewCheckpointCalculator creates a stateful calculator for CRR checkpoint advancing.
func NewCheckpointCalculator(
	pd PDMetaReader,
	upstream UpstreamStorageReader,
	downstream DownstreamObjectChecker,
	cfg CheckpointCalculatorConfig,
) (*CheckpointCalculator, error) {
	if pd == nil {
		return nil, fmt.Errorf("pd reader must not be nil")
	}
	if upstream == nil {
		return nil, fmt.Errorf("upstream storage must not be nil")
	}
	if downstream == nil {
		return nil, fmt.Errorf("downstream checker must not be nil")
	}
	if cfg.TaskName == "" {
		return nil, fmt.Errorf("task name must not be empty")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	return &CheckpointCalculator{
		pd:            pd,
		upstream:      upstream,
		downstream:    downstream,
		taskName:      cfg.TaskName,
		pollInterval:  cfg.PollInterval,
		syncedByStore: map[uint64]uint64{},
	}, nil
}

// ComputeNextCheckpoint waits for upstream checkpoint progress, confirms the
// required files are synced to downstream, and returns the safe checkpoint.
func (c *CheckpointCalculator) ComputeNextCheckpoint(ctx context.Context) (uint64, error) {
	upstreamCheckpoint, err := c.waitUpstreamCheckpointAdvance(ctx)
	if err != nil {
		return 0, err
	}

	aliveStores, err := c.loadAliveStores(ctx)
	if err != nil {
		return 0, err
	}

	round, err := c.planRound(ctx, aliveStores)
	if err != nil {
		return 0, err
	}
	if err := c.waitDownstreamSync(ctx, round.pendingPaths); err != nil {
		return 0, err
	}

	c.advanceSyncedState(aliveStores, round.maxFlushTSByStore)
	c.lastCheckpoint = upstreamCheckpoint
	return upstreamCheckpoint, nil
}

// SyncedTS returns the latest synced_ts tracked by this calculator.
func (c *CheckpointCalculator) SyncedTS() uint64 {
	return c.syncedTS
}

// LastCheckpoint returns the most recent returned checkpoint.
func (c *CheckpointCalculator) LastCheckpoint() uint64 {
	return c.lastCheckpoint
}

type parsedMetaFile struct {
	path    string
	flushTS uint64
	storeID uint64
}

type roundPlan struct {
	pendingPaths      map[string]struct{}
	maxFlushTSByStore map[uint64]uint64
}

func (c *CheckpointCalculator) waitUpstreamCheckpointAdvance(ctx context.Context) (uint64, error) {
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

func (c *CheckpointCalculator) loadAliveStores(ctx context.Context) (map[uint64]struct{}, error) {
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

func (c *CheckpointCalculator) planRound(
	ctx context.Context,
	aliveStores map[uint64]struct{},
) (roundPlan, error) {
	metaFiles, err := c.collectNewMetaFiles(ctx)
	if err != nil {
		return roundPlan{}, err
	}

	plan := roundPlan{
		pendingPaths:      make(map[string]struct{}),
		maxFlushTSByStore: make(map[uint64]uint64),
	}

	for _, metaFile := range metaFiles {
		if metaFile.storeID != 0 {
			if _, ok := aliveStores[metaFile.storeID]; !ok {
				continue
			}
		}

		metaBytes, err := c.upstream.ReadFile(ctx, metaFile.path)
		if err != nil {
			return roundPlan{}, fmt.Errorf("read upstream backupmeta %s: %w", metaFile.path, err)
		}

		meta := &backuppb.Metadata{}
		if err := meta.Unmarshal(metaBytes); err != nil {
			return roundPlan{}, fmt.Errorf("unmarshal backupmeta %s: %w", metaFile.path, err)
		}

		storeID, err := resolveStoreID(metaFile.storeID, meta.GetStoreId(), metaFile.path)
		if err != nil {
			return roundPlan{}, err
		}
		if _, ok := aliveStores[storeID]; !ok {
			continue
		}

		plan.pendingPaths[metaFile.path] = struct{}{}
		for _, logPath := range extractDataFilePaths(meta) {
			plan.pendingPaths[logPath] = struct{}{}
		}
		if metaFile.flushTS > plan.maxFlushTSByStore[storeID] {
			plan.maxFlushTSByStore[storeID] = metaFile.flushTS
		}
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

func (c *CheckpointCalculator) collectNewMetaFiles(ctx context.Context) ([]parsedMetaFile, error) {
	metaFiles := make([]parsedMetaFile, 0, 128)
	err := c.upstream.WalkDir(ctx, &storeapi.WalkOption{SubDir: stream.GetStreamBackupMetaPrefix()},
		func(filePath string, size int64) error {
			_ = size
			if !strings.HasSuffix(filePath, metaSuffix) {
				return nil
			}
			baseName := strings.TrimSuffix(path.Base(filePath), metaSuffix)
			parsed, err := backupmetas.ParseName(baseName)
			if err != nil {
				return fmt.Errorf("parse backupmeta name %s: %w", filePath, err)
			}
			if parsed.FlushTS <= c.syncedTS {
				return nil
			}
			metaFiles = append(metaFiles, parsedMetaFile{
				path:    filePath,
				flushTS: parsed.FlushTS,
				storeID: parsed.StoreID,
			})
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("walk upstream backupmeta prefix: %w", err)
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		if metaFiles[i].flushTS == metaFiles[j].flushTS {
			return metaFiles[i].path < metaFiles[j].path
		}
		return metaFiles[i].flushTS < metaFiles[j].flushTS
	})
	return metaFiles, nil
}

func (c *CheckpointCalculator) waitDownstreamSync(ctx context.Context, pendingPaths map[string]struct{}) error {
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

func (c *CheckpointCalculator) advanceSyncedState(
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
