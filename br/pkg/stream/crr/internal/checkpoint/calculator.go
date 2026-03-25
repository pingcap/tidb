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
	"fmt"
	"maps"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	DefaultPollInterval        = 2 * time.Second
	DefaultMetaReadConcurrency = 16
)

// EventType identifies a checkpoint calculation progress event.
type EventType string

const (
	EventWaitingUpstream    EventType = "waiting_upstream"
	EventUpstreamAdvanced   EventType = "upstream_advanced"
	EventRoundPlanned       EventType = "round_planned"
	EventWaitingDownstream  EventType = "waiting_downstream"
	EventCheckpointAdvanced EventType = "checkpoint_advanced"
	EventCalculationFailed  EventType = "calculation_failed"
)

// CheckpointEvent describes a calculator progress event.
type CheckpointEvent struct {
	Type EventType
	Time time.Time

	TaskName string

	LoopIteration      uint64
	UpstreamCheckpoint uint64
	SafeCheckpoint     uint64
	SyncedTS           uint64

	AliveStoreCount  int
	PendingFileCount int

	Statistic *FileStatistic

	Err error
}

// FileStatistic summarizes the files observed in the current calculation round.
type FileStatistic struct {
	UpstreamReadMetaFileCount       int
	EstimatedSyncLogFileCount       int
	DownstreamCheckFileCount        int
	PlannedFileSuffixCounts         map[string]int
	DownstreamCheckFileSuffixCounts map[string]int
}

// Observer receives progress events emitted by the calculator.
// Implementations must not block or mutate the calculator.
type Observer interface {
	OnCheckpointEvent(event CheckpointEvent)
}

// PDMetaReader defines the upstream metadata APIs used by checkpoint calculation.
type PDMetaReader interface {
	GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error)
	Stores(ctx context.Context) ([]streamhelper.Store, error)
}

// UpstreamStorageReader defines the upstream storage APIs used by checkpoint calculation.
type UpstreamStorageReader interface {
	WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error
	ReadFile(ctx context.Context, name string) ([]byte, error)
	URI() string
}

// ObjectSyncChecker reports whether an object required by CRR is already safe to
// consume on the restore side.
type ObjectSyncChecker interface {
	FileSynced(ctx context.Context, name string) (bool, error)
}

type fileExistenceChecker interface {
	FileExists(ctx context.Context, name string) (bool, error)
}

type existenceSyncChecker struct {
	checker fileExistenceChecker
}

// NewExistenceSyncChecker adapts a plain existence checker to ObjectSyncChecker.
func NewExistenceSyncChecker(checker fileExistenceChecker) ObjectSyncChecker {
	return existenceSyncChecker{checker: checker}
}

func (c existenceSyncChecker) FileSynced(ctx context.Context, name string) (bool, error) {
	return c.checker.FileExists(ctx, name)
}

// CheckpointCalculatorConfig controls checkpoint calculation behavior.
type CheckpointCalculatorConfig struct {
	TaskName            string
	PollInterval        time.Duration
	MetaReadConcurrency int
}

// PersistentState captures the calculator progress needed to resume after restart.
type PersistentState struct {
	LastCheckpoint uint64            `json:"last_checkpoint"`
	SyncedTS       uint64            `json:"synced_ts"`
	SyncedByStore  map[uint64]uint64 `json:"synced_by_store,omitempty"`
}

// Calculator calculates a downstream-safe checkpoint for CRR.
//
// It is stateful and expected to be reused across rounds.
type Calculator struct {
	deps     CalculatorDeps
	cfg      CheckpointCalculatorConfig
	observer Observer
	state    calculatorState
}

// CalculatorDeps groups the external dependencies used by checkpoint calculation.
type CalculatorDeps struct {
	PD       PDMetaReader
	Upstream UpstreamStorageReader
	Sync     ObjectSyncChecker
}

type calculatorState struct {
	lastCheckpoint uint64
	syncedTS       uint64
	syncedByStore  map[uint64]uint64
}

// NewCalculator creates a stateful calculator for CRR checkpoint advancing.
func NewCalculator(
	deps CalculatorDeps,
	cfg CheckpointCalculatorConfig,
	observer Observer,
) (*Calculator, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}
	if cfg.TaskName == "" {
		return nil, fmt.Errorf("task name must not be empty")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.MetaReadConcurrency <= 0 {
		cfg.MetaReadConcurrency = DefaultMetaReadConcurrency
	}

	return &Calculator{
		deps:     deps,
		cfg:      cfg,
		observer: observer,
		state: calculatorState{
			syncedByStore: map[uint64]uint64{},
		},
	}, nil
}

// ComputeNextCheckpoint waits for upstream checkpoint progress, confirms the
// required files are synced, and returns the safe checkpoint.
func (c *Calculator) ComputeNextCheckpoint(ctx context.Context) (checkpoint uint64, err error) {
	failpoint.InjectCall("begin-calculate-checkpoint")
	var statistic *FileStatistic
	defer func() {
		if err != nil {
			c.observeCalculationFailed(err, statistic)
		}
	}()

	upstreamCheckpoint, advanced, err := c.pollUpstreamCheckpoint(ctx)
	if err != nil {
		return 0, err
	}
	if !advanced {
		return c.state.lastCheckpoint, nil
	}

	aliveStores, err := c.loadAliveStores(ctx)
	if err != nil {
		return 0, err
	}

	round, err := c.planRound(ctx)
	if err != nil {
		return 0, err
	}
	statistic = c.observeRoundPlanned(upstreamCheckpoint, aliveStores, round)
	if err := c.waitObjectSync(ctx, round.pendingPaths, &round.statistic); err != nil {
		statistic = round.statistic.snapshot()
		return 0, err
	}
	statistic = round.statistic.snapshot()

	c.advanceSyncedState(aliveStores, round.maxFlushTSByStore)
	c.state.lastCheckpoint = upstreamCheckpoint
	c.observeCheckpointAdvanced(upstreamCheckpoint, aliveStores, statistic)
	return upstreamCheckpoint, nil
}

// SyncedTS returns the latest synced_ts tracked by this calculator.
func (c *Calculator) SyncedTS() uint64 {
	return c.state.syncedTS
}

// LastCheckpoint returns the most recent returned checkpoint.
func (c *Calculator) LastCheckpoint() uint64 {
	return c.state.lastCheckpoint
}

// StateSnapshot returns a snapshot of calculator progress suitable for persistence.
func (c *Calculator) StateSnapshot() PersistentState {
	return PersistentState{
		LastCheckpoint: c.state.lastCheckpoint,
		SyncedTS:       c.state.syncedTS,
		SyncedByStore:  maps.Clone(c.state.syncedByStore),
	}
}

// RestorePersistentState overwrites the calculator progress before checkpoint
// calculation begins.
func (c *Calculator) RestorePersistentState(state PersistentState) error {
	if c.state.lastCheckpoint != 0 {
		return fmt.Errorf("cannot restore persistent state after checkpoint calculation started")
	}
	c.state.lastCheckpoint = state.LastCheckpoint
	c.state.syncedTS = state.SyncedTS
	c.state.syncedByStore = maps.Clone(state.SyncedByStore)
	if c.state.syncedByStore == nil {
		c.state.syncedByStore = map[uint64]uint64{}
	}
	return nil
}

func (d CalculatorDeps) validate() error {
	if d.PD == nil {
		return fmt.Errorf("pd reader must not be nil")
	}
	if d.Upstream == nil {
		return fmt.Errorf("upstream storage must not be nil")
	}
	if d.Sync == nil {
		return fmt.Errorf("object sync checker must not be nil")
	}
	return validateIncrementalMetaScanStorage(d.Upstream.URI())
}

func (c *Calculator) observe(event CheckpointEvent) {
	if c.observer == nil {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	c.observer.OnCheckpointEvent(event)
}

func (c *Calculator) observeCalculationFailed(err error, statistic *FileStatistic) {
	c.observe(CheckpointEvent{
		Type:      EventCalculationFailed,
		TaskName:  c.cfg.TaskName,
		Statistic: statistic,
		Err:       err,
	})
}

func (c *Calculator) observeRoundPlanned(
	upstreamCheckpoint uint64,
	aliveStores map[uint64]struct{},
	round roundPlan,
) *FileStatistic {
	statistic := round.statistic.snapshot()
	c.observe(CheckpointEvent{
		Type:               EventRoundPlanned,
		TaskName:           c.cfg.TaskName,
		UpstreamCheckpoint: upstreamCheckpoint,
		AliveStoreCount:    len(aliveStores),
		PendingFileCount:   len(round.pendingPaths),
		Statistic:          statistic,
	})
	return statistic
}

func (c *Calculator) observeCheckpointAdvanced(
	upstreamCheckpoint uint64,
	aliveStores map[uint64]struct{},
	statistic *FileStatistic,
) {
	c.observe(CheckpointEvent{
		Type:               EventCheckpointAdvanced,
		TaskName:           c.cfg.TaskName,
		UpstreamCheckpoint: upstreamCheckpoint,
		SafeCheckpoint:     upstreamCheckpoint,
		SyncedTS:           c.state.syncedTS,
		AliveStoreCount:    len(aliveStores),
		Statistic:          statistic,
	})
}
