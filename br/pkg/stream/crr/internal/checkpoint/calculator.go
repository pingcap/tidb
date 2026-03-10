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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	defaultPollInterval        = 2 * time.Second
	defaultMetaReadConcurrency = 16
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

	Err error
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

// DownstreamObjectChecker defines downstream checks allowed by checkpoint calculation.
//
// It intentionally only allows existence checks. The calculator must not read
// object contents from downstream storage.
type DownstreamObjectChecker interface {
	FileExists(ctx context.Context, name string) (bool, error)
}

// CheckpointCalculatorConfig controls checkpoint calculation behavior.
type CheckpointCalculatorConfig struct {
	TaskName            string
	PollInterval        time.Duration
	MetaReadConcurrency int
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
	PD         PDMetaReader
	Upstream   UpstreamStorageReader
	Downstream DownstreamObjectChecker
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
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.MetaReadConcurrency <= 0 {
		cfg.MetaReadConcurrency = defaultMetaReadConcurrency
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
// required files are synced to downstream, and returns the safe checkpoint.
func (c *Calculator) ComputeNextCheckpoint(ctx context.Context) (checkpoint uint64, err error) {
	failpoint.InjectCall("begin-calculate-checkpoint")
	defer func() {
		if err != nil {
			c.observe(CheckpointEvent{
				Type:     EventCalculationFailed,
				TaskName: c.cfg.TaskName,
				Err:      err,
			})
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

	round, err := c.planRound(ctx, aliveStores)
	if err != nil {
		return 0, err
	}
	c.observe(CheckpointEvent{
		Type:               EventRoundPlanned,
		TaskName:           c.cfg.TaskName,
		UpstreamCheckpoint: upstreamCheckpoint,
		AliveStoreCount:    len(aliveStores),
		PendingFileCount:   len(round.pendingPaths),
	})
	if err := c.waitDownstreamSync(ctx, round.pendingPaths); err != nil {
		return 0, err
	}

	c.advanceSyncedState(aliveStores, round.maxFlushTSByStore)
	c.state.lastCheckpoint = upstreamCheckpoint
	c.observe(CheckpointEvent{
		Type:               EventCheckpointAdvanced,
		TaskName:           c.cfg.TaskName,
		UpstreamCheckpoint: upstreamCheckpoint,
		SafeCheckpoint:     upstreamCheckpoint,
		SyncedTS:           c.state.syncedTS,
		AliveStoreCount:    len(aliveStores),
	})
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

func (d CalculatorDeps) validate() error {
	if d.PD == nil {
		return fmt.Errorf("pd reader must not be nil")
	}
	if d.Upstream == nil {
		return fmt.Errorf("upstream storage must not be nil")
	}
	if d.Downstream == nil {
		return fmt.Errorf("downstream checker must not be nil")
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
