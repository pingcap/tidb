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
	defaultPollInterval        = 500 * time.Millisecond
	defaultMetaReadConcurrency = 16
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
	deps  calculatorDeps
	cfg   CheckpointCalculatorConfig
	state calculatorState
}

type calculatorDeps struct {
	pd         PDMetaReader
	upstream   UpstreamStorageReader
	downstream DownstreamObjectChecker
}

type calculatorState struct {
	lastCheckpoint uint64
	syncedTS       uint64
	syncedByStore  map[uint64]uint64
}

// NewCalculator creates a stateful calculator for CRR checkpoint advancing.
func NewCalculator(
	pd PDMetaReader,
	upstream UpstreamStorageReader,
	downstream DownstreamObjectChecker,
	cfg CheckpointCalculatorConfig,
) (*Calculator, error) {
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
	if cfg.MetaReadConcurrency <= 0 {
		cfg.MetaReadConcurrency = defaultMetaReadConcurrency
	}
	if err := validateIncrementalMetaScanStorage(upstream.URI()); err != nil {
		return nil, err
	}

	return &Calculator{
		deps: calculatorDeps{
			pd:         pd,
			upstream:   upstream,
			downstream: downstream,
		},
		cfg: cfg,
		state: calculatorState{
			syncedByStore: map[uint64]uint64{},
		},
	}, nil
}

// ComputeNextCheckpoint waits for upstream checkpoint progress, confirms the
// required files are synced to downstream, and returns the safe checkpoint.
func (c *Calculator) ComputeNextCheckpoint(ctx context.Context) (uint64, error) {
	failpoint.InjectCall("begin-calculate-checkpoint")

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
	c.state.lastCheckpoint = upstreamCheckpoint
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
