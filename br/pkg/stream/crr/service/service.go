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

package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
)

const DefaultRetryInterval = time.Second

// Config controls the outer worker loop and embedded checkpoint calculator behavior.
type Config struct {
	CalculatorConfig
	RetryInterval time.Duration
}

type upstreamCheckpointWaiter interface {
	WaitGlobalCheckpointAdvance(ctx context.Context, taskName string, current uint64) error
}

type fileExistenceChecker interface {
	FileExists(ctx context.Context, name string) (bool, error)
}

// ObjectSyncChecker reports whether an object is already safe for CRR restore-side use.
type ObjectSyncChecker = checkpoint.ObjectSyncChecker

// NewExistenceSyncChecker adapts a plain existence checker to ObjectSyncChecker.
func NewExistenceSyncChecker(checker fileExistenceChecker) ObjectSyncChecker {
	return checkpoint.NewExistenceSyncChecker(checker)
}

// Deps are the external dependencies needed to build the CRR checkpoint calculator.
type Deps struct {
	PD       checkpoint.PDMetaReader
	Watcher  upstreamCheckpointWaiter
	Upstream checkpoint.UpstreamStorageReader
	Sync     ObjectSyncChecker
}

// CalculatorConfig controls the inner checkpoint calculator behavior.
type CalculatorConfig = checkpoint.CheckpointCalculatorConfig

// Service wraps the CRR checkpoint calculator with status tracking and HTTP exposure.
type Service struct {
	calc     *checkpoint.Calculator
	status   *statusStore
	observer *statusObserver
	pd       upstreamCheckpointWaiter

	cfg Config
}

// New creates a CRR service with an attached calculator observer.
func New(
	deps Deps,
	cfg Config,
) (*Service, error) {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultRetryInterval
	}
	if deps.Watcher == nil {
		return nil, fmt.Errorf("checkpoint watcher must not be nil")
	}

	status := newStatusStore(cfg.TaskName)
	observer := newStatusObserver(status)
	calc, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:       deps.PD,
			Upstream: deps.Upstream,
			Sync:     deps.Sync,
		},
		cfg.CalculatorConfig,
		observer,
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		calc:     calc,
		status:   status,
		observer: observer,
		pd:       deps.Watcher,
		cfg:      cfg,
	}, nil
}

// Run starts the CRR checkpoint calculation loop and returns when the context is canceled.
func (s *Service) Run(ctx context.Context) error {
	s.status.start()
	defer s.status.stop()

	for {
		if ctx.Err() != nil {
			return nil
		}

		s.observer.BeginCalculationRound()
		lastCheckpoint := s.calc.LastCheckpoint()
		nextCheckpoint, err := s.calc.ComputeNextCheckpoint(ctx)
		if err == nil {
			if nextCheckpoint == lastCheckpoint {
				if err := s.waitCheckpointAdvance(ctx, lastCheckpoint); err != nil {
					if shouldStop(ctx, err) {
						return nil
					}
					s.recordFailure(err)
					if err := sleepContext(ctx, s.cfg.RetryInterval); err != nil {
						return nil
					}
				}
			}
			continue
		}
		if shouldStop(ctx, err) {
			return nil
		}
		s.recordFailure(err)
		if err := sleepContext(ctx, s.cfg.RetryInterval); err != nil {
			return nil
		}
	}
}

func (s *Service) waitCheckpointAdvance(ctx context.Context, current uint64) error {
	return s.pd.WaitGlobalCheckpointAdvance(ctx, s.cfg.TaskName, current)
}

func (s *Service) recordFailure(err error) {
	s.observer.OnCheckpointEvent(checkpoint.CheckpointEvent{
		Type:     checkpoint.EventCalculationFailed,
		TaskName: s.cfg.TaskName,
		Err:      err,
	})
}

func shouldStop(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return true
		}
	}
	return false
}

// Status returns a consistent snapshot of the current service status.
func (s *Service) Status() StatusSnapshot {
	return s.status.snapshotCopy()
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
