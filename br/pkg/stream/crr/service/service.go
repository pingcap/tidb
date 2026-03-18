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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"go.uber.org/zap"
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

// PersistentState captures the persisted calculator progress used for service restart.
type PersistentState = checkpoint.PersistentState

// ResumeStateStore persists and restores service resume state.
type ResumeStateStore interface {
	LoadState(ctx context.Context) (*PersistentState, error)
	SaveState(ctx context.Context, state PersistentState) error
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
	State    ResumeStateStore
}

// CalculatorConfig controls the inner checkpoint calculator behavior.
type CalculatorConfig = checkpoint.CheckpointCalculatorConfig

// Service wraps the CRR checkpoint calculator with status tracking and HTTP exposure.
type Service struct {
	calc     *checkpoint.Calculator
	status   *statusStore
	observer *statusObserver
	pd       upstreamCheckpointWaiter

	state ResumeStateStore
	cfg   Config

	resumeStateInitialized bool
	pendingResumeState     *PersistentState
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
		state:    deps.State,
		cfg:      cfg,
	}, nil
}

// Run starts the CRR checkpoint calculation loop and returns when the context is canceled.
func (s *Service) Run(ctx context.Context) error {
	s.status.start()
	defer s.status.stop()

	for {
		if ctx.Err() != nil {
			s.flushPendingResumeStateOnShutdown()
			return nil
		}
		if err := s.prepareResumeState(ctx); err != nil {
			if shouldStop(ctx, err) {
				return nil
			}
			s.recordServiceFailure(err)
			if err := sleepContext(ctx, s.cfg.RetryInterval); err != nil {
				return nil
			}
			continue
		}

		s.observer.BeginCalculationRound()
		lastCheckpoint := s.calc.LastCheckpoint()
		nextCheckpoint, err := s.calc.ComputeNextCheckpoint(ctx)
		if err == nil {
			s.queueResumeStateSave(lastCheckpoint, nextCheckpoint)
			if err := s.flushPendingResumeState(ctx); err != nil {
				if shouldStop(ctx, err) {
					s.flushPendingResumeStateOnShutdown()
					return nil
				}
				s.recordServiceFailure(err)
				if err := sleepContext(ctx, s.cfg.RetryInterval); err != nil {
					return nil
				}
				continue
			}
			if nextCheckpoint == lastCheckpoint {
				if err := s.waitCheckpointAdvance(ctx, lastCheckpoint); err != nil {
					if shouldStop(ctx, err) {
						return nil
					}
					s.recordServiceFailure(err)
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
		if err := sleepContext(ctx, s.cfg.RetryInterval); err != nil {
			return nil
		}
	}
}

func (s *Service) waitCheckpointAdvance(ctx context.Context, current uint64) error {
	return s.pd.WaitGlobalCheckpointAdvance(ctx, s.cfg.TaskName, current)
}

// recordServiceFailure reports failures that happen outside ComputeNextCheckpoint.
func (s *Service) recordServiceFailure(err error) {
	s.observer.OnCheckpointEvent(checkpoint.CheckpointEvent{
		Type:     checkpoint.EventCalculationFailed,
		Time:     time.Now(),
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

func (s *Service) prepareResumeState(ctx context.Context) error {
	if !s.resumeStateInitialized {
		return s.initializeResumeState(ctx)
	}
	return s.flushPendingResumeState(ctx)
}

func (s *Service) initializeResumeState(ctx context.Context) error {
	if s.state != nil {
		state, err := s.state.LoadState(ctx)
		if err != nil {
			return fmt.Errorf("load resume state: %w", err)
		}
		if state != nil {
			if err := s.calc.RestorePersistentState(*state); err != nil {
				return err
			}
			s.status.setPersistentState(*state)
		}
	}
	s.status.clearFailure()
	s.resumeStateInitialized = true
	return nil
}

func (s *Service) queueResumeStateSave(lastCheckpoint uint64, nextCheckpoint uint64) {
	if s.state == nil || nextCheckpoint <= lastCheckpoint {
		return
	}
	state := s.calc.StateSnapshot()
	s.pendingResumeState = &state
}

func (s *Service) flushPendingResumeState(ctx context.Context) error {
	if s.pendingResumeState == nil {
		return nil
	}
	if err := s.state.SaveState(ctx, *s.pendingResumeState); err != nil {
		return fmt.Errorf("save resume state: %w", err)
	}
	s.pendingResumeState = nil
	s.status.clearFailure()
	return nil
}

func (s *Service) flushPendingResumeStateOnShutdown() {
	if s.pendingResumeState == nil || s.state == nil {
		return
	}
	persistCtx, cancel := context.WithTimeout(context.Background(), s.cfg.RetryInterval)
	defer cancel()
	if err := s.flushPendingResumeState(persistCtx); err != nil {
		log.Warn("failed to flush pending CRR resume state during shutdown",
			zap.String("task", s.cfg.TaskName),
			zap.Error(err),
		)
	}
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
