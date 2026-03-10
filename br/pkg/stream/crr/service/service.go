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
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const defaultRetryInterval = time.Second

// Config controls the outer worker loop around checkpoint calculation.
type Config struct {
	RetryInterval time.Duration
}

// Deps are the external dependencies needed to build the CRR checkpoint calculator.
type Deps struct {
	PD interface {
		GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error)
		Stores(ctx context.Context) ([]streamhelper.Store, error)
	}
	Upstream interface {
		storeapi.Storage
	}
	Downstream interface {
		FileExists(ctx context.Context, name string) (bool, error)
	}
}

// CalculatorConfig controls the inner checkpoint calculator behavior.
type CalculatorConfig struct {
	TaskName            string
	PollInterval        time.Duration
	MetaReadConcurrency int
}

// Service wraps the CRR checkpoint calculator with status tracking and HTTP exposure.
type Service struct {
	calc     *checkpoint.Calculator
	status   *statusStore
	observer *statusObserver

	retryInterval time.Duration
}

// New creates a CRR service with an attached calculator observer.
func New(
	deps Deps,
	calcCfg CalculatorConfig,
	cfg Config,
) (*Service, error) {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	status := newStatusStore(calcCfg.TaskName)
	observer := newStatusObserver(status)
	calc, err := checkpoint.NewCalculator(
		checkpoint.CalculatorDeps{
			PD:         deps.PD,
			Upstream:   deps.Upstream,
			Downstream: deps.Downstream,
		},
		checkpoint.CheckpointCalculatorConfig{
			TaskName:            calcCfg.TaskName,
			PollInterval:        calcCfg.PollInterval,
			MetaReadConcurrency: calcCfg.MetaReadConcurrency,
		},
		observer,
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		calc:          calc,
		status:        status,
		observer:      observer,
		retryInterval: cfg.RetryInterval,
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
		_, err := s.calc.ComputeNextCheckpoint(ctx)
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return nil
			}
		}
		if err := sleepContext(ctx, s.retryInterval); err != nil {
			return nil
		}
	}
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
