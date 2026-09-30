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

package orphandata

import (
	"context"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

// ActiveProducerChecker reports whether any task that can create or own
// global-sort objects is still running. While it reports true, objects in cloud
// storage may still be in use or pending cleanup, so they cannot be treated as
// orphan data.
type ActiveProducerChecker interface {
	HasActiveProducers(context.Context) (bool, error)
}

// Config configures an orphan data Monitor.
type Config struct {
	ActiveProducerChecker ActiveProducerChecker
	// StorageURI returns the cloud storage URI to scan. It is a function so the
	// monitor reads the latest value, which may change with the system variable.
	StorageURI func() string
	Logger     *zap.Logger
}

type noActiveProducerChecker struct{}

func (noActiveProducerChecker) HasActiveProducers(context.Context) (bool, error) {
	return false, nil
}

type storeFactory func(context.Context, string) (storeapi.Storage, error)

// Monitor scans orphan global-sort objects and publishes their size, but only
// when no producer is active, so the reported size is orphan data.
type Monitor struct {
	cfg Config
	// storeFactory builds the object store for a scan; tests substitute it.
	storeFactory storeFactory
}

// NewMonitor creates an orphan data Monitor.
func NewMonitor(cfg Config) *Monitor {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ActiveProducerChecker == nil {
		cfg.ActiveProducerChecker = noActiveProducerChecker{}
	}
	if cfg.StorageURI == nil {
		cfg.StorageURI = func() string { return "" }
	}
	return &Monitor{
		cfg:          cfg,
		storeFactory: newStore,
	}
}

func newStore(ctx context.Context, uri string) (storeapi.Storage, error) {
	backend, err := objstore.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	return objstore.NewWithDefaultOpt(ctx, backend)
}

// Trigger scans orphan global-sort objects and publishes their size. It runs
// synchronously and is not safe for concurrent use: the scheduler cleanup loop
// calls it serially from its own goroutine, which also owns cancellation.
func (m *Monitor) Trigger(ctx context.Context) {
	storageURI := m.cfg.StorageURI()
	if storageURI == "" {
		return
	}

	hasActiveProducers, err := m.cfg.ActiveProducerChecker.HasActiveProducers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			m.cfg.Logger.Warn("global sort orphan data monitor failed to check active producers", zap.Error(err))
		}
		return
	}
	if hasActiveProducers {
		return
	}

	storage, err := m.storeFactory(ctx, storageURI)
	if err != nil {
		if ctx.Err() == nil {
			m.cfg.Logger.Warn("global sort orphan data monitor failed to create storage")
		}
		return
	}
	defer storage.Close()

	scan, err := Scan(ctx, storage)
	if err != nil {
		if ctx.Err() == nil {
			m.cfg.Logger.Warn("global sort orphan data monitor failed to scan storage")
		}
		return
	}

	hasActiveProducers, err = m.cfg.ActiveProducerChecker.HasActiveProducers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			m.cfg.Logger.Warn("global sort orphan data monitor failed to check active producers", zap.Error(err))
		}
		return
	}
	if hasActiveProducers {
		m.cfg.Logger.Info("global sort orphan data monitor discarded scan because tasks appeared")
		return
	}

	metrics.GlobalSortOrphanDataSize.Set(float64(scan.SizeBytes))
	// an idle cluster scans on every cleanup interval, so keep a clean scan at
	// debug level and only report it as info when orphan data was found.
	logFn := m.cfg.Logger.Debug
	if scan.ObjectCount > 0 {
		logFn = m.cfg.Logger.Info
	}
	logFn("global sort orphan data monitor success",
		zap.Int64("orphan-data-size-bytes", scan.SizeBytes),
		zap.Int64("orphan-data-object-count", scan.ObjectCount),
		zap.Strings("sample-prefixes", scan.SamplePrefixes),
		zap.Bool("sample-prefixes-omitted", scan.SamplePrefixesOmitted))
}
