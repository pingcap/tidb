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
	"strings"

	"github.com/pingcap/errors"
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
	// GetStorageURI returns the cloud storage URI to scan. The monitor calls it
	// on every run, so it reads the latest value of the system variable.
	GetStorageURI func() string
	Logger        *zap.Logger
	// RetainedPrefixes are object-store namespaces that other DXF components
	// intentionally keep after a task finishes. Objects under them are managed
	// by dedicated retention policies and are not orphan data.
	RetainedPrefixes []string
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
	if cfg.GetStorageURI == nil {
		cfg.GetStorageURI = func() string { return "" }
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
	storageURI := m.cfg.GetStorageURI()
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
			m.cfg.Logger.Warn("global sort orphan data monitor failed to create storage", zap.Error(err))
		}
		return
	}
	defer storage.Close()

	stats, err := scanOrphanData(ctx, storage, m.cfg.RetainedPrefixes)
	if err != nil {
		if ctx.Err() == nil {
			m.cfg.Logger.Warn("global sort orphan data monitor failed to scan storage", zap.Error(err))
		}
		return
	}

	// Re-check after the scan. A producer that appears and disappears entirely
	// between the two checks (empty -> active -> empty) is not detected. We
	// accept that ABA window for now: a task removes its own objects before its
	// row leaves the task table, so any value published from a scan that raced a
	// cleanup is corrected by the next scan.
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

	metrics.GlobalSortOrphanDataSize.Set(float64(stats.sizeBytes))
	m.cfg.Logger.Info("global sort orphan data monitor success",
		zap.Int64("size-bytes", stats.sizeBytes),
		zap.Int64("object-count", stats.objectCount),
		zap.Strings("sample-objects", stats.sampleObjects),
		zap.Bool("sample-truncated", stats.truncated))
}

// sampleObjectLimit bounds how many object names are kept for diagnostics.
const sampleObjectLimit = 10

// isRetainedObjectPath reports whether path belongs to a retained namespace.
func isRetainedObjectPath(path string, retainedPrefixes []string) bool {
	for _, prefix := range retainedPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// scanStats summarizes the global-sort orphan objects found by a scan.
type scanStats struct {
	sizeBytes     int64
	objectCount   int64
	sampleObjects []string
	truncated     bool
}

// scanOrphanData walks storage and returns global-sort orphan object statistics,
// skipping objects under retainedPrefixes.
func scanOrphanData(ctx context.Context, storage storeapi.Storage, retainedPrefixes []string) (scanStats, error) {
	var stats scanStats
	err := storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		if isRetainedObjectPath(path, retainedPrefixes) {
			return nil
		}
		stats.objectCount++
		if len(stats.sampleObjects) < sampleObjectLimit {
			stats.sampleObjects = append(stats.sampleObjects, path)
		} else {
			stats.truncated = true
		}
		if size < 0 {
			return nil
		}
		stats.sizeBytes += size
		return nil
	})
	if err != nil {
		return scanStats{}, errors.Annotate(err, "scan global sort orphan objects")
	}
	return stats, nil
}
