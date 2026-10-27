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

// Config configures an orphan data Cleaner.
type Config struct {
	ActiveProducerChecker ActiveProducerChecker
	// GetStorageURI returns the cloud storage URI to scan. The cleaner calls it
	// on every run, so it reads the latest value of the system variable.
	GetStorageURI func() string
	Logger        *zap.Logger
	// RetainedPrefixes are object-store namespaces that other DXF components
	// intentionally keep after a task finishes. Objects under them are managed
	// by dedicated retention policies and are not orphan data.
	RetainedPrefixes []string
}

type missingActiveProducerChecker struct{}

func (missingActiveProducerChecker) HasActiveProducers(context.Context) (bool, error) {
	return true, nil
}

type storeFactory func(context.Context, string) (storeapi.Storage, error)

// Cleaner measures and deletes orphan global-sort objects when no producer is
// active. Its gauge records the size observed before cleanup.
type Cleaner struct {
	cfg Config
	// storeFactory builds the object store for a scan; tests substitute it.
	storeFactory storeFactory
}

// NewCleaner creates an orphan data Cleaner.
func NewCleaner(cfg Config) *Cleaner {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ActiveProducerChecker == nil {
		cfg.ActiveProducerChecker = missingActiveProducerChecker{}
	}
	if cfg.GetStorageURI == nil {
		cfg.GetStorageURI = func() string { return "" }
	}
	return &Cleaner{
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

// Trigger measures orphan global-sort objects and deletes a bounded batch of
// scanned keys after confirming that no producer is active. It runs
// synchronously and is not safe for concurrent use: the scheduler cleanup loop
// calls it serially from its own goroutine, which also owns cancellation.
func (c *Cleaner) Trigger(ctx context.Context) {
	storageURI := c.cfg.GetStorageURI()
	if storageURI == "" {
		return
	}

	hasActiveProducers, err := c.cfg.ActiveProducerChecker.HasActiveProducers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.cfg.Logger.Warn("global sort orphan data cleaner failed to check active producers", zap.Error(err))
		}
		return
	}
	if hasActiveProducers {
		return
	}

	storage, err := c.storeFactory(ctx, storageURI)
	if err != nil {
		if ctx.Err() == nil {
			c.cfg.Logger.Warn("global sort orphan data cleaner failed to create storage", zap.Error(err))
		}
		return
	}
	defer storage.Close()

	stats, err := scanOrphanData(ctx, storage, c.cfg.RetainedPrefixes)
	if err != nil {
		if ctx.Err() == nil {
			c.cfg.Logger.Warn("global sort orphan data cleaner failed to scan storage", zap.Error(err))
		}
		return
	}

	// Re-check after the scan, then delete only the exact captured keys: never
	// expand a scanned namespace with another listing. Normal task IDs are not
	// reused, so a new task cannot own these keys. A task's row remains until
	// its normal cleanup finishes, making a task that starts and finishes
	// between the two checks harmless. Late writers can recreate orphan keys;
	// a future sweep will find them.
	hasActiveProducers, err = c.cfg.ActiveProducerChecker.HasActiveProducers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.cfg.Logger.Warn("global sort orphan data cleaner failed to check active producers", zap.Error(err))
		}
		return
	}
	if hasActiveProducers {
		c.cfg.Logger.Info("global sort orphan data cleaner discarded scan because tasks appeared")
		return
	}

	// Publish the complete observation before cleanup, even if deletion fails or
	// the batch limit leaves some objects for a future sweep.
	metrics.GlobalSortOrphanDataSize.WithLabelValues().Set(float64(stats.sizeBytes))
	if len(stats.deletionCandidates) > 0 {
		if err := storage.DeleteFiles(ctx, stats.deletionCandidates); err != nil {
			if ctx.Err() == nil {
				// Storage errors can embed credentials from the supplied URI.
				c.cfg.Logger.Warn("global sort orphan data cleaner failed to delete objects",
					zap.Int("deletion-candidate-count", len(stats.deletionCandidates)))
			}
		}
	}
	c.cfg.Logger.Info("global sort orphan data cleaner finished",
		zap.Int64("size-bytes", stats.sizeBytes),
		zap.Int64("object-count", stats.objectCount),
		zap.Int("deletion-candidate-count", len(stats.deletionCandidates)),
		zap.Strings("sample-objects", stats.sampleObjects),
		zap.Bool("sample-truncated", stats.truncated))
}

// Reset removes the published orphan data size. The scheduler calls it on owner
// change, so a former owner stops exporting the last value it observed. The
// series is deleted rather than set to zero, so a measured zero stays
// distinguishable from "not measured".
func (c *Cleaner) Reset() {
	metrics.GlobalSortOrphanDataSize.Reset()
}

const (
	// sampleObjectLimit bounds how many object names are kept for diagnostics.
	sampleObjectLimit = 10
	// cleanupObjectLimit bounds each sweep to one object-store deletion batch.
	cleanupObjectLimit = 1000
)

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
	sizeBytes          int64
	objectCount        int64
	sampleObjects      []string
	truncated          bool
	deletionCandidates []string
}

// scanOrphanData walks all storage to measure orphan objects and captures a
// bounded set of exact keys for deletion, skipping retainedPrefixes.
func scanOrphanData(ctx context.Context, storage storeapi.Storage, retainedPrefixes []string) (scanStats, error) {
	var stats scanStats
	err := storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		if isRetainedObjectPath(path, retainedPrefixes) {
			return nil
		}
		stats.objectCount++
		if len(stats.deletionCandidates) < cleanupObjectLimit {
			stats.deletionCandidates = append(stats.deletionCandidates, path)
		}
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
