// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictedkv

import (
	"context"
	"fmt"
	"path"
	"sync/atomic"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// MaxConflictRowFileSize is the maximum size of the conflict row file.
// exported for testing.
var MaxConflictRowFileSize int64 = 8 * units.GiB

// maxTotalConflictRowFileSize is the hard limit of all conflict-row files in
// one collect-conflicts subtask.
//
// We hardcode this to 1GiB intentionally and do not expose it as a user config
// for now. The goal is to collect usage feedback first before deciding whether
// we should introduce a configurable knob in future.
var maxTotalConflictRowFileSize int64 = units.GiB

// CollectResult is the result of the collector.
type CollectResult struct {
	// total number of conflicted rows.
	RowCount      int64
	TotalFileSize int64
	// RowRecordingCapped is true if conflicted-row file recording is stopped due to
	// maxTotalConflictRowFileSize.
	RowRecordingCapped bool
	// it's the checksum of the encoded KV of the conflicted rows.
	Checksum *verification.KVChecksum
	// name of the files which records the conflicted rows
	Filenames []string
}

// NewCollectResult creates a new collect result.
func NewCollectResult(keyspace []byte) *CollectResult {
	return &CollectResult{
		Checksum:  verification.NewKVChecksumWithKeyspace(keyspace),
		Filenames: make([]string, 0, 1),
	}
}

// Merge merges other results.
func (r *CollectResult) Merge(other *CollectResult) {
	if other == nil {
		return
	}
	r.RowCount += other.RowCount
	r.TotalFileSize += other.TotalFileSize
	r.RowRecordingCapped = r.RowRecordingCapped || other.RowRecordingCapped
	r.Checksum.Add(other.Checksum)
	r.Filenames = append(r.Filenames, other.Filenames...)
}

// Collector collects info about conflicted rows.
type Collector struct {
	logger         *zap.Logger
	store          storeapi.Storage
	filenamePrefix string
	kvGroup        string
	handler        Handler
	result         *CollectResult
	hdlSet         *BoundedHandleSet
	// total recorded file size is shared across collectors in one collect-conflicts
	// subtask, to enforce one global hard limit.
	sharedTotalFileSize *atomic.Int64

	fileSeq      int
	currFileSize int64
	writer       objectio.Writer
	// once true, no more conflicted rows will be written to files.
	stopRecording bool
}

// NewCollector creates a new conflicted KV info collector.
func NewCollector(
	targetTbl table.Table,
	logger *zap.Logger,
	objStore storeapi.Storage,
	store tidbkv.Storage,
	filenamePrefix string,
	kvGroup string,
	encoder *importer.TableKVEncoder,
	globalSet, localSet *BoundedHandleSet,
	sharedTotalFileSize *atomic.Int64,
	progressCollector execute.Collector,
	trafficRec TrafficRecorder,
) *Collector {
	// Safety check: if caller doesn't pass the shared counter, allocate one to
	// avoid nil-pointer panics when recordRowToFile does atomic Add.
	if sharedTotalFileSize == nil {
		sharedTotalFileSize = &atomic.Int64{}
	}
	collector := &Collector{
		logger:              logger,
		store:               objStore,
		filenamePrefix:      filenamePrefix,
		kvGroup:             kvGroup,
		result:              NewCollectResult(store.GetCodec().GetKeyspace()),
		hdlSet:              localSet,
		sharedTotalFileSize: sharedTotalFileSize,
	}
	base := NewBaseHandler(targetTbl, kvGroup, encoder, collector, progressCollector, logger)
	var h Handler
	if kvGroup == external.DataKVGroup {
		h = NewDataKVHandler(base)
	} else {
		h = NewIndexKVHandler(base, NewLazyRefreshedSnapshot(store, trafficRec), NewHandleFilter(globalSet))
	}
	collector.handler = h
	return collector
}

// Run starts the collector to collect conflicted KV info.
func (c *Collector) Run(ctx context.Context, ch chan *external.KVPair) (err error) {
	if err = c.handler.PreRun(); err != nil {
		return err
	}
	return c.handler.Run(ctx, ch)
}

// HandleEncodedRow handles the re-encoded row from conflict KV.
func (c *Collector) HandleEncodedRow(ctx context.Context, handle tidbkv.Handle,
	row []types.Datum, kvPairs *kv.Pairs) error {
	// every conflicted row from data KV group must be recorded, but for index KV
	// group, they might come from the same row, so we only need to record it on
	// the first time we meet it.
	// currently, we use memory to do this check, if it's too large, we just skip
	// the checking and skip later checksum.
	//
	// an alternative solution is to upload those handles to sort storage and
	// check them in another pass later.
	if c.kvGroup != external.DataKVGroup {
		c.hdlSet.Add(handle)
	}

	if err := c.recordRowToFile(ctx, row); err != nil {
		return err
	}
	c.result.RowCount++
	c.result.Checksum.Update(kvPairs.Pairs)
	return nil
}

func (c *Collector) recordRowToFile(ctx context.Context, row []types.Datum) error {
	if c.stopRecording {
		return nil
	}

	str, err := types.DatumsToString(row, true)
	if err != nil {
		return errors.Trace(err)
	}
	contentSize := int64(len(str) + 1)
	// We intentionally check the hard limit after Add to keep cross-collector
	// accounting simple. A small overflow above the threshold is acceptable for
	// now while we collect real-world feedback for this feature.
	limit := maxTotalConflictRowFileSize
	failpoint.InjectCall("mockTotalConflictRowFileSizeLimit", &limit)
	globalTotalSize := c.sharedTotalFileSize.Add(contentSize)
	if globalTotalSize > limit {
		return c.onTotalSizeLimitExceeded(ctx, globalTotalSize, contentSize, limit)
	}

	if c.writer == nil || c.currFileSize >= MaxConflictRowFileSize {
		if err := c.switchFile(ctx); err != nil {
			return errors.Trace(err)
		}
		c.currFileSize = 0
	}

	content := []byte(str + "\n")
	if _, err = c.writer.Write(ctx, content); err != nil {
		return errors.Trace(err)
	}
	c.result.TotalFileSize += contentSize
	c.currFileSize += contentSize
	return nil
}

func (c *Collector) switchFile(ctx context.Context) error {
	if c.writer != nil {
		err := c.writer.Close(ctx)
		c.writer = nil
		if err != nil {
			c.logger.Warn("failed to close conflict row writer", zap.Error(err))
			return errors.Trace(err)
		}
	}
	c.fileSeq++
	filename := getRowFileName(c.filenamePrefix, c.fileSeq)
	c.logger.Info("switch conflict row file", zap.String("filename", filename),
		zap.String("lastFileSize", units.BytesSize(float64(c.currFileSize))))
	writer, err := c.store.Create(ctx, filename, &storeapi.WriterOption{
		Concurrency: 20,
		PartSize:    external.MinUploadPartSize,
	})
	if err != nil {
		return errors.Trace(err)
	}
	c.result.Filenames = append(c.result.Filenames, filename)
	c.writer = writer
	return nil
}

func (c *Collector) onTotalSizeLimitExceeded(ctx context.Context, totalSize, rowSize, limit int64) error {
	if c.stopRecording {
		return nil
	}
	c.stopRecording = true
	c.result.RowRecordingCapped = true
	c.logger.Info("conflicted row files reached the hardcoded total size limit, stop recording more rows",
		zap.String("sizeLimit", units.BytesSize(float64(limit))),
		zap.String("currentTotalFileSize", units.BytesSize(float64(totalSize))),
		zap.String("nextRowSize", units.BytesSize(float64(rowSize))),
	)
	if c.writer != nil {
		err := c.writer.Close(ctx)
		c.writer = nil
		c.currFileSize = 0
		if err != nil {
			c.logger.Warn("failed to close conflict row writer", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

// Close the collector.
func (c *Collector) Close(ctx context.Context) error {
	var firstErr common.OnceError
	firstErr.Set(c.handler.Close(ctx))
	if c.writer != nil {
		firstErr.Set(c.writer.Close(ctx))
	}
	return firstErr.Get()
}

// GetCollectResult returns the collect result.
func (c *Collector) GetCollectResult() *CollectResult {
	return c.result
}

// getRowFileName returns the file name to store the conflict rows.
// user can check this file to resolve the conflict rows manually.
func getRowFileName(prefix string, seq int) string {
	return path.Join(prefix, fmt.Sprintf("data-%04d.txt", seq))
}
