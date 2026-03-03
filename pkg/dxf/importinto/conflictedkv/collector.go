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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
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

// CollectResult is the result of the collector.
type CollectResult struct {
	// total number of conflicted rows.
	RowCount      int64
	TotalFileSize int64
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

	fileSeq      int
	currFileSize int64
	writer       objectio.Writer
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
) *Collector {
	collector := &Collector{
		logger:         logger,
		store:          objStore,
		filenamePrefix: filenamePrefix,
		kvGroup:        kvGroup,
		result:         NewCollectResult(store.GetCodec().GetKeyspace()),
		hdlSet:         localSet,
	}
	base := NewBaseHandler(targetTbl, kvGroup, encoder, collector, logger)
	var h Handler
	if kvGroup == external.DataKVGroup {
		h = NewDataKVHandler(base)
	} else {
		h = NewIndexKVHandler(base, NewLazyRefreshedSnapshot(store), NewHandleFilter(globalSet))
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
	if c.writer == nil || c.currFileSize >= MaxConflictRowFileSize {
		if err := c.switchFile(ctx); err != nil {
			return errors.Trace(err)
		}
		c.currFileSize = 0
	}
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

	str, err := types.DatumsToString(row, true)
	if err != nil {
		return errors.Trace(err)
	}
	content := []byte(str + "\n")
	if _, err = c.writer.Write(ctx, content); err != nil {
		return errors.Trace(err)
	}

	c.result.RowCount++
	c.result.TotalFileSize += int64(len(content))
	c.result.Checksum.Update(kvPairs.Pairs)
	c.currFileSize += int64(len(content))
	return nil
}

func (c *Collector) switchFile(ctx context.Context) error {
	if c.writer != nil {
		if err := c.writer.Close(ctx); err != nil {
			c.logger.Warn("failed to close conflict row writer", zap.Error(err))
			return errors.Trace(err)
		}
		c.writer = nil
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
