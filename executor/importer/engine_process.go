// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"
	"io"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

// encode and write all data in this engine as sst files, and write index in indexEngine
// todo: would be better if merge with backend/local/Engine
type engineProcessor struct {
	engineID      int32
	fullTableName string
	backend       backend.Backend
	tableInfo     *checkpoints.TidbTableInfo
	logger        *zap.Logger
	tableImporter *tableImporter
	kvSorted      bool
	rowOrdered    bool
	indexEngine   *backend.OpenedEngine
	chunks        []*checkpoints.ChunkCheckpoint
	kvStore       kv.Storage
}

func (ep *engineProcessor) process(ctx context.Context) (*backend.ClosedEngine, error) {
	ep.logger.Info("encode kv data and write start")
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: ep.tableInfo,
	}
	if !ep.rowOrdered {
		dataEngineCfg.Local.Compact = true
		dataEngineCfg.Local.CompactConcurrency = 4
		dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	}
	dataEngine, err := ep.backend.OpenEngine(ctx, dataEngineCfg, ep.fullTableName, ep.engineID)
	if err != nil {
		return nil, err
	}

	err = ep.localSort(ctx, dataEngine)
	// ctx maybe canceled, so to avoid Close engine failed, we use `context.Background()` here
	// todo: remove ctx param in Close()
	closeCtx := ctx
	if common.IsContextCanceledError(err) {
		closeCtx = context.Background()
	}
	closedDataEngine, err2 := dataEngine.Close(closeCtx)
	if err2 != nil {
		ep.logger.Warn("flush all chunk checkpoints failed before manually exits", zap.Error(err2))
	}
	return closedDataEngine, err
}

// sort data in all chunks, then close the opened data engine
func (ep *engineProcessor) localSort(ctx context.Context, dataEngine *backend.OpenedEngine) (err error) {
	dataWriterCfg := &backend.LocalWriterConfig{
		IsKVSorted: ep.kvSorted,
	}
	closer := multiCloser{
		logger: ep.logger,
	}
	defer func() {
		if err != nil {
			closer.Close()
		}
	}()
	for _, chunk := range ep.chunks {
		var (
			parser                  mydump.Parser
			encoder                 kvEncoder
			dataWriter, indexWriter *backend.LocalEngineWriter
		)
		closer.reset()
		parser, err = ep.tableImporter.getParser(ctx, chunk)
		if err != nil {
			return err
		}
		closer.add(parser)
		encoder, err = ep.tableImporter.getKVEncoder(chunk)
		if err != nil {
			return err
		}
		closer.add(encoder)
		// todo: on panic which will be recovered since we run in tidb, we need to make sure all opened fd is closed.
		dataWriter, err = dataEngine.LocalWriter(ctx, dataWriterCfg)
		if err != nil {
			return err
		}
		closer.addFn(func() error {
			_, err2 := dataWriter.Close(ctx)
			return err2
		})
		indexWriter, err = ep.indexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return err
		}
		closer.addFn(func() error {
			_, err2 := indexWriter.Close(ctx)
			return err2
		})

		cp := &chunkProcessor{
			parser:      parser,
			chunkInfo:   chunk,
			logger:      ep.logger.With(zap.String("key", chunk.GetKey())),
			kvsCh:       make(chan []deliveredRow, maxKVQueueSize),
			dataWriter:  dataWriter,
			indexWriter: indexWriter,
			encoder:     encoder,
			kvCodec:     ep.kvStore.GetCodec(),
		}
		// todo: process in parallel
		err = cp.process(ctx)
		if err != nil {
			return err
		}
		// chunk process is responsible to close data/index writer
		cp.close(ctx)
	}
	return nil
}

type multiCloser struct {
	closers []func() error
	logger  *zap.Logger
}

func (m *multiCloser) add(c io.Closer) {
	m.closers = append(m.closers, c.Close)
}

func (m *multiCloser) addFn(c func() error) {
	m.closers = append(m.closers, c)
}

func (m *multiCloser) reset() {
	m.closers = m.closers[:0]
}

func (m *multiCloser) Close() {
	// close in reverse append order
	for i := len(m.closers) - 1; i >= 0; i-- {
		fn := m.closers[i]
		if err := fn(); err != nil {
			m.logger.Warn("failed to close", zap.Error(err))
		}
	}
}
