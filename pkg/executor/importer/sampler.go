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

package importer

import (
	"context"
	goerrors "errors"
	"math/rand"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/table/tables"
	"go.uber.org/zap"
)

const (
	// maxSampleFileCount is the maximum number of files to sample.
	maxSampleFileCount = 3
	// totalSampleRowCount is the total number of rows to sample.
	// we want to sample about 30 rows in total, if we have 3 files, we sample
	// 10 rows per file. if we have less files, we sample more rows per file.
	totalSampleRowCount = maxSampleFileCount * 10
)

var (
	// maxSampleFileSize is the maximum file size to sample.
	// if we sample maxSampleFileCount files, we only read 10 rows at most, if
	// each row >= 1MiB, the index KV size ratio is quite small even with large
	//number of indices. such as each index KV is 3K, we have 100 indices, it
	// means 300K index KV size per row, the ratio is about 0.3.
	// so even if we sample less rows due to very long rows, it's still ok for
	// our resource params calculation.
	// if total files < maxSampleFileCount, the total file size is small, the
	// accuracy of the ratio is not that important.
	maxSampleFileSize int64 = 10 * units.MiB
	stopIterErr             = goerrors.New("stop iteration")
)

func (e *LoadDataController) sampleIndexSizeRatio(
	ctx context.Context,
	ksCodec []byte,
) (float64, error) {
	if len(e.dataFiles) == 0 {
		return 0, nil
	}
	perm := rand.Perm(len(e.dataFiles))
	files := make([]*mydump.SourceFileMeta, min(len(e.dataFiles), maxSampleFileCount))
	for i := range files {
		files[i] = e.dataFiles[perm[i]]
	}
	rowsPerFile := totalSampleRowCount / len(files)
	var (
		totalDataKVSize, totalIndexKVSize uint64
		firstErr                          error
	)
	for _, file := range files {
		dataKVSize, indexKVSize, err := e.sampleIndexRatioForOneFile(ctx, file, ksCodec, rowsPerFile)
		if firstErr == nil {
			firstErr = err
		}
		totalDataKVSize += dataKVSize
		totalIndexKVSize += indexKVSize
	}
	if totalDataKVSize == 0 {
		return 0, firstErr
	}
	return float64(totalIndexKVSize) / float64(totalDataKVSize), firstErr
}

func (e *LoadDataController) sampleIndexRatioForOneFile(
	ctx context.Context,
	file *mydump.SourceFileMeta,
	ksCodec []byte,
	maxRowCount int,
) (dataKVSize, indexKVSize uint64, err error) {
	chunk := &checkpoints.ChunkCheckpoint{
		Key:       checkpoints.ChunkCheckpointKey{Path: file.Path},
		FileMeta:  *file,
		Chunk:     mydump.Chunk{EndOffset: maxSampleFileSize},
		Timestamp: time.Now().Unix(),
	}
	idAlloc := kv.NewPanickingAllocators(e.Table.Meta().SepAutoInc())
	tbl, err := tables.TableFromMeta(idAlloc, e.Table.Meta())
	if err != nil {
		return 0, 0, errors.Annotatef(err, "failed to tables.TableFromMeta %s", e.Table.Meta().Name)
	}
	encoder, err := e.getKVEncoder(e.logger, chunk, tbl)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err2 := encoder.Close(); err2 != nil {
			e.logger.Warn("close encoder failed", zap.Error(err2))
		}
	}()
	parser, err := e.getParser(ctx, chunk)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if err2 := parser.Close(); err2 != nil {
			e.logger.Warn("close parser failed", zap.Error(err2))
		}
	}()

	var count int
	sendFn := func(context.Context, *encodedKVGroupBatch) error {
		count++
		if count >= maxRowCount {
			return stopIterErr
		}
		return nil
	}
	chunkEnc := &chunkEncoder{
		chunkName:     chunk.GetKey(),
		readFn:        parserEncodeReader(parser, chunk.Chunk.EndOffset, chunk.GetKey()),
		sendFn:        sendFn,
		encoder:       encoder,
		keyspace:      ksCodec,
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(ksCodec),
	}
	err = chunkEnc.encodeLoop(ctx)
	if goerrors.Is(err, stopIterErr) {
		err = nil
	}
	dataKVSize, indexKVSize = chunkEnc.groupChecksum.DataAndIndexSumSize()
	return dataKVSize, indexKVSize, err
}
