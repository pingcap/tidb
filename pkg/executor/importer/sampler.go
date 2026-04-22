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
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
)

// SampledKVSizeResult contains the sampled source bytes and encoded KV sizes.
type SampledKVSizeResult struct {
	SourceSize  int64
	DataKVSize  uint64
	IndexKVSize uint64
}

// KVSizeSampleConfig contains the parser and encoder settings required by file-based KV size sampling.
type KVSizeSampleConfig struct {
	Format           string
	SQLMode          mysql.SQLMode
	Charset          *string
	ImportantSysVars map[string]string
	FieldNullDef     []string
	LineFieldsInfo   plannercore.LineFieldsInfo
	IgnoreLines      uint64

	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	ColumnAssignments  []*ast.Assignment
}

type kvSizeSampler struct {
	cfg           *KVSizeSampleConfig
	table         table.Table
	dataStore     storeapi.Storage
	dataFiles     []*mydump.SourceFileMeta
	logger        *zap.Logger
	fieldMappings []*FieldMapping
	insertColumns []*table.Column
	colAssignMu   sync.Mutex
}

// TotalKVSize returns the total encoded KV size in the sample.
func (r *SampledKVSizeResult) TotalKVSize() int64 {
	return int64(r.DataKVSize + r.IndexKVSize)
}

// SampleFileImportKVSize samples source rows with nextgen's KV encoder and returns
// the sampled source bytes and encoded KV sizes.
func SampleFileImportKVSize(
	ctx context.Context,
	cfg *KVSizeSampleConfig,
	tbl table.Table,
	dataStore storeapi.Storage,
	dataFiles []*mydump.SourceFileMeta,
	ksCodec []byte,
	logger *zap.Logger,
) (*SampledKVSizeResult, error) {
	sampler, err := newKVSizeSampler(cfg, tbl, dataStore, dataFiles, logger)
	if err != nil {
		return nil, err
	}
	return sampler.sample(ctx, ksCodec)
}

func (e *LoadDataController) sampleIndexSizeRatio(
	ctx context.Context,
	ksCodec []byte,
) (float64, error) {
	result, err := e.sampleKVSize(ctx, ksCodec)
	if err != nil {
		return 0, err
	}
	if result.DataKVSize == 0 {
		return 0, nil
	}
	return float64(result.IndexKVSize) / float64(result.DataKVSize), nil
}

func (e *LoadDataController) sampleKVSize(
	ctx context.Context,
	ksCodec []byte,
) (*SampledKVSizeResult, error) {
	sampler, err := newKVSizeSampler(e.buildKVSizeSampleConfig(), e.Table, e.dataStore, e.dataFiles, e.logger)
	if err != nil {
		return nil, err
	}
	return sampler.sample(ctx, ksCodec)
}

func (e *LoadDataController) buildKVSizeSampleConfig() *KVSizeSampleConfig {
	return &KVSizeSampleConfig{
		Format:             e.Format,
		SQLMode:            e.SQLMode,
		Charset:            e.Charset,
		ImportantSysVars:   e.ImportantSysVars,
		FieldNullDef:       append([]string(nil), e.FieldNullDef...),
		LineFieldsInfo:     e.LineFieldsInfo,
		IgnoreLines:        e.IgnoreLines,
		ColumnsAndUserVars: e.ColumnsAndUserVars,
		ColumnAssignments:  e.ColumnAssignments,
	}
}

func newKVSizeSampler(
	cfg *KVSizeSampleConfig,
	tbl table.Table,
	dataStore storeapi.Storage,
	dataFiles []*mydump.SourceFileMeta,
	logger *zap.Logger,
) (*kvSizeSampler, error) {
	if cfg == nil {
		return nil, errors.New("kv size sample config is nil")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if err := validateKVSizeSampleConfig(cfg); err != nil {
		return nil, err
	}
	fieldMappings, columnNames := buildFieldMappings(tbl, cfg.ColumnsAndUserVars)
	insertColumns, err := buildInsertColumns(tbl, columnNames, cfg.ColumnAssignments)
	if err != nil {
		return nil, err
	}
	return &kvSizeSampler{
		cfg:           cfg,
		table:         tbl,
		dataStore:     dataStore,
		dataFiles:     dataFiles,
		logger:        logger,
		fieldMappings: fieldMappings,
		insertColumns: insertColumns,
	}, nil
}

func validateKVSizeSampleConfig(cfg *KVSizeSampleConfig) error {
	switch cfg.Format {
	case DataFormatCSV, DataFormatSQL, DataFormatParquet:
	default:
		return exeerrors.ErrLoadDataUnsupportedFormat.GenWithStackByArgs(cfg.Format)
	}
	if len(cfg.LineFieldsInfo.FieldsEnclosedBy) > 0 &&
		(strings.HasPrefix(cfg.LineFieldsInfo.FieldsEnclosedBy, cfg.LineFieldsInfo.FieldsTerminatedBy) ||
			strings.HasPrefix(cfg.LineFieldsInfo.FieldsTerminatedBy, cfg.LineFieldsInfo.FieldsEnclosedBy)) {
		return exeerrors.ErrLoadDataWrongFormatConfig.GenWithStackByArgs("FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other")
	}
	return nil
}

func (s *kvSizeSampler) CreateColAssignSimpleExprs(
	ctx expression.BuildContext,
) (_ []expression.Expression, _ []contextutil.SQLWarn, retErr error) {
	return createColAssignSimpleExprs(s.cfg.ColumnAssignments, ctx, &s.colAssignMu)
}

func (s *kvSizeSampler) generateCSVConfig() *config.CSVConfig {
	return generateCSVConfig(s.cfg.FieldNullDef, s.cfg.LineFieldsInfo, true, false)
}

func (s *kvSizeSampler) getParser(
	ctx context.Context,
	chunk *checkpoints.ChunkCheckpoint,
) (mydump.Parser, error) {
	info := LoadDataReaderInfo{
		Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
			reader, err := mydump.OpenReader(ctx, &chunk.FileMeta, s.dataStore, compressedio.DecompressConfig{
				ZStdDecodeConcurrency: 1,
			})
			if err != nil {
				return nil, errors.Trace(err)
			}
			return reader, nil
		},
		Remote: &chunk.FileMeta,
	}
	parser, err := newLoadDataParser(
		ctx,
		s.logger,
		s.cfg.Format,
		s.cfg.SQLMode,
		s.cfg.Charset,
		s.generateCSVConfig(),
		s.dataStore,
		info,
	)
	if err != nil {
		return nil, err
	}
	parserReady := false
	defer func() {
		if parserReady {
			return
		}
		if err2 := parser.Close(); err2 != nil {
			s.logger.Warn("close parser failed", zap.Error(err2))
		}
	}()
	if chunk.Chunk.Offset == 0 {
		if err = HandleSkipNRows(parser, s.cfg.IgnoreLines); err != nil {
			return nil, err
		}
		parser.SetRowID(chunk.Chunk.PrevRowIDMax)
	} else {
		if err = parser.SetPos(chunk.Chunk.Offset, chunk.Chunk.PrevRowIDMax); err != nil {
			return nil, err
		}
	}
	parserReady = true
	return parser, nil
}

func (s *kvSizeSampler) getKVEncoder(
	logger *zap.Logger,
	chunk *checkpoints.ChunkCheckpoint,
	encTable table.Table,
) (*TableKVEncoder, error) {
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        s.cfg.SQLMode,
			Timestamp:      chunk.Timestamp,
			SysVars:        s.cfg.ImportantSysVars,
			AutoRandomSeed: chunk.Chunk.PrevRowIDMax,
		},
		Path:   chunk.FileMeta.Path,
		Table:  encTable,
		Logger: log.Logger{Logger: logger.With(zap.String("path", chunk.FileMeta.Path))},
	}
	return newTableKVEncoderInner(cfg, s, s.fieldMappings, s.insertColumns)
}

func (s *kvSizeSampler) sample(
	ctx context.Context,
	ksCodec []byte,
) (*SampledKVSizeResult, error) {
	if len(s.dataFiles) == 0 {
		return &SampledKVSizeResult{}, nil
	}
	perm := rand.Perm(len(s.dataFiles))
	files := make([]*mydump.SourceFileMeta, min(len(s.dataFiles), maxSampleFileCount))
	for i := range files {
		files[i] = s.dataFiles[perm[i]]
	}
	rowsPerFile := totalSampleRowCount / len(files)
	result := &SampledKVSizeResult{}
	var firstErr error
	for _, file := range files {
		sourceSize, dataKVSize, indexKVSize, err := s.sampleOneFile(ctx, file, ksCodec, rowsPerFile)
		if firstErr == nil {
			firstErr = err
		}
		result.SourceSize += sourceSize
		result.DataKVSize += dataKVSize
		result.IndexKVSize += indexKVSize
	}
	return result, firstErr
}

func (s *kvSizeSampler) sampleOneFile(
	ctx context.Context,
	file *mydump.SourceFileMeta,
	ksCodec []byte,
	maxRowCount int,
) (sourceSize int64, dataKVSize, indexKVSize uint64, err error) {
	chunk := &checkpoints.ChunkCheckpoint{
		Key:       checkpoints.ChunkCheckpointKey{Path: file.Path},
		FileMeta:  *file,
		Chunk:     mydump.Chunk{EndOffset: maxSampleFileSize},
		Timestamp: time.Now().Unix(),
	}
	idAlloc := kv.NewPanickingAllocators(s.table.Meta().SepAutoInc())
	tbl, err := tables.TableFromMeta(idAlloc, s.table.Meta())
	if err != nil {
		return 0, 0, 0, errors.Annotatef(err, "failed to tables.TableFromMeta %s", s.table.Meta().Name)
	}
	encoder, err := s.getKVEncoder(s.logger, chunk, tbl)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err2 := encoder.Close(); err2 != nil {
			s.logger.Warn("close encoder failed", zap.Error(err2))
		}
	}()
	parser, err := s.getParser(ctx, chunk)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err2 := parser.Close(); err2 != nil {
			s.logger.Warn("close parser failed", zap.Error(err2))
		}
	}()

	var (
		count   int
		kvBatch = newEncodedKVGroupBatch(ksCodec, maxRowCount)
	)
	for count < maxRowCount {
		startPos, _ := parser.Pos()
		if s.cfg.Format != DataFormatParquet && startPos >= chunk.Chunk.EndOffset {
			break
		}

		readErr := parser.ReadRow()
		if readErr != nil {
			if errors.Cause(readErr) == io.EOF {
				break
			}
			return 0, 0, 0, common.ErrEncodeKV.Wrap(readErr).GenWithStackByArgs(chunk.GetKey(), startPos)
		}

		lastRow := parser.LastRow()
		sourceSize += s.sampledRowSourceSize(parser, startPos, lastRow)

		kvs, encodeErr := encoder.Encode(lastRow.Row, nil, lastRow.RowID)
		parser.RecycleRow(lastRow)
		if encodeErr != nil {
			return 0, 0, 0, common.ErrEncodeKV.Wrap(encodeErr).GenWithStackByArgs(chunk.GetKey(), startPos)
		}
		if _, err = kvBatch.add(kvs); err != nil {
			return 0, 0, 0, err
		}
		count++
	}
	dataKVSize, indexKVSize = kvBatch.groupChecksum.DataAndIndexSumSize()
	return sourceSize, dataKVSize, indexKVSize, nil
}

func (s *kvSizeSampler) sampledRowSourceSize(parser mydump.Parser, startPos int64, row mydump.Row) int64 {
	// Sampling needs per-row source bytes, not buffered reader progress.
	// SQL/CSV parsers expose byte offsets through Pos(), including compressed
	// input where Pos() tracks uncompressed bytes and stays aligned with the
	// RealSize-based source totals. Parquet Pos() is row-count based and must
	// fall back to the row-size estimate.
	if s.cfg.Format == DataFormatParquet {
		return int64(row.Length)
	}
	endPos, _ := parser.Pos()
	if rowDelta := endPos - startPos; rowDelta > 0 {
		return rowDelta
	}
	return int64(row.Length)
}
