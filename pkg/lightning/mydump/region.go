// Copyright 2019 PingCAP, Inc.
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

package mydump

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	tableRegionSizeWarningThreshold int64 = 1024 * 1024 * 1024
	// the increment ratio of large CSV file size threshold by `region-split-size`
	largeCSVLowerThresholdRation = 10
	// TableFileSizeINF for compressed size, for lightning 10TB is a relatively big value and will strongly affect efficiency
	// It's used to make sure compressed files can be read until EOF. Because we can't get the exact decompressed size of the compressed files.
	TableFileSizeINF = 10 * 1024 * tableRegionSizeWarningThreshold
	// CompressSizeFactor is used to adjust compressed data size
	CompressSizeFactor = 5
)

// TableRegion contains information for a table region during import.
type TableRegion struct {
	EngineID int32

	DB         string
	Table      string
	FileMeta   SourceFileMeta
	ExtendData ExtendColumnData

	Chunk Chunk
}

// RowIDMin returns the minimum row ID of this table region.
func (reg *TableRegion) RowIDMin() int64 {
	return reg.Chunk.PrevRowIDMax + 1
}

// Rows returns the row counts of this table region.
func (reg *TableRegion) Rows() int64 {
	return reg.Chunk.RowIDMax - reg.Chunk.PrevRowIDMax
}

// Offset gets the offset in the file of this table region.
func (reg *TableRegion) Offset() int64 {
	return reg.Chunk.Offset
}

// Size gets the size of this table region.
func (reg *TableRegion) Size() int64 {
	return reg.Chunk.EndOffset - reg.Chunk.Offset
}

// AllocateEngineIDs allocates the table engine IDs.
func AllocateEngineIDs(
	filesRegions []*TableRegion,
	dataFileSizes []float64,
	batchSize float64,
	batchImportRatio float64,
	engineConcurrency float64,
) {
	totalDataFileSize := 0.0
	for _, dataFileSize := range dataFileSizes {
		totalDataFileSize += dataFileSize
	}

	// No need to batch if the size is too small :)
	if totalDataFileSize <= batchSize {
		return
	}

	curEngineID := int32(0)
	curEngineSize := 0.0
	curBatchSize := batchSize

	// import() step will not be concurrent.
	// If multiple Batch end times are close, it will result in multiple
	// Batch import serials. We need use a non-uniform batch size to create a pipeline effect.
	// Here we calculate the total number of engines, which is needed to compute the scale up
	//
	//     Total/B1 = 1/(1-R) * (N - 1/beta(N, R))
	//              ≲ N/(1-R)
	//
	// We use a simple brute force search since the search space is small.
	ratio := totalDataFileSize * (1 - batchImportRatio) / batchSize
	n := math.Ceil(ratio)
	logGammaNPlusR, _ := math.Lgamma(n + batchImportRatio)
	logGammaN, _ := math.Lgamma(n)
	logGammaR, _ := math.Lgamma(batchImportRatio)
	invBetaNR := math.Exp(logGammaNPlusR - logGammaN - logGammaR) // 1/B(N, R) = Γ(N+R)/Γ(N)Γ(R)
	for {
		if n <= 0 || n > engineConcurrency {
			n = engineConcurrency
			break
		}
		realRatio := n - invBetaNR
		if realRatio >= ratio {
			// we don't have enough engines. reduce the batch size to keep the pipeline smooth.
			curBatchSize = totalDataFileSize * (1 - batchImportRatio) / realRatio
			break
		}
		invBetaNR *= 1 + batchImportRatio/n // Γ(X+1) = X * Γ(X)
		n += 1.0
	}

	for i, dataFileSize := range dataFileSizes {
		filesRegions[i].EngineID = curEngineID
		curEngineSize += dataFileSize

		if curEngineSize >= curBatchSize {
			curEngineSize = 0
			curEngineID++

			i := float64(curEngineID)
			// calculate the non-uniform batch size
			if i >= n {
				curBatchSize = batchSize
			} else {
				// B_(i+1) = B_i * (I/W/(N-i) + 1)
				curBatchSize *= batchImportRatio/(n-i) + 1.0
			}
		}
	}
}

// DataDivideConfig config used to divide data files into chunks/engines(regions in this context).
type DataDivideConfig struct {
	ColumnCnt int
	// limit of engine size, we have a complex algorithm to calculate the best engine size, see AllocateEngineIDs.
	EngineDataSize int64
	// max chunk size(inside this file we named it region which collides with TiKV region)
	MaxChunkSize int64
	// number of concurrent workers to dive data files
	Concurrency int
	// number of engine runs concurrently, need this to calculate the best engine size for pipelining local-sort and import.
	// todo: remove those 2 params, the algorithm seems useless, since we can import concurrently now, the foundation
	// assumption of the algorithm is broken.
	EngineConcurrency int
	// used together with prev param. it is 0.75 nearly all the time, see Mydumper.BatchImportRatio.
	// this variable is defined as speed-write-to-TiKV / speed-to-do-local-sort
	BatchImportRatio float64
	// used to split large CSV files, to limit concurrency of data read/seek operations
	// when nil, no limit.
	IOWorkers *worker.Pool
	// we need it read row-count for parquet, and to read line terminator to split large CSV files
	Store     storage.ExternalStorage
	TableMeta *MDTableMeta

	// only used when split large CSV files.
	StrictFormat           bool
	DataCharacterSet       string
	DataInvalidCharReplace string
	ReadBlockSize          int64
	CSV                    config.CSVConfig
}

// NewDataDivideConfig creates a new DataDivideConfig from lightning cfg.
func NewDataDivideConfig(cfg *config.Config,
	columns int,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
	meta *MDTableMeta,
) *DataDivideConfig {
	return &DataDivideConfig{
		ColumnCnt:              columns,
		EngineDataSize:         int64(cfg.Mydumper.BatchSize),
		MaxChunkSize:           int64(cfg.Mydumper.MaxRegionSize),
		Concurrency:            cfg.App.RegionConcurrency,
		EngineConcurrency:      cfg.App.TableConcurrency,
		BatchImportRatio:       cfg.Mydumper.BatchImportRatio,
		IOWorkers:              ioWorkers,
		Store:                  store,
		TableMeta:              meta,
		StrictFormat:           cfg.Mydumper.StrictFormat,
		DataCharacterSet:       cfg.Mydumper.DataCharacterSet,
		DataInvalidCharReplace: cfg.Mydumper.DataInvalidCharReplace,
		ReadBlockSize:          int64(cfg.Mydumper.ReadBlockSize),
		CSV:                    cfg.Mydumper.CSV,
	}
}

// MakeTableRegions create a new table region.
// row-id range of returned TableRegion is increasing monotonically
func MakeTableRegions(
	ctx context.Context,
	cfg *DataDivideConfig,
) ([]*TableRegion, error) {
	// Split files into regions
	type fileRegionRes struct {
		info    FileInfo
		regions []*TableRegion
		sizes   []float64
		err     error
	}

	start := time.Now()

	concurrency := max(cfg.Concurrency, 2)
	var fileRegionsMap sync.Map

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	meta := cfg.TableMeta
	for _, info := range meta.DataFiles {
		info := info
		eg.Go(func() error {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}

			var (
				regions []*TableRegion
				sizes   []float64
				err     error
			)
			dataFileSize := info.FileMeta.FileSize
			if info.FileMeta.Type == SourceTypeParquet {
				regions, sizes, err = makeParquetFileRegion(egCtx, cfg, info)
			} else if info.FileMeta.Type == SourceTypeCSV && cfg.StrictFormat &&
				info.FileMeta.Compression == CompressionNone &&
				dataFileSize > cfg.MaxChunkSize+cfg.MaxChunkSize/largeCSVLowerThresholdRation {
				// If a csv file is overlarge, we need to split it into multiple regions.
				// Note: We can only split a csv file whose format is strict.
				// We increase the check threshold by 1/10 of the `max-region-size` because the source file size dumped by tools
				// like dumpling might be slight exceed the threshold when it is equal `max-region-size`, so we can
				// avoid split a lot of small chunks.
				// If a csv file is compressed, we can't split it now because we can't get the exact size of a row.
				regions, sizes, err = SplitLargeCSV(egCtx, cfg, info)
			} else {
				regions, sizes, err = MakeSourceFileRegion(egCtx, cfg, info)
			}
			if err != nil {
				log.FromContext(egCtx).Error("make source file region error", zap.Error(err), zap.String("file_path", info.FileMeta.Path))
				return err
			}
			result := fileRegionRes{info: info, regions: regions, sizes: sizes, err: err}
			fileRegionsMap.Store(info.FileMeta.Path, result)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	filesRegions := make([]*TableRegion, 0, len(meta.DataFiles))
	dataFileSizes := make([]float64, 0, len(meta.DataFiles))
	// rebase row-id for all chunk
	rowIDBase := int64(0)
	for _, dataFile := range meta.DataFiles {
		v, ok := fileRegionsMap.Load(dataFile.FileMeta.Path)
		if !ok {
			return nil, errors.Errorf("file %s not found in MakeTableRegions", dataFile.FileMeta.Path)
		}
		//nolint: forcetypeassert
		fileRegionsRes := v.(fileRegionRes)
		for _, region := range fileRegionsRes.regions {
			region.Chunk.PrevRowIDMax += rowIDBase
			region.Chunk.RowIDMax += rowIDBase
		}
		filesRegions = append(filesRegions, fileRegionsRes.regions...)
		dataFileSizes = append(dataFileSizes, fileRegionsRes.sizes...)
		rowIDBase = fileRegionsRes.regions[len(fileRegionsRes.regions)-1].Chunk.RowIDMax
	}

	batchSize := CalculateBatchSize(float64(cfg.EngineDataSize), meta.IsRowOrdered, float64(meta.TotalSize))

	log.FromContext(ctx).Info("makeTableRegions", zap.Int("filesCount", len(meta.DataFiles)),
		zap.Int64("MaxChunkSize", cfg.MaxChunkSize),
		zap.Int("RegionsCount", len(filesRegions)),
		zap.Float64("BatchSize", batchSize),
		zap.Duration("cost", time.Since(start)))
	AllocateEngineIDs(filesRegions, dataFileSizes, batchSize, cfg.BatchImportRatio, float64(cfg.EngineConcurrency))
	return filesRegions, nil
}

// CalculateBatchSize calculates batch size according to row order and file size.
func CalculateBatchSize(mydumperBatchSize float64, isRowOrdered bool, totalSize float64) float64 {
	batchSize := mydumperBatchSize
	if batchSize <= 0 {
		if isRowOrdered {
			batchSize = float64(config.DefaultBatchSize)
		} else {
			batchSize = math.Max(float64(config.DefaultBatchSize), totalSize)
		}
	}
	return batchSize
}

// MakeSourceFileRegion create a new source file region.
func MakeSourceFileRegion(
	ctx context.Context,
	cfg *DataDivideConfig,
	fi FileInfo,
) ([]*TableRegion, []float64, error) {
	divisor := int64(cfg.ColumnCnt)
	isCsvFile := fi.FileMeta.Type == SourceTypeCSV
	if !isCsvFile {
		divisor += 2
	}

	fileSize := fi.FileMeta.FileSize
	rowIDMax := fileSize / divisor
	// for compressed files, suggest the compress ratio is 1% to calculate the rowIDMax.
	// set fileSize to INF to make sure compressed files can be read until EOF. Because we can't get the exact size of the compressed files.
	if fi.FileMeta.Compression != CompressionNone {
		// RealSize the estimated file size. There are some cases that the first few bytes of this compressed file
		// has smaller compress ratio than the whole compressed file. So we still need to multiply this factor to
		// make sure the rowIDMax computation is correct.
		rowIDMax = fi.FileMeta.RealSize * CompressSizeFactor / divisor
		fileSize = TableFileSizeINF
	}
	tableRegion := &TableRegion{
		DB:       cfg.TableMeta.DB,
		Table:    cfg.TableMeta.Name,
		FileMeta: fi.FileMeta,
		Chunk: Chunk{
			Offset:       0,
			EndOffset:    fileSize,
			RealOffset:   0,
			PrevRowIDMax: 0,
			RowIDMax:     rowIDMax,
		},
	}

	regionSize := tableRegion.Size()
	if fi.FileMeta.Compression != CompressionNone {
		regionSize = fi.FileMeta.RealSize
	}
	if regionSize > tableRegionSizeWarningThreshold {
		log.FromContext(ctx).Warn(
			"file is too big to be processed efficiently; we suggest splitting it at 256 MB each",
			zap.String("file", fi.FileMeta.Path),
			zap.Int64("size", regionSize))
	}
	return []*TableRegion{tableRegion}, []float64{float64(fi.FileMeta.RealSize)}, nil
}

// because parquet files can't seek efficiently, there is no benefit in split.
// parquet file are column orient, so the offset is read line number
func makeParquetFileRegion(
	ctx context.Context,
	cfg *DataDivideConfig,
	dataFile FileInfo,
) ([]*TableRegion, []float64, error) {
	numberRows := dataFile.FileMeta.Rows
	var err error
	// for safety
	if numberRows <= 0 {
		numberRows, err = ReadParquetFileRowCountByFile(ctx, cfg.Store, dataFile.FileMeta)
		if err != nil {
			return nil, nil, err
		}
	}
	region := &TableRegion{
		DB:       cfg.TableMeta.DB,
		Table:    cfg.TableMeta.Name,
		FileMeta: dataFile.FileMeta,
		Chunk: Chunk{
			Offset:       0,
			EndOffset:    numberRows,
			RealOffset:   0,
			PrevRowIDMax: 0,
			RowIDMax:     numberRows,
		},
	}
	return []*TableRegion{region}, []float64{float64(dataFile.FileMeta.FileSize)}, nil
}

// SplitLargeCSV splits a large csv file into multiple regions, the size of
// each regions is specified by `config.MaxRegionSize`.
// Note: We split the file coarsely, thus the format of csv file is needed to be
// strict.
// e.g.
// - CSV file with header is invalid
// - a complete tuple split into multiple lines is invalid
func SplitLargeCSV(
	ctx context.Context,
	cfg *DataDivideConfig,
	dataFile FileInfo,
) (regions []*TableRegion, dataFileSizes []float64, err error) {
	maxRegionSize := cfg.MaxChunkSize
	dataFileSizes = make([]float64, 0, dataFile.FileMeta.FileSize/maxRegionSize+1)
	startOffset, endOffset := int64(0), maxRegionSize
	var columns []string
	var prevRowIdxMax int64
	if cfg.CSV.Header {
		r, err := cfg.Store.Open(ctx, dataFile.FileMeta.Path, nil)
		if err != nil {
			return nil, nil, err
		}
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
		if err != nil {
			_ = r.Close()
			return nil, nil, err
		}
		parser, err := NewCSVParser(ctx, &cfg.CSV, r, cfg.ReadBlockSize, cfg.IOWorkers, true, charsetConvertor)
		if err != nil {
			return nil, nil, err
		}
		if err = parser.ReadColumns(); err != nil {
			_ = parser.Close()
			return nil, nil, err
		}
		if cfg.CSV.HeaderSchemaMatch {
			columns = parser.Columns()
		}
		startOffset, _ = parser.Pos()
		endOffset = startOffset + maxRegionSize
		if endOffset > dataFile.FileMeta.FileSize {
			endOffset = dataFile.FileMeta.FileSize
		}
		_ = parser.Close()
	}
	divisor := int64(cfg.ColumnCnt)
	for {
		curRowsCnt := (endOffset - startOffset) / divisor
		rowIDMax := prevRowIdxMax + curRowsCnt
		if endOffset != dataFile.FileMeta.FileSize {
			r, err := cfg.Store.Open(ctx, dataFile.FileMeta.Path, nil)
			if err != nil {
				return nil, nil, err
			}
			// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
			charsetConvertor, err := NewCharsetConvertor(cfg.DataCharacterSet, cfg.DataInvalidCharReplace)
			if err != nil {
				_ = r.Close()
				return nil, nil, err
			}
			parser, err := NewCSVParser(ctx, &cfg.CSV, r, cfg.ReadBlockSize, cfg.IOWorkers, false, charsetConvertor)
			if err != nil {
				return nil, nil, err
			}
			if err = parser.SetPos(endOffset, 0); err != nil {
				_ = parser.Close()
				return nil, nil, err
			}
			_, pos, err := parser.ReadUntilTerminator()
			if err != nil {
				if !errors.ErrorEqual(err, io.EOF) {
					_ = parser.Close()
					return nil, nil, err
				}
				log.FromContext(ctx).Warn("file contains no terminator at end",
					zap.String("path", dataFile.FileMeta.Path),
					zap.String("terminator", cfg.CSV.Terminator))
				pos = dataFile.FileMeta.FileSize
			}
			endOffset = pos
			_ = parser.Close()
		}
		regions = append(regions,
			&TableRegion{
				DB:       cfg.TableMeta.DB,
				Table:    cfg.TableMeta.Name,
				FileMeta: dataFile.FileMeta,
				Chunk: Chunk{
					Offset:       startOffset,
					EndOffset:    endOffset,
					PrevRowIDMax: prevRowIdxMax,
					RowIDMax:     rowIDMax,
					Columns:      columns,
				},
			})
		dataFileSizes = append(dataFileSizes, float64(endOffset-startOffset))
		prevRowIdxMax = rowIDMax
		if endOffset == dataFile.FileMeta.FileSize {
			break
		}
		startOffset = endOffset
		if endOffset += maxRegionSize; endOffset > dataFile.FileMeta.FileSize {
			endOffset = dataFile.FileMeta.FileSize
		}
	}
	return regions, dataFileSizes, nil
}
