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
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
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
	tableConcurrency float64,
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
	// We use a simple brute force search since the search space is extremely small.
	ratio := totalDataFileSize * (1 - batchImportRatio) / batchSize
	n := math.Ceil(ratio)
	logGammaNPlusR, _ := math.Lgamma(n + batchImportRatio)
	logGammaN, _ := math.Lgamma(n)
	logGammaR, _ := math.Lgamma(batchImportRatio)
	invBetaNR := math.Exp(logGammaNPlusR - logGammaN - logGammaR) // 1/B(N, R) = Γ(N+R)/Γ(N)Γ(R)
	for {
		if n <= 0 || n > tableConcurrency {
			n = tableConcurrency
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

// MakeTableRegions create a new table region.
func MakeTableRegions(
	ctx context.Context,
	meta *MDTableMeta,
	columns int,
	cfg *config.Config,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
) ([]*TableRegion, error) {
	// Split files into regions
	type fileRegionRes struct {
		info    FileInfo
		regions []*TableRegion
		sizes   []float64
		err     error
	}

	start := time.Now()

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	concurrency := mathutil.Max(cfg.App.RegionConcurrency, 2)
	fileChan := make(chan FileInfo, concurrency)
	resultChan := make(chan fileRegionRes, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for info := range fileChan {
				regions, sizes, err := MakeSourceFileRegion(execCtx, meta, info, columns, cfg, ioWorkers, store)
				select {
				case resultChan <- fileRegionRes{info: info, regions: regions, sizes: sizes, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					log.FromContext(ctx).Error("make source file region error", zap.Error(err), zap.String("file_path", info.FileMeta.Path))
					break
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	errChan := make(chan error, 1)
	fileRegionsMap := make(map[string]fileRegionRes, len(meta.DataFiles))
	go func() {
		for res := range resultChan {
			if res.err != nil {
				errChan <- res.err
				return
			}
			fileRegionsMap[res.info.FileMeta.Path] = res
		}
		errChan <- nil
	}()

	for _, dataFile := range meta.DataFiles {
		select {
		case fileChan <- dataFile:
		case <-ctx.Done():
			close(fileChan)
			return nil, ctx.Err()
		case err := <-errChan:
			return nil, err
		}
	}
	close(fileChan)
	err := <-errChan
	if err != nil {
		return nil, err
	}

	filesRegions := make([]*TableRegion, 0, len(meta.DataFiles))
	dataFileSizes := make([]float64, 0, len(meta.DataFiles))
	prevRowIDMax := int64(0)
	for _, dataFile := range meta.DataFiles {
		fileRegionsRes := fileRegionsMap[dataFile.FileMeta.Path]
		var delta int64
		if len(fileRegionsRes.regions) > 0 {
			delta = prevRowIDMax - fileRegionsRes.regions[0].Chunk.PrevRowIDMax
		}

		for _, region := range fileRegionsRes.regions {
			region.Chunk.PrevRowIDMax += delta
			region.Chunk.RowIDMax += delta
		}
		filesRegions = append(filesRegions, fileRegionsRes.regions...)
		dataFileSizes = append(dataFileSizes, fileRegionsRes.sizes...)
		prevRowIDMax = fileRegionsRes.regions[len(fileRegionsRes.regions)-1].Chunk.RowIDMax
	}

	batchSize := float64(cfg.Mydumper.BatchSize)
	if cfg.Mydumper.BatchSize <= 0 {
		if meta.IsRowOrdered {
			batchSize = float64(config.DefaultBatchSize)
		} else {
			batchSize = math.Max(float64(config.DefaultBatchSize), float64(meta.TotalSize))
		}
	}

	log.FromContext(ctx).Info("makeTableRegions", zap.Int("filesCount", len(meta.DataFiles)),
		zap.Int64("MaxRegionSize", int64(cfg.Mydumper.MaxRegionSize)),
		zap.Int("RegionsCount", len(filesRegions)),
		zap.Float64("BatchSize", batchSize),
		zap.Duration("cost", time.Since(start)))
	AllocateEngineIDs(filesRegions, dataFileSizes, batchSize, cfg.Mydumper.BatchImportRatio, float64(cfg.App.TableConcurrency))
	return filesRegions, nil
}

// MakeSourceFileRegion create a new source file region.
func MakeSourceFileRegion(
	ctx context.Context,
	meta *MDTableMeta,
	fi FileInfo,
	columns int,
	cfg *config.Config,
	ioWorkers *worker.Pool,
	store storage.ExternalStorage,
) ([]*TableRegion, []float64, error) {
	if fi.FileMeta.Type == SourceTypeParquet {
		_, region, err := makeParquetFileRegion(ctx, store, meta, fi, 0)
		if err != nil {
			return nil, nil, err
		}
		return []*TableRegion{region}, []float64{float64(fi.FileMeta.FileSize)}, nil
	}

	dataFileSize := fi.FileMeta.FileSize
	divisor := int64(columns)
	isCsvFile := fi.FileMeta.Type == SourceTypeCSV
	if !isCsvFile {
		divisor += 2
	}
	// If a csv file is overlarge, we need to split it into multiple regions.
	// Note: We can only split a csv file whose format is strict.
	// We increase the check threshold by 1/10 of the `max-region-size` because the source file size dumped by tools
	// like dumpling might be slight exceed the threshold when it is equal `max-region-size`, so we can
	// avoid split a lot of small chunks.
	// If a csv file is compressed, we can't split it now because we can't get the exact size of a row.
	if isCsvFile && cfg.Mydumper.StrictFormat && fi.FileMeta.Compression == CompressionNone &&
		dataFileSize > int64(cfg.Mydumper.MaxRegionSize+cfg.Mydumper.MaxRegionSize/largeCSVLowerThresholdRation) {
		_, regions, subFileSizes, err := SplitLargeFile(ctx, meta, cfg, fi, divisor, 0, ioWorkers, store)
		return regions, subFileSizes, err
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
		DB:       meta.DB,
		Table:    meta.Name,
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
	store storage.ExternalStorage,
	meta *MDTableMeta,
	dataFile FileInfo,
	prevRowIdxMax int64,
) (int64, *TableRegion, error) {
	r, err := store.Open(ctx, dataFile.FileMeta.Path)
	if err != nil {
		return prevRowIdxMax, nil, errors.Trace(err)
	}
	numberRows, err := ReadParquetFileRowCount(ctx, store, r, dataFile.FileMeta.Path)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	rowIDMax := prevRowIdxMax + numberRows
	region := &TableRegion{
		DB:       meta.DB,
		Table:    meta.Name,
		FileMeta: dataFile.FileMeta,
		Chunk: Chunk{
			Offset:       0,
			EndOffset:    numberRows,
			PrevRowIDMax: prevRowIdxMax,
			RowIDMax:     rowIDMax,
		},
	}
	return rowIDMax, region, nil
}

// SplitLargeFile splits a large csv file into multiple regions, the size of
// each regions is specified by `config.MaxRegionSize`.
// Note: We split the file coarsely, thus the format of csv file is needed to be
// strict.
// e.g.
// - CSV file with header is invalid
// - a complete tuple split into multiple lines is invalid
func SplitLargeFile(
	ctx context.Context,
	meta *MDTableMeta,
	cfg *config.Config,
	dataFile FileInfo,
	divisor int64,
	prevRowIdxMax int64,
	ioWorker *worker.Pool,
	store storage.ExternalStorage,
) (prevRowIDMax int64, regions []*TableRegion, dataFileSizes []float64, err error) {
	maxRegionSize := int64(cfg.Mydumper.MaxRegionSize)
	dataFileSizes = make([]float64, 0, dataFile.FileMeta.FileSize/maxRegionSize+1)
	startOffset, endOffset := int64(0), maxRegionSize
	var columns []string
	if cfg.Mydumper.CSV.Header {
		r, err := store.Open(ctx, dataFile.FileMeta.Path)
		if err != nil {
			return 0, nil, nil, err
		}
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return 0, nil, nil, err
		}
		parser, err := NewCSVParser(ctx, &cfg.Mydumper.CSV, r, int64(cfg.Mydumper.ReadBlockSize), ioWorker, true, charsetConvertor)
		if err != nil {
			return 0, nil, nil, err
		}
		if err = parser.ReadColumns(); err != nil {
			return 0, nil, nil, err
		}
		if cfg.Mydumper.CSV.HeaderSchemaMatch {
			columns = parser.Columns()
		}
		startOffset, _ = parser.Pos()
		endOffset = startOffset + maxRegionSize
		if endOffset > dataFile.FileMeta.FileSize {
			endOffset = dataFile.FileMeta.FileSize
		}
	}
	for {
		curRowsCnt := (endOffset - startOffset) / divisor
		rowIDMax := prevRowIdxMax + curRowsCnt
		if endOffset != dataFile.FileMeta.FileSize {
			r, err := store.Open(ctx, dataFile.FileMeta.Path)
			if err != nil {
				return 0, nil, nil, err
			}
			// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
			charsetConvertor, err := NewCharsetConvertor(cfg.Mydumper.DataCharacterSet, cfg.Mydumper.DataInvalidCharReplace)
			if err != nil {
				return 0, nil, nil, err
			}
			parser, err := NewCSVParser(ctx, &cfg.Mydumper.CSV, r, int64(cfg.Mydumper.ReadBlockSize), ioWorker, false, charsetConvertor)
			if err != nil {
				return 0, nil, nil, err
			}
			if err = parser.SetPos(endOffset, prevRowIDMax); err != nil {
				return 0, nil, nil, err
			}
			_, pos, err := parser.ReadUntilTerminator()
			if err != nil {
				if !errors.ErrorEqual(err, io.EOF) {
					return 0, nil, nil, err
				}
				log.FromContext(ctx).Warn("file contains no terminator at end",
					zap.String("path", dataFile.FileMeta.Path),
					zap.String("terminator", cfg.Mydumper.CSV.Terminator))
				pos = dataFile.FileMeta.FileSize
			}
			endOffset = pos
			parser.Close()
		}
		regions = append(regions,
			&TableRegion{
				DB:       meta.DB,
				Table:    meta.Name,
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
	return prevRowIdxMax, regions, dataFileSizes, nil
}
