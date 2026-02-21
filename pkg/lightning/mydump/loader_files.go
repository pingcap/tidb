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
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

// FileHandler is the interface to handle the file give the path and size.
// It is mainly used in the `FileIterator` as parameters.
type FileHandler func(ctx context.Context, path string, size int64) error

// FileIterator is the interface to iterate files in a data source.
// Use this interface to customize the file iteration policy.
type FileIterator interface {
	IterateFiles(ctx context.Context, hdl FileHandler) error
}

type allFileIterator struct {
	store        storeapi.Storage
	maxScanFiles int
}

func (iter *allFileIterator) IterateFiles(ctx context.Context, hdl FileHandler) error {
	// `filepath.Walk` yields the paths in a deterministic (lexicographical) order,
	// meaning the file and chunk orders will be the same everytime it is called
	// (as long as the source is immutable).
	totalScannedFileCount := 0
	err := iter.store.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		totalScannedFileCount++
		if iter.maxScanFiles > 0 && totalScannedFileCount > iter.maxScanFiles {
			return common.ErrTooManySourceFiles
		}
		return hdl(ctx, path, size)
	})

	return errors.Trace(err)
}

func (s *mdLoaderSetup) constructFileInfo(ctx context.Context, f RawFile) (*FileInfo, error) {
	path, size := f.Path, f.Size
	logger := log.Wrap(logutil.Logger(ctx)).With(zap.String("path", path))
	res, err := s.loader.fileRouter.Route(filepath.ToSlash(path))
	if err != nil {
		return nil, errors.Annotatef(err, "apply file routing on file '%s' failed", path)
	}
	if res == nil {
		logger.Info("file is filtered by file router", zap.String("category", "loader"))
		return nil, nil
	}

	info := &FileInfo{
		TableName: filter.Table{Schema: res.Schema, Name: res.Name},
		FileMeta:  SourceFileMeta{Path: path, Type: res.Type, Compression: res.Compression, SortKey: res.Key, FileSize: size, RealSize: size},
	}

	if s.loader.shouldSkip(&info.TableName) {
		logger.Debug("ignoring table file", zap.String("category", "filter"))
		return nil, nil
	}

	switch res.Type {
	case SourceTypeSQL, SourceTypeCSV:
		if !s.setupCfg.SkipRealSizeEstimation {
			info.FileMeta.RealSize = EstimateRealSizeForFile(ctx, info.FileMeta, s.loader.GetStore())
		}
	case SourceTypeParquet:
		if s.setupCfg.SkipRealSizeEstimation {
			break
		}
		tableName := info.TableName.String()

		// Only sample once for each table
		_, loaded := s.sampledParquetInfos.LoadOrStore(tableName, parquetInfo{})
		if !loaded {
			rows, rowSize, err := SampleStatisticsFromParquet(ctx, info.FileMeta.Path, s.loader.GetStore())
			if err != nil {
				logger.Error("fail to sample parquet row size", zap.String("category", "loader"),
					zap.String("schema", res.Schema), zap.String("table", res.Name),
					zap.Stringer("type", res.Type), zap.Error(err))
				return nil, errors.Trace(err)
			}
			compressionRatio := float64(info.FileMeta.FileSize) / (rowSize * float64(rows))
			s.sampledParquetInfos.Store(tableName, parquetInfo{rowSize, compressionRatio})
		}
	}

	logger.Debug("file route result", zap.String("schema", res.Schema),
		zap.String("table", res.Name), zap.Stringer("type", res.Type))

	return info, nil
}

func (l *MDLoader) shouldSkip(table *filter.Table) bool {
	if len(table.Name) == 0 {
		return !l.filter.MatchSchema(table.Schema)
	}
	return !l.filter.MatchTable(table.Schema, table.Name)
}

func (s *mdLoaderSetup) route() error {
	r := s.loader.router
	if r == nil {
		return nil
	}

	type dbInfo struct {
		fileMeta SourceFileMeta
		count    int // means file count(db/table/view schema and table data)
	}

	knownDBNames := make(map[string]*dbInfo)
	for _, info := range s.dbSchemas {
		knownDBNames[info.TableName.Schema] = &dbInfo{
			fileMeta: info.FileMeta,
			count:    1,
		}
	}
	for _, info := range s.tableSchemas {
		if _, ok := knownDBNames[info.TableName.Schema]; !ok {
			knownDBNames[info.TableName.Schema] = &dbInfo{
				fileMeta: info.FileMeta,
				count:    1,
			}
		}
		knownDBNames[info.TableName.Schema].count++
	}
	for _, info := range s.viewSchemas {
		if _, ok := knownDBNames[info.TableName.Schema]; !ok {
			knownDBNames[info.TableName.Schema] = &dbInfo{
				fileMeta: info.FileMeta,
				count:    1,
			}
		}
		knownDBNames[info.TableName.Schema].count++
	}
	for _, info := range s.tableDatas {
		if _, ok := knownDBNames[info.TableName.Schema]; !ok {
			knownDBNames[info.TableName.Schema] = &dbInfo{
				fileMeta: info.FileMeta,
				count:    1,
			}
		}
		knownDBNames[info.TableName.Schema].count++
	}

	runRoute := func(arr []FileInfo) error {
		for i, info := range arr {
			rawDB, rawTable := info.TableName.Schema, info.TableName.Name
			targetDB, targetTable, err := r.Route(rawDB, rawTable)
			if err != nil {
				return errors.Trace(err)
			}
			if targetDB != rawDB {
				oldInfo := knownDBNames[rawDB]
				oldInfo.count--
				newInfo, ok := knownDBNames[targetDB]
				if !ok {
					newInfo = &dbInfo{fileMeta: oldInfo.fileMeta, count: 1}
					s.dbSchemas = append(s.dbSchemas, FileInfo{
						TableName: filter.Table{Schema: targetDB},
						FileMeta:  oldInfo.fileMeta,
					})
				}
				newInfo.count++
				knownDBNames[targetDB] = newInfo
			}
			arr[i].TableName = filter.Table{Schema: targetDB, Name: targetTable}
			extendCols, extendVals := r.FetchExtendColumn(rawDB, rawTable, s.sourceID)
			if len(extendCols) > 0 {
				arr[i].FileMeta.ExtendData = ExtendColumnData{
					Columns: extendCols,
					Values:  extendVals,
				}
			}
		}
		return nil
	}

	// route for schema table and view
	if err := runRoute(s.dbSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := runRoute(s.tableSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := runRoute(s.viewSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := runRoute(s.tableDatas); err != nil {
		return errors.Trace(err)
	}
	// remove all schemas which has been entirely routed away(file count > 0)
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	remainingSchemas := s.dbSchemas[:0]
	for _, info := range s.dbSchemas {
		if dbInfo := knownDBNames[info.TableName.Schema]; dbInfo.count > 0 {
			remainingSchemas = append(remainingSchemas, info)
		} else if dbInfo.count < 0 {
			// this should not happen if there are no bugs in the code
			return common.ErrTableRoute.GenWithStack("something wrong happened when route %s", info.TableName.String())
		}
	}
	s.dbSchemas = remainingSchemas
	return nil
}

func (s *mdLoaderSetup) insertDB(f FileInfo) (*MDDatabaseMeta, bool) {
	dbIndex, ok := s.dbIndexMap[f.TableName.Schema]
	if ok {
		return s.loader.dbs[dbIndex], true
	}
	s.dbIndexMap[f.TableName.Schema] = len(s.loader.dbs)
	ptr := &MDDatabaseMeta{
		Name:       f.TableName.Schema,
		SchemaFile: f,
		charSet:    s.loader.charSet,
	}
	s.loader.dbs = append(s.loader.dbs, ptr)
	return ptr, false
}

func (s *mdLoaderSetup) insertTable(fileInfo FileInfo) (tblMeta *MDTableMeta, dbExists bool, tableExists bool) {
	dbFileInfo := FileInfo{
		TableName: filter.Table{
			Schema: fileInfo.TableName.Schema,
		},
		FileMeta: SourceFileMeta{Type: SourceTypeSchemaSchema},
	}
	dbMeta, dbExists := s.insertDB(dbFileInfo)
	tableIndex, ok := s.tableIndexMap[fileInfo.TableName]
	if ok {
		return dbMeta.Tables[tableIndex], dbExists, true
	}
	s.tableIndexMap[fileInfo.TableName] = len(dbMeta.Tables)
	ptr := &MDTableMeta{
		DB:           fileInfo.TableName.Schema,
		Name:         fileInfo.TableName.Name,
		SchemaFile:   fileInfo,
		DataFiles:    make([]FileInfo, 0, 16),
		charSet:      s.loader.charSet,
		IndexRatio:   0.0,
		IsRowOrdered: true,
	}
	dbMeta.Tables = append(dbMeta.Tables, ptr)
	return ptr, dbExists, false
}

func (s *mdLoaderSetup) insertView(fileInfo FileInfo) (dbExists bool, tableExists bool) {
	dbFileInfo := FileInfo{
		TableName: filter.Table{
			Schema: fileInfo.TableName.Schema,
		},
		FileMeta: SourceFileMeta{Type: SourceTypeSchemaSchema},
	}
	dbMeta, dbExists := s.insertDB(dbFileInfo)
	_, ok := s.tableIndexMap[fileInfo.TableName]
	if ok {
		meta := &MDTableMeta{
			DB:           fileInfo.TableName.Schema,
			Name:         fileInfo.TableName.Name,
			SchemaFile:   fileInfo,
			charSet:      s.loader.charSet,
			IndexRatio:   0.0,
			IsRowOrdered: true,
		}
		dbMeta.Views = append(dbMeta.Views, meta)
	}
	return dbExists, ok
}

// GetDatabases gets the list of scanned MDDatabaseMeta for the loader.
func (l *MDLoader) GetDatabases() []*MDDatabaseMeta {
	return l.dbs
}

// GetStore gets the external storage used by the loader.
func (l *MDLoader) GetStore() storeapi.Storage {
	return l.store
}

// GetAllFiles gets all the files for the loader.
func (l *MDLoader) GetAllFiles() map[string]FileInfo {
	allFiles := make(map[string]FileInfo)
	for _, dbMeta := range l.dbs {
		for _, tblMeta := range dbMeta.Tables {
			for _, file := range tblMeta.DataFiles {
				allFiles[file.FileMeta.Path] = file
			}
			if tblMeta.SchemaFile.FileMeta.Path != "" {
				allFiles[tblMeta.SchemaFile.FileMeta.Path] = tblMeta.SchemaFile
			}
		}
	}
	return allFiles
}

func calculateFileBytes(ctx context.Context,
	dataFile string,
	compressType compressedio.CompressType,
	store storeapi.Storage,
	offset int64) (tot int, pos int64, err error) {
	bytes := make([]byte, sampleCompressedFileSize)
	reader, err := store.Open(ctx, dataFile, nil)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	defer reader.Close()

	decompressConfig := compressedio.DecompressConfig{ZStdDecodeConcurrency: 1}
	compressReader, err := objstore.NewLimitedInterceptReader(reader, compressType, decompressConfig, offset)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	readBytes := func() error {
		n, err2 := compressReader.Read(bytes)
		if err2 != nil && errors.Cause(err2) != io.EOF && errors.Cause(err) != io.ErrUnexpectedEOF {
			return err2
		}
		tot += n
		return err2
	}

	if offset == 0 {
		err = readBytes()
		if err != nil && errors.Cause(err) != io.EOF && errors.Cause(err) != io.ErrUnexpectedEOF {
			return 0, 0, err
		}
		pos, err = compressReader.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
		return tot, pos, nil
	}

	for {
		err = readBytes()
		if err != nil {
			break
		}
	}
	if err != nil && errors.Cause(err) != io.EOF && errors.Cause(err) != io.ErrUnexpectedEOF {
		return 0, 0, errors.Trace(err)
	}
	return tot, offset, nil
}

// EstimateRealSizeForFile estimate the real size for the file.
// If the file is not compressed, the real size is the same as the file size.
// If the file is compressed, the real size is the estimated uncompressed size.
func EstimateRealSizeForFile(ctx context.Context, fileMeta SourceFileMeta, store storeapi.Storage) int64 {
	if fileMeta.Compression == CompressionNone {
		return fileMeta.FileSize
	}
	compressRatio, err := SampleFileCompressRatio(ctx, fileMeta, store)
	if err != nil {
		logutil.Logger(ctx).Error("fail to calculate data file compress ratio",
			zap.String("category", "loader"),
			zap.String("path", fileMeta.Path),
			zap.Stringer("type", fileMeta.Type), zap.Error(err),
		)
		return fileMeta.FileSize
	}
	return int64(compressRatio * float64(fileMeta.FileSize))
}

// SampleFileCompressRatio samples the compress ratio of the compressed file. Exported for test.
func SampleFileCompressRatio(ctx context.Context, fileMeta SourceFileMeta, store storeapi.Storage) (float64, error) {
	failpoint.Inject("SampleFileCompressPercentage", func(val failpoint.Value) {
		switch v := val.(type) {
		case string:
			failpoint.Return(1.0, errors.New(v))
		case int:
			failpoint.Return(float64(v)/100, nil)
		}
	})
	if fileMeta.Compression == CompressionNone {
		return 1, nil
	}
	compressType, err := ToStorageCompressType(fileMeta.Compression)
	if err != nil {
		return 0, err
	}
	// We use the following method to sample the compress ratio of the first few bytes of the file.
	// 1. read first time aiming to find a valid compressed file offset. If we continue read now, the compress reader will
	// request more data from file reader buffer them in its memory. We can't compute an accurate compress ratio.
	// 2. we use a second reading and limit the file reader only read n bytes(n is the valid position we find in the first reading).
	// Then we read all the data out from the compress reader. The data length m we read out is the uncompressed data length.
	// Use m/n to compute the compress ratio.
	// read first time, aims to find a valid end pos in compressed file
	_, pos, err := calculateFileBytes(ctx, fileMeta.Path, compressType, store, 0)
	if err != nil {
		return 0, err
	}
	// read second time, original reader ends at first time's valid pos, compute sample data compress ratio
	tot, pos, err := calculateFileBytes(ctx, fileMeta.Path, compressType, store, pos)
	if err != nil {
		return 0, err
	}
	return float64(tot) / float64(pos), nil
}
