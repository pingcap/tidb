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
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

// sampleCompressedFileSize represents how many bytes need to be sampled for compressed files
const (
	sampleCompressedFileSize = 4 * 1024
	maxSampleParquetDataSize = 8 * 1024
	maxSampleParquetRowCount = 500
)

// MDDatabaseMeta contains some parsed metadata for a database in the source by MyDumper Loader.
type MDDatabaseMeta struct {
	Name       string
	SchemaFile FileInfo
	Tables     []*MDTableMeta
	Views      []*MDTableMeta
	charSet    string
}

// NewMDDatabaseMeta creates an Mydumper database meta with specified character set.
func NewMDDatabaseMeta(charSet string) *MDDatabaseMeta {
	return &MDDatabaseMeta{
		charSet: charSet,
	}
}

// GetSchema gets the schema SQL for a source database.
func (m *MDDatabaseMeta) GetSchema(ctx context.Context, store storage.ExternalStorage) string {
	if m.SchemaFile.FileMeta.Path != "" {
		schema, err := ExportStatement(ctx, store, m.SchemaFile, m.charSet)
		if err != nil {
			log.FromContext(ctx).Warn("failed to extract table schema",
				zap.String("Path", m.SchemaFile.FileMeta.Path),
				log.ShortError(err),
			)
		} else if schemaStr := strings.TrimSpace(string(schema)); schemaStr != "" {
			return schemaStr
		}
	}
	// set default if schema sql is empty or failed to extract.
	return common.SprintfWithIdentifiers("CREATE DATABASE IF NOT EXISTS %s", m.Name)
}

// MDTableMeta contains some parsed metadata for a table in the source by MyDumper Loader.
type MDTableMeta struct {
	DB         string
	Name       string
	SchemaFile FileInfo
	DataFiles  []FileInfo
	charSet    string
	TotalSize  int64
	IndexRatio float64
	// default to true, and if we do precheck, this var is updated using data sampling result, so it's not accurate.
	IsRowOrdered bool
}

// SourceFileMeta contains some analyzed metadata for a source file by MyDumper Loader.
type SourceFileMeta struct {
	Path        string
	Type        SourceType
	Compression Compression
	SortKey     string
	// FileSize is the size of the file in the storage.
	FileSize int64
	// WARNING: variables below are not persistent
	ExtendData ExtendColumnData
	// RealSize is same as FileSize if the file is not compressed and not parquet.
	// If the file is compressed, RealSize is the estimated uncompressed size.
	// If the file is parquet, RealSize is the estimated data size after convert
	// to row oriented storage.
	RealSize int64
	Rows     int64 // only for parquet
}

// NewMDTableMeta creates an Mydumper table meta with specified character set.
func NewMDTableMeta(charSet string) *MDTableMeta {
	return &MDTableMeta{
		charSet: charSet,
	}
}

// GetSchema gets the table-creating SQL for a source table.
func (m *MDTableMeta) GetSchema(ctx context.Context, store storage.ExternalStorage) (string, error) {
	schemaFilePath := m.SchemaFile.FileMeta.Path
	if len(schemaFilePath) <= 0 {
		return "", errors.Errorf("schema file is missing for the table '%s.%s'", m.DB, m.Name)
	}
	fileExists, err := store.FileExists(ctx, schemaFilePath)
	if err != nil {
		return "", errors.Annotate(err, "check table schema file exists error")
	}
	if !fileExists {
		return "", errors.Errorf("the provided schema file (%s) for the table '%s.%s' doesn't exist",
			schemaFilePath, m.DB, m.Name)
	}
	schema, err := ExportStatement(ctx, store, m.SchemaFile, m.charSet)
	if err != nil {
		log.FromContext(ctx).Error("failed to extract table schema",
			zap.String("Path", m.SchemaFile.FileMeta.Path),
			log.ShortError(err),
		)
		return "", errors.Trace(err)
	}
	return string(schema), nil
}

// MDLoaderSetupConfig stores the configs when setting up a MDLoader.
// This can control the behavior when constructing an MDLoader.
type MDLoaderSetupConfig struct {
	// MaxScanFiles specifies the maximum number of files to scan.
	// If the value is <= 0, it means the number of data source files will be scanned as many as possible.
	MaxScanFiles int
	// ReturnPartialResultOnError specifies whether the currently scanned files are analyzed,
	// and return the partial result.
	ReturnPartialResultOnError bool
	// FileIter controls the file iteration policy when constructing a MDLoader.
	FileIter FileIterator
}

// DefaultMDLoaderSetupConfig generates a default MDLoaderSetupConfig.
func DefaultMDLoaderSetupConfig() *MDLoaderSetupConfig {
	return &MDLoaderSetupConfig{
		MaxScanFiles:               0, // By default, the loader will scan all the files.
		ReturnPartialResultOnError: false,
		FileIter:                   nil,
	}
}

// MDLoaderSetupOption is the option type for setting up a MDLoaderSetupConfig.
type MDLoaderSetupOption func(cfg *MDLoaderSetupConfig)

// WithMaxScanFiles generates an option that limits the max scan files when setting up a MDLoader.
func WithMaxScanFiles(maxScanFiles int) MDLoaderSetupOption {
	return func(cfg *MDLoaderSetupConfig) {
		if maxScanFiles > 0 {
			cfg.MaxScanFiles = maxScanFiles
			cfg.ReturnPartialResultOnError = true
		}
	}
}

// ReturnPartialResultOnError generates an option that controls
// whether return the partial scanned result on error when setting up a MDLoader.
func ReturnPartialResultOnError(supportPartialResult bool) MDLoaderSetupOption {
	return func(cfg *MDLoaderSetupConfig) {
		cfg.ReturnPartialResultOnError = supportPartialResult
	}
}

// WithFileIterator generates an option that specifies the file iteration policy.
func WithFileIterator(fileIter FileIterator) MDLoaderSetupOption {
	return func(cfg *MDLoaderSetupConfig) {
		cfg.FileIter = fileIter
	}
}

// LoaderConfig is the configuration for constructing a MDLoader.
type LoaderConfig struct {
	// SourceID is the unique identifier for the data source, it's used in DM only.
	// must be used together with Routes.
	SourceID string
	// SourceURL is the URL of the data source.
	SourceURL string
	// Routes is the routing rules for the tables, exclusive with FileRouters.
	// it's deprecated in lightning, but still used in DM.
	// when used this, DefaultFileRules must be true.
	Routes config.Routes
	// CharacterSet is the character set of the schema sql files.
	CharacterSet string
	// Filter is the filter for the tables, files related to filtered-out tables are not loaded.
	// must be specified, else all tables are filtered out, see config.GetDefaultFilter.
	Filter      []string
	FileRouters []*config.FileRouteRule
	// CaseSensitive indicates whether Routes and Filter are case-sensitive.
	CaseSensitive bool
	// DefaultFileRules indicates whether to use the default file routing rules.
	// If it's true, the default file routing rules will be appended to the FileRouters.
	// a little confusing, but it's true only when FileRouters is empty.
	DefaultFileRules bool
}

// NewLoaderCfg creates loader config from lightning config.
func NewLoaderCfg(cfg *config.Config) LoaderConfig {
	return LoaderConfig{
		SourceID:         cfg.Mydumper.SourceID,
		SourceURL:        cfg.Mydumper.SourceDir,
		Routes:           cfg.Routes,
		CharacterSet:     cfg.Mydumper.CharacterSet,
		Filter:           cfg.Mydumper.Filter,
		FileRouters:      cfg.Mydumper.FileRouters,
		CaseSensitive:    cfg.Mydumper.CaseSensitive,
		DefaultFileRules: cfg.Mydumper.DefaultFileRules,
	}
}

// MDLoader is for 'Mydumper File Loader', which loads the files in the data source and generates a set of metadata.
type MDLoader struct {
	store      storage.ExternalStorage
	dbs        []*MDDatabaseMeta
	filter     filter.Filter
	router     *regexprrouter.RouteTable
	fileRouter FileRouter
	charSet    string
}

type mdLoaderSetup struct {
	sourceID      string
	loader        *MDLoader
	dbSchemas     []FileInfo
	tableSchemas  []FileInfo
	viewSchemas   []FileInfo
	tableDatas    []FileInfo
	dbIndexMap    map[string]int
	tableIndexMap map[filter.Table]int
	setupCfg      *MDLoaderSetupConfig
}

// NewLoader constructs a MyDumper loader that scanns the data source and constructs a set of metadatas.
func NewLoader(ctx context.Context, cfg LoaderConfig, opts ...MDLoaderSetupOption) (*MDLoader, error) {
	u, err := storage.ParseBackend(cfg.SourceURL, nil)
	if err != nil {
		return nil, common.NormalizeError(err)
	}
	s, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, common.NormalizeError(err)
	}

	return NewLoaderWithStore(ctx, cfg, s, opts...)
}

// NewLoaderWithStore constructs a MyDumper loader with the provided external storage that scanns the data source and constructs a set of metadatas.
func NewLoaderWithStore(ctx context.Context, cfg LoaderConfig,
	store storage.ExternalStorage, opts ...MDLoaderSetupOption) (*MDLoader, error) {
	var r *regexprrouter.RouteTable
	var err error

	mdLoaderSetupCfg := DefaultMDLoaderSetupConfig()
	for _, o := range opts {
		o(mdLoaderSetupCfg)
	}
	if mdLoaderSetupCfg.FileIter == nil {
		mdLoaderSetupCfg.FileIter = &allFileIterator{
			store:        store,
			maxScanFiles: mdLoaderSetupCfg.MaxScanFiles,
		}
	}

	if len(cfg.Routes) > 0 && len(cfg.FileRouters) > 0 {
		return nil, common.ErrInvalidConfig.GenWithStack("table route is deprecated, can't config both [routes] and [mydumper.files]")
	}

	if len(cfg.Routes) > 0 {
		r, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, cfg.Routes)
		if err != nil {
			return nil, common.ErrInvalidConfig.Wrap(err).GenWithStack("invalid table route rule")
		}
	}

	f, err := filter.Parse(cfg.Filter)
	if err != nil {
		return nil, common.ErrInvalidConfig.Wrap(err).GenWithStack("parse filter failed")
	}
	if !cfg.CaseSensitive {
		f = filter.CaseInsensitive(f)
	}

	fileRouteRules := cfg.FileRouters
	if cfg.DefaultFileRules {
		fileRouteRules = append(fileRouteRules, defaultFileRouteRules...)
	}

	fileRouter, err := NewFileRouter(fileRouteRules, log.FromContext(ctx))
	if err != nil {
		return nil, common.ErrInvalidConfig.Wrap(err).GenWithStack("parse file routing rule failed")
	}

	mdl := &MDLoader{
		store:      store,
		filter:     f,
		router:     r,
		charSet:    cfg.CharacterSet,
		fileRouter: fileRouter,
	}

	setup := mdLoaderSetup{
		sourceID:      cfg.SourceID,
		loader:        mdl,
		dbIndexMap:    make(map[string]int),
		tableIndexMap: make(map[filter.Table]int),
		setupCfg:      mdLoaderSetupCfg,
	}

	if err := setup.setup(ctx); err != nil {
		if mdLoaderSetupCfg.ReturnPartialResultOnError {
			return mdl, errors.Trace(err)
		}
		return nil, errors.Trace(err)
	}

	return mdl, nil
}

type fileType int

const (
	fileTypeDatabaseSchema fileType = iota
	fileTypeTableSchema
	fileTypeTableData
)

// String implements the Stringer interface.
func (ftype fileType) String() string {
	switch ftype {
	case fileTypeDatabaseSchema:
		return "database schema"
	case fileTypeTableSchema:
		return "table schema"
	case fileTypeTableData:
		return "table data"
	default:
		return "(unknown)"
	}
}

// FileInfo contains the information for a data file in a table.
type FileInfo struct {
	TableName filter.Table
	FileMeta  SourceFileMeta
}

// ExtendColumnData contains the extended column names and values information for a table.
type ExtendColumnData struct {
	Columns []string
	Values  []string
}

// setup the `s.loader.dbs` slice by scanning all *.sql files inside `dir`.
//
// The database and tables are inserted in a consistent order, so creating an
// MDLoader twice with the same data source is going to produce the same array,
// even after killing Lightning.
//
// This is achieved by using `filepath.Walk` internally which guarantees the
// files are visited in lexicographical order (note that this does not mean the
// databases and tables in the end are ordered lexicographically since they may
// be stored in different subdirectories).
//
// Will sort tables by table size, this means that the big table is imported
// at the latest, which to avoid large table take a long time to import and block
// small table to release index worker.
func (s *mdLoaderSetup) setup(ctx context.Context) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/
	var gerr error
	fileIter := s.setupCfg.FileIter
	if fileIter == nil {
		return errors.New("file iterator is not defined")
	}
	if err := fileIter.IterateFiles(ctx, s.constructFileInfo); err != nil {
		if !s.setupCfg.ReturnPartialResultOnError {
			return common.ErrStorageUnknown.Wrap(err).GenWithStack("list file failed")
		}
		gerr = err
	}
	if err := s.route(); err != nil {
		return common.ErrTableRoute.Wrap(err).GenWithStackByArgs()
	}

	// setup database schema
	if len(s.dbSchemas) != 0 {
		for _, fileInfo := range s.dbSchemas {
			if _, dbExists := s.insertDB(fileInfo); dbExists && s.loader.router == nil {
				return common.ErrInvalidSchemaFile.GenWithStack("invalid database schema file, duplicated item - %s", fileInfo.FileMeta.Path)
			}
		}
	}

	if len(s.tableSchemas) != 0 {
		// setup table schema
		for _, fileInfo := range s.tableSchemas {
			if _, _, tableExists := s.insertTable(fileInfo); tableExists && s.loader.router == nil {
				return common.ErrInvalidSchemaFile.GenWithStack("invalid table schema file, duplicated item - %s", fileInfo.FileMeta.Path)
			}
		}
	}

	if len(s.viewSchemas) != 0 {
		// setup view schema
		for _, fileInfo := range s.viewSchemas {
			_, tableExists := s.insertView(fileInfo)
			if !tableExists {
				// we are not expect the user only has view schema without table schema when user use dumpling to get view.
				// remove the last `-view.sql` from path as the relate table schema file path
				return common.ErrInvalidSchemaFile.GenWithStack("invalid view schema file, miss host table schema for view '%s'", fileInfo.TableName.Name)
			}
		}
	}

	// Sql file for restore data
	for _, fileInfo := range s.tableDatas {
		// set a dummy `FileInfo` here without file meta because we needn't restore the table schema
		tableMeta, _, _ := s.insertTable(FileInfo{TableName: fileInfo.TableName})
		tableMeta.DataFiles = append(tableMeta.DataFiles, fileInfo)
		tableMeta.TotalSize += fileInfo.FileMeta.RealSize
	}

	for _, dbMeta := range s.loader.dbs {
		// Put the small table in the front of the slice which can avoid large table
		// take a long time to import and block small table to release index worker.
		meta := dbMeta
		sort.SliceStable(meta.Tables, func(i, j int) bool {
			return meta.Tables[i].TotalSize < meta.Tables[j].TotalSize
		})

		// sort each table source files by sort-key
		for _, tbMeta := range meta.Tables {
			dataFiles := tbMeta.DataFiles
			sort.SliceStable(dataFiles, func(i, j int) bool {
				return dataFiles[i].FileMeta.SortKey < dataFiles[j].FileMeta.SortKey
			})
		}
	}

	return gerr
}

// FileHandler is the interface to handle the file give the path and size.
// It is mainly used in the `FileIterator` as parameters.
type FileHandler func(ctx context.Context, path string, size int64) error

// FileIterator is the interface to iterate files in a data source.
// Use this interface to customize the file iteration policy.
type FileIterator interface {
	IterateFiles(ctx context.Context, hdl FileHandler) error
}

type allFileIterator struct {
	store        storage.ExternalStorage
	maxScanFiles int
}

func (iter *allFileIterator) IterateFiles(ctx context.Context, hdl FileHandler) error {
	// `filepath.Walk` yields the paths in a deterministic (lexicographical) order,
	// meaning the file and chunk orders will be the same everytime it is called
	// (as long as the source is immutable).
	totalScannedFileCount := 0
	err := iter.store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		totalScannedFileCount++
		if iter.maxScanFiles > 0 && totalScannedFileCount > iter.maxScanFiles {
			return common.ErrTooManySourceFiles
		}
		return hdl(ctx, path, size)
	})

	return errors.Trace(err)
}

func (s *mdLoaderSetup) constructFileInfo(ctx context.Context, path string, size int64) error {
	logger := log.FromContext(ctx).With(zap.String("path", path))
	res, err := s.loader.fileRouter.Route(filepath.ToSlash(path))
	if err != nil {
		return errors.Annotatef(err, "apply file routing on file '%s' failed", path)
	}
	if res == nil {
		logger.Info("file is filtered by file router", zap.String("category", "loader"))
		return nil
	}

	info := FileInfo{
		TableName: filter.Table{Schema: res.Schema, Name: res.Name},
		FileMeta:  SourceFileMeta{Path: path, Type: res.Type, Compression: res.Compression, SortKey: res.Key, FileSize: size, RealSize: size},
	}

	if s.loader.shouldSkip(&info.TableName) {
		logger.Debug("ignoring table file", zap.String("category", "filter"))

		return nil
	}

	switch res.Type {
	case SourceTypeSchemaSchema:
		s.dbSchemas = append(s.dbSchemas, info)
	case SourceTypeTableSchema:
		s.tableSchemas = append(s.tableSchemas, info)
	case SourceTypeViewSchema:
		s.viewSchemas = append(s.viewSchemas, info)
	case SourceTypeSQL, SourceTypeCSV:
		if info.FileMeta.Compression != CompressionNone {
			compressRatio, err2 := SampleFileCompressRatio(ctx, info.FileMeta, s.loader.GetStore())
			if err2 != nil {
				logger.Error("fail to calculate data file compress ratio", zap.String("category", "loader"),
					zap.String("schema", res.Schema), zap.String("table", res.Name), zap.Stringer("type", res.Type))
			} else {
				info.FileMeta.RealSize = int64(compressRatio * float64(info.FileMeta.FileSize))
			}
		}
		s.tableDatas = append(s.tableDatas, info)
	case SourceTypeParquet:
		parquestDataSize, err2 := SampleParquetDataSize(ctx, info.FileMeta, s.loader.GetStore())
		if err2 != nil {
			logger.Error("fail to sample parquet data size", zap.String("category", "loader"),
				zap.String("schema", res.Schema), zap.String("table", res.Name), zap.Stringer("type", res.Type), zap.Error(err2))
		} else {
			info.FileMeta.RealSize = parquestDataSize
		}
		s.tableDatas = append(s.tableDatas, info)
	}

	logger.Debug("file route result", zap.String("schema", res.Schema),
		zap.String("table", res.Name), zap.Stringer("type", res.Type))

	return nil
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
func (l *MDLoader) GetStore() storage.ExternalStorage {
	return l.store
}

func calculateFileBytes(ctx context.Context,
	dataFile string,
	compressType storage.CompressType,
	store storage.ExternalStorage,
	offset int64) (tot int, pos int64, err error) {
	bytes := make([]byte, sampleCompressedFileSize)
	reader, err := store.Open(ctx, dataFile, nil)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	defer reader.Close()

	decompressConfig := storage.DecompressConfig{ZStdDecodeConcurrency: 1}
	compressReader, err := storage.NewLimitedInterceptReader(reader, compressType, decompressConfig, offset)
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

// SampleFileCompressRatio samples the compress ratio of the compressed file.
func SampleFileCompressRatio(ctx context.Context, fileMeta SourceFileMeta, store storage.ExternalStorage) (float64, error) {
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

// SampleParquetDataSize samples the data size of the parquet file.
func SampleParquetDataSize(ctx context.Context, fileMeta SourceFileMeta, store storage.ExternalStorage) (int64, error) {
	totalRowCount, err := ReadParquetFileRowCountByFile(ctx, store, fileMeta)
	if totalRowCount == 0 || err != nil {
		return 0, err
	}

	reader, err := store.Open(ctx, fileMeta.Path, nil)
	if err != nil {
		return 0, err
	}
	parser, err := NewParquetParser(ctx, store, reader, fileMeta.Path)
	if err != nil {
		//nolint: errcheck
		reader.Close()
		return 0, err
	}
	//nolint: errcheck
	defer parser.Close()

	var (
		rowSize  int64
		rowCount int64
	)
	for {
		err = parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return 0, err
		}
		lastRow := parser.LastRow()
		rowCount++
		rowSize += int64(lastRow.Length)
		parser.RecycleRow(lastRow)
		if rowSize > maxSampleParquetDataSize || rowCount > maxSampleParquetRowCount {
			break
		}
	}
	size := int64(float64(totalRowCount) / float64(rowCount) * float64(rowSize))
	return size, nil
}
