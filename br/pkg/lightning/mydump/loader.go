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
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

type MDDatabaseMeta struct {
	Name       string
	SchemaFile FileInfo
	Tables     []*MDTableMeta
	Views      []*MDTableMeta
	charSet    string
}

func (m *MDDatabaseMeta) GetSchema(ctx context.Context, store storage.ExternalStorage) (string, error) {
	schema, err := ExportStatement(ctx, store, m.SchemaFile, m.charSet)
	if err != nil {
		log.L().Warn("failed to extract table schema",
			zap.String("Path", m.SchemaFile.FileMeta.Path),
			log.ShortError(err),
		)
		schema = nil
	}
	schemaStr := strings.TrimSpace(string(schema))
	// set default if schema sql is empty
	if len(schemaStr) == 0 {
		schemaStr = "CREATE DATABASE IF NOT EXISTS " + common.EscapeIdentifier(m.Name)
	}

	return schemaStr, nil
}

type MDTableMeta struct {
	DB           string
	Name         string
	SchemaFile   FileInfo
	DataFiles    []FileInfo
	charSet      string
	TotalSize    int64
	IndexRatio   float64
	IsRowOrdered bool
}

type SourceFileMeta struct {
	Path        string
	Type        SourceType
	Compression Compression
	SortKey     string
	FileSize    int64
}

func (m *MDTableMeta) GetSchema(ctx context.Context, store storage.ExternalStorage) (string, error) {
	schema, err := ExportStatement(ctx, store, m.SchemaFile, m.charSet)
	if err != nil {
		log.L().Error("failed to extract table schema",
			zap.String("Path", m.SchemaFile.FileMeta.Path),
			log.ShortError(err),
		)
		return "", err
	}
	return string(schema), nil
}

/*
	Mydumper File Loader
*/
type MDLoader struct {
	store      storage.ExternalStorage
	dbs        []*MDDatabaseMeta
	filter     filter.Filter
	router     *router.Table
	fileRouter FileRouter
	charSet    string
}

type mdLoaderSetup struct {
	loader        *MDLoader
	dbSchemas     []FileInfo
	tableSchemas  []FileInfo
	viewSchemas   []FileInfo
	tableDatas    []FileInfo
	dbIndexMap    map[string]int
	tableIndexMap map[filter.Table]int
}

func NewMyDumpLoader(ctx context.Context, cfg *config.Config) (*MDLoader, error) {
	u, err := storage.ParseBackend(cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewMyDumpLoaderWithStore(ctx, cfg, s)
}

func NewMyDumpLoaderWithStore(ctx context.Context, cfg *config.Config, store storage.ExternalStorage) (*MDLoader, error) {
	var r *router.Table
	var err error

	if len(cfg.Routes) > 0 && len(cfg.Mydumper.FileRouters) > 0 {
		return nil, errors.New("table route is deprecated, can't config both [routes] and [mydumper.files]")
	}

	if len(cfg.Routes) > 0 {
		r, err = router.NewTableRouter(cfg.Mydumper.CaseSensitive, cfg.Routes)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// use the legacy black-white-list if defined. otherwise use the new filter.
	var f filter.Filter
	if cfg.HasLegacyBlackWhiteList() {
		f, err = filter.ParseMySQLReplicationRules(&cfg.BWList)
	} else {
		f, err = filter.Parse(cfg.Mydumper.Filter)
	}
	if err != nil {
		return nil, errors.Annotate(err, "parse filter failed")
	}
	if !cfg.Mydumper.CaseSensitive {
		f = filter.CaseInsensitive(f)
	}

	fileRouteRules := cfg.Mydumper.FileRouters
	if cfg.Mydumper.DefaultFileRules {
		fileRouteRules = append(fileRouteRules, defaultFileRouteRules...)
	}

	fileRouter, err := NewFileRouter(fileRouteRules)
	if err != nil {
		return nil, errors.Annotate(err, "parser file routing rule failed")
	}

	mdl := &MDLoader{
		store:      store,
		filter:     f,
		router:     r,
		charSet:    cfg.Mydumper.CharacterSet,
		fileRouter: fileRouter,
	}

	setup := mdLoaderSetup{
		loader:        mdl,
		dbIndexMap:    make(map[string]int),
		tableIndexMap: make(map[filter.Table]int),
	}

	if err := setup.setup(ctx, mdl.store); err != nil {
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

type FileInfo struct {
	TableName filter.Table
	FileMeta  SourceFileMeta
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
func (s *mdLoaderSetup) setup(ctx context.Context, store storage.ExternalStorage) error {
	/*
		Mydumper file names format
			db    —— {db}-schema-create.sql
			table —— {db}.{table}-schema.sql
			sql   —— {db}.{table}.{part}.sql / {db}.{table}.sql
	*/
	if err := s.listFiles(ctx, store); err != nil {
		return errors.Annotate(err, "list file failed")
	}
	if err := s.route(); err != nil {
		return errors.Trace(err)
	}

	// setup database schema
	if len(s.dbSchemas) != 0 {
		for _, fileInfo := range s.dbSchemas {
			if _, dbExists := s.insertDB(fileInfo); dbExists && s.loader.router == nil {
				return errors.Errorf("invalid database schema file, duplicated item - %s", fileInfo.FileMeta.Path)
			}
		}
	}

	if len(s.tableSchemas) != 0 {
		// setup table schema
		for _, fileInfo := range s.tableSchemas {
			if _, _, tableExists := s.insertTable(fileInfo); tableExists && s.loader.router == nil {
				return errors.Errorf("invalid table schema file, duplicated item - %s", fileInfo.FileMeta.Path)
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
				return errors.Errorf("invalid view schema file, miss host table schema for view '%s'", fileInfo.TableName.Name)
			}
		}
	}

	// Sql file for restore data
	for _, fileInfo := range s.tableDatas {
		// set a dummy `FileInfo` here without file meta because we needn't restore the table schema
		tableMeta, _, _ := s.insertTable(FileInfo{TableName: fileInfo.TableName})
		tableMeta.DataFiles = append(tableMeta.DataFiles, fileInfo)
		tableMeta.TotalSize += fileInfo.FileMeta.FileSize
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

	return nil
}

func (s *mdLoaderSetup) listFiles(ctx context.Context, store storage.ExternalStorage) error {
	// `filepath.Walk` yields the paths in a deterministic (lexicographical) order,
	// meaning the file and chunk orders will be the same everytime it is called
	// (as long as the source is immutable).
	err := store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		logger := log.With(zap.String("path", path))

		res, err := s.loader.fileRouter.Route(filepath.ToSlash(path))
		if err != nil {
			return errors.Annotatef(err, "apply file routing on file '%s' failed", path)
		}
		if res == nil {
			logger.Info("[loader] file is filtered by file router")
			return nil
		}

		info := FileInfo{
			TableName: filter.Table{Schema: res.Schema, Name: res.Name},
			FileMeta:  SourceFileMeta{Path: path, Type: res.Type, Compression: res.Compression, SortKey: res.Key, FileSize: size},
		}

		if s.loader.shouldSkip(&info.TableName) {
			logger.Debug("[filter] ignoring table file")

			return nil
		}

		switch res.Type {
		case SourceTypeSchemaSchema:
			s.dbSchemas = append(s.dbSchemas, info)
		case SourceTypeTableSchema:
			s.tableSchemas = append(s.tableSchemas, info)
		case SourceTypeViewSchema:
			s.viewSchemas = append(s.viewSchemas, info)
		case SourceTypeSQL, SourceTypeCSV, SourceTypeParquet:
			s.tableDatas = append(s.tableDatas, info)
		}

		logger.Debug("file route result", zap.String("schema", res.Schema),
			zap.String("table", res.Name), zap.Stringer("type", res.Type))

		return nil
	})

	return errors.Trace(err)
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
		count    int
	}

	knownDBNames := make(map[string]dbInfo)
	for _, info := range s.dbSchemas {
		knownDBNames[info.TableName.Schema] = dbInfo{
			fileMeta: info.FileMeta,
			count:    1,
		}
	}
	for _, info := range s.tableSchemas {
		dbInfo := knownDBNames[info.TableName.Schema]
		dbInfo.count++
		knownDBNames[info.TableName.Schema] = dbInfo
	}
	for _, info := range s.viewSchemas {
		dbInfo := knownDBNames[info.TableName.Schema]
		dbInfo.count++
	}

	run := func(arr []FileInfo) error {
		for i, info := range arr {
			dbName, tableName, err := r.Route(info.TableName.Schema, info.TableName.Name)
			if err != nil {
				return errors.Trace(err)
			}
			if dbName != info.TableName.Schema {
				oldInfo := knownDBNames[info.TableName.Schema]
				oldInfo.count--
				knownDBNames[info.TableName.Schema] = oldInfo

				newInfo, ok := knownDBNames[dbName]
				newInfo.count++
				if !ok {
					newInfo.fileMeta = oldInfo.fileMeta
					s.dbSchemas = append(s.dbSchemas, FileInfo{
						TableName: filter.Table{Schema: dbName},
						FileMeta:  oldInfo.fileMeta,
					})
				}
				knownDBNames[dbName] = newInfo
			}
			arr[i].TableName = filter.Table{Schema: dbName, Name: tableName}
		}
		return nil
	}

	if err := run(s.tableSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := run(s.viewSchemas); err != nil {
		return errors.Trace(err)
	}
	if err := run(s.tableDatas); err != nil {
		return errors.Trace(err)
	}

	// remove all schemas which has been entirely routed away
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	remainingSchemas := s.dbSchemas[:0]
	for _, info := range s.dbSchemas {
		if knownDBNames[info.TableName.Schema].count > 0 {
			remainingSchemas = append(remainingSchemas, info)
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

func (s *mdLoaderSetup) insertTable(fileInfo FileInfo) (*MDTableMeta, bool, bool) {
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

func (s *mdLoaderSetup) insertView(fileInfo FileInfo) (bool, bool) {
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

func (l *MDLoader) GetDatabases() []*MDDatabaseMeta {
	return l.dbs
}

func (l *MDLoader) GetStore() storage.ExternalStorage {
	return l.store
}
