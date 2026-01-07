// Copyright 2025 PingCAP, Inc.
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

package importsdk

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"go.uber.org/zap"
)

// FileScanner defines the interface for scanning files
type FileScanner interface {
	CreateSchemasAndTables(ctx context.Context) error
	CreateSchemaAndTableByName(ctx context.Context, schema, table string) error
	GetTableMetas(ctx context.Context) ([]*TableMeta, error)
	GetTableMetaByName(ctx context.Context, db, table string) (*TableMeta, error)
	GetTotalSize(ctx context.Context) int64
	Close() error
}

type fileScanner struct {
	sourcePath string
	db     *sql.DB
	store  objstore.ExternalStorage
	loader *mydump.MDLoader
	logger     log.Logger
	config     *SDKConfig
}

// NewFileScanner creates a new FileScanner
func NewFileScanner(ctx context.Context, sourcePath string, db *sql.DB, cfg *SDKConfig) (FileScanner, error) {
	u, err := objstore.ParseBackend(sourcePath, nil)
	if err != nil {
		return nil, errors.Annotatef(ErrParseStorageURL, "source=%s, err=%v", sourcePath, err)
	}
	store, err := objstore.New(ctx, u, &objstore.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Annotatef(ErrCreateExternalStorage, "source=%s, err=%v", sourcePath, err)
	}

	ldrCfg := mydump.LoaderConfig{
		SourceURL:        sourcePath,
		Filter:           cfg.filter,
		FileRouters:      cfg.fileRouteRules,
		DefaultFileRules: len(cfg.fileRouteRules) == 0,
		CharacterSet:     cfg.charset,
		Routes:           cfg.routes,
	}

	var loaderOptions []mydump.MDLoaderSetupOption
	if cfg.maxScanFiles != nil && *cfg.maxScanFiles > 0 {
		loaderOptions = append(loaderOptions, mydump.WithMaxScanFiles(*cfg.maxScanFiles))
	}
	if cfg.concurrency > 0 {
		loaderOptions = append(loaderOptions, mydump.WithScanFileConcurrency(cfg.concurrency))
	}

	// TODO: we can skip some time-consuming operation in constructFileInfo (like get real size of compressed file).
	loader, err := mydump.NewLoaderWithStore(ctx, ldrCfg, store, loaderOptions...)
	if err != nil {
		if loader == nil || !errors.ErrorEqual(err, common.ErrTooManySourceFiles) {
			return nil, errors.Annotatef(ErrCreateLoader, "source=%s, charset=%s, err=%v", sourcePath, cfg.charset, err)
		}
	}

	return &fileScanner{
		sourcePath: sourcePath,
		db:         db,
		store:      store,
		loader:     loader,
		logger:     cfg.logger,
		config:     cfg,
	}, nil
}

func (s *fileScanner) CreateSchemasAndTables(ctx context.Context) error {
	dbMetas := s.loader.GetDatabases()
	if len(dbMetas) == 0 {
		return errors.Annotatef(ErrNoDatabasesFound, "source=%s", s.sourcePath)
	}

	// Create all schemas and tables
	importer := mydump.NewSchemaImporter(
		s.logger,
		s.config.sqlMode,
		s.db,
		s.store,
		s.config.concurrency,
	)

	err := importer.Run(ctx, dbMetas)
	if err != nil {
		return errors.Annotatef(ErrCreateSchema, "source=%s, db_count=%d, err=%v", s.sourcePath, len(dbMetas), err)
	}

	return nil
}

// CreateSchemaAndTableByName creates specific table and database schema from source
func (s *fileScanner) CreateSchemaAndTableByName(ctx context.Context, schema, table string) error {
	dbMetas := s.loader.GetDatabases()
	// Find the specific table
	for _, dbMeta := range dbMetas {
		if dbMeta.Name != schema {
			continue
		}

		for _, tblMeta := range dbMeta.Tables {
			if tblMeta.Name != table {
				continue
			}

			importer := mydump.NewSchemaImporter(
				s.logger,
				s.config.sqlMode,
				s.db,
				s.store,
				s.config.concurrency,
			)

			err := importer.Run(ctx, []*mydump.MDDatabaseMeta{{
				Name:       dbMeta.Name,
				SchemaFile: dbMeta.SchemaFile,
				Tables:     []*mydump.MDTableMeta{tblMeta},
			}})
			if err != nil {
				return errors.Annotatef(ErrCreateSchema, "source=%s, schema=%s, table=%s, err=%v", s.sourcePath, schema, table, err)
			}

			return nil
		}

		return errors.Annotatef(ErrTableNotFound, "schema=%s, table=%s", schema, table)
	}

	return errors.Annotatef(ErrSchemaNotFound, "schema=%s", schema)
}

func (s *fileScanner) GetTableMetas(context.Context) ([]*TableMeta, error) {
	dbMetas := s.loader.GetDatabases()
	allFiles := s.loader.GetAllFiles()
	var results []*TableMeta
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			tableMeta, err := s.buildTableMeta(dbMeta, tblMeta, allFiles)
			if err != nil {
				if s.config.skipInvalidFiles {
					s.logger.Warn("skipping table due to invalid files", zap.String("database", dbMeta.Name), zap.String("table", tblMeta.Name), zap.Error(err))
					continue
				}
				return nil, err
			}
			results = append(results, tableMeta)
		}
	}

	return results, nil
}

func (s *fileScanner) GetTotalSize(ctx context.Context) int64 {
	var total int64
	dbMetas := s.loader.GetDatabases()
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			total += tblMeta.TotalSize
		}
	}
	return total
}

func (s *fileScanner) Close() error {
	if s.store != nil {
		s.store.Close()
	}
	return nil
}

func (s *fileScanner) buildTableMeta(
	dbMeta *mydump.MDDatabaseMeta,
	tblMeta *mydump.MDTableMeta,
	allDataFiles map[string]mydump.FileInfo,
) (*TableMeta, error) {
	tableMeta := &TableMeta{
		Database:   dbMeta.Name,
		Table:      tblMeta.Name,
		DataFiles:  make([]DataFileMeta, 0, len(tblMeta.DataFiles)),
		SchemaFile: tblMeta.SchemaFile.FileMeta.Path,
	}

	// Process data files
	dataFiles, totalSize := processDataFiles(tblMeta.DataFiles)
	tableMeta.DataFiles = dataFiles
	tableMeta.TotalSize = totalSize

	if len(tblMeta.DataFiles) == 0 {
		s.logger.Warn("table has no data files", zap.String("database", dbMeta.Name), zap.String("table", tblMeta.Name))
		return tableMeta, nil
	}

	wildcard, err := generateWildcardPath(tblMeta.DataFiles, allDataFiles)
	if err != nil {
		return nil, errors.Trace(err)
	}
	uri := s.store.URI()
	// import into only support absolute path
	uri = strings.TrimPrefix(uri, "file://")
	tableMeta.WildcardPath = strings.TrimSuffix(uri, "/") + "/" + wildcard

	return tableMeta, nil
}

// processDataFiles converts mydump data files to DataFileMeta and calculates total size
func processDataFiles(files []mydump.FileInfo) ([]DataFileMeta, int64) {
	dataFiles := make([]DataFileMeta, 0, len(files))
	var totalSize int64

	for _, dataFile := range files {
		fileMeta := createDataFileMeta(dataFile)
		dataFiles = append(dataFiles, fileMeta)
		totalSize += dataFile.FileMeta.RealSize
	}

	return dataFiles, totalSize
}

// createDataFileMeta creates a DataFileMeta from a mydump.DataFile
func createDataFileMeta(file mydump.FileInfo) DataFileMeta {
	return DataFileMeta{
		Path:        file.FileMeta.Path,
		Size:        file.FileMeta.RealSize,
		Format:      file.FileMeta.Type,
		Compression: file.FileMeta.Compression,
	}
}

func (s *fileScanner) GetTableMetaByName(ctx context.Context, db, table string) (*TableMeta, error) {
	dbMetas := s.loader.GetDatabases()
	allFiles := s.loader.GetAllFiles()

	for _, dbMeta := range dbMetas {
		if dbMeta.Name != db {
			continue
		}
		for _, tblMeta := range dbMeta.Tables {
			if tblMeta.Name != table {
				continue
			}
			return s.buildTableMeta(dbMeta, tblMeta, allFiles)
		}
	}
	return nil, errors.Annotatef(ErrTableNotFound, "table %s.%s not found", db, table)
}
