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
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
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
	db         *sql.DB
	store      storage.ExternalStorage
	loader     *mydump.MDLoader
	logger     log.Logger
	config     *SDKConfig
}

// NewFileScanner creates a new FileScanner
func NewFileScanner(ctx context.Context, sourcePath string, db *sql.DB, cfg *SDKConfig) (FileScanner, error) {
	u, err := storage.ParseBackend(sourcePath, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to parse storage backend URL (source=%s). Please verify the URL format and credentials", sourcePath)
	}
	store, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Annotatef(err, "failed to create external storage (source=%s). Check network/connectivity and permissions", sourcePath)
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

	loader, err := mydump.NewLoaderWithStore(ctx, ldrCfg, store, loaderOptions...)
	if err != nil {
		if loader == nil || !errors.ErrorEqual(err, common.ErrTooManySourceFiles) {
			return nil, errors.Annotatef(err, "failed to create MyDump loader (source=%s, charset=%s, filter=%v). Please check dump layout and router rules", sourcePath, cfg.charset, cfg.filter)
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
		return errors.Annotatef(ErrNoDatabasesFound, "source=%s. Ensure the path contains valid dump files (*.sql, *.csv, *.parquet, etc.) and filter rules are correct", s.sourcePath)
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
		return errors.Annotatef(err, "creating schemas and tables failed (source=%s, db_count=%d, concurrency=%d)", s.sourcePath, len(dbMetas), s.config.concurrency)
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
				return errors.Annotatef(err, "creating schema and table failed (source=%s, concurrency=%d, schema=%s, table=%s)", s.sourcePath, s.config.concurrency, schema, table)
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
				retErr := errors.Wrapf(err, "failed to build metadata for table %s.%s",
					dbMeta.Name, tblMeta.Name)
				if s.config.skipInvalidFiles {
					continue
				}
				return nil, retErr
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
		return nil, errors.Annotatef(err, "failed to build wildcard for table=%s.%s", dbMeta.Name, tblMeta.Name)
	}
	uri := s.store.URI()
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

// generateWildcardPath creates a wildcard pattern path that matches only this table's files
func generateWildcardPath(
	files []mydump.FileInfo,
	allFiles map[string]mydump.FileInfo,
) (string, error) {
	tableFiles := make(map[string]struct{}, len(files))
	for _, df := range files {
		tableFiles[df.FileMeta.Path] = struct{}{}
	}

	if len(files) == 0 {
		return "", errors.Annotate(ErrNoTableDataFiles, "cannot generate wildcard pattern because the table has no data files")
	}

	// If there's only one file, we can just return its path
	if len(files) == 1 {
		return files[0].FileMeta.Path, nil
	}

	// Try Mydumper-specific pattern first
	p := generateMydumperPattern(files[0])
	if p != "" && isValidPattern(p, tableFiles, allFiles) {
		return p, nil
	}

	// Fallback to generic prefix/suffix pattern
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.FileMeta.Path)
	}
	p = generatePrefixSuffixPattern(paths)
	if p != "" && isValidPattern(p, tableFiles, allFiles) {
		return p, nil
	}
	return "", errors.Annotatef(ErrWildcardNotSpecific, "failed to find a wildcard that matches all and only the table's files.")
}

// isValidPattern checks if a wildcard pattern matches only the table's files
func isValidPattern(pattern string, tableFiles map[string]struct{}, allFiles map[string]mydump.FileInfo) bool {
	if pattern == "" {
		return false
	}

	for path := range allFiles {
		isMatch, err := filepath.Match(pattern, path)
		if err != nil {
			return false // Invalid pattern
		}
		_, isTableFile := tableFiles[path]

		// If pattern matches a file that's not from our table, it's invalid
		if isMatch && !isTableFile {
			return false
		}

		// If pattern doesn't match our table's file, it's also invalid
		if !isMatch && isTableFile {
			return false
		}
	}

	return true
}

// generateMydumperPattern generates a wildcard pattern for Mydumper-formatted data files
// belonging to a specific table, based on their naming convention.
// It returns a pattern string that matches all data files for the table, or an empty string if not applicable.
func generateMydumperPattern(file mydump.FileInfo) string {
	dbName, tableName := file.TableName.Schema, file.TableName.Name
	if dbName == "" || tableName == "" {
		return ""
	}

	// compute dirPrefix and basename
	full := file.FileMeta.Path
	dirPrefix, name := "", full
	if idx := strings.LastIndex(full, "/"); idx >= 0 {
		dirPrefix = full[:idx+1]
		name = full[idx+1:]
	}

	// compression ext from filename when compression exists (last suffix like .gz/.zst)
	compExt := ""
	if file.FileMeta.Compression != mydump.CompressionNone {
		compExt = filepath.Ext(name)
	}

	// data ext after stripping compression ext
	base := strings.TrimSuffix(name, compExt)
	dataExt := filepath.Ext(base)
	return dirPrefix + dbName + "." + tableName + ".*" + dataExt + compExt
}

// longestCommonPrefix finds the longest string that is a prefix of all strings in the slice
func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	prefix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for i < len(prefix) && i < len(s) && prefix[i] == s[i] {
			i++
		}
		prefix = prefix[:i]
		if prefix == "" {
			break
		}
	}

	return prefix
}

// longestCommonSuffix finds the longest string that is a suffix of all strings in the slice, starting after the given prefix length
func longestCommonSuffix(strs []string, prefixLen int) string {
	if len(strs) == 0 {
		return ""
	}

	suffix := strs[0][prefixLen:]
	for _, s := range strs[1:] {
		remaining := s[prefixLen:]
		i := 0
		for i < len(suffix) && i < len(remaining) && suffix[len(suffix)-i-1] == remaining[len(remaining)-i-1] {
			i++
		}
		suffix = suffix[len(suffix)-i:]
		if suffix == "" {
			break
		}
	}

	return suffix
}

// generatePrefixSuffixPattern returns a wildcard pattern that matches all and only the given paths
// by finding the longest common prefix and suffix among them, and placing a '*' wildcard in between.
func generatePrefixSuffixPattern(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	prefix := longestCommonPrefix(paths)
	suffix := longestCommonSuffix(paths, len(prefix))

	return prefix + "*" + suffix
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
