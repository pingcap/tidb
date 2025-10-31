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
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// SDK defines the interface for cloud import services
type SDK interface {
	// CreateSchemasAndTables creates all database schemas and tables from source path
	CreateSchemasAndTables(ctx context.Context) error

	// GetTableMetas returns metadata for all tables in the source path
	GetTableMetas(ctx context.Context) ([]*TableMeta, error)

	// GetTableMetaByName returns metadata for a specific table
	GetTableMetaByName(ctx context.Context, schema, table string) (*TableMeta, error)

	// CreateSchemaAndTableByName creates specific table and database schema from source
	CreateSchemaAndTableByName(ctx context.Context, schema, table string) error

	// GetTotalSize returns the cumulative size (in bytes) of all data files under the source path
	GetTotalSize(ctx context.Context) int64

	// Close releases resources used by the SDK
	Close() error
}

// TableMeta contains metadata for a table to be imported
type TableMeta struct {
	Database     string
	Table        string
	DataFiles    []DataFileMeta
	TotalSize    int64  // In bytes
	WildcardPath string // Wildcard pattern that matches only this table's data files
	SchemaFile   string // Path to the table schema file, if available
}

// DataFileMeta contains metadata for a data file
type DataFileMeta struct {
	Path        string
	Size        int64
	Format      mydump.SourceType
	Compression mydump.Compression
}

// ImportSDK implements SDK interface
type ImportSDK struct {
	sourcePath string
	db         *sql.DB
	store      storage.ExternalStorage
	loader     *mydump.MDLoader
	logger     log.Logger
	config     *sdkConfig
}

// NewImportSDK creates a new CloudImportSDK instance
func NewImportSDK(ctx context.Context, sourcePath string, db *sql.DB, options ...SDKOption) (SDK, error) {
	cfg := defaultSDKConfig()
	for _, opt := range options {
		opt(cfg)
	}

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

	return &ImportSDK{
		sourcePath: sourcePath,
		db:         db,
		store:      store,
		loader:     loader,
		logger:     cfg.logger,
		config:     cfg,
	}, nil
}

// SDKOption customizes the SDK configuration
type SDKOption func(*sdkConfig)

type sdkConfig struct {
	// Loader options
	concurrency      int
	sqlMode          mysql.SQLMode
	fileRouteRules   []*config.FileRouteRule
	filter           []string
	charset          string
	maxScanFiles     *int
	skipInvalidFiles bool

	// General options
	logger log.Logger
}

func defaultSDKConfig() *sdkConfig {
	return &sdkConfig{
		concurrency: 4,
		filter:      config.GetDefaultFilter(),
		logger:      log.L(),
		charset:     "auto",
	}
}

// WithConcurrency sets the number of concurrent DB/Table creation workers.
func WithConcurrency(n int) SDKOption {
	return func(cfg *sdkConfig) {
		if n > 0 {
			cfg.concurrency = n
		}
	}
}

// WithLogger specifies a custom logger
func WithLogger(logger log.Logger) SDKOption {
	return func(cfg *sdkConfig) {
		cfg.logger = logger
	}
}

// WithSQLMode specifies the SQL mode for schema parsing
func WithSQLMode(mode mysql.SQLMode) SDKOption {
	return func(cfg *sdkConfig) {
		cfg.sqlMode = mode
	}
}

// WithFilter specifies a filter for the loader
func WithFilter(filter []string) SDKOption {
	return func(cfg *sdkConfig) {
		cfg.filter = filter
	}
}

// WithFileRouters specifies custom file routing rules
func WithFileRouters(routers []*config.FileRouteRule) SDKOption {
	return func(cfg *sdkConfig) {
		cfg.fileRouteRules = routers
	}
}

// WithCharset specifies the character set for import (default "auto").
func WithCharset(cs string) SDKOption {
	return func(cfg *sdkConfig) {
		if cs != "" {
			cfg.charset = cs
		}
	}
}

// WithMaxScanFiles specifies custom file scan limitation
func WithMaxScanFiles(limit int) SDKOption {
	return func(cfg *sdkConfig) {
		if limit > 0 {
			cfg.maxScanFiles = &limit
		}
	}
}

// WithSkipInvalidFiles specifies whether sdk need raise error on found invalid files
func WithSkipInvalidFiles(skip bool) SDKOption {
	return func(cfg *sdkConfig) {
		cfg.skipInvalidFiles = skip
	}
}

// CreateSchemasAndTables implements the CloudImportSDK interface
func (sdk *ImportSDK) CreateSchemasAndTables(ctx context.Context) error {
	dbMetas := sdk.loader.GetDatabases()
	if len(dbMetas) == 0 {
		return errors.Annotatef(ErrNoDatabasesFound, "source=%s. Ensure the path contains valid dump files (*.sql, *.csv, *.parquet, etc.) and filter rules are correct", sdk.sourcePath)
	}

	// Create all schemas and tables
	importer := mydump.NewSchemaImporter(
		sdk.logger,
		sdk.config.sqlMode,
		sdk.db,
		sdk.store,
		sdk.config.concurrency,
	)

	err := importer.Run(ctx, dbMetas)
	if err != nil {
		return errors.Annotatef(err, "creating schemas and tables failed (source=%s, db_count=%d, concurrency=%d)", sdk.sourcePath, len(dbMetas), sdk.config.concurrency)
	}

	return nil
}

// CreateSchemaAndTableByName creates specific table and database schema from source
func (sdk *ImportSDK) CreateSchemaAndTableByName(ctx context.Context, schema, table string) error {
	dbMetas := sdk.loader.GetDatabases()
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
				sdk.logger,
				sdk.config.sqlMode,
				sdk.db,
				sdk.store,
				sdk.config.concurrency,
			)

			err := importer.Run(ctx, []*mydump.MDDatabaseMeta{{
				Name:       dbMeta.Name,
				SchemaFile: dbMeta.SchemaFile,
				Tables:     []*mydump.MDTableMeta{tblMeta},
			}})
			if err != nil {
				return errors.Annotatef(err, "creating schema and table failed (source=%s, concurrency=%d, schema=%s, table=%s)", sdk.sourcePath, sdk.config.concurrency, schema, table)
			}

			return nil
		}

		return errors.Annotatef(ErrTableNotFound, "schema=%s, table=%s", schema, table)
	}

	return errors.Annotatef(ErrSchemaNotFound, "schema=%s", schema)
}

// GetTableMetas implements the CloudImportSDK interface
func (sdk *ImportSDK) GetTableMetas(context.Context) ([]*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()
	allFiles := sdk.loader.GetAllFiles()
	var results []*TableMeta
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			tableMeta, err := sdk.buildTableMeta(dbMeta, tblMeta, allFiles)
			if err != nil {
				retErr := errors.Wrapf(err, "failed to build metadata for table %s.%s",
					dbMeta.Name, tblMeta.Name)
				if sdk.config.skipInvalidFiles {
					continue
				}
				return nil, retErr
			}
			results = append(results, tableMeta)
		}
	}

	return results, nil
}

// GetTableMetaByName implements CloudImportSDK interface
func (sdk *ImportSDK) GetTableMetaByName(_ context.Context, schema, table string) (*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()
	allFiles := sdk.loader.GetAllFiles()
	// Find the specific table
	for _, dbMeta := range dbMetas {
		if dbMeta.Name != schema {
			continue
		}

		for _, tblMeta := range dbMeta.Tables {
			if tblMeta.Name != table {
				continue
			}

			return sdk.buildTableMeta(dbMeta, tblMeta, allFiles)
		}

		return nil, errors.Annotatef(ErrTableNotFound, "schema=%s, table=%s", schema, table)
	}

	return nil, errors.Annotatef(ErrSchemaNotFound, "schema=%s", schema)
}

// GetTotalSize implements CloudImportSDK interface
func (sdk *ImportSDK) GetTotalSize(ctx context.Context) int64 {
	var total int64
	dbMetas := sdk.loader.GetDatabases()
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			total += tblMeta.TotalSize
		}
	}
	return total
}

// buildTableMeta creates a TableMeta from database and table metadata
func (sdk *ImportSDK) buildTableMeta(
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

	wildcard, err := generateWildcardPath(tblMeta.DataFiles, allDataFiles)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to build wildcard for table=%s.%s", dbMeta.Name, tblMeta.Name)
	}
	tableMeta.WildcardPath = strings.TrimSuffix(sdk.store.URI(), "/") + "/" + wildcard

	return tableMeta, nil
}

// Close implements CloudImportSDK interface
func (sdk *ImportSDK) Close() error {
	// close external storage
	if sdk.store != nil {
		sdk.store.Close()
	}
	return nil
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

// TODO: add error code and doc for cloud sdk
// Sentinel errors to categorize common failure scenarios for clearer user messages.
var (
	// ErrNoDatabasesFound indicates that the dump source contains no recognizable databases.
	ErrNoDatabasesFound = errors.New("no databases found in the source path")
	// ErrSchemaNotFound indicates the target schema doesn't exist in the dump source.
	ErrSchemaNotFound = errors.New("schema not found")
	// ErrTableNotFound indicates the target table doesn't exist in the dump source.
	ErrTableNotFound = errors.New("table not found")
	// ErrNoTableDataFiles indicates a table has zero data files and thus cannot proceed.
	ErrNoTableDataFiles = errors.New("no data files for table")
	// ErrWildcardNotSpecific indicates a wildcard cannot uniquely match the table's files.
	ErrWildcardNotSpecific = errors.New("cannot generate a unique wildcard pattern for the table's data files")
)
