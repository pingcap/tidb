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

package importer

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// CloudImportSDK defines the interface for cloud import services
type CloudImportSDK interface {
	// CreateSchemasAndTables creates all database schemas and tables from source path
	CreateSchemasAndTables(ctx context.Context) error

	// GetTablesMeta returns metadata for all tables in the source path
	GetTablesMeta(ctx context.Context) ([]*TableMeta, error)

	// GetTableMetaByName returns metadata for a specific table
	GetTableMetaByName(ctx context.Context, schema, table string) (*TableMeta, error)

	// GetTotalSize returns the cumulative size (in bytes) of all data files under the source path
	GetTotalSize(ctx context.Context) (int64, error)

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

// ImportSDK implements CloudImportSDK interface
type ImportSDK struct {
	sourcePath string
	db         *sql.DB
	store      storage.ExternalStorage
	loader     *mydump.MDLoader
	logger     log.Logger
	config     *sdkConfig
	mu         sync.Mutex // Protects concurrent access to internal state
}

// NewImportSDK creates a new CloudImportSDK instance
func NewImportSDK(ctx context.Context, sourcePath string, db *sql.DB, options ...SDKOption) (CloudImportSDK, error) {
	cfg := defaultSDKConfig()
	for _, opt := range options {
		opt(cfg)
	}

	u, err := storage.ParseBackend(sourcePath, nil)
	if err != nil {
		return nil, errors.Annotate(err, "failed to parse storage backend URL")
	}
	store, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Annotate(err, "failed to create external storage")
	}

	ldrCfg := mydump.LoaderConfig{
		SourceURL:        sourcePath,
		Filter:           cfg.filter,
		FileRouters:      cfg.fileRouteRules,
		DefaultFileRules: len(cfg.fileRouteRules) == 0,
		CharacterSet:     cfg.charset,
	}

	loader, err := mydump.NewLoaderWithStore(ctx, ldrCfg, store)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create MyDump loader")
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
	concurrency    int
	sqlMode        mysql.SQLMode
	fileRouteRules []*config.FileRouteRule
	filter         []string
	charset        string

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

// CreateSchemasAndTables implements the CloudImportSDK interface
func (sdk *ImportSDK) CreateSchemasAndTables(ctx context.Context) error {
	dbMetas := sdk.loader.GetDatabases()
	if len(dbMetas) == 0 {
		return errors.New("no databases found in the source path")
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
		return errors.Annotate(err, "failed to create schemas and tables")
	}

	return nil
}

// GetTablesMeta implements the CloudImportSDK interface
func (sdk *ImportSDK) GetTablesMeta(ctx context.Context) ([]*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()
	var results []*TableMeta

	// First, collect all data files across all tables for validation
	allDataFiles := make(map[string]mydump.FileInfo)
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			for _, file := range tblMeta.DataFiles {
				allDataFiles[file.FileMeta.Path] = file
			}
			if tblMeta.SchemaFile.FileMeta.Path != "" {
				allDataFiles[tblMeta.SchemaFile.FileMeta.Path] = tblMeta.SchemaFile
			}
		}
	}

	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			tableMeta, err := sdk.buildTableMeta(dbMeta, tblMeta, allDataFiles)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to build metadata for table %s.%s",
					dbMeta.Name, tblMeta.Name)
			}
			results = append(results, tableMeta)
		}
	}

	return results, nil
}

// GetTableMetaByName implements CloudImportSDK interface
func (sdk *ImportSDK) GetTableMetaByName(ctx context.Context, schema, table string) (*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()

	// Collect all data files (and schema files) for pattern matching
	allDataFiles := make(map[string]mydump.FileInfo)
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			for _, file := range tblMeta.DataFiles {
				allDataFiles[file.FileMeta.Path] = file
			}
			if tblMeta.SchemaFile.FileMeta.Path != "" {
				allDataFiles[tblMeta.SchemaFile.FileMeta.Path] = tblMeta.SchemaFile
			}
		}
	}

	// Find the specific table
	for _, dbMeta := range dbMetas {
		if dbMeta.Name != schema {
			continue
		}

		for _, tblMeta := range dbMeta.Tables {
			if tblMeta.Name != table {
				continue
			}

			return sdk.buildTableMeta(dbMeta, tblMeta, allDataFiles)
		}

		return nil, errors.Errorf("table '%s' not found in schema '%s'", table, schema)
	}

	return nil, errors.Errorf("schema '%s' not found", schema)
}

// GetTotalSize implements CloudImportSDK interface
func (sdk *ImportSDK) GetTotalSize(ctx context.Context) (int64, error) {
	tables, err := sdk.GetTablesMeta(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, tbl := range tables {
		total += tbl.TotalSize
	}
	return total, nil
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
	dataFiles, totalSize, err := sdk.processDataFiles(tblMeta.DataFiles)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableMeta.DataFiles = dataFiles
	tableMeta.TotalSize = totalSize

	// Generate wildcard path for this table's data files
	tableFilePaths := make(map[string]struct{}, len(tblMeta.DataFiles))
	for _, df := range tblMeta.DataFiles {
		tableFilePaths[df.FileMeta.Path] = struct{}{}
	}

	wildcard, err := sdk.generateWildcard(tblMeta.DataFiles, tableFilePaths, allDataFiles)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableMeta.WildcardPath = strings.TrimSuffix(sdk.store.URI(), "/") + "/" + wildcard

	return tableMeta, nil
}

// Close implements CloudImportSDK interface
func (sdk *ImportSDK) Close() error {
	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	// close external storage
	if sdk.store != nil {
		sdk.store.Close()
	}
	return nil
}

// processDataFiles converts mydump data files to DataFileMeta and calculates total size
func (sdk *ImportSDK) processDataFiles(files []mydump.FileInfo) ([]DataFileMeta, int64, error) {
	dataFiles := make([]DataFileMeta, 0, len(files))
	var totalSize int64

	for _, dataFile := range files {
		fileMeta := createDataFileMeta(dataFile)
		dataFiles = append(dataFiles, fileMeta)
		totalSize += dataFile.FileMeta.RealSize
	}

	return dataFiles, totalSize, nil
}

// createDataFileMeta creates a DataFileMeta from a mydump.DataFile
func createDataFileMeta(file mydump.FileInfo) DataFileMeta {
	return DataFileMeta{
		Path:        file.FileMeta.Path,
		Size:        file.FileMeta.FileSize,
		Format:      file.FileMeta.Type,
		Compression: file.FileMeta.Compression,
	}
}

// generateWildcard creates a wildcard pattern that matches only this table's files
func (sdk *ImportSDK) generateWildcard(
	files []mydump.FileInfo,
	tableFiles map[string]struct{},
	allFiles map[string]mydump.FileInfo,
) (string, error) {
	if len(files) == 0 {
		return "", errors.New("no data files to generate wildcard pattern")
	}

	// If there's only one file, we can just return its path
	if len(files) == 1 {
		return files[0].FileMeta.Path, nil
	}

	// Extract file paths
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.FileMeta.Path)
	}

	// Try different pattern generation strategies in order of specificity
	patterns := []string{
		generateMydumperPattern(paths),     // Specific to Mydumper format
		generatePrefixSuffixPattern(paths), // Generic prefix/suffix pattern
	}

	// Use the first valid pattern
	for _, pattern := range patterns {
		if pattern != "" && validatePattern(pattern, tableFiles, allFiles) {
			return pattern, nil
		}
	}

	return "", errors.New("unable to generate a specific wildcard pattern for this table's data files")
}

// validatePattern checks if a wildcard pattern matches only the table's files
func validatePattern(pattern string, tableFiles map[string]struct{}, allFiles map[string]mydump.FileInfo) bool {
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

// generateMydumperPattern creates a pattern optimized for Mydumper naming conventions
func generateMydumperPattern(paths []string) string {
	// Check if paths appear to follow Mydumper naming convention
	if len(paths) == 0 {
		return ""
	}

	// Extract common database and table names from filenames
	dbName, tableName := extractMydumperNames(paths)
	if dbName == "" || tableName == "" {
		return "" // Not a Mydumper pattern or inconsistent names
	}

	// Check if there's a directory component in the paths
	dirPrefix := extractCommonDirectory(paths)

	// Generate pattern based on Mydumper format
	if dirPrefix == "" {
		// Files are in the current directory
		return dbName + "." + tableName + ".*.sql"
	}

	// Files are in a specific directory
	return dirPrefix + dbName + "." + tableName + ".*.sql"
}

// extractMydumperNames extracts database and table names from Mydumper-formatted paths
func extractMydumperNames(paths []string) (string, string) {
	// Extract filenames from paths
	filenames := make([]string, 0, len(paths))
	for _, path := range paths {
		// Get just the filename part
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash >= 0 {
			filenames = append(filenames, path[lastSlash+1:])
		} else {
			filenames = append(filenames, path)
		}
	}

	// Check if all filenames follow Mydumper data file pattern ({db}.{table}.sql or {db}.{table}.{part}.sql)
	var dbName, tableName string
	for i, filename := range filenames {
		// Skip schema files
		if strings.HasSuffix(filename, "-schema.sql") || strings.HasSuffix(filename, "-schema-create.sql") {
			continue
		}

		// Skip non-SQL files (Mydumper typically produces .sql files)
		if !strings.HasSuffix(filename, ".sql") {
			return "", ""
		}

		// Parse "{db}.{table}.sql" or "{db}.{table}.{part}.sql"
		parts := strings.Split(filename, ".")
		if len(parts) < 3 {
			return "", "" // Not enough parts for Mydumper format
		}

		currentDB := parts[0]
		currentTable := parts[1]

		// In first iteration, set the names
		if i == 0 {
			dbName = currentDB
			tableName = currentTable
			continue
		}

		// In subsequent iterations, verify consistency
		if dbName != currentDB || tableName != currentTable {
			return "", "" // Inconsistent names, not following Mydumper pattern
		}
	}

	return dbName, tableName
}

// extractCommonDirectory gets the common directory prefix from paths
func extractCommonDirectory(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	// Find common prefix for all paths
	prefix := longestCommonPrefix(paths)

	// Find last directory separator in prefix
	lastSlash := strings.LastIndex(prefix, "/")
	if lastSlash >= 0 {
		return prefix[:lastSlash+1]
	}

	return ""
}

// longestCommonPrefix finds the longest string that is a prefix of all strings in the slice
func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	prefix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for ; i < len(prefix) && i < len(s) && prefix[i] == s[i]; i++ {
		}
		prefix = prefix[:i]
		if prefix == "" {
			break
		}
	}

	return prefix
}

// longestCommonSuffix finds the longest string that is a suffix of all strings in the slice
func longestCommonSuffix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	suffix := strs[0]
	for _, s := range strs[1:] {
		i := 0
		for ; i < len(suffix) && i < len(s) &&
			suffix[len(suffix)-i-1] == s[len(s)-i-1]; i++ {
		}
		suffix = suffix[len(suffix)-i:]
		if suffix == "" {
			break
		}
	}

	return suffix
}

// generatePrefixSuffixPattern creates a wildcard pattern using common prefix and suffix
func generatePrefixSuffixPattern(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return paths[0]
	}

	prefix := longestCommonPrefix(paths)
	suffix := longestCommonSuffix(paths)

	minLen := len(paths[0])
	for _, p := range paths[1:] {
		if len(p) < minLen {
			minLen = len(p)
		}
	}
	maxSuffixLen := minLen - len(prefix)
	if len(suffix) > maxSuffixLen {
		suffix = suffix[len(suffix)-maxSuffixLen:]
	}

	return prefix + "*" + suffix
}
