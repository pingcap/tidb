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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// TableMeta contains metadata for a table to be imported.
type TableMeta struct {
	Database     string
	Table        string
	DataFiles    []DataFileMeta
	TotalSize    int64  // In bytes
	WildcardPath string // Wildcard pattern matching this table's files
	SchemaFile   string // Path to the table schema file, if available
}

// DataFileMeta contains metadata for a data file.
type DataFileMeta struct {
	Path        string
	Size        int64
	Format      mydump.SourceType
	Compression mydump.Compression
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

// GetTableMetas implements the SDK interface.
func (sdk *ImportSDK) GetTableMetas(context.Context) ([]*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()
	allFiles := sdk.loader.GetAllFiles()
	var results []*TableMeta
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			tableMeta, err := sdk.buildTableMeta(dbMeta, tblMeta, allFiles)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to build metadata for table %s.%s", dbMeta.Name, tblMeta.Name)
			}
			results = append(results, tableMeta)
		}
	}

	return results, nil
}

// GetTableMetaByName implements the SDK interface.
func (sdk *ImportSDK) GetTableMetaByName(_ context.Context, schema, table string) (*TableMeta, error) {
	dbMetas := sdk.loader.GetDatabases()
	allFiles := sdk.loader.GetAllFiles()
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

// GetTotalSize implements the SDK interface.
func (sdk *ImportSDK) GetTotalSize(context.Context) int64 {
	var total int64
	dbMetas := sdk.loader.GetDatabases()
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			total += tblMeta.TotalSize
		}
	}
	return total
}

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

func createDataFileMeta(file mydump.FileInfo) DataFileMeta {
	return DataFileMeta{
		Path:        file.FileMeta.Path,
		Size:        file.FileMeta.RealSize,
		Format:      file.FileMeta.Type,
		Compression: file.FileMeta.Compression,
	}
}
