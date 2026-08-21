// Copyright 2026 PingCAP, Inc.
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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func writeSourceObject(t *testing.T, root, path, contents string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
	require.NoError(t, os.WriteFile(fullPath, []byte(contents), 0o644))
}

func newTestFileScanner(t *testing.T, root string, options ...SDKOption) SourceScanner {
	t.Helper()
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cfg := defaultSDKConfig()
	WithEstimateRealSize(false)(cfg)
	for _, option := range options {
		option(cfg)
	}
	scanner, err := NewFileScanner(context.Background(), "file://"+root, db, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, scanner.Close()) })
	sourceScanner, ok := scanner.(SourceScanner)
	require.True(t, ok)
	return sourceScanner
}

func TestScanAuroraSnapshotSource(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db/db.users/1/part-00000-a.gz.parquet", "aaa")
	writeSourceObject(t, root, "db/db.users/2/part-00000-b.gz.parquet", "bbbb")
	writeSourceObject(t, root, "db/db.orders/part-00000-c.gz.parquet", "ccccc")
	writeSourceObject(t, root, "analytics/analytics.events/1/part-00000-d.gz.parquet", "dddddd")
	writeSourceObject(t, root, "db.users-schema.sql", "CREATE TABLE users (id BIGINT);")
	writeSourceObject(t, root, "export_info.json", "{}")

	scanner := newTestFileScanner(t, root)
	result, err := scanner.ScanSource(context.Background())
	require.NoError(t, err)
	require.Equal(t, SourceLayoutAuroraRDSSnapshot, result.Layout)
	require.Equal(t, "mixed", result.Evidence.PathForm)
	require.Empty(t, result.Evidence.ExportRoot)
	require.True(t, result.Inventory.Complete)
	require.Equal(t, int64(6), result.Inventory.ScannedObjectCount)
	require.Equal(t, int64(4), result.Inventory.ImportableObjectCount)
	require.Equal(t, int64(4), result.Inventory.MappedObjectCount)
	require.Equal(t, int64(18), result.Inventory.TotalObjectBytes)
	require.NotEmpty(t, result.Inventory.Digest)
	require.Len(t, result.Tables, 3)

	tables := make(map[string]*TableMeta, len(result.Tables))
	for _, table := range result.Tables {
		tables[table.Database+"."+table.Table] = table
	}
	require.Equal(t, int64(2), tables["db.users"].ObjectCount)
	require.Equal(t, int64(7), tables["db.users"].TotalObjectSize)
	require.Contains(t, tables["db.users"].WildcardPath, "db/db.users/*/part-00000-*.gz.parquet")
	require.Equal(t, int64(1), tables["db.orders"].ObjectCount)
	require.Equal(t, int64(5), tables["db.orders"].TotalObjectSize)
	require.Contains(t, tables["db.orders"].WildcardPath, "db/db.orders/part-00000-c.gz.parquet")
	require.Equal(t, int64(1), tables["analytics.events"].ObjectCount)
	require.Equal(t, int64(6), tables["analytics.events"].TotalObjectSize)
}

func TestScanAuroraSnapshotRejectsMixedLayout(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db/db.users/1/part-00000-a.parquet", "a")
	writeSourceObject(t, root, "other.table.0001.parquet", "b")

	scanner := newTestFileScanner(t, root)
	_, err := scanner.ScanSource(context.Background())
	var scanErr *SourceScanError
	require.ErrorAs(t, err, &scanErr)
	require.Equal(t, SourceScanErrorMixedLayout, scanErr.Code)
	require.Equal(t, int64(1), scanErr.Count)
}

func TestScanAuroraSnapshotRejectsNonParquetData(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db/db.users/1/part-00000-a.parquet", "a")
	writeSourceObject(t, root, "db.other.0001.csv.gz", "b")

	scanner := newTestFileScanner(t, root)
	_, err := scanner.ScanSource(context.Background())
	var scanErr *SourceScanError
	require.ErrorAs(t, err, &scanErr)
	require.Equal(t, SourceScanErrorMixedLayout, scanErr.Code)
	require.Equal(t, int64(1), scanErr.Count)
	require.Equal(t, []string{"db.other.0001.csv.gz"}, scanErr.Samples)
}

func TestScanAuroraSnapshotRejectsMultipleExportRoots(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "export-1/db/db.users/1/part-a.parquet", "a")
	writeSourceObject(t, root, "export-2/db/db.orders/1/part-b.parquet", "b")

	scanner := newTestFileScanner(t, root)
	_, err := scanner.ScanSource(context.Background())
	var scanErr *SourceScanError
	require.ErrorAs(t, err, &scanErr)
	require.Equal(t, SourceScanErrorMultipleExportRoots, scanErr.Code)
	require.Equal(t, int64(2), scanErr.Count)
}

func TestScanAuroraSnapshotRejectsAmbiguousLayout(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(
		t,
		root,
		"parent/first.table/database/schema.table/part.parquet",
		"a",
	)

	scanner := newTestFileScanner(t, root)
	_, err := scanner.ScanSource(context.Background())
	var scanErr *SourceScanError
	require.ErrorAs(t, err, &scanErr)
	require.Equal(t, SourceScanErrorAmbiguousLayout, scanErr.Code)
	require.Equal(t, int64(1), scanErr.Count)
}

func TestScanAuroraSnapshotRejectsIncompleteScan(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db/db.users/1/part-a.parquet", "a")
	writeSourceObject(t, root, "db/db.users/1/part-b.parquet", "b")

	scanner := newTestFileScanner(t, root, WithMaxScanFiles(1))
	_, err := scanner.ScanSource(context.Background())
	var scanErr *SourceScanError
	require.True(t, errors.As(err, &scanErr))
	require.Equal(t, SourceScanErrorIncompleteScan, scanErr.Code)
}

func TestScanSourceInventoryDigestChangesWithObject(t *testing.T) {
	root := t.TempDir()
	path := "db/db.users/1/part-a.parquet"
	writeSourceObject(t, root, path, "a")

	first, err := newTestFileScanner(t, root).ScanSource(context.Background())
	require.NoError(t, err)
	second, err := newTestFileScanner(t, root).ScanSource(context.Background())
	require.NoError(t, err)
	require.Equal(t, first.Inventory.Digest, second.Inventory.Digest)

	writeSourceObject(t, root, path, "changed")
	changed, err := newTestFileScanner(t, root).ScanSource(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, first.Inventory.Digest, changed.Inventory.Digest)
}

func TestScanSourcePreservesDefaultLayout(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db.table.0001.csv", "1,2\n")

	result, err := newTestFileScanner(t, root).ScanSource(context.Background())
	require.NoError(t, err)
	require.Equal(t, SourceLayoutDefault, result.Layout)
	require.True(t, result.Inventory.Complete)
	require.Equal(t, int64(1), result.Inventory.ImportableObjectCount)
	require.Equal(t, int64(1), result.Inventory.MappedObjectCount)
	require.Equal(t, int64(4), result.Inventory.TotalObjectBytes)
	require.Len(t, result.Tables, 1)
	require.Equal(t, "db", result.Tables[0].Database)
	require.Equal(t, "table", result.Tables[0].Table)
}

func TestScanSourcePreservesExplicitFileRouter(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "db/db.users/1/part-a.parquet", "a")

	rules := []*config.FileRouteRule{{
		Pattern: `.*\.parquet$`,
		Schema:  "custom",
		Table:   "target",
		Type:    "parquet",
	}}
	result, err := newTestFileScanner(
		t,
		root,
		WithFileRouters(rules),
	).ScanSource(context.Background())
	require.NoError(t, err)
	require.Equal(t, SourceLayoutDefault, result.Layout)
	require.Len(t, result.Tables, 1)
	require.Equal(t, "custom", result.Tables[0].Database)
	require.Equal(t, "target", result.Tables[0].Table)
}

func TestScanSourceRejectsNoImportableFiles(t *testing.T) {
	root := t.TempDir()
	writeSourceObject(t, root, "export_info.json", "{}")

	_, err := newTestFileScanner(t, root).ScanSource(context.Background())
	var scanErr *SourceScanError
	require.ErrorAs(t, err, &scanErr)
	require.Equal(t, SourceScanErrorNoImportableFiles, scanErr.Code)
}
