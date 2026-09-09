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
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestAuroraSourceSafety(t *testing.T) {
	const first = "export-a/db/db.users/1/part-a.parquet"
	for _, tc := range []struct {
		name    string
		paths   []string
		options []SDKOption
		want    string
		err     string
		count   int
	}{
		{name: "dotted table", paths: []string{"export/sales/sales.order.items/1/part-a.parquet"}, want: "sales/order.items"},
		{name: "dotted database", paths: []string{"export/sales.v1/sales.v1.order.items/part-a.parquet"}, want: "sales.v1/order.items"},
		{name: "literal percent", paths: []string{"export/db%20/db%20.order%2Eitems/1/part-a.parquet"}, want: "db%20/order%2Eitems"},
		{name: "single export scoped URL", paths: []string{"db/db.users/1/part-a.parquet"}, want: "db/users"},
		{name: "opaque parquet leaf", paths: []string{"export/db/db.users/1/chunk-a.parquet"}, want: "db/users"},
		{name: "uppercase suffix", paths: []string{"prefix.with.dots/export/db/db.users/00042/PART-a.GZ.PARQUET"}, want: "db/users"},
		{name: "literal encoded slash", paths: []string{"export/db/db.order%2Fitems/part-a.parquet"}, want: "db/order%2Fitems"},
		{name: "ambiguous table", paths: []string{"export/db/db.order_items/1/part-a.parquet"}, err: "ambiguous"},
		{name: "ambiguous database", paths: []string{"export/my_db/my_db.users/1/part-a.parquet"}, err: "ambiguous"},
		{name: "non-native space", paths: []string{"export/my db/my db.users/1/part-a.parquet"}, err: "ambiguous"},
		{name: "two roots same table", paths: []string{first, "export-b/db/db.users/1/part-b.parquet"}, err: "multiple"},
		{name: "two roots different tables", paths: []string{first, "export-b/other/other.orders/part-b.parquet"}, err: "multiple"},
		{name: "root is full prefix", paths: []string{"prefix/a/export/db/db.users/part-a.parquet", "prefix/b/export/db/db.users/part-b.parquet"}, err: "multiple"},
		{name: "empty and nonempty roots", paths: []string{"db/db.users/part-a.parquet", first}, err: "multiple"},
		{name: "filter cannot hide root", paths: []string{first, "export-b/other/other.orders/part-b.parquet"}, options: []SDKOption{WithFilter([]string{"db.users"})}, err: "multiple"},
		{name: "skip cannot hide mixed", paths: []string{first, "db.orders.1.csv"}, options: []SDKOption{WithSkipInvalidFiles(true)}, err: "mixed"},
		{name: "unmatched parquet", paths: []string{first, "unmatched.parquet"}, err: "mixed"},
		{name: "compressed parquet", paths: []string{first, "db.orders.1.parquet.gz"}, err: "mixed"},
		{name: "inconsistent directory", paths: []string{"archive/customer/staging.users/1/part-a.parquet"}, err: "inconsistent"},
		{name: "invalid batch", paths: []string{first, "export-a/db/db.orders/batch/part-b.parquet"}, err: "unsupported"},
		{name: "extra depth", paths: []string{first, "export-a/db/db.orders/1/extra/part-b.parquet"}, err: "unsupported"},
		{name: "non-parquet table object", paths: []string{first, "export-a/db/db.orders/1/part-b.csv"}, err: "unsupported"},
		{name: "truncated aurora", paths: []string{first, "export-a/db/db.users/2/part-b.parquet"}, options: []SDKOption{WithMaxScanFiles(1)}, err: "incomplete"},
		{name: "truncated before aurora", paths: []string{"aaa.tbl.1.csv", first}, options: []SDKOption{WithMaxScanFiles(1)}, err: "incomplete"},
		{name: "generic nested parquet", paths: []string{"backup/v1.0/db.users.0000.parquet"}, want: "db/users"},
		{name: "generic basename wins", paths: []string{"backup/customer/customer.orders/1/db.users.0000.parquet"}, want: "db/users"},
		{name: "single root filter", paths: []string{first, "export-a/other/other.orders/part-b.parquet"}, options: []SDKOption{WithFilter([]string{"db.users"})}, want: "db/users", count: 1},
		{name: "explicit router overrides detection", paths: []string{"a/my_db/my_db.table_name/part-a.parquet", "b/my_db/my_db.table_name/part-b.parquet"}, options: []SDKOption{WithFileRouters([]*config.FileRouteRule{{Pattern: `.*\.parquet$`, Schema: "target", Table: "chosen", Type: "parquet"}})}, want: "target/chosen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, path := range tc.paths {
				full := filepath.Join(dir, path)
				require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
				require.NoError(t, os.WriteFile(full, []byte("data"), 0o644))
			}
			cfg := defaultSDKConfig()
			WithEstimateRealSize(false)(cfg)
			for _, opt := range tc.options {
				opt(cfg)
			}
			scanner, err := NewFileScanner(context.Background(), "file://"+dir, nil, cfg)
			if scanner != nil {
				t.Cleanup(func() { require.NoError(t, scanner.Close()) })
			}
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				require.Nil(t, scanner)
				return
			}
			require.NoError(t, err)
			metas, err := scanner.GetTableMetas(context.Background())
			require.NoError(t, err)
			require.Len(t, metas, 1)
			require.Equal(t, tc.want, metas[0].Database+"/"+metas[0].Table)
			count := tc.count
			if count == 0 {
				count = len(tc.paths)
			}
			require.Len(t, metas[0].DataFiles, count)
		})
	}
}

type storageWithURI struct {
	storeapi.Storage
	uri string
}

func (s *storageWithURI) URI() string { return s.uri }

func TestAuroraWildcardURIPreservesRawKey(t *testing.T) {
	scanner := &fileScanner{
		auroraSource: true,
		store:        &storageWithURI{uri: "s3://bucket/prefix%2E/"},
	}
	key := "export/db/db.order%2Eitems/part-a.parquet"
	file := mydump.FileInfo{FileMeta: mydump.SourceFileMeta{Path: key, Type: mydump.SourceTypeParquet}}
	meta, err := scanner.buildTableMeta(&mydump.MDDatabaseMeta{Name: "db"},
		&mydump.MDTableMeta{Name: "order%2Eitems", DataFiles: []mydump.FileInfo{file}},
		map[string]mydump.FileInfo{key: file})
	require.NoError(t, err)
	u, err := url.Parse(meta.WildcardPath)
	require.NoError(t, err)
	require.Equal(t, "/prefix%2E/"+key, u.Path)
}
