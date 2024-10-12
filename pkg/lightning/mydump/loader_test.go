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

package mydump_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	md "github.com/pingcap/tidb/pkg/lightning/mydump"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type testMydumpLoaderSuite struct {
	cfg       *config.Config
	sourceDir string
}

func newConfigWithSourceDir(sourceDir string) *config.Config {
	path, _ := filepath.Abs(sourceDir)
	return &config.Config{
		Mydumper: config.MydumperRuntime{
			SourceDir:        "file://" + filepath.ToSlash(path),
			Filter:           []string{"*.*"},
			DefaultFileRules: true,
		},
	}
}

func newTestMydumpLoaderSuite(t *testing.T) *testMydumpLoaderSuite {
	var s testMydumpLoaderSuite
	var err error
	s.sourceDir = t.TempDir()
	require.Nil(t, err)
	s.cfg = newConfigWithSourceDir(s.sourceDir)
	return &s
}

func (s *testMydumpLoaderSuite) touch(t *testing.T, filename ...string) {
	components := make([]string, len(filename)+1)
	components = append(components, s.sourceDir)
	components = append(components, filename...)
	path := filepath.Join(components...)
	err := os.WriteFile(path, nil, 0o644)
	require.Nil(t, err)
}

func (s *testMydumpLoaderSuite) mkdir(t *testing.T, dirname string) {
	path := filepath.Join(s.sourceDir, dirname)
	err := os.Mkdir(path, 0o755)
	require.Nil(t, err)
}

func TestLoader(t *testing.T) {
	ctx := context.Background()
	cfg := newConfigWithSourceDir("./not-exists")
	_, err := md.NewLoader(ctx, md.NewLoaderCfg(cfg))
	// will check schema in tidb and data file later in DataCheck.
	require.NoError(t, err)

	cfg = newConfigWithSourceDir("./examples")
	mdl, err := md.NewLoader(ctx, md.NewLoaderCfg(cfg))
	require.NoError(t, err)

	dbMetas := mdl.GetDatabases()
	require.Len(t, dbMetas, 1)
	dbMeta := dbMetas[0]
	require.Equal(t, "mocker_test", dbMeta.Name)
	require.Len(t, dbMeta.Tables, 4)

	expected := []struct {
		name      string
		dataFiles int
	}{
		{name: "i", dataFiles: 1},
		{name: "report_case_high_risk", dataFiles: 1},
		{name: "tbl_multi_index", dataFiles: 1},
		{name: "tbl_autoid", dataFiles: 1},
	}

	for i, table := range expected {
		assert.Equal(t, table.name, dbMeta.Tables[i].Name)
		assert.Equal(t, table.dataFiles, len(dbMeta.Tables[i].DataFiles))
	}
}

func TestEmptyDB(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)
	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	// will check schema in tidb and data file later in DataCheck.
	require.NoError(t, err)
}

func TestDuplicatedDB(t *testing.T) {
	/*
		Path/
			a/
				db-schema-create.sql
			b/
				db-schema-create.sql
	*/
	s := newTestMydumpLoaderSuite(t)
	s.mkdir(t, "a")
	s.touch(t, "a", "db-schema-create.sql")
	s.mkdir(t, "b")
	s.touch(t, "b", "db-schema-create.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.Regexp(t, `invalid database schema file, duplicated item - .*[/\\]db-schema-create\.sql`, err)
}

func TestTableNoHostDB(t *testing.T) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl-schema.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	dir := s.sourceDir
	err := os.WriteFile(filepath.Join(dir, "notdb-schema-create.sql"), nil, 0o644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, "db.tbl-schema.sql"), nil, 0o644)
	require.NoError(t, err)

	_, err = md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
}

func TestDuplicatedTable(t *testing.T) {
	/*
		Path/
			db-schema-create.sql
			a/
				db.tbl-schema.sql
			b/
				db.tbl-schema.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "db-schema-create.sql")
	s.mkdir(t, "a")
	s.touch(t, "a", "db.tbl-schema.sql")
	s.mkdir(t, "b")
	s.touch(t, "b", "db.tbl-schema.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.Regexp(t, `invalid table schema file, duplicated item - .*db\.tbl-schema\.sql`, err)
}

func TestTableInfoNotFound(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.cfg.Mydumper.CharacterSet = "auto"

	s.touch(t, "db-schema-create.sql")
	s.touch(t, "db.tbl-schema.sql")

	ctx := context.Background()
	store, err := storage.NewLocalStorage(s.sourceDir)
	require.NoError(t, err)

	loader, err := md.NewLoader(ctx, md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	for _, dbMeta := range loader.GetDatabases() {
		logger, buffer := log.MakeTestLogger()
		logCtx := log.NewContext(ctx, logger)
		dbSQL := dbMeta.GetSchema(logCtx, store)
		require.Equal(t, "CREATE DATABASE IF NOT EXISTS `db`", dbSQL)
		for _, tblMeta := range dbMeta.Tables {
			sql, err := tblMeta.GetSchema(logCtx, store)
			require.Equal(t, "", sql)
			require.NoError(t, err)
		}
		require.NotContains(t, buffer.Stripped(), "failed to extract table schema")
	}
}

func TestTableUnexpectedError(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)
	s.touch(t, "db-schema-create.sql")
	s.touch(t, "db.tbl-schema.sql")

	ctx := context.Background()
	store, err := storage.NewLocalStorage(s.sourceDir)
	require.NoError(t, err)

	loader, err := md.NewLoader(ctx, md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	for _, dbMeta := range loader.GetDatabases() {
		for _, tblMeta := range dbMeta.Tables {
			sql, err := tblMeta.GetSchema(ctx, store)
			require.Equal(t, "", sql)
			require.Contains(t, err.Error(), "failed to decode db.tbl-schema.sql as : Unsupported encoding ")
		}
	}
}

func TestMissingTableSchema(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.cfg.Mydumper.CharacterSet = "auto"

	s.touch(t, "db.tbl.csv")

	ctx := context.Background()
	store, err := storage.NewLocalStorage(s.sourceDir)
	require.NoError(t, err)

	loader, err := md.NewLoader(ctx, md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	for _, dbMeta := range loader.GetDatabases() {
		for _, tblMeta := range dbMeta.Tables {
			_, err := tblMeta.GetSchema(ctx, store)
			require.ErrorContains(t, err, "schema file is missing for the table")
		}
	}
}

func TestDataNoHostDB(t *testing.T) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "notdb-schema-create.sql")
	s.touch(t, "db.tbl.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	// will check schema in tidb and data file later in DataCheck.
	require.NoError(t, err)
}

func TestDataNoHostTable(t *testing.T) {
	/*
		Path/
			db-schema-create.sql
			db.tbl.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "db-schema-create.sql")
	s.touch(t, "db.tbl.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	// will check schema in tidb and data file later in DataCheck.
	require.NoError(t, err)
}

func TestViewNoHostDB(t *testing.T) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl-schema-view.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "notdb-schema-create.sql")
	s.touch(t, "db.tbl-schema-view.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.Contains(t, err.Error(), `invalid view schema file, miss host table schema for view 'tbl'`)
}

func TestViewNoHostTable(t *testing.T) {
	/*
		Path/
			db-schema-create.sql
			db.tbl-schema-view.sql
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "db-schema-create.sql")
	s.touch(t, "db.tbl-schema-view.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.Contains(t, err.Error(), `invalid view schema file, miss host table schema for view 'tbl'`)
}

func TestDataWithoutSchema(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	dir := s.sourceDir
	p := filepath.Join(dir, "db.tbl.sql")
	err := os.WriteFile(p, nil, 0o644)
	require.NoError(t, err)

	mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	require.Equal(t, []*md.MDDatabaseMeta{{
		Name: "db",
		SchemaFile: md.FileInfo{
			TableName: filter.Table{
				Schema: "db",
				Name:   "",
			},
			FileMeta: md.SourceFileMeta{Type: md.SourceTypeSchemaSchema},
		},
		Tables: []*md.MDTableMeta{{
			DB:           "db",
			Name:         "tbl",
			SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "db", Name: "tbl"}},
			DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "db", Name: "tbl"}, FileMeta: md.SourceFileMeta{Path: "db.tbl.sql", Type: md.SourceTypeSQL}}},
			IsRowOrdered: true,
			IndexRatio:   0.0,
		}},
	}}, mdl.GetDatabases())
}

func TestTablesWithDots(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "db-schema-create.sql")
	s.touch(t, "db.tbl.with.dots-schema.sql")
	s.touch(t, "db.tbl.with.dots.0001.sql")
	s.touch(t, "db.0002-schema.sql")
	s.touch(t, "db.0002.sql")

	// insert some tables with file name structures which we're going to ignore.
	s.touch(t, "db.v-schema-trigger.sql")
	s.touch(t, "db.v-schema-post.sql")
	s.touch(t, "db.sql")
	s.touch(t, "db-schema.sql")

	mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	require.Equal(t, []*md.MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "db", Name: ""}, FileMeta: md.SourceFileMeta{Path: "db-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
		Tables: []*md.MDTableMeta{
			{
				DB:           "db",
				Name:         "0002",
				SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "db", Name: "0002"}, FileMeta: md.SourceFileMeta{Path: "db.0002-schema.sql", Type: md.SourceTypeTableSchema}},
				DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "db", Name: "0002"}, FileMeta: md.SourceFileMeta{Path: "db.0002.sql", Type: md.SourceTypeSQL}}},
				IsRowOrdered: true,
				IndexRatio:   0.0,
			},
			{
				DB:           "db",
				Name:         "tbl.with.dots",
				SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "db", Name: "tbl.with.dots"}, FileMeta: md.SourceFileMeta{Path: "db.tbl.with.dots-schema.sql", Type: md.SourceTypeTableSchema}},
				DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "db", Name: "tbl.with.dots"}, FileMeta: md.SourceFileMeta{Path: "db.tbl.with.dots.0001.sql", Type: md.SourceTypeSQL, SortKey: "0001"}}},
				IsRowOrdered: true,
				IndexRatio:   0.0,
			},
		},
	}}, mdl.GetDatabases())
}

func TestRouter(t *testing.T) {
	// route db and table but with some table not hit rules
	{
		s := newTestMydumpLoaderSuite(t)
		s.cfg.Routes = []*router.TableRule{
			{
				SchemaPattern: "a*",
				TablePattern:  "t*",
				TargetSchema:  "b",
				TargetTable:   "u",
			},
		}

		s.touch(t, "a0-schema-create.sql")
		s.touch(t, "a0.t0-schema.sql")
		s.touch(t, "a0.t0.1.sql")
		s.touch(t, "a0.t1-schema.sql")
		s.touch(t, "a0.t1.1.sql")

		s.touch(t, "a1-schema-create.sql")
		s.touch(t, "a1.s1-schema.sql")
		s.touch(t, "a1.s1.1.sql")
		s.touch(t, "a1.t2-schema.sql")
		s.touch(t, "a1.t2.1.sql")

		s.touch(t, "a1.v1-schema.sql")
		s.touch(t, "a1.v1-schema-view.sql")

		mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
		require.NoError(t, err)
		dbs := mdl.GetDatabases()
		// hit rules: a0.t0 -> b.u, a0.t1 -> b.0, a1.t2 -> b.u
		// not hit: a1.s1, a1.v1
		expectedDBS := []*md.MDDatabaseMeta{
			{
				Name:       "a0",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "a0", Name: ""}, FileMeta: md.SourceFileMeta{Path: "a0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
			},
			{
				Name:       "a1",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "a1", Name: ""}, FileMeta: md.SourceFileMeta{Path: "a1-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "a1",
						Name:         "s1",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "a1", Name: "s1"}, FileMeta: md.SourceFileMeta{Path: "a1.s1-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "a1", Name: "s1"}, FileMeta: md.SourceFileMeta{Path: "a1.s1.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
					{
						DB:           "a1",
						Name:         "v1",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "a1", Name: "v1"}, FileMeta: md.SourceFileMeta{Path: "a1.v1-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
				Views: []*md.MDTableMeta{
					{
						DB:           "a1",
						Name:         "v1",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "a1", Name: "v1"}, FileMeta: md.SourceFileMeta{Path: "a1.v1-schema-view.sql", Type: md.SourceTypeViewSchema}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
			{
				Name:       "b",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "b", Name: ""}, FileMeta: md.SourceFileMeta{Path: "a0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:         "b",
						Name:       "u",
						SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "b", Name: "u"}, FileMeta: md.SourceFileMeta{Path: "a0.t0-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles: []md.FileInfo{
							{TableName: filter.Table{Schema: "b", Name: "u"}, FileMeta: md.SourceFileMeta{Path: "a0.t0.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}},
							{TableName: filter.Table{Schema: "b", Name: "u"}, FileMeta: md.SourceFileMeta{Path: "a0.t1.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}},
							{TableName: filter.Table{Schema: "b", Name: "u"}, FileMeta: md.SourceFileMeta{Path: "a1.t2.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}},
						},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
		}
		require.Equal(t, expectedDBS, dbs)
	}

	// only route schema but with some db not hit rules
	{
		s := newTestMydumpLoaderSuite(t)
		s.cfg.Routes = []*router.TableRule{
			{
				SchemaPattern: "c*",
				TargetSchema:  "c",
			},
		}
		s.touch(t, "c0-schema-create.sql")
		s.touch(t, "c0.t3-schema.sql")
		s.touch(t, "c0.t3.1.sql")

		s.touch(t, "d0-schema-create.sql")
		mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
		require.NoError(t, err)
		dbs := mdl.GetDatabases()
		// hit rules: c0.t3 -> c.t3
		// not hit: d0
		expectedDBS := []*md.MDDatabaseMeta{
			{
				Name:       "d0",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "d0", Name: ""}, FileMeta: md.SourceFileMeta{Path: "d0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
			},
			{
				Name:       "c",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "c", Name: ""}, FileMeta: md.SourceFileMeta{Path: "c0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "c",
						Name:         "t3",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "c", Name: "t3"}, FileMeta: md.SourceFileMeta{Path: "c0.t3-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "c", Name: "t3"}, FileMeta: md.SourceFileMeta{Path: "c0.t3.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
		}
		require.Equal(t, expectedDBS, dbs)
	}

	// route schema and table but not have table data
	{
		s := newTestMydumpLoaderSuite(t)
		s.cfg.Routes = []*router.TableRule{
			{
				SchemaPattern: "e*",
				TablePattern:  "f*",
				TargetSchema:  "v",
				TargetTable:   "vv",
			},
		}
		s.touch(t, "e0-schema-create.sql")
		s.touch(t, "e0.f0-schema.sql")
		s.touch(t, "e0.f0-schema-view.sql")

		mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
		require.NoError(t, err)
		dbs := mdl.GetDatabases()
		// hit rules: e0.f0 -> v.vv
		expectedDBS := []*md.MDDatabaseMeta{
			{
				Name:       "e0",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "e0", Name: ""}, FileMeta: md.SourceFileMeta{Path: "e0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
			},
			{
				Name:       "v",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "v", Name: ""}, FileMeta: md.SourceFileMeta{Path: "e0-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "v",
						Name:         "vv",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "v", Name: "vv"}, FileMeta: md.SourceFileMeta{Path: "e0.f0-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
				Views: []*md.MDTableMeta{
					{
						DB:           "v",
						Name:         "vv",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "v", Name: "vv"}, FileMeta: md.SourceFileMeta{Path: "e0.f0-schema-view.sql", Type: md.SourceTypeViewSchema}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
		}
		require.Equal(t, expectedDBS, dbs)
	}

	// route by regex
	{
		s := newTestMydumpLoaderSuite(t)
		s.cfg.Routes = []*router.TableRule{
			{
				SchemaPattern: "~.*regexpr[1-9]+",
				TablePattern:  "~.*regexprtable",
				TargetSchema:  "downstream_db",
				TargetTable:   "downstream_table",
			},
			{
				SchemaPattern: "~.bdb.*",
				TargetSchema:  "db",
			},
		}

		s.touch(t, "test_regexpr1-schema-create.sql")
		s.touch(t, "test_regexpr1.test_regexprtable-schema.sql")
		s.touch(t, "test_regexpr1.test_regexprtable.1.sql")

		s.touch(t, "zbdb-schema-create.sql")
		s.touch(t, "zbdb.table-schema.sql")
		s.touch(t, "zbdb.table.1.sql")

		mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
		require.NoError(t, err)
		dbs := mdl.GetDatabases()
		// hit rules: test_regexpr1.test_regexprtable -> downstream_db.downstream_table, zbdb.table -> db.table
		expectedDBS := []*md.MDDatabaseMeta{
			{
				Name:       "test_regexpr1",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "test_regexpr1", Name: ""}, FileMeta: md.SourceFileMeta{Path: "test_regexpr1-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
			},
			{
				Name:       "db",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "db", Name: ""}, FileMeta: md.SourceFileMeta{Path: "zbdb-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "db",
						Name:         "table",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: md.SourceFileMeta{Path: "zbdb.table-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "db", Name: "table"}, FileMeta: md.SourceFileMeta{Path: "zbdb.table.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
			{
				Name:       "downstream_db",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "downstream_db", Name: ""}, FileMeta: md.SourceFileMeta{Path: "test_regexpr1-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "downstream_db",
						Name:         "downstream_table",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "downstream_db", Name: "downstream_table"}, FileMeta: md.SourceFileMeta{Path: "test_regexpr1.test_regexprtable-schema.sql", Type: md.SourceTypeTableSchema}},
						DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "downstream_db", Name: "downstream_table"}, FileMeta: md.SourceFileMeta{Path: "test_regexpr1.test_regexprtable.1.sql", Type: md.SourceTypeSQL, SortKey: "1"}}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
					},
				},
			},
		}
		require.Equal(t, expectedDBS, dbs)
	}

	// only route db and only route some tables
	{
		s := newTestMydumpLoaderSuite(t)
		s.cfg.Routes = []*router.TableRule{
			// only route schema
			{
				SchemaPattern: "web",
				TargetSchema:  "web_test",
			},
			// only route one table
			{
				SchemaPattern: "x",
				TablePattern:  "t1*",
				TargetSchema:  "x2",
				TargetTable:   "t",
			},
		}

		s.touch(t, "web-schema-create.sql")
		s.touch(t, "x-schema-create.sql")
		s.touch(t, "x.t10-schema.sql") // hit rules, new name is x2.t
		s.touch(t, "x.t20-schema.sql") // not hit rules, name is x.t20

		mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
		require.NoError(t, err)
		dbs := mdl.GetDatabases()
		// hit rules: web -> web_test, x.t10 -> x2.t
		// not hit: x.t20
		expectedDBS := []*md.MDDatabaseMeta{
			{
				Name:       "x",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "x", Name: ""}, FileMeta: md.SourceFileMeta{Path: "x-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "x",
						Name:         "t20",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "x", Name: "t20"}, FileMeta: md.SourceFileMeta{Path: "x.t20-schema.sql", Type: md.SourceTypeTableSchema}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
						DataFiles:    []md.FileInfo{},
					},
				},
			},
			{
				Name:       "web_test",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "web_test", Name: ""}, FileMeta: md.SourceFileMeta{Path: "web-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
			},
			{
				Name:       "x2",
				SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "x2", Name: ""}, FileMeta: md.SourceFileMeta{Path: "x-schema-create.sql", Type: md.SourceTypeSchemaSchema}},
				Tables: []*md.MDTableMeta{
					{
						DB:           "x2",
						Name:         "t",
						SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "x2", Name: "t"}, FileMeta: md.SourceFileMeta{Path: "x.t10-schema.sql", Type: md.SourceTypeTableSchema}},
						IndexRatio:   0.0,
						IsRowOrdered: true,
						DataFiles:    []md.FileInfo{},
					},
				},
			},
		}
		require.Equal(t, expectedDBS, dbs)
	}
}

func TestRoutesPanic(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)
	s.cfg.Routes = []*router.TableRule{
		{
			SchemaPattern: "test1",
			TargetSchema:  "test",
		},
	}

	s.touch(t, "test1.dump_test.001.sql")
	s.touch(t, "test1.dump_test.002.sql")
	s.touch(t, "test1.dump_test.003.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
}

func TestBadRouterRule(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.cfg.Routes = []*router.TableRule{{
		SchemaPattern: "a*b",
		TargetSchema:  "ab",
	}}

	s.touch(t, "a1b-schema-create.sql")

	_, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.Regexp(t, `.*pattern a\*b not valid`, err.Error())
}

func TestFileRouting(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.cfg.Mydumper.DefaultFileRules = false
	s.cfg.Mydumper.FileRouters = []*config.FileRouteRule{
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z0-9_]+)/schema\.sql$`,
			Schema:  "$1",
			Type:    "schema-schema",
		},
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z0-9]+)/([a-z0-9_]+)-table\.sql$`,
			Schema:  "$1",
			Table:   "$2",
			Type:    "table-schema",
		},
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z0-9]+)/([a-z0-9_]+)-view\.sql$`,
			Schema:  "$1",
			Table:   "$2",
			Type:    "view-schema",
		},
		{
			Pattern: `(?i)^(?:[^./]*/)*([a-z][a-z0-9_]*)/([a-z]+)[0-9]*(?:\.([0-9]+))?\.(sql|csv)$`,
			Schema:  "$1",
			Table:   "$2",
			Type:    "$4",
		},
		{
			Pattern: `^(?:[^./]*/)*([a-z]+)(?:\.([0-9]+))?\.(sql|csv)$`,
			Schema:  "d2",
			Table:   "$1",
			Type:    "$3",
		},
	}

	s.mkdir(t, "d1")
	s.mkdir(t, "d2")
	s.touch(t, "d1/schema.sql")
	s.touch(t, "d1/test-table.sql")
	s.touch(t, "d1/test0.sql")
	s.touch(t, "d1/test1.sql")
	s.touch(t, "d1/test2.001.sql")
	s.touch(t, "d1/v1-table.sql")
	s.touch(t, "d1/v1-view.sql")
	s.touch(t, "d1/t1-schema-create.sql")
	s.touch(t, "d2/schema.sql")
	s.touch(t, "d2/abc-table.sql")
	s.touch(t, "abc.1.sql")

	mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	require.Equal(t, []*md.MDDatabaseMeta{
		{
			Name:       "d1",
			SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "d1", Name: ""}, FileMeta: md.SourceFileMeta{Path: filepath.FromSlash("d1/schema.sql"), Type: md.SourceTypeSchemaSchema}},
			Tables: []*md.MDTableMeta{
				{
					DB:   "d1",
					Name: "test",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "d1", Name: "test"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/test-table.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles: []md.FileInfo{
						{
							TableName: filter.Table{Schema: "d1", Name: "test"},
							FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/test0.sql"), Type: md.SourceTypeSQL},
						},
						{
							TableName: filter.Table{Schema: "d1", Name: "test"},
							FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/test1.sql"), Type: md.SourceTypeSQL},
						},
						{
							TableName: filter.Table{Schema: "d1", Name: "test"},
							FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/test2.001.sql"), Type: md.SourceTypeSQL},
						},
					},
					IndexRatio:   0.0,
					IsRowOrdered: true,
				},
				{
					DB:   "d1",
					Name: "v1",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "d1", Name: "v1"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/v1-table.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{},
					IndexRatio:   0.0,
					IsRowOrdered: true,
				},
			},
			Views: []*md.MDTableMeta{
				{
					DB:   "d1",
					Name: "v1",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "d1", Name: "v1"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d1/v1-view.sql"), Type: md.SourceTypeViewSchema},
					},
					IndexRatio:   0.0,
					IsRowOrdered: true,
				},
			},
		},
		{
			Name:       "d2",
			SchemaFile: md.FileInfo{TableName: filter.Table{Schema: "d2", Name: ""}, FileMeta: md.SourceFileMeta{Path: filepath.FromSlash("d2/schema.sql"), Type: md.SourceTypeSchemaSchema}},
			Tables: []*md.MDTableMeta{
				{
					DB:   "d2",
					Name: "abc",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "d2", Name: "abc"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("d2/abc-table.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "d2", Name: "abc"}, FileMeta: md.SourceFileMeta{Path: "abc.1.sql", Type: md.SourceTypeSQL}}},
					IndexRatio:   0.0,
					IsRowOrdered: true,
				},
			},
		},
	}, mdl.GetDatabases())
}

func TestInputWithSpecialChars(t *testing.T) {
	/*
		Path/
			test-schema-create.sql
			test.t%22-schema.sql
			test.t%22.0.sql
			test.t%2522-schema.sql
			test.t%2522.0.csv
			test.t%gg-schema.sql
			test.t%gg.csv
			test.t+gg-schema.sql
			test.t+gg.csv

			db%22.t%2522-schema.sql
			db%22.t%2522.0.csv
	*/
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "test-schema-create.sql")
	s.touch(t, "test.t%22-schema.sql")
	s.touch(t, "test.t%22.sql")
	s.touch(t, "test.t%2522-schema.sql")
	s.touch(t, "test.t%2522.csv")
	s.touch(t, "test.t%gg-schema.sql")
	s.touch(t, "test.t%gg.csv")
	s.touch(t, "test.t+gg-schema.sql")
	s.touch(t, "test.t+gg.csv")

	s.touch(t, "db%22-schema-create.sql")
	s.touch(t, "db%22.t%2522-schema.sql")
	s.touch(t, "db%22.t%2522.0.csv")

	mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))
	require.NoError(t, err)
	require.Equal(t, []*md.MDDatabaseMeta{
		{
			Name:       `db"`,
			SchemaFile: md.FileInfo{TableName: filter.Table{Schema: `db"`, Name: ""}, FileMeta: md.SourceFileMeta{Path: filepath.FromSlash("db%22-schema-create.sql"), Type: md.SourceTypeSchemaSchema}},
			Tables: []*md.MDTableMeta{
				{
					DB:   `db"`,
					Name: "t%22",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: `db"`, Name: "t%22"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("db%22.t%2522-schema.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles: []md.FileInfo{
						{
							TableName: filter.Table{Schema: `db"`, Name: "t%22"},
							FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("db%22.t%2522.0.csv"), Type: md.SourceTypeCSV, SortKey: "0"},
						},
					},
					IndexRatio:   0,
					IsRowOrdered: true,
				},
			},
		},
		{
			Name:       "test",
			SchemaFile: md.FileInfo{TableName: filter.Table{Schema: `test`, Name: ""}, FileMeta: md.SourceFileMeta{Path: filepath.FromSlash("test-schema-create.sql"), Type: md.SourceTypeSchemaSchema}},
			Tables: []*md.MDTableMeta{
				{
					DB:   "test",
					Name: `t"`,
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "test", Name: `t"`},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("test.t%22-schema.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "test", Name: `t"`}, FileMeta: md.SourceFileMeta{Path: "test.t%22.sql", Type: md.SourceTypeSQL}}},
					IndexRatio:   0,
					IsRowOrdered: true,
				},
				{
					DB:   "test",
					Name: "t%22",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "test", Name: "t%22"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("test.t%2522-schema.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "test", Name: "t%22"}, FileMeta: md.SourceFileMeta{Path: "test.t%2522.csv", Type: md.SourceTypeCSV}}},
					IndexRatio:   0,
					IsRowOrdered: true,
				},
				{
					DB:   "test",
					Name: "t%gg",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "test", Name: "t%gg"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("test.t%gg-schema.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "test", Name: "t%gg"}, FileMeta: md.SourceFileMeta{Path: "test.t%gg.csv", Type: md.SourceTypeCSV}}},
					IndexRatio:   0,
					IsRowOrdered: true,
				},
				{
					DB:   "test",
					Name: "t+gg",
					SchemaFile: md.FileInfo{
						TableName: filter.Table{Schema: "test", Name: "t+gg"},
						FileMeta:  md.SourceFileMeta{Path: filepath.FromSlash("test.t+gg-schema.sql"), Type: md.SourceTypeTableSchema},
					},
					DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "test", Name: "t+gg"}, FileMeta: md.SourceFileMeta{Path: "test.t+gg.csv", Type: md.SourceTypeCSV}}},
					IndexRatio:   0,
					IsRowOrdered: true,
				},
			},
		},
	}, mdl.GetDatabases())
}

func TestMaxScanFilesOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memStore := storage.NewMemStorage()
	require.NoError(t, memStore.WriteFile(ctx, "/test-src/db1.tbl1-schema.sql",
		[]byte("CREATE TABLE db1.tbl1 ( id INTEGER, val VARCHAR(255) );"),
	))
	require.NoError(t, memStore.WriteFile(ctx, "/test-src/db1-schema-create.sql",
		[]byte("CREATE DATABASE db1;"),
	))
	const dataFilesCount = 200
	maxScanFilesCount := 500
	for i := 0; i < dataFilesCount; i++ {
		require.NoError(t, memStore.WriteFile(ctx, fmt.Sprintf("/test-src/db1.tbl1.%d.sql", i),
			[]byte(fmt.Sprintf("INSERT INTO db1.tbl1 (id, val) VALUES (%d, 'aaa%d');", i, i)),
		))
	}
	cfg := newConfigWithSourceDir("/test-src")

	mdl, err := md.NewLoaderWithStore(ctx, md.NewLoaderCfg(cfg), memStore)
	require.NoError(t, err)
	require.NotNil(t, mdl)
	dbMetas := mdl.GetDatabases()
	require.Equal(t, 1, len(dbMetas))
	dbMeta := dbMetas[0]
	require.Equal(t, 1, len(dbMeta.Tables))
	tbl := dbMeta.Tables[0]
	require.Equal(t, dataFilesCount, len(tbl.DataFiles))

	mdl, err = md.NewLoaderWithStore(ctx, md.NewLoaderCfg(cfg), memStore,
		md.WithMaxScanFiles(maxScanFilesCount),
	)
	require.NoError(t, err)
	require.NotNil(t, mdl)
	dbMetas = mdl.GetDatabases()
	require.Equal(t, 1, len(dbMetas))
	dbMeta = dbMetas[0]
	require.Equal(t, 1, len(dbMeta.Tables))
	tbl = dbMeta.Tables[0]
	require.Equal(t, dataFilesCount, len(tbl.DataFiles))

	maxScanFilesCount = 100
	mdl, err = md.NewLoaderWithStore(ctx, md.NewLoaderCfg(cfg), memStore,
		md.WithMaxScanFiles(maxScanFilesCount),
	)
	require.EqualError(t, err, common.ErrTooManySourceFiles.Error())
	require.NotNil(t, mdl)
	dbMetas = mdl.GetDatabases()
	require.Equal(t, 1, len(dbMetas))
	dbMeta = dbMetas[0]
	require.Equal(t, 1, len(dbMeta.Tables))
	tbl = dbMeta.Tables[0]
	require.Equal(t, maxScanFilesCount-2, len(tbl.DataFiles))
}

func TestExternalDataRoutes(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)

	s.touch(t, "test_1-schema-create.sql")
	s.touch(t, "test_1.t1-schema.sql")
	s.touch(t, "test_1.t1.sql")
	s.touch(t, "test_2-schema-create.sql")
	s.touch(t, "test_2.t2-schema.sql")
	s.touch(t, "test_2.t2.sql")
	s.touch(t, "test_3-schema-create.sql")
	s.touch(t, "test_3.t1-schema.sql")
	s.touch(t, "test_3.t1.sql")
	s.touch(t, "test_3.t3-schema.sql")
	s.touch(t, "test_3.t3.sql")

	s.cfg.Mydumper.SourceID = "mysql-01"
	s.cfg.Routes = []*router.TableRule{
		{
			TableExtractor: &router.TableExtractor{
				TargetColumn: "c_table",
				TableRegexp:  "t(.*)",
			},
			SchemaExtractor: &router.SchemaExtractor{
				TargetColumn: "c_schema",
				SchemaRegexp: "test_(.*)",
			},
			SourceExtractor: &router.SourceExtractor{
				TargetColumn: "c_source",
				SourceRegexp: "mysql-(.*)",
			},
			SchemaPattern: "test_*",
			TablePattern:  "t*",
			TargetSchema:  "test",
			TargetTable:   "t",
		},
	}

	mdl, err := md.NewLoader(context.Background(), md.NewLoaderCfg(s.cfg))

	require.NoError(t, err)
	var database *md.MDDatabaseMeta
	for _, db := range mdl.GetDatabases() {
		if db.Name == "test" {
			require.Nil(t, database)
			database = db
		}
	}
	require.NotNil(t, database)
	require.Len(t, database.Tables, 1)
	require.Len(t, database.Tables[0].DataFiles, 4)
	expectExtendCols := []string{"c_table", "c_schema", "c_source"}
	expectedExtendVals := [][]string{
		{"1", "1", "01"},
		{"2", "2", "01"},
		{"1", "3", "01"},
		{"3", "3", "01"},
	}
	for i, fileInfo := range database.Tables[0].DataFiles {
		require.Equal(t, expectExtendCols, fileInfo.FileMeta.ExtendData.Columns)
		require.Equal(t, expectedExtendVals[i], fileInfo.FileMeta.ExtendData.Values)
	}
}

func TestSampleFileCompressRatio(t *testing.T) {
	s := newTestMydumpLoaderSuite(t)
	store, err := storage.NewLocalStorage(s.sourceDir)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	byteArray := make([]byte, 0, 4096)
	bf := bytes.NewBuffer(byteArray)
	compressWriter := gzip.NewWriter(bf)
	csvData := []byte("aaaa\n")
	for i := 0; i < 1000; i++ {
		_, err = compressWriter.Write(csvData)
		require.NoError(t, err)
	}
	err = compressWriter.Flush()
	require.NoError(t, err)

	fileName := "test_1.t1.csv.gz"
	err = store.WriteFile(ctx, fileName, bf.Bytes())
	require.NoError(t, err)

	ratio, err := md.SampleFileCompressRatio(ctx, md.SourceFileMeta{
		Path:        fileName,
		Compression: md.CompressionGZ,
	}, store)
	require.NoError(t, err)
	require.InDelta(t, ratio, 5000.0/float64(bf.Len()), 1e-5)
}

func testSampleParquetDataSize(t *testing.T, count int) {
	s := newTestMydumpLoaderSuite(t)
	store, err := storage.NewLocalStorage(s.sourceDir)
	require.NoError(t, err)

	type row struct {
		ID    int64  `parquet:"name=id, type=INT64"`
		Key   string `parquet:"name=key, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
		Value string `parquet:"name=value, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	byteArray := make([]byte, 0, 40*1024)
	bf := bytes.NewBuffer(byteArray)
	pwriter, err := writer.NewParquetWriterFromWriter(bf, new(row), 4)
	require.NoError(t, err)
	pwriter.RowGroupSize = 128 * 1024 * 1024 //128M
	pwriter.PageSize = 8 * 1024              //8K
	pwriter.CompressionType = parquet.CompressionCodec_SNAPPY
	seed := time.Now().Unix()
	t.Logf("seed: %d. To reproduce the random behaviour, manually set `rand.New(rand.NewSource(seed))`", seed)
	rnd := rand.New(rand.NewSource(seed))
	totalRowSize := 0
	for i := 0; i < count; i++ {
		kl := rnd.Intn(20) + 1
		key := make([]byte, kl)
		kl, err = rnd.Read(key)
		require.NoError(t, err)
		vl := rnd.Intn(20) + 1
		value := make([]byte, vl)
		vl, err = rnd.Read(value)
		require.NoError(t, err)

		totalRowSize += kl + vl + 8
		row := row{
			ID:    int64(i),
			Key:   string(key[:kl]),
			Value: string(value[:vl]),
		}
		err = pwriter.Write(row)
		require.NoError(t, err)
	}
	err = pwriter.WriteStop()
	require.NoError(t, err)

	fileName := "test_1.t1.parquet"
	err = store.WriteFile(ctx, fileName, bf.Bytes())
	require.NoError(t, err)

	size, err := md.SampleParquetDataSize(ctx, md.SourceFileMeta{
		Path: fileName,
	}, store)
	require.NoError(t, err)
	// expected error within 10%, so delta = totalRowSize / 10
	require.InDelta(t, totalRowSize, size, float64(totalRowSize)/10)
}

func TestSampleParquetDataSize(t *testing.T) {
	t.Run("count=1000", func(t *testing.T) { testSampleParquetDataSize(t, 1000) })
	t.Run("count=0", func(t *testing.T) { testSampleParquetDataSize(t, 0) })
}

func TestSetupOptions(t *testing.T) {
	// those functions are only used in other components, add this to avoid they
	// be deleted mistakenly.
	_ = md.WithMaxScanFiles
	_ = md.ReturnPartialResultOnError
	_ = md.WithFileIterator
}
