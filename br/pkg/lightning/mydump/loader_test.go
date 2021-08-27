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
	"context"
	"os"
	"path/filepath"
	"testing"

	. "github.com/pingcap/check"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	md "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var _ = Suite(&testMydumpLoaderSuite{})

func TestMydumps(t *testing.T) {
	TestingT(t)
}

type testMydumpLoaderSuite struct {
	cfg       *config.Config
	sourceDir string
}

func (s *testMydumpLoaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpLoaderSuite) TearDownSuite(c *C) {}

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

func (s *testMydumpLoaderSuite) SetUpTest(c *C) {
	s.sourceDir = c.MkDir()
	s.cfg = newConfigWithSourceDir(s.sourceDir)
}

func (s *testMydumpLoaderSuite) touch(c *C, filename ...string) {
	components := make([]string, len(filename)+1)
	components = append(components, s.sourceDir)
	components = append(components, filename...)
	path := filepath.Join(components...)
	err := os.WriteFile(path, nil, 0o644)
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) mkdir(c *C, dirname string) {
	path := filepath.Join(s.sourceDir, dirname)
	err := os.Mkdir(path, 0o755)
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestLoader(c *C) {
	ctx := context.Background()
	cfg := newConfigWithSourceDir("./not-exists")
	_, err := md.NewMyDumpLoader(ctx, cfg)
	// will check schema in tidb and data file later in DataCheck.
	c.Assert(err, IsNil)

	cfg = newConfigWithSourceDir("./examples")
	mdl, err := md.NewMyDumpLoader(ctx, cfg)
	c.Assert(err, IsNil)

	dbMetas := mdl.GetDatabases()
	c.Assert(len(dbMetas), Equals, 1)
	dbMeta := dbMetas[0]
	c.Assert(dbMeta.Name, Equals, "mocker_test")
	c.Assert(len(dbMeta.Tables), Equals, 4)

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
		c.Assert(dbMeta.Tables[i].Name, Equals, table.name)
		c.Assert(len(dbMeta.Tables[i].DataFiles), Equals, table.dataFiles)
	}
}

func (s *testMydumpLoaderSuite) TestEmptyDB(c *C) {
	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	// will check schema in tidb and data file later in DataCheck.
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestDuplicatedDB(c *C) {
	/*
		Path/
			a/
				db-schema-create.sql
			b/
				db-schema-create.sql
	*/
	s.mkdir(c, "a")
	s.touch(c, "a", "db-schema-create.sql")
	s.mkdir(c, "b")
	s.touch(c, "b", "db-schema-create.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid database schema file, duplicated item - .*[/\\]db-schema-create\.sql`)
}

func (s *testMydumpLoaderSuite) TestTableNoHostDB(c *C) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl-schema.sql
	*/

	dir := s.sourceDir
	err := os.WriteFile(filepath.Join(dir, "notdb-schema-create.sql"), nil, 0o644)
	c.Assert(err, IsNil)
	err = os.WriteFile(filepath.Join(dir, "db.tbl-schema.sql"), nil, 0o644)
	c.Assert(err, IsNil)

	_, err = md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestDuplicatedTable(c *C) {
	/*
		Path/
			db-schema-create.sql
			a/
				db.tbl-schema.sql
			b/
				db.tbl-schema.sql
	*/

	s.touch(c, "db-schema-create.sql")
	s.mkdir(c, "a")
	s.touch(c, "a", "db.tbl-schema.sql")
	s.mkdir(c, "b")
	s.touch(c, "b", "db.tbl-schema.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid table schema file, duplicated item - .*db\.tbl-schema\.sql`)
}

func (s *testMydumpLoaderSuite) TestTableInfoNotFound(c *C) {
	s.cfg.Mydumper.CharacterSet = "auto"

	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl-schema.sql")

	ctx := context.Background()
	store, err := storage.NewLocalStorage(s.sourceDir)
	c.Assert(err, IsNil)

	loader, err := md.NewMyDumpLoader(ctx, s.cfg)
	c.Assert(err, IsNil)
	for _, dbMeta := range loader.GetDatabases() {
		for _, tblMeta := range dbMeta.Tables {
			sql, err := tblMeta.GetSchema(ctx, store)
			c.Assert(sql, Equals, "")
			c.Assert(err, IsNil)
		}
	}
}

func (s *testMydumpLoaderSuite) TestTableUnexpectedError(c *C) {
	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl-schema.sql")

	ctx := context.Background()
	store, err := storage.NewLocalStorage(s.sourceDir)
	c.Assert(err, IsNil)

	loader, err := md.NewMyDumpLoader(ctx, s.cfg)
	c.Assert(err, IsNil)
	for _, dbMeta := range loader.GetDatabases() {
		for _, tblMeta := range dbMeta.Tables {
			sql, err := tblMeta.GetSchema(ctx, store)
			c.Assert(sql, Equals, "")
			c.Assert(err, ErrorMatches, "failed to decode db.tbl-schema.sql as : Unsupported encoding ")
		}
	}
}

func (s *testMydumpLoaderSuite) TestDataNoHostDB(c *C) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "notdb-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	// will check schema in tidb and data file later in DataCheck.
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestDataNoHostTable(c *C) {
	/*
		Path/
			db-schema-create.sql
			db.tbl.sql
	*/

	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	// will check schema in tidb and data file later in DataCheck.
	c.Assert(err, IsNil)
}

func (s *testMydumpLoaderSuite) TestViewNoHostDB(c *C) {
	/*
		Path/
			notdb-schema-create.sql
			db.tbl-schema-view.sql
	*/
	s.touch(c, "notdb-schema-create.sql")
	s.touch(c, "db.tbl-schema-view.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid view schema file, miss host table schema for view 'tbl'`)
}

func (s *testMydumpLoaderSuite) TestViewNoHostTable(c *C) {
	/*
		Path/
			db-schema-create.sql
			db.tbl-schema-view.sql
	*/

	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl-schema-view.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `invalid view schema file, miss host table schema for view 'tbl'`)
}

func (s *testMydumpLoaderSuite) TestDataWithoutSchema(c *C) {
	dir := s.sourceDir
	p := filepath.Join(dir, "db.tbl.sql")
	err := os.WriteFile(p, nil, 0o644)
	c.Assert(err, IsNil)

	mdl, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)
	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: "",
		Tables: []*md.MDTableMeta{{
			DB:           "db",
			Name:         "tbl",
			SchemaFile:   md.FileInfo{TableName: filter.Table{Schema: "db", Name: "tbl"}},
			DataFiles:    []md.FileInfo{{TableName: filter.Table{Schema: "db", Name: "tbl"}, FileMeta: md.SourceFileMeta{Path: "db.tbl.sql", Type: md.SourceTypeSQL}}},
			IsRowOrdered: true,
			IndexRatio:   0.0,
		}},
	}})
}

func (s *testMydumpLoaderSuite) TestTablesWithDots(c *C) {
	s.touch(c, "db-schema-create.sql")
	s.touch(c, "db.tbl.with.dots-schema.sql")
	s.touch(c, "db.tbl.with.dots.0001.sql")
	s.touch(c, "db.0002-schema.sql")
	s.touch(c, "db.0002.sql")

	// insert some tables with file name structures which we're going to ignore.
	s.touch(c, "db.v-schema-trigger.sql")
	s.touch(c, "db.v-schema-post.sql")
	s.touch(c, "db.sql")
	s.touch(c, "db-schema.sql")

	mdl, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)
	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{{
		Name:       "db",
		SchemaFile: "db-schema-create.sql",
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
	}})
}

func (s *testMydumpLoaderSuite) TestRouter(c *C) {
	s.cfg.Routes = []*router.TableRule{
		{
			SchemaPattern: "a*",
			TablePattern:  "t*",
			TargetSchema:  "b",
			TargetTable:   "u",
		},
		{
			SchemaPattern: "c*",
			TargetSchema:  "c",
		},
		{
			SchemaPattern: "e*",
			TablePattern:  "f*",
			TargetSchema:  "v",
			TargetTable:   "vv",
		},
	}

	/*
		Path/
			a0-schema-create.sql
			a0.t0-schema.sql
			a0.t0.1.sql
			a0.t1-schema.sql
			a0.t1.1.sql
			a1-schema-create.sql
			a1.s1-schema.sql
			a1.s1.1.schema.sql
			a1.t2-schema.sql
			a1.t2.1.sql
			a1.v1-schema.sql
			a1.v1-schema-view.sql
			c0-schema-create.sql
			c0.t3-schema.sql
			c0.t3.1.sql
			d0-schema-create.sql
			e0-schema-create.sql
			e0.f0-schema.sql
			e0.f0-schema-view.sql
	*/

	s.touch(c, "a0-schema-create.sql")
	s.touch(c, "a0.t0-schema.sql")
	s.touch(c, "a0.t0.1.sql")
	s.touch(c, "a0.t1-schema.sql")
	s.touch(c, "a0.t1.1.sql")

	s.touch(c, "a1-schema-create.sql")
	s.touch(c, "a1.s1-schema.sql")
	s.touch(c, "a1.s1.1.sql")
	s.touch(c, "a1.t2-schema.sql")
	s.touch(c, "a1.t2.1.sql")
	s.touch(c, "a1.v1-schema.sql")
	s.touch(c, "a1.v1-schema-view.sql")

	s.touch(c, "c0-schema-create.sql")
	s.touch(c, "c0.t3-schema.sql")
	s.touch(c, "c0.t3.1.sql")

	s.touch(c, "d0-schema-create.sql")

	s.touch(c, "e0-schema-create.sql")
	s.touch(c, "e0.f0-schema.sql")
	s.touch(c, "e0.f0-schema-view.sql")

	mdl, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)
	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{
		{
			Name:       "a1",
			SchemaFile: "a1-schema-create.sql",
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
			Name:       "d0",
			SchemaFile: "d0-schema-create.sql",
		},
		{
			Name:       "b",
			SchemaFile: "a0-schema-create.sql",
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
		{
			Name:       "c",
			SchemaFile: "c0-schema-create.sql",
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
		{
			Name:       "v",
			SchemaFile: "e0-schema-create.sql",
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
	})
}

func (s *testMydumpLoaderSuite) TestBadRouterRule(c *C) {
	s.cfg.Routes = []*router.TableRule{{
		SchemaPattern: "a*b",
		TargetSchema:  "ab",
	}}

	s.touch(c, "a1b-schema-create.sql")

	_, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, ErrorMatches, `.*pattern a\*b not valid`)
}

func (s *testMydumpLoaderSuite) TestFileRouting(c *C) {
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

	s.mkdir(c, "d1")
	s.mkdir(c, "d2")
	s.touch(c, "d1/schema.sql")
	s.touch(c, "d1/test-table.sql")
	s.touch(c, "d1/test0.sql")
	s.touch(c, "d1/test1.sql")
	s.touch(c, "d1/test2.001.sql")
	s.touch(c, "d1/v1-table.sql")
	s.touch(c, "d1/v1-view.sql")
	s.touch(c, "d1/t1-schema-create.sql")
	s.touch(c, "d2/schema.sql")
	s.touch(c, "d2/abc-table.sql")
	s.touch(c, "abc.1.sql")

	mdl, err := md.NewMyDumpLoader(context.Background(), s.cfg)
	c.Assert(err, IsNil)
	c.Assert(mdl.GetDatabases(), DeepEquals, []*md.MDDatabaseMeta{
		{
			Name:       "d1",
			SchemaFile: filepath.FromSlash("d1/schema.sql"),
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
			SchemaFile: filepath.FromSlash("d2/schema.sql"),
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
	})
}
