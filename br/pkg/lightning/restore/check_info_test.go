// Copyright 2021 PingCAP, Inc.
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

package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	tmock "github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&checkInfoSuite{})

type checkInfoSuite struct{}

const passed CheckType = "pass"

func (s *checkInfoSuite) TestCheckCSVHeader(c *C) {
	dir := c.MkDir()
	ctx := context.Background()
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	type tableSource struct {
		Name    string
		SQL     string
		Sources []string
	}

	cases := []struct {
		ignoreColumns []*config.IgnoreColumns
		// empty msg means check pass
		level   CheckType
		Sources map[string][]*tableSource
	}{

		{
			nil,

			passed,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"aa,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			passed,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"aa,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\n",
						},
					},
				},
			},
		},
		{
			nil,

			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
				},
			},
		},
		{
			nil,

			Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, should still be warn
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, but has other unique key
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`), unique key uk (`b`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// ignore primary key, non other unique key
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(16), b varchar(8), PRIMARY KEY (`a`), KEY idx_b (`b`))",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// non unique key, but data type inconsistent
		{
			nil,
			Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// non unique key, but ignore inconsistent field
		{
			[]*config.IgnoreColumns{
				{
					DB:      "db",
					Table:   "tbl1",
					Columns: []string{"a"},
				},
			},
			Warn,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		// multiple tables, test the choose priority
		{
			nil,
			Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(8), b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
					{
						"tbl2",
						"create table tbl1 (a varchar(8) primary key, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
		{
			nil,
			Critical,
			map[string][]*tableSource{
				"db": {
					{
						"tbl1",
						"create table tbl1 (a varchar(8), b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
						},
					},
				},
				"db2": {
					{
						"tbl2",
						"create table tbl1 (a bigint, b varchar(8));",
						[]string{
							"a,b\r\ntest1,test2\r\n",
							"a,b\r\ntest3,test4\r\n",
						},
					},
				},
			},
		},
	}

	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize: config.ReadBlockSize,
			CSV: config.CSVConfig{
				Separator:       ",",
				Delimiter:       `"`,
				Header:          false,
				NotNull:         false,
				Null:            `\N`,
				BackslashEscape: true,
				TrimLastSep:     false,
			},
		},
	}
	rc := &Controller{
		cfg:       cfg,
		store:     mockStore,
		ioWorkers: worker.NewPool(context.Background(), 1, "io"),
	}

	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	se := tmock.NewContext()

	for _, ca := range cases {
		rc.checkTemplate = NewSimpleTemplate()
		cfg.Mydumper.IgnoreColumns = ca.ignoreColumns
		rc.dbInfos = make(map[string]*checkpoints.TidbDBInfo)

		dbMetas := make([]*mydump.MDDatabaseMeta, 0)
		for db, tbls := range ca.Sources {
			tblMetas := make([]*mydump.MDTableMeta, 0, len(tbls))
			dbInfo := &checkpoints.TidbDBInfo{
				Name:   db,
				Tables: make(map[string]*checkpoints.TidbTableInfo),
			}
			rc.dbInfos[db] = dbInfo

			for _, tbl := range tbls {
				node, err := p.ParseOneStmt(tbl.SQL, "", "")
				c.Assert(err, IsNil)
				core, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 0xabcdef)
				c.Assert(err, IsNil)
				core.State = model.StatePublic
				dbInfo.Tables[tbl.Name] = &checkpoints.TidbTableInfo{
					ID:   core.ID,
					DB:   db,
					Name: tbl.Name,
					Core: core,
				}

				fileInfos := make([]mydump.FileInfo, 0, len(tbl.Sources))
				for i, s := range tbl.Sources {
					fileName := fmt.Sprintf("%s.%s.%d.csv", db, tbl.Name, i)
					err = os.WriteFile(filepath.Join(dir, fileName), []byte(s), 0o644)
					c.Assert(err, IsNil)
					fileInfos = append(fileInfos, mydump.FileInfo{
						FileMeta: mydump.SourceFileMeta{
							Path:     fileName,
							Type:     mydump.SourceTypeCSV,
							FileSize: int64(len(s)),
						},
					})
				}
				tblMetas = append(tblMetas, &mydump.MDTableMeta{
					DB:        db,
					Name:      tbl.Name,
					DataFiles: fileInfos,
				})
			}
			dbMetas = append(dbMetas, &mydump.MDDatabaseMeta{
				Name:   db,
				Tables: tblMetas,
			})
		}

		err := rc.checkCSVHeader(ctx, dbMetas)
		c.Assert(err, IsNil)
		if ca.level != passed {
			c.Assert(rc.checkTemplate.FailedCount(ca.level), Equals, 1)
		}
	}

}
