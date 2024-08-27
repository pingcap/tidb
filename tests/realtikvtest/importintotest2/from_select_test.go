// Copyright 2024 PingCAP, Inc.
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

package importintotest

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func (s *mockGCSSuite) TestImportFromSelectBasic() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(id int, v varchar(64))")
	s.tk.MustExec("create table dst(id int, v varchar(64))")
	s.tk.MustExec("insert into src values(4, 'aaaaaa'), (5, 'bbbbbb'), (6, 'cccccc'), (7, 'dddddd')")

	s.ErrorIs(s.tk.ExecToErr(`import into dst FROM select id from src`), plannererrors.ErrWrongValueCountOnRow)
	s.ErrorIs(s.tk.ExecToErr(`import into dst(id) FROM select * from src`), plannererrors.ErrWrongValueCountOnRow)

	s.tk.MustExec(`import into dst FROM select * from src`)
	s.Equal(uint64(4), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), "Records: 4,")
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("4 aaaaaa", "5 bbbbbb", "6 cccccc", "7 dddddd"))

	// non-empty table
	s.ErrorContains(s.tk.ExecToErr(`import into dst FROM select * from src`), "target table is not empty")

	// with where
	s.tk.MustExec("truncate table dst")
	s.tk.MustExec(`import into dst FROM select * from src where id > 5`)
	s.Equal(uint64(2), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), "Records: 2,")
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("6 cccccc", "7 dddddd"))

	// parallel
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(8)`)
	s.tk.MustExec("truncate table src")
	s.tk.MustExec("truncate table dst")
	var count = 5000
	values := make([]string, 0, count)
	queryResult := make([]string, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, fmt.Sprintf("(%d, 'abc-%d')", i, i))
		queryResult = append(queryResult, fmt.Sprintf("%d abc-%d", i, i))
	}
	slices.Sort(queryResult)
	s.tk.MustExec("insert into src values " + strings.Join(values, ","))
	s.tk.MustExec(`import into dst FROM select * from src with thread = 8`)
	s.Equal(uint64(count), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), fmt.Sprintf("Records: %d,", count))
	s.tk.MustQuery("select * from dst").Sort().Check(testkit.Rows(queryResult...))
}

func (s *mockGCSSuite) TestImportFromSelectColumnList() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(id int, a varchar(64))")
	s.tk.MustExec("create table dst(id int auto_increment primary key, a varchar(64), b int default 10, c int)")
	s.tk.MustExec("insert into src values(4, 'aaaaaa'), (5, 'bbbbbb'), (6, 'cccccc'), (7, 'dddddd')")
	s.tk.MustExec(`import into dst(c, a) FROM select * from src order by id`)
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 aaaaaa 10 4", "2 bbbbbb 10 5", "3 cccccc 10 6", "4 dddddd 10 7"))

	s.tk.MustExec("truncate table dst")
	s.tk.MustExec("create table src2(id int, a varchar(64))")
	s.tk.MustExec("insert into src2 values(4, 'four'), (5, 'five')")
	s.tk.MustExec(`import into dst(c, a) FROM select y.id, y.a from src x join src2 y on x.id = y.id order by y.id`)
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 four 10 4", "2 five 10 5"))
}

func (s *mockGCSSuite) TestWriteAfterImportFromSelect() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table dt(id int, v varchar(64))")
	s.tk.MustExec("insert into dt values(4, 'aaaaaa'), (5, 'bbbbbb'), (6, 'cccccc'), (7, 'dddddd')")
	s.testWriteAfterImport(`import into t FROM select * from from_select.dt`, importer.DataSourceTypeQuery)
}

func (s *mockGCSSuite) TestImportFromSelectStaleRead() {
	s.prepareAndUseDB("from_select")
	// set tidb_snapshot might fail without this, not familiar about this part.
	s.tk.MustExec(`replace into mysql.tidb(variable_name, variable_value) values ('tikv_gc_safe_point', '20240131-00:00:00.000 +0800')`)
	s.tk.MustExec("create table src(id int, v varchar(64))")
	s.tk.MustExec("insert into src values(1, 'a')")
	time.Sleep(100 * time.Millisecond)
	now := s.tk.MustQuery("select now(6)").Rows()[0][0].(string)
	time.Sleep(100 * time.Millisecond)
	s.tk.MustExec("insert into src values(2, 'b')")
	s.tk.MustQuery("select * from src").Check(testkit.Rows("1 a", "2 b"))
	staleReadSQL := fmt.Sprintf("select * from src as of timestamp '%s'", now)
	s.tk.MustQuery(staleReadSQL).Check(testkit.Rows("1 a"))
	s.tk.MustExec("create table dst(id int, v varchar(64))")

	//
	// in below cases, dst table not exists at time 'now'
	//
	// using set tidb_snapshot
	s.tk.MustExec("set tidb_snapshot = '" + now + "'")
	s.ErrorIs(s.tk.ExecToErr("import into dst from "+staleReadSQL), infoschema.ErrTableNotExists)
	s.ErrorIs(s.tk.ExecToErr("import into dst from select * from src"), infoschema.ErrTableNotExists)
	// using AS OF TIMESTAMP
	s.tk.MustExec("set tidb_snapshot = ''")
	s.tk.MustExec("import into dst from " + staleReadSQL)
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 a"))

	//
	// in below cases, table exists at time 'now', and it's the latest version too.
	//
	s.tk.MustExec("truncate table dst")
	time.Sleep(100 * time.Millisecond)
	now = s.tk.MustQuery("select now(6)").Rows()[0][0].(string)
	time.Sleep(100 * time.Millisecond)
	staleReadSQL = fmt.Sprintf("select * from src as of timestamp '%s'", now)
	s.tk.MustExec("insert into src values(3, 'c')")
	s.tk.MustQuery("select * from src").Check(testkit.Rows("1 a", "2 b", "3 c"))
	// using set tidb_snapshot
	s.tk.MustExec("set tidb_snapshot = '" + now + "'")
	s.ErrorContains(s.tk.ExecToErr("import into dst from "+staleReadSQL),
		"can not execute write statement when 'tidb_snapshot' is set")
	s.ErrorContains(s.tk.ExecToErr("import into dst from select * from src"),
		"can not execute write statement when 'tidb_snapshot' is set")
	// using AS OF TIMESTAMP
	s.tk.MustExec("set tidb_snapshot = ''")
	s.tk.MustExec("import into dst from " + staleReadSQL)
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 a", "2 b"))

	//
	// in below cases, table exists at time 'now', and it's NOT the latest version.
	//
	s.tk.MustExec("truncate table dst")
	// using set tidb_snapshot
	s.tk.MustExec("set tidb_snapshot = '" + now + "'")
	s.ErrorContains(s.tk.ExecToErr("import into dst from "+staleReadSQL),
		"can not execute IMPORT statement when 'tidb_snapshot' is set")
	s.ErrorContains(s.tk.ExecToErr("import into dst from select * from src"),
		"can not execute IMPORT statement when 'tidb_snapshot' is set")
	// using AS OF TIMESTAMP
	s.tk.MustExec("set tidb_snapshot = ''")
	s.tk.MustExec("import into dst from " + staleReadSQL)
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 a", "2 b"))
}
