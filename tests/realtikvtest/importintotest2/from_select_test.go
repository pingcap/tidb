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
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
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

	// append into non-empty table
	s.tk.MustExec(`import into dst FROM select * from src`)
	s.Equal(uint64(4), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), "Records: 4,")
	s.tk.MustQuery("select id, v, count(*) from dst group by id, v order by id").Check(
		testkit.Rows("4 aaaaaa 2", "5 bbbbbb 2", "6 cccccc 2", "7 dddddd 2"),
	)

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

	s.tk.MustExec("create table t(id varchar(100));")
	s.tk.MustExec("import into t from select 1;")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1"))
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

func (s *mockGCSSuite) TestImportFromSelectAutoIncrementWithHiddenRowID() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("set @@auto_increment_increment = 66")
	s.tk.MustExec("set @@auto_increment_offset = 2")
	defer func() {
		s.tk.MustExec("set @@auto_increment_increment = 1")
		s.tk.MustExec("set @@auto_increment_offset = 1")
	}()

	s.tk.MustExec("create table src(v int)")
	s.tk.MustExec("insert into src values(10), (20), (30)")
	s.tk.MustExec("create table dst(id int auto_increment primary key nonclustered, v int)")
	s.tk.MustExec("insert into dst(v) values(0)")

	s.tk.MustExec("import into dst(v) from select v from src order by v with thread = 1")
	s.tk.MustQuery("select v from dst order by v").Check(testkit.Rows("0", "10", "20", "30"))
	s.tk.MustQuery("select count(*), count(distinct _tidb_rowid), count(distinct id) from dst").
		Check(testkit.Rows("4 4 4"))
	s.tk.MustQuery("select count(*) from dst where (id - 2) % 66 = 0").Check(testkit.Rows("4"))
	s.tk.MustQuery("select count(*) from dst where v in (10, 20, 30) and id != _tidb_rowid").
		Check(testkit.Rows("3"))

	s.tk.MustExec("insert into dst(v) values(40), (50)")
	s.tk.MustQuery("select v from dst order by v").Check(testkit.Rows("0", "10", "20", "30", "40", "50"))
	s.tk.MustQuery("select count(*), count(distinct _tidb_rowid), count(distinct id) from dst").
		Check(testkit.Rows("6 6 6"))
	s.tk.MustQuery("select count(*) from dst where (id - 2) % 66 = 0").Check(testkit.Rows("6"))
}

func (s *mockGCSSuite) TestImportFromSelectNonEmptyTablePKConflict() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(id int, v int)")
	s.tk.MustExec("create table dst(id int primary key, v int)")
	s.tk.MustExec("insert into dst values(1, 10)")
	s.tk.MustExec("insert into src values(1, 20), (2, 30)")

	err := s.tk.ExecToErr("import into dst from select * from src order by id")
	s.ErrorContains(err, "found data conflict records in table dst")
	s.ErrorContains(err, "primary key is '1'")
	s.tk.MustQuery("select * from dst order by id").Check(testkit.Rows("1 10"))
}

func (s *mockGCSSuite) TestImportFromSelectNonEmptyTableUKConflict() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(id int, uk int, v int)")
	s.tk.MustExec("create table dst(id int primary key, uk int, v int, unique key uk_idx(uk))")
	s.tk.MustExec("insert into dst values(1, 10, 100)")
	s.tk.MustExec("insert into src values(2, 10, 200), (3, 30, 300)")

	err := s.tk.ExecToErr("import into dst from select * from src order by id")
	s.ErrorContains(err, "found index conflict records in table dst")
	s.ErrorContains(err, "index name is 'dst.uk_idx'")
	s.ErrorContains(err, "unique key is '[10]'")
	s.tk.MustQuery("select * from dst order by id").Check(testkit.Rows("1 10 100"))
}

func (s *mockGCSSuite) TestImportFromSelectNonEmptyTableAutoRandom() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(v int)")
	s.tk.MustExec("insert into src values(10), (20)")

	s.tk.MustExec("create table seed(id bigint primary key auto_random, v int)")
	s.tk.MustExec("import into seed(v) from select * from src order by v with thread = 1")
	seedID := s.tk.MustQuery("select id from seed order by v limit 1").Rows()[0][0]

	s.tk.MustExec("create table dst(id bigint primary key auto_random, v int)")
	s.tk.MustExec("set @@allow_auto_random_explicit_insert = 1")
	s.tk.MustExec(fmt.Sprintf("insert into dst values(%v, 5)", seedID))
	s.tk.MustExec("set @@allow_auto_random_explicit_insert = 0")

	s.tk.MustExec("import into dst(v) from select * from src order by v with thread = 1")
	s.tk.MustQuery("select v from dst order by v").Check(testkit.Rows("5", "10", "20"))
	s.tk.MustQuery("select count(*), count(distinct id) from dst").Check(testkit.Rows("3 3"))
	s.tk.MustQuery(fmt.Sprintf("select count(*) from dst where id = %v", seedID)).Check(testkit.Rows("1"))
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

func (s *mockGCSSuite) TestImportFromSelectPrivilege() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("DROP USER IF EXISTS 'testuser'@'localhost';")
	s.tk.MustExec("DROP TABLE IF EXISTS test.t")
	s.tk.MustExec("DROP TABLE IF EXISTS test.t1")
	s.tk.MustExec(`CREATE USER 'testuser'@'localhost';`)
	s.tk.MustExec(`CREATE TABLE test.t (a INT, b INT, c INT);`)
	s.tk.MustExec(`CREATE TABLE test.t1 (a INT, b INT, c INT);`)
	userTk := testkit.NewTestKit(s.T(), s.store)
	err := userTk.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "localhost"}, nil, nil, nil)
	require.NoError(s.T(), err)
	s.tk.MustExec("GRANT ALL ON test.t1 TO 'testuser'@'localhost'")

	importSQL := "IMPORT INTO test.t1 FROM SELECT a,b,c FROM test.t"
	grantSQL := "GRANT SELECT(%s) ON test.t TO 'testuser'@'localhost'"

	for _, colName := range []string{"a", "b", "c"} {
		userTk.MustGetErrCode(importSQL, mysql.ErrColumnaccessDenied)
		s.tk.MustExec(fmt.Sprintf(grantSQL, colName))
	}
	userTk.MustExec(importSQL)
}

func (s *mockGCSSuite) TestCastNegativeToUnsigned() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table dt(id int unsigned)")
	s.ErrorContains(s.tk.ExecToErr("import into dt from select -1"), "constant -1 overflows int")
	s.tk.MustExec("set sql_mode=''")
	s.tk.MustExec("import into dt from select -1")
	s.tk.MustQuery("select * from dt").Check(testkit.Rows("0"))
}
