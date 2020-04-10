// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	"golang.org/x/net/context"
)

func (s *testSuite5) TestShowVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database showdatabase")
	tk.MustExec("use showdatabase")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec(`create user 'show'@'%'`)
	tk.MustExec(`flush privileges`)

	tk1 := testkit.NewTestKit(c, s.store)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	// No ShowDatabases privilege, this user would see nothing except INFORMATION_SCHEMA.
	tk.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// After grant, the user can see the database.
	tk.MustExec(`grant select on showdatabase.t1 to 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "showdatabase"))

	// The user can see t1 but not t2.
	tk1.MustExec("use showdatabase")
	tk1.MustQuery("show tables").Check(testkit.Rows("t1"))

	// After revoke, show database result should be just except INFORMATION_SCHEMA.
	tk.MustExec(`revoke select on showdatabase.t1 from 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Grant any global privilege would make show databases available.
	tk.MustExec(`grant CREATE on *.* to 'show'@'%'`)
	tk.MustExec(`flush privileges`)
	rows := tk1.MustQuery("show databases").Rows()
	c.Assert(len(rows), GreaterEqual, 2) // At least INFORMATION_SCHEMA and showdatabase

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec("drop database showdatabase")
}

// mockSessionManager is a mocked session manager that wraps one session
// it returns only this session's current process info as processlist for test.
type mockSessionManager struct {
	session.Session
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager) Kill(cid uint64, query bool) {
}

type stats struct {
}

func (s stats) GetScope(status string) variable.ScopeFlag { return variable.DefaultStatusVarScopeFlag }

func (s stats) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	var a, b interface{}
	b = "123"
	m["test_interface_nil"] = a
	m["test_interface"] = b
	m["test_interface_slice"] = []interface{}{"a", "b", "c"}
	return m, nil
}

func (s *testSuite5) TestShowWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0))

	// Test Warning level 'Error'
	testSQL = `create table show_warnings (a int)`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_warnings' already exists"))
	tk.MustQuery("select @@error_count").Check(testutil.RowsWithSep("|", "1"))

	// Test Warning level 'Note'
	testSQL = `create table show_warnings_2 (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table if not exists show_warnings_2 like show_warnings`
	tk.Exec(testSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Note|1050|Table 'test.show_warnings_2' already exists"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "1"))
	tk.MustQuery("select @@warning_count").Check(testutil.RowsWithSep("|", "0"))
}

func (s *testSuite5) TestShowErrors(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_errors (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table show_errors (a int)`
	tk.Exec(testSQL)

	tk.MustQuery("show errors").Check(testutil.RowsWithSep("|", "Error|1050|Table 'test.show_errors' already exists"))
}

func (s *testSuite5) TestIssue3641(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	_, err := tk.Exec("show tables;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	_, err = tk.Exec("show table status;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
}

// TestShow2 is moved from session_test
func (s *testSuite5) TestShow2(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("set global autocommit=0")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 0"))
	tk.MustExec("set global autocommit = 1")
	tk2 := testkit.NewTestKit(c, s.store)
	// TODO: In MySQL, the result is "autocommit ON".
	tk2.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit 1"))

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec("drop table if exists test_full_column")
	tk.MustExec(`create table test_full_column(
					c_int int,
					c_float float,
					c_bit bit,
					c_bool bool,
					c_char char(1) charset ascii collate ascii_bin,
					c_nchar national char(1) charset ascii collate ascii_bin,
					c_binary binary,
					c_varchar varchar(1) charset ascii collate ascii_bin,
					c_varchar_default varchar(20) charset ascii collate ascii_bin default 'cUrrent_tImestamp',
					c_nvarchar national varchar(1) charset ascii collate ascii_bin,
					c_varbinary varbinary(1),
					c_year year,
					c_date date,
					c_time time,
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_timestamp_default_4 timestamp(3) default current_timestamp(3) on update current_timestamp(3),
					c_blob blob,
					c_tinyblob tinyblob,
					c_mediumblob mediumblob,
					c_longblob longblob,
					c_text text charset ascii collate ascii_bin,
					c_tinytext tinytext charset ascii collate ascii_bin,
					c_mediumtext mediumtext charset ascii collate ascii_bin,
					c_longtext longtext charset ascii collate ascii_bin,
					c_json json,
					c_enum enum('1') charset ascii collate ascii_bin,
					c_set set('1') charset ascii collate ascii_bin
				);`)

	tk.MustQuery(`show full columns from test_full_column`).Check(testkit.Rows(
		"" +
			"c_int int(11) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_float float <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_bit bit(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_bool tinyint(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_char char(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_nchar char(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_binary binary(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_varchar varchar(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_varchar_default varchar(20) ascii_bin YES  cUrrent_tImestamp  select,insert,update,references ]\n" +
			"[c_nvarchar varchar(1) ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_varbinary varbinary(1) <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_year year <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_date date <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_time time <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_datetime datetime <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_datetime_default datetime <nil> YES  CURRENT_TIMESTAMP  select,insert,update,references ]\n" +
			"[c_datetime_default_2 datetime(2) <nil> YES  CURRENT_TIMESTAMP(2)  select,insert,update,references ]\n" +
			"[c_timestamp timestamp <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_timestamp_default timestamp <nil> YES  CURRENT_TIMESTAMP  select,insert,update,references ]\n" +
			"[c_timestamp_default_3 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3)  select,insert,update,references ]\n" +
			"[c_timestamp_default_4 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3) select,insert,update,references ]\n" +
			"[c_blob blob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_tinyblob tinyblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_mediumblob mediumblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_longblob longblob <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_text text ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_tinytext tinytext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_mediumtext mediumtext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_longtext longtext ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_json json <nil> YES  <nil>  select,insert,update,references ]\n" +
			"[c_enum enum('1') ascii_bin YES  <nil>  select,insert,update,references ]\n" +
			"[c_set set('1') ascii_bin YES  <nil>  select,insert,update,references "))

	tk.MustExec("drop table if exists test_full_column")

	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table if not exists t (c int) comment '注释'`)
	tk.MustQuery(`show columns from t`).Check(testutil.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery("show collation where Charset = 'utf8' and Collation = 'utf8_bin'").Check(testutil.RowsWithSep(",", "utf8_bin,utf8,83,,Yes,1"))
	tk.MustQuery("show tables").Check(testkit.Rows("t"))
	tk.MustQuery("show full tables").Check(testkit.Rows("t BASE TABLE"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).String()
	r := tk.MustQuery("show table status from test like 't'")
	timeStr := time.Now().Format("2006-01-02 15:04:05")
	r.Check(testkit.Rows(fmt.Sprintf("t InnoDB 10 Compact 100 100 100 100 100 100 100 %s %s %s utf8_general_ci   注释", createTime, timeStr, timeStr)))

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, []byte("012345678901234567890"))

	tk.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))
	tk.MustQuery("show grants for current_user").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'`))
}

func (s *testSuite5) TestCollation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	rs, err := tk.Exec("show collation;")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[1].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[2].Column.Tp, Equals, mysql.TypeLonglong)
	c.Assert(fields[3].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[4].Column.Tp, Equals, mysql.TypeVarchar)
	c.Assert(fields[5].Column.Tp, Equals, mysql.TypeLonglong)
}

func (s *testSuite5) TestShowTableStatus(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_table_partition=1;")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint);`)

	// It's not easy to test the result contents because every time the test runs, "Create_time" changed.
	tk.MustExec("show table status;")
	rs, err := tk.Exec("show table status;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	err = rs.Close()
	c.Assert(errors.ErrorStack(err), Equals, "")

	for i := range rows {
		row := rows[i]
		c.Assert(row.GetString(0), Equals, "t")
		c.Assert(row.GetString(1), Equals, "InnoDB")
		c.Assert(row.GetInt64(2), Equals, int64(10))
		c.Assert(row.GetString(3), Equals, "Compact")
	}
	tk.MustExec(`drop table if exists tp;`)
	tk.MustExec(`create table tp (a int)
 		partition by range(a)
 		( partition p0 values less than (10),
		  partition p1 values less than (20),
		  partition p2 values less than (maxvalue)
  		);`)
	rs, err = tk.Exec("show table status from test like 'tp';")
	c.Assert(errors.ErrorStack(err), Equals, "")
	rows, err = session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rows[0].GetString(16), Equals, "partitioned")
}

func (s *testSuite5) TestShowSlow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// The test result is volatile, because
	// 1. Slow queries is stored in domain, which may be affected by other tests.
	// 2. Collecting slow queries is a asynchronous process, check immediately may not get the expected result.
	// 3. Make slow query like "select sleep(1)" would slow the CI.
	// So, we just cover the code but do not check the result.
	tk.MustQuery(`admin show slow recent 3`)
	tk.MustQuery(`admin show slow top 3`)
	tk.MustQuery(`admin show slow top internal 3`)
	tk.MustQuery(`admin show slow top all 3`)
}

func (s *testSuite5) TestShowOpenTables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("show open tables")
	tk.MustQuery("show open tables in test")
}

func (s *testSuite5) TestShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists different_charset`)
	tk.MustExec(`create table different_charset(ch1 varchar(10) charset utf8, ch2 varchar(10) charset binary);`)
	tk.MustQuery(`show create table different_charset`).Check(testutil.RowsWithSep("|",
		""+
			"different_charset CREATE TABLE `different_charset` (\n"+
			"  `ch1` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `ch2` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table `t` (\n" +
		"`a` timestamp not null default current_timestamp,\n" +
		"`b` timestamp(3) default current_timestamp(3),\n" +
		"`c` datetime default current_timestamp,\n" +
		"`d` datetime(4) default current_timestamp(4),\n" +
		"`e` varchar(20) default 'cUrrent_tImestamp',\n" +
		"`f` datetime(2) default current_timestamp(2) on update current_timestamp(2),\n" +
		"`g` timestamp(2) default current_timestamp(2) on update current_timestamp(2))")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"+
			"  `c` datetime DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `d` datetime(4) DEFAULT CURRENT_TIMESTAMP(4),\n"+
			"  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n"+
			"  `f` datetime(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2),\n"+
			"  `g` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec("create table t (a int, b int) shard_row_id_bits = 4 pre_split_regions=3;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000 SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop table t")

	tk.MustExec("CREATE TABLE `log` (" +
		"`LOG_ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT," +
		"`ROUND_ID` bigint(20) UNSIGNED NOT NULL," +
		"`USER_ID` int(10) UNSIGNED NOT NULL," +
		"`USER_IP` int(10) UNSIGNED DEFAULT NULL," +
		"`END_TIME` datetime NOT NULL," +
		"`USER_TYPE` int(11) DEFAULT NULL," +
		"`APP_ID` int(11) DEFAULT NULL," +
		"PRIMARY KEY (`LOG_ID`,`END_TIME`)," +
		"KEY `IDX_EndTime` (`END_TIME`)," +
		"KEY `IDX_RoundId` (`ROUND_ID`)," +
		"KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488 " +
		"PARTITION BY RANGE ( month(`end_time`) ) (" +
		"PARTITION p1 VALUES LESS THAN (2)," +
		"PARTITION p2 VALUES LESS THAN (3)," +
		"PARTITION p3 VALUES LESS THAN (4)," +
		"PARTITION p4 VALUES LESS THAN (5)," +
		"PARTITION p5 VALUES LESS THAN (6)," +
		"PARTITION p6 VALUES LESS THAN (7)," +
		"PARTITION p7 VALUES LESS THAN (8)," +
		"PARTITION p8 VALUES LESS THAN (9)," +
		"PARTITION p9 VALUES LESS THAN (10)," +
		"PARTITION p10 VALUES LESS THAN (11)," +
		"PARTITION p11 VALUES LESS THAN (12)," +
		"PARTITION p12 VALUES LESS THAN (MAXVALUE))")
	tk.MustQuery("show create table log").Check(testutil.RowsWithSep("|",
		"log CREATE TABLE `log` (\n"+
			"  `LOG_ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,\n"+
			"  `ROUND_ID` bigint(20) UNSIGNED NOT NULL,\n"+
			"  `USER_ID` int(10) UNSIGNED NOT NULL,\n"+
			"  `USER_IP` int(10) UNSIGNED DEFAULT NULL,\n"+
			"  `END_TIME` datetime NOT NULL,\n"+
			"  `USER_TYPE` int(11) DEFAULT NULL,\n"+
			"  `APP_ID` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`LOG_ID`,`END_TIME`),\n"+
			"  KEY `IDX_EndTime` (`END_TIME`),\n"+
			"  KEY `IDX_RoundId` (`ROUND_ID`),\n"+
			"  KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488\n"+
			"PARTITION BY RANGE ( month(`end_time`) ) (\n"+
			"  PARTITION p1 VALUES LESS THAN (2),\n"+
			"  PARTITION p2 VALUES LESS THAN (3),\n"+
			"  PARTITION p3 VALUES LESS THAN (4),\n"+
			"  PARTITION p4 VALUES LESS THAN (5),\n"+
			"  PARTITION p5 VALUES LESS THAN (6),\n"+
			"  PARTITION p6 VALUES LESS THAN (7),\n"+
			"  PARTITION p7 VALUES LESS THAN (8),\n"+
			"  PARTITION p8 VALUES LESS THAN (9),\n"+
			"  PARTITION p9 VALUES LESS THAN (10),\n"+
			"  PARTITION p10 VALUES LESS THAN (11),\n"+
			"  PARTITION p11 VALUES LESS THAN (12),\n"+
			"  PARTITION p12 VALUES LESS THAN (MAXVALUE)\n"+
			")"))

	tk.MustExec("create table t (a int, b int) shard_row_id_bits = 4 pre_split_regions=3;")
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000 SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop table t")

	//for issue #11831
	tk.MustExec("create table ttt4(a varchar(123) default null collate utf8mb4_unicode_ci)engine=innodb default charset=utf8mb4 collate=utf8mb4_unicode_ci;")
	tk.MustQuery("show create table `ttt4`").Check(testutil.RowsWithSep("|",
		""+
			"ttt4 CREATE TABLE `ttt4` (\n"+
			"  `a` varchar(123) COLLATE utf8mb4_unicode_ci DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	))
	tk.MustExec("create table ttt5(a varchar(123) default null)engine=innodb default charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table `ttt5`").Check(testutil.RowsWithSep("|",
		""+
			"ttt5 CREATE TABLE `ttt5` (\n"+
			"  `a` varchar(123) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

// Override testAutoRandomSuite to test auto id cache.
func (s *testAutoRandomSuite) TestAutoIdCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment key) auto_id_cache = 10")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=10 */",
	))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment unique, b int key) auto_id_cache 100")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `b` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`b`),\n"+
			"  UNIQUE KEY `a` (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=100 */",
	))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int key) auto_id_cache 5")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=5 */",
	))
}

func (s *testSuite5) TestShowEscape(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists `t``abl\"e`")
	tk.MustExec("create table `t``abl\"e`(`c``olum\"n` int(11) primary key)")
	tk.MustQuery("show create table `t``abl\"e`").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE `t``abl\"e` (\n"+
			"  `c``olum\"n` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`c``olum\"n`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// ANSI_QUOTES will change the SHOW output
	tk.MustExec("set @old_sql_mode=@@sql_mode")
	tk.MustExec("set sql_mode=ansi_quotes")
	tk.MustQuery("show create table \"t`abl\"\"e\"").Check(testutil.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE \"t`abl\"\"e\" (\n"+
			"  \"c`olum\"\"n\" int(11) NOT NULL,\n"+
			"  PRIMARY KEY (\"c`olum\"\"n\")\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("rename table \"t`abl\"\"e\" to t")
	tk.MustExec("set sql_mode=@old_sql_mode")
}
