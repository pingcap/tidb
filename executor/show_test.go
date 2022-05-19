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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	parsertypes "github.com/pingcap/tidb/parser/types"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestShowVisibility(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database showdatabase")
	tk.MustExec("use showdatabase")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec(`create user 'show'@'%'`)

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil))

	// No ShowDatabases privilege, this user would see nothing except INFORMATION_SCHEMA.
	tk.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// After grant, the user can see the database.
	tk.MustExec(`grant select on showdatabase.t1 to 'show'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "showdatabase"))

	// The user can see t1 but not t2.
	tk1.MustExec("use showdatabase")
	tk1.MustQuery("show tables").Check(testkit.Rows("t1"))

	// After revoke, show database result should be just except INFORMATION_SCHEMA.
	tk.MustExec(`revoke select on showdatabase.t1 from 'show'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Grant any global privilege would make show databases available.
	tk.MustExec(`grant CREATE on *.* to 'show'@'%'`)
	rows := tk1.MustQuery("show databases").Rows()
	require.GreaterOrEqual(t, len(rows), 2)

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec("drop database showdatabase")
}

func TestShowDatabasesInfoSchemaFirst(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA"))
	tk.MustExec(`create user 'show'@'%'`)

	tk.MustExec(`create database AAAA`)
	tk.MustExec(`create database BBBB`)
	tk.MustExec(`grant select on AAAA.* to 'show'@'%'`)
	tk.MustExec(`grant select on BBBB.* to 'show'@'%'`)

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil))
	tk1.MustQuery("show databases").Check(testkit.Rows("INFORMATION_SCHEMA", "AAAA", "BBBB"))

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec(`drop database AAAA`)
	tk.MustExec(`drop database BBBB`)
}

func TestShowWarnings(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect DOUBLE value: 'a'"))
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect DOUBLE value: 'a'"))
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())

	// Test Warning level 'Error'
	testSQL = `create table show_warnings (a int)`
	_, _ = tk.Exec(testSQL)
	// FIXME: Table 'test.show_warnings' already exists
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Error|1050|Table 'test.show_warnings' already exists"))
	tk.MustQuery("select @@error_count").Check(testkit.RowsWithSep("|", "1"))

	// Test Warning level 'Note'
	testSQL = `create table show_warnings_2 (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table if not exists show_warnings_2 like show_warnings`
	_, err := tk.Exec(testSQL)
	require.NoError(t, err)
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1050|Table 'test.show_warnings_2' already exists"))
	tk.MustQuery("select @@warning_count").Check(testkit.RowsWithSep("|", "1"))
	tk.MustQuery("select @@warning_count").Check(testkit.RowsWithSep("|", "0"))
}

func TestShowErrors(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_errors (a int)`
	tk.MustExec(testSQL)
	testSQL = `create table show_errors (a int)`
	// FIXME: 'test.show_errors' already exists
	_, _ = tk.Exec(testSQL)

	tk.MustQuery("show errors").Check(testkit.RowsWithSep("|", "Error|1050|Table 'test.show_errors' already exists"))

	// eliminate previous errors
	tk.MustExec("select 1")
	_, _ = tk.Exec("create invalid")
	tk.MustQuery("show errors").Check(testkit.RowsWithSep("|", "Error|1064|You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 14 near \"invalid\" "))
}

func TestShowWarningsForExprPushdown(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings_expr_pushdown (a int, value date)`
	tk.MustExec(testSQL)

	// create tiflash replica
	{
		is := dom.InfoSchema()
		db, exists := is.SchemaByName(model.NewCIStr("test"))
		require.True(t, exists)
		for _, tblInfo := range db.Tables {
			if tblInfo.Name.L == "show_warnings_expr_pushdown" {
				tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
					Count:     1,
					Available: true,
				}
			}
		}
	}
	tk.MustExec("set tidb_allow_mpp=0")
	tk.MustExec("explain select * from show_warnings_expr_pushdown t where md5(value) = '2020-01-01'")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Scalar function 'md5'(signature: MD5, return type: var_string(32)) is not supported to push down to tiflash now."))
	tk.MustExec("explain select max(md5(value)) from show_warnings_expr_pushdown group by a")
	require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Scalar function 'md5'(signature: MD5, return type: var_string(32)) is not supported to push down to tiflash now.", "Warning|1105|Aggregation can not be pushed to tiflash because arguments of AggFunc `max` contains unsupported exprs"))
	tk.MustExec("explain select max(a) from show_warnings_expr_pushdown group by md5(value)")
	require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Scalar function 'md5'(signature: MD5, return type: var_string(32)) is not supported to push down to tiflash now.", "Warning|1105|Aggregation can not be pushed to tiflash because groupByItems contain unsupported exprs"))
	tk.MustExec("set tidb_opt_distinct_agg_push_down=0")
	tk.MustExec("explain select max(distinct a) from show_warnings_expr_pushdown group by value")
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	// tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Aggregation can not be pushed to storage layer in non-mpp mode because it contains agg function with distinct"))
}

func TestShowGrantsPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user show_grants")
	tk.MustExec("show grants for show_grants")
	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "%"}, nil, nil))
	err := tk1.QueryToErr("show grants for root")
	require.EqualError(t, executor.ErrDBaccessDenied.GenWithStackByArgs("show_grants", "%", mysql.SystemDB), err.Error())
	// Test show grants for user with auth host name `%`.
	tk2 := testkit.NewTestKit(t, store)
	require.True(t, tk2.Session().Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "127.0.0.1", AuthUsername: "show_grants", AuthHostname: "%"}, nil, nil))
	tk2.MustQuery("show grants")
}

func TestShowStatsPrivilege(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user show_stats")
	tk1 := testkit.NewTestKit(t, store)

	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show_stats", Hostname: "%"}, nil, nil))
	eqErr := plannercore.ErrDBaccessDenied.GenWithStackByArgs("show_stats", "%", mysql.SystemDB)
	_, err := tk1.Exec("show stats_meta")
	require.EqualError(t, err, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_BUCKETS")
	require.EqualError(t, err, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_HEALTHY")
	require.EqualError(t, err, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_HISTOGRAMS")
	require.EqualError(t, err, eqErr.Error())
	tk.MustExec("grant select on mysql.* to show_stats")
	tk1.MustExec("show stats_meta")
	tk1.MustExec("SHOW STATS_BUCKETS")
	tk1.MustExec("SHOW STATS_HEALTHY")
	tk1.MustExec("SHOW STATS_HISTOGRAMS")
}

func TestIssue18878(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil))
	tk.MustQuery("select user()").Check(testkit.Rows("root@127.0.0.1"))
	tk.MustQuery("show grants")
	tk.MustQuery("select user()").Check(testkit.Rows("root@127.0.0.1"))
	err := tk.QueryToErr("show grants for root@127.0.0.1")
	require.Equal(t, privileges.ErrNonexistingGrant.FastGenByArgs("root", "127.0.0.1").Error(), err.Error())
	err = tk.QueryToErr("show grants for root@localhost")
	require.Equal(t, privileges.ErrNonexistingGrant.FastGenByArgs("root", "localhost").Error(), err.Error())
	err = tk.QueryToErr("show grants for root@1.1.1.1")
	require.Equal(t, privileges.ErrNonexistingGrant.FastGenByArgs("root", "1.1.1.1").Error(), err.Error())
	tk.MustExec("create user `show_grants`@`127.0.%`")
	err = tk.QueryToErr("show grants for `show_grants`@`127.0.0.1`")
	require.Equal(t, privileges.ErrNonexistingGrant.FastGenByArgs("show_grants", "127.0.0.1").Error(), err.Error())
	tk.MustQuery("show grants for `show_grants`@`127.0.%`")
}

func TestIssue17794(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER 'root'@'8.8.%'")
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "9.9.9.9", AuthHostname: "%"}, nil, nil))

	tk1 := testkit.NewTestKit(t, store)
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "8.8.8.8", AuthHostname: "8.8.%"}, nil, nil))
	tk.MustQuery("show grants").Check(testkit.Rows("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"))
	tk1.MustQuery("show grants").Check(testkit.Rows("GRANT USAGE ON *.* TO 'root'@'8.8.%'"))
}

func TestIssue3641(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Exec("show tables;")
	require.Equal(t, plannercore.ErrNoDB.Error(), err.Error())
	_, err = tk.Exec("show table status;")
	require.Equal(t, plannercore.ErrNoDB.Error(), err.Error())
}

func TestIssue10549(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE newdb;")
	tk.MustExec("CREATE ROLE 'app_developer';")
	tk.MustExec("GRANT ALL ON newdb.* TO 'app_developer';")
	tk.MustExec("CREATE USER 'dev';")
	tk.MustExec("GRANT 'app_developer' TO 'dev';")
	tk.MustExec("SET DEFAULT ROLE app_developer TO 'dev';")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "dev", Hostname: "%", AuthUsername: "dev", AuthHostname: "%"}, nil, nil))
	tk.MustQuery("SHOW DATABASES;").Check(testkit.Rows("INFORMATION_SCHEMA", "newdb"))
	tk.MustQuery("SHOW GRANTS;").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON newdb.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON newdb.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR dev").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
}

func TestIssue11165(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE ROLE 'r_manager';")
	tk.MustExec("CREATE USER 'manager'@'localhost';")
	tk.MustExec("GRANT 'r_manager' TO 'manager'@'localhost';")

	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "manager", Hostname: "localhost", AuthUsername: "manager", AuthHostname: "localhost"}, nil, nil))
	tk.MustExec("SET DEFAULT ROLE ALL TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE NONE TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE 'r_manager' TO 'manager'@'localhost';")
}

// TestShow2 is moved from session_test
func TestShow2(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set global autocommit=0")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit OFF"))
	tk.MustExec("set global autocommit = 1")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Rows("autocommit ON"))

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
			"[c_year year(4) <nil> YES  <nil>  select,insert,update,references ]\n" +
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
	tk.MustExec("create or replace definer='root'@'localhost' view v as select * from t")
	tk.MustQuery(`show columns from t`).Check(testkit.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe t`).Check(testkit.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`show columns from v`).Check(testkit.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe v`).Check(testkit.RowsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery("show collation where Charset = 'utf8' and Collation = 'utf8_bin'").Check(testkit.RowsWithSep(",", "utf8_bin,utf8,83,Yes,Yes,1"))
	tk.MustExec(`drop sequence if exists seq`)
	tk.MustExec(`create sequence seq`)
	tk.MustQuery("show tables").Check(testkit.Rows("seq", "t", "v"))
	tk.MustQuery("show full tables").Check(testkit.Rows("seq SEQUENCE", "t BASE TABLE", "v VIEW"))

	// Bug 19427
	tk.MustQuery("SHOW FULL TABLES in INFORMATION_SCHEMA like 'VIEWS'").Check(testkit.Rows("VIEWS SYSTEM VIEW"))
	tk.MustQuery("SHOW FULL TABLES in information_schema like 'VIEWS'").Check(testkit.Rows("VIEWS SYSTEM VIEW"))
	tk.MustQuery("SHOW FULL TABLES in metrics_schema like 'uptime'").Check(testkit.Rows("uptime SYSTEM VIEW"))

	is := dom.InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	// The Hostname is the actual host
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	r := tk.MustQuery("show table status from test like 't'")
	r.Check(testkit.Rows(fmt.Sprintf("t InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   注释", createTime)))

	tk.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
	tk.MustQuery("show grants for current_user").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
}

func TestShowCreateUser(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED BY 'root';`)
	tk.MustQuery("show create user 'test_show_create_user'@'%'").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	tk.MustExec(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED BY 'test';`)
	tk.MustQuery("show create user 'test_show_create_user'@'localhost';").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	// Case: the user exists but the host portion doesn't match
	err := tk.QueryToErr("show create user 'test_show_create_user'@'asdf';")
	require.Equal(t, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'test_show_create_user'@'asdf'").Error(), err.Error())

	// Case: a user that doesn't exist
	err = tk.QueryToErr("show create user 'aaa'@'localhost';")
	require.Equal(t, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'aaa'@'localhost'").Error(), err.Error())

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, nil)
	tk.MustQuery("show create user current_user").
		Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	tk.MustQuery("show create user current_user()").
		Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	tk.MustExec("create user 'check_priv'")

	// "show create user" for other user requires the SELECT privilege on mysql database.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use mysql")
	succ := tk1.Session().Auth(&auth.UserIdentity{Username: "check_priv", Hostname: "127.0.0.1", AuthUsername: "test_show", AuthHostname: "asdf"}, nil, nil)
	require.True(t, succ)
	err = tk1.QueryToErr("show create user 'root'@'%'")
	require.Error(t, err)

	// "show create user" for current user doesn't check privileges.
	tk1.MustQuery("show create user current_user").
		Check(testkit.Rows("CREATE USER 'check_priv'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	// Creating users with `IDENTIFIED WITH 'caching_sha2_password'`
	tk.MustExec("CREATE USER 'sha_test'@'%' IDENTIFIED WITH 'caching_sha2_password' BY 'temp_passwd'")

	// Compare only the start of the output as the salt changes every time.
	rows := tk.MustQuery("SHOW CREATE USER 'sha_test'@'%'")
	require.Equal(t, "CREATE USER 'sha_test'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$", rows.Rows()[0][0].(string)[:78])
	// Creating users with `IDENTIFIED WITH 'auth-socket'`
	tk.MustExec("CREATE USER 'sock'@'%' IDENTIFIED WITH 'auth_socket'")

	// Compare only the start of the output as the salt changes every time.
	rows = tk.MustQuery("SHOW CREATE USER 'sock'@'%'")
	require.Equal(t, "CREATE USER 'sock'@'%' IDENTIFIED WITH 'auth_socket' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK", rows.Rows()[0][0].(string))
	tk.MustExec("CREATE USER 'sock2'@'%' IDENTIFIED WITH 'auth_socket' AS 'sock3'")

	// Compare only the start of the output as the salt changes every time.
	rows = tk.MustQuery("SHOW CREATE USER 'sock2'@'%'")
	require.Equal(t, "CREATE USER 'sock2'@'%' IDENTIFIED WITH 'auth_socket' AS 'sock3' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK", rows.Rows()[0][0].(string))
}

func TestUnprivilegedShow(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE testshow")
	tk.MustExec("USE testshow")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")

	tk.MustExec(`CREATE USER 'lowprivuser'`) // no grants

	tk.Session().Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	rs, err := tk.Exec("SHOW TABLE STATUS FROM testshow")
	require.NoError(t, err)
	require.NotNil(t, rs)

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("GRANT ALL ON testshow.t1 TO 'lowprivuser'")
	tk.Session().Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	is := dom.InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("testshow"), model.NewCIStr("t1"))
	require.NoError(t, err)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format("2006-01-02 15:04:05")

	tk.MustQuery("show table status from testshow").Check(testkit.Rows(fmt.Sprintf("t1 InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   ", createTime)))

}

func TestCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	rs, err := tk.Exec("show collation;")
	require.NoError(t, err)
	fields := rs.Fields()
	require.Equal(t, mysql.TypeVarchar, fields[0].Column.GetType())
	require.Equal(t, mysql.TypeVarchar, fields[1].Column.GetType())
	require.Equal(t, mysql.TypeLonglong, fields[2].Column.GetType())
	require.Equal(t, mysql.TypeVarchar, fields[3].Column.GetType())
	require.Equal(t, mysql.TypeVarchar, fields[4].Column.GetType())
	require.Equal(t, mysql.TypeLonglong, fields[5].Column.GetType())
}

func TestShowTableStatus(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint);`)

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	// It's not easy to test the result contents because every time the test runs, "Create_time" changed.
	tk.MustExec("show table status;")
	rs, err := tk.Exec("show table status;")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	err = rs.Close()
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	for i := range rows {
		row := rows[i]
		require.Equal(t, "t", row.GetString(0))
		require.Equal(t, "InnoDB", row.GetString(1))
		require.Equal(t, int64(10), row.GetInt64(2))
		require.Equal(t, "Compact", row.GetString(3))
	}
	tk.MustExec(`drop table if exists tp;`)
	tk.MustExec(`create table tp (a int)
 		partition by range(a)
 		( partition p0 values less than (10),
		  partition p1 values less than (20),
		  partition p2 values less than (maxvalue)
  		);`)
	rs, err = tk.Exec("show table status from test like 'tp';")
	require.NoError(t, err)
	rows, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	require.Equal(t, "partitioned", rows[0].GetString(16))

	tk.MustExec("create database UPPER_CASE")
	tk.MustExec("use UPPER_CASE")
	tk.MustExec("create table t (i int)")
	rs, err = tk.Exec("show table status")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	err = rs.Close()
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	tk.MustExec("use upper_case")
	rs, err = tk.Exec("show table status")
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	err = rs.Close()
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	tk.MustExec("drop database UPPER_CASE")
}

func TestShowSlow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestShowOpenTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show open tables")
	tk.MustQuery("show open tables in test")
}
func TestShowCreateViewDefiner(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%", AuthUsername: "root", AuthHostname: "%"}, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("create or replace view v1 as select 1")
	tk.MustQuery("show create view v1").Check(testkit.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1`|utf8mb4|utf8mb4_bin"))
	tk.MustExec("drop view v1")
}

func TestShowCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustQuery("show create table v1").Check(testkit.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a` AS `a`,`test`.`t1`.`b` AS `b` FROM `test`.`t1`|utf8mb4|utf8mb4_bin"))
	tk.MustQuery("show create view v1").Check(testkit.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a` AS `a`,`test`.`t1`.`b` AS `b` FROM `test`.`t1`|utf8mb4|utf8mb4_bin"))
	tk.MustExec("drop view v1")
	tk.MustExec("drop table t1")

	tk.MustExec("drop view if exists v")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v as select JSON_MERGE('{}', '{}') as col;")
	tk.MustQuery("show create view v").Check(testkit.RowsWithSep("|", "v|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v` (`col`) AS SELECT JSON_MERGE(_UTF8MB4'{}', _UTF8MB4'{}') AS `col`|utf8mb4|utf8mb4_bin"))
	tk.MustExec("drop view if exists v")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int,b int)")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select avg(a),t1.* from t1 group by a")
	tk.MustQuery("show create view v1").Check(testkit.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`avg(a)`, `a`, `b`) AS SELECT AVG(`a`) AS `avg(a)`,`test`.`t1`.`a` AS `a`,`test`.`t1`.`b` AS `b` FROM `test`.`t1` GROUP BY `a`|utf8mb4|utf8mb4_bin"))
	tk.MustExec("drop view v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select a+b, t1.* , a as c from t1")
	tk.MustQuery("show create view v1").Check(testkit.RowsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` SQL SECURITY DEFINER VIEW `v1` (`a+b`, `a`, `b`, `c`) AS SELECT `a`+`b` AS `a+b`,`test`.`t1`.`a` AS `a`,`test`.`t1`.`b` AS `b`,`a` AS `c` FROM `test`.`t1`|utf8mb4|utf8mb4_bin"))
	tk.MustExec("drop table t1")
	tk.MustExec("drop view v1")

	// For issue #9211
	tk.MustExec("create table t(c int, b int as (c + 1))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(c int, b int as (c + 1) not null)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create table `t`").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t ( a char(10) charset utf8 collate utf8_bin, b char(10) as (rtrim(a)));")
	tk.MustQuery("show create table `t`").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` char(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `b` char(10) GENERATED ALWAYS AS (rtrim(`a`)) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t")

	tk.MustExec(`drop table if exists different_charset`)
	tk.MustExec(`create table different_charset(ch1 varchar(10) charset utf8, ch2 varchar(10) charset binary);`)
	tk.MustQuery(`show create table different_charset`).Check(testkit.RowsWithSep("|",
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
	tk.MustQuery("show create table `t`").Check(testkit.RowsWithSep("|",
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
	tk.MustQuery("show create table `t`").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop table t")

	// for issue #20446
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c int unsigned default 0);")
	tk.MustQuery("show create table `t1`").Check(testkit.RowsWithSep("|",
		""+
			"t1 CREATE TABLE `t1` (\n"+
			"  `c` int(10) unsigned DEFAULT '0'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop table t1")

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
		"PARTITION `p1` VALUES LESS THAN (2)," +
		"PARTITION `p2` VALUES LESS THAN (3)," +
		"PARTITION `p3` VALUES LESS THAN (4)," +
		"PARTITION `p4` VALUES LESS THAN (5)," +
		"PARTITION `p5` VALUES LESS THAN (6)," +
		"PARTITION `p6` VALUES LESS THAN (7)," +
		"PARTITION `p7` VALUES LESS THAN (8)," +
		"PARTITION `p8` VALUES LESS THAN (9)," +
		"PARTITION `p9` VALUES LESS THAN (10)," +
		"PARTITION `p10` VALUES LESS THAN (11)," +
		"PARTITION `p11` VALUES LESS THAN (12)," +
		"PARTITION `p12` VALUES LESS THAN (MAXVALUE))")
	tk.MustQuery("show create table log").Check(testkit.RowsWithSep("|",
		"log CREATE TABLE `log` (\n"+
			"  `LOG_ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"+
			"  `ROUND_ID` bigint(20) unsigned NOT NULL,\n"+
			"  `USER_ID` int(10) unsigned NOT NULL,\n"+
			"  `USER_IP` int(10) unsigned DEFAULT NULL,\n"+
			"  `END_TIME` datetime NOT NULL,\n"+
			"  `USER_TYPE` int(11) DEFAULT NULL,\n"+
			"  `APP_ID` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`LOG_ID`,`END_TIME`) /*T![clustered_index] NONCLUSTERED */,\n"+
			"  KEY `IDX_EndTime` (`END_TIME`),\n"+
			"  KEY `IDX_RoundId` (`ROUND_ID`),\n"+
			"  KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488\n"+
			"PARTITION BY RANGE (MONTH(`end_time`))\n"+
			"(PARTITION `p1` VALUES LESS THAN (2),\n"+
			" PARTITION `p2` VALUES LESS THAN (3),\n"+
			" PARTITION `p3` VALUES LESS THAN (4),\n"+
			" PARTITION `p4` VALUES LESS THAN (5),\n"+
			" PARTITION `p5` VALUES LESS THAN (6),\n"+
			" PARTITION `p6` VALUES LESS THAN (7),\n"+
			" PARTITION `p7` VALUES LESS THAN (8),\n"+
			" PARTITION `p8` VALUES LESS THAN (9),\n"+
			" PARTITION `p9` VALUES LESS THAN (10),\n"+
			" PARTITION `p10` VALUES LESS THAN (11),\n"+
			" PARTITION `p11` VALUES LESS THAN (12),\n"+
			" PARTITION `p12` VALUES LESS THAN (MAXVALUE))"))

	// for issue #11831
	tk.MustExec("create table ttt4(a varchar(123) default null collate utf8mb4_unicode_ci)engine=innodb default charset=utf8mb4 collate=utf8mb4_unicode_ci;")
	tk.MustQuery("show create table `ttt4`").Check(testkit.RowsWithSep("|",
		""+
			"ttt4 CREATE TABLE `ttt4` (\n"+
			"  `a` varchar(123) COLLATE utf8mb4_unicode_ci DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	))
	tk.MustExec("create table ttt5(a varchar(123) default null)engine=innodb default charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table `ttt5`").Check(testkit.RowsWithSep("|",
		""+
			"ttt5 CREATE TABLE `ttt5` (\n"+
			"  `a` varchar(123) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// for expression index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b real);")
	tk.MustExec("alter table t add index expr_idx((a*b+1));")
	tk.MustQuery("show create table t;").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` double DEFAULT NULL,\n"+
			"  KEY `expr_idx` ((`a` * `b` + 1))\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Fix issue #15175, show create table sequence_name.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("show create table seq;").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// Test for issue #15633, 'binary' collation should be ignored in the result of 'show create table'.
	tk.MustExec(`drop table if exists binary_collate`)
	tk.MustExec(`create table binary_collate(a varchar(10)) default collate=binary;`)
	tk.MustQuery(`show create table binary_collate`).Check(testkit.RowsWithSep("|",
		""+
			"binary_collate CREATE TABLE `binary_collate` (\n"+
			"  `a` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=binary", // binary collate is ignored
	))
	tk.MustExec(`drop table if exists binary_collate`)
	tk.MustExec(`create table binary_collate(a varchar(10)) default charset=binary collate=binary;`)
	tk.MustQuery(`show create table binary_collate`).Check(testkit.RowsWithSep("|",
		""+
			"binary_collate CREATE TABLE `binary_collate` (\n"+
			"  `a` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=binary", // binary collate is ignored
	))
	tk.MustExec(`drop table if exists binary_collate`)
	tk.MustExec(`create table binary_collate(a varchar(10)) default charset=utf8mb4 collate=utf8mb4_bin;`)
	tk.MustQuery(`show create table binary_collate`).Check(testkit.RowsWithSep("|",
		""+
			"binary_collate CREATE TABLE `binary_collate` (\n"+
			"  `a` varchar(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", // non-binary collate is kept.
	))
	// Test for issue #17 in bug competition, default num and sequence should be shown without quote.
	tk.MustExec(`drop table if exists default_num`)
	tk.MustExec("create table default_num(a int default 11)")
	tk.MustQuery("show create table default_num").Check(testkit.RowsWithSep("|",
		""+
			"default_num CREATE TABLE `default_num` (\n"+
			"  `a` int(11) DEFAULT '11'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`drop table if exists default_varchar`)
	tk.MustExec("create table default_varchar(a varchar(10) default \"haha\")")
	tk.MustQuery("show create table default_varchar").Check(testkit.RowsWithSep("|",
		""+
			"default_varchar CREATE TABLE `default_varchar` (\n"+
			"  `a` varchar(10) DEFAULT 'haha'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`drop table if exists default_sequence`)
	tk.MustExec("create table default_sequence(a int default nextval(seq))")
	tk.MustQuery("show create table default_sequence").Check(testkit.RowsWithSep("|",
		""+
			"default_sequence CREATE TABLE `default_sequence` (\n"+
			"  `a` int(11) DEFAULT nextval(`test`.`seq`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// TiDB defaults (and only supports) foreign_key_checks=0
	// This means that the child table can be created before the parent table.
	// This behavior is required for mysqldump restores.
	tk.MustExec(`DROP TABLE IF EXISTS parent, child`)
	tk.MustExec(`CREATE TABLE child (id INT NOT NULL PRIMARY KEY auto_increment, parent_id INT NOT NULL, INDEX par_ind (parent_id), CONSTRAINT child_ibfk_1 FOREIGN KEY (parent_id) REFERENCES parent(id))`)
	tk.MustExec(`CREATE TABLE parent ( id INT NOT NULL PRIMARY KEY auto_increment )`)
	tk.MustQuery(`show create table child`).Check(testkit.RowsWithSep("|",
		""+
			"child CREATE TABLE `child` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `parent_id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n"+
			"  KEY `par_ind` (`parent_id`),\n"+
			"  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test Foreign keys + ON DELETE / ON UPDATE
	tk.MustExec(`DROP TABLE child`)
	tk.MustExec(`CREATE TABLE child (id INT NOT NULL PRIMARY KEY auto_increment, parent_id INT NOT NULL, INDEX par_ind (parent_id), CONSTRAINT child_ibfk_1 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL ON UPDATE CASCADE)`)
	tk.MustQuery(`show create table child`).Check(testkit.RowsWithSep("|",
		""+
			"child CREATE TABLE `child` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `parent_id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n"+
			"  KEY `par_ind` (`parent_id`),\n"+
			"  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE SET NULL ON UPDATE CASCADE\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test issue #20327
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b char(10) as ('a'));")
	result := tk.MustQuery("show create table t;").Rows()[0][1]
	require.Regexp(t, `(?s).*GENERATED ALWAYS AS \(_utf8mb4'a'\).*`, result)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b char(10) as (_utf8'a'));")
	result = tk.MustQuery("show create table t;").Rows()[0][1]
	require.Regexp(t, `(?s).*GENERATED ALWAYS AS \(_utf8'a'\).*`, result)
	// Test show list partition table
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec(`create table t (id int, name varchar(10), unique index idx (id)) partition by list  (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) DEFAULT NULL,\n"+
			"  `name` varchar(10) DEFAULT NULL,\n"+
			"  UNIQUE KEY `idx` (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
			"PARTITION BY LIST (`id`)\n"+
			"(PARTITION `p0` VALUES IN (3,5,6,9,17),\n"+
			" PARTITION `p1` VALUES IN (1,2,10,11,19,20),\n"+
			" PARTITION `p2` VALUES IN (4,12,13,14,18),\n"+
			" PARTITION `p3` VALUES IN (7,8,15,16,NULL))"))
	// Test show list column partition table
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec(`create table t (id int, name varchar(10), unique index idx (id)) partition by list columns (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) DEFAULT NULL,\n"+
			"  `name` varchar(10) DEFAULT NULL,\n"+
			"  UNIQUE KEY `idx` (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
			"PARTITION BY LIST COLUMNS(`id`)\n"+
			"(PARTITION `p0` VALUES IN (3,5,6,9,17),\n"+
			" PARTITION `p1` VALUES IN (1,2,10,11,19,20),\n"+
			" PARTITION `p2` VALUES IN (4,12,13,14,18),\n"+
			" PARTITION `p3` VALUES IN (7,8,15,16,NULL))"))
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec(`create table t (id int, name varchar(10), unique index idx (id, name)) partition by list columns (id, name) (
    	partition p0 values in ((3, '1'), (5, '5')),
    	partition p1 values in ((1, '1')));`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) DEFAULT NULL,\n"+
			"  `name` varchar(10) DEFAULT NULL,\n"+
			"  UNIQUE KEY `idx` (`id`,`name`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
			"PARTITION BY LIST COLUMNS(`id`,`name`)\n"+
			"(PARTITION `p0` VALUES IN ((3,\"1\"),(5,\"5\")),\n"+
			" PARTITION `p1` VALUES IN ((1,\"1\")))"))
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec(`create table t (id int primary key, v varchar(255) not null, key idx_v (v) comment 'foo\'bar')`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  `v` varchar(255) NOT NULL,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n"+
			"  KEY `idx_v` (`v`) COMMENT 'foo''bar'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// For issue #29922
	tk.MustExec("CREATE TABLE `thash` (\n  `id` bigint unsigned NOT NULL,\n  `data` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)\nPARTITION BY HASH (`id`)\n(PARTITION pEven COMMENT = \"Even ids\",\n PARTITION pOdd COMMENT = \"Odd ids\");")
	tk.MustQuery("show create table `thash`").Check(testkit.RowsWithSep("|", ""+
		"thash CREATE TABLE `thash` (\n"+
		"  `id` bigint(20) unsigned NOT NULL,\n"+
		"  `data` varchar(255) DEFAULT NULL,\n"+
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY HASH (`id`)\n"+
		"(PARTITION `pEven` COMMENT 'Even ids',\n"+
		" PARTITION `pOdd` COMMENT 'Odd ids')",
	))
	// empty edge case
	tk.MustExec("drop table if exists `thash`")
	tk.MustExec("CREATE TABLE `thash` (\n  `id` bigint unsigned NOT NULL,\n  `data` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)\nPARTITION BY HASH (`id`);")
	tk.MustQuery("show create table `thash`").Check(testkit.RowsWithSep("|", ""+
		"thash CREATE TABLE `thash` (\n"+
		"  `id` bigint(20) unsigned NOT NULL,\n"+
		"  `data` varchar(255) DEFAULT NULL,\n"+
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY HASH (`id`) PARTITIONS 1",
	))

	// default value escape character '\\' display case
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b varchar(20) default '\\\\');")
	tk.MustQuery("show create table t;").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL,\n"+
			"  `b` varchar(20) DEFAULT '\\\\',\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestShowCreateTablePlacement(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	defer tk.MustExec(`DROP TABLE IF EXISTS t`)

	// case for policy
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create placement policy x " +
		"FOLLOWERS=2 " +
		"CONSTRAINTS=\"[+disk=ssd]\" ")
	defer tk.MustExec(`DROP PLACEMENT POLICY IF EXISTS x`)
	tk.MustExec("create table t(a int)" +
		"PLACEMENT POLICY=\"x\"")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`x` */",
	))

	// case for policy with quotes
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int)" +
		"/*T![placement] PLACEMENT POLICY=\"x\" */")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin "+
			"/*T![placement] PLACEMENT POLICY=`x` */",
	))

	// Partitioned tables
	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("set @old_list_part = @@tidb_enable_list_partition")
	defer tk.MustExec("set @@tidb_enable_list_partition = @old_list_part")
	tk.MustExec("set tidb_enable_list_partition = 1")
	tk.MustExec("create table t(a int, b varchar(255))" +
		"/*T![placement] PLACEMENT POLICY=\"x\" */" +
		"PARTITION BY LIST (a)\n" +
		"(PARTITION pLow VALUES in (1,2,3,5,8) COMMENT 'a comment' placement policy 'x'," +
		" PARTITION pMid VALUES in (9) COMMENT 'another comment'," +
		"partition pMax values IN (10,11,12))")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`x` */\n"+
		"PARTITION BY LIST (`a`)\n"+
		"(PARTITION `pLow` VALUES IN (1,2,3,5,8) COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMid` VALUES IN (9) COMMENT 'another comment',\n"+
		" PARTITION `pMax` VALUES IN (10,11,12))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"PARTITION BY LIST COLUMNS (b)\n" +
		"(PARTITION pLow VALUES in ('1','2','3','5','8') COMMENT 'a comment' placement policy 'x'," +
		"partition pMax values IN ('10','11','12'))")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY LIST COLUMNS(`b`)\n"+
		"(PARTITION `pLow` VALUES IN (\"1\",\"2\",\"3\",\"5\",\"8\") COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES IN (\"10\",\"11\",\"12\"))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"PARTITION BY LIST COLUMNS (a,b)\n" +
		"(PARTITION pLow VALUES in ((1,'1'),(2,'2'),(3,'3'),(5,'5'),(8,'8')) COMMENT 'a comment' placement policy 'x'," +
		"partition pMax values IN ((10,'10'),(11,'11'),(12,'12')))")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY LIST COLUMNS(`a`,`b`)\n"+
		"(PARTITION `pLow` VALUES IN ((1,\"1\"),(2,\"2\"),(3,\"3\"),(5,\"5\"),(8,\"8\")) COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES IN ((10,\"10\"),(11,\"11\"),(12,\"12\")))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"PARTITION BY RANGE (a)\n" +
		"(PARTITION pLow VALUES less than (1000000) COMMENT 'a comment' placement policy 'x'," +
		"partition pMax values LESS THAN (MAXVALUE))")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY RANGE (`a`)\n"+
		"(PARTITION `pLow` VALUES LESS THAN (1000000) COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"PARTITION BY RANGE COLUMNS (b)\n" +
		"(PARTITION pLow VALUES less than ('1000000') COMMENT 'a comment' placement policy 'x'," +
		"partition pMax values LESS THAN (MAXVALUE))")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY RANGE COLUMNS(`b`)\n"+
		"(PARTITION `pLow` VALUES LESS THAN (\"1000000\") COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	// RANGE COLUMNS with multiple columns is not supported!
	tk.MustExec("create table t(a int, b varchar(255))" +
		"/*T![placement] PLACEMENT POLICY=\"x\" */" +
		"PARTITION BY RANGE COLUMNS (a,b)\n" +
		"(PARTITION pLow VALUES less than (1000000,'1000000') COMMENT 'a comment' placement policy 'x'," +
		" PARTITION pMidLow VALUES less than (1000000,MAXVALUE) COMMENT 'another comment' placement policy 'x'," +
		" PARTITION pMadMax VALUES less than (MAXVALUE,'1000000') COMMENT ='Not a comment' placement policy 'x'," +
		"partition pMax values LESS THAN (MAXVALUE, MAXVALUE))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type RANGE, treat as normal table"))
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`x` */",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"/*T![placement] PLACEMENT POLICY=\"x\" */" +
		"PARTITION BY HASH (a) PARTITIONS 2")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`x` */\n"+
		"PARTITION BY HASH (`a`) PARTITIONS 2",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)
	tk.MustExec("create table t(a int, b varchar(255))" +
		"PARTITION BY HASH (a)\n" +
		"(PARTITION pLow COMMENT 'a comment' placement policy 'x'," +
		"partition pMax)")
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|", ""+
		"t CREATE TABLE `t` (\n"+
		"  `a` int(11) DEFAULT NULL,\n"+
		"  `b` varchar(255) DEFAULT NULL\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
		"PARTITION BY HASH (`a`)\n"+
		"(PARTITION `pLow` COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax`)",
	))
	tk.MustExec(`DROP TABLE t`)
}

func TestShowCreateTableAutoRandom(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Basic show create table.
	tk.MustExec("create table auto_random_tbl1 (a bigint primary key auto_random(3), b varchar(255))")
	tk.MustQuery("show create table `auto_random_tbl1`").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl1 CREATE TABLE `auto_random_tbl1` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(3) */,\n"+
			"  `b` varchar(255) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Implicit auto_random value should be shown explicitly.
	tk.MustExec("create table auto_random_tbl2 (a bigint auto_random primary key, b char)")
	tk.MustQuery("show create table auto_random_tbl2").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl2 CREATE TABLE `auto_random_tbl2` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` char(1) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Special version comment can be shown in TiDB with new version.
	tk.MustExec("create table auto_random_tbl3 (a bigint /*T![auto_rand] auto_random */ primary key)")
	tk.MustQuery("show create table auto_random_tbl3").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl3 CREATE TABLE `auto_random_tbl3` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	// Test show auto_random table option.
	tk.MustExec("create table auto_random_tbl4 (a bigint primary key auto_random(5), b varchar(255)) auto_random_base = 100")
	tk.MustQuery("show create table `auto_random_tbl4`").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl4 CREATE TABLE `auto_random_tbl4` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` varchar(255) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=100 */",
	))
	// Test implicit auto_random with auto_random table option.
	tk.MustExec("create table auto_random_tbl5 (a bigint auto_random primary key, b char) auto_random_base 50")
	tk.MustQuery("show create table auto_random_tbl5").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl5 CREATE TABLE `auto_random_tbl5` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` char(1) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=50 */",
	))
	// Test auto_random table option already with special comment.
	tk.MustExec("create table auto_random_tbl6 (a bigint /*T![auto_rand] auto_random */ primary key) auto_random_base 200")
	tk.MustQuery("show create table auto_random_tbl6").Check(testkit.RowsWithSep("|",
		""+
			"auto_random_tbl6 CREATE TABLE `auto_random_tbl6` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=200 */",
	))
}

// TestAutoIdCache overrides testAutoRandomSuite to test auto id cache.
func TestAutoIdCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment key) auto_id_cache = 10")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=10 */",
	))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment unique, b int key) auto_id_cache 100")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `b` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`b`) /*T![clustered_index] CLUSTERED */,\n"+
			"  UNIQUE KEY `a` (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=100 */",
	))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int key) auto_id_cache 5")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=5 */",
	))
}

func TestShowCreateStmtIgnoreLocalTemporaryTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// SHOW CREATE VIEW ignores local temporary table with the same name
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create view v1 as select 1")
	tk.MustExec("create temporary table v1 (a int)")
	tk.MustQuery("show create table v1").Check(testkit.RowsWithSep("|",
		""+
			"v1 CREATE TEMPORARY TABLE `v1` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop view v1")
	err := tk.ExecToErr("show create view v1")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))

	// SHOW CREATE SEQUENCE ignores local temporary table with the same name
	tk.MustExec("drop view if exists seq1")
	tk.MustExec("create sequence seq1")
	tk.MustExec("create temporary table seq1 (a int)")
	tk.MustQuery("show create sequence seq1").Check(testkit.RowsWithSep("|",
		"seq1 CREATE SEQUENCE `seq1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB",
	))
	tk.MustExec("drop sequence seq1")
	err = tk.ExecToErr("show create sequence seq1")
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
}

func TestAutoRandomBase(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint primary key auto_random(5), b int unique key auto_increment) auto_random_base = 100, auto_increment = 100")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n"+
			"  UNIQUE KEY `b` (`b`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=100 /*T![auto_rand_base] AUTO_RANDOM_BASE=100 */",
	))

	tk.MustExec("insert into t(`a`) values (1000)")
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n"+
			"  UNIQUE KEY `b` (`b`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5100 /*T![auto_rand_base] AUTO_RANDOM_BASE=6001 */",
	))
}

func TestAutoRandomWithLargeSignedShowTableRegions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists auto_random_db;")
	defer tk.MustExec("drop database if exists auto_random_db;")
	tk.MustExec("use auto_random_db;")
	tk.MustExec("drop table if exists t;")

	tk.MustExec("create table t (a bigint unsigned auto_random primary key clustered);")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	// 18446744073709541615 is MaxUint64 - 10000.
	// 18446744073709551615 is the MaxUint64.
	tk.MustQuery("split table t between (18446744073709541615) and (18446744073709551615) regions 2;").
		Check(testkit.Rows("1 1"))
	startKey := tk.MustQuery("show table t regions;").Rows()[1][1].(string)
	idx := strings.Index(startKey, "_r_")
	require.False(t, idx == -1)
	require.Falsef(t, startKey[idx+3] == '-', "actual key: %s", startKey)
}

func TestShowEscape(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists `t``abl\"e`")
	tk.MustExec("create table `t``abl\"e`(`c``olum\"n` int(11) primary key)")
	tk.MustQuery("show create table `t``abl\"e`").Check(testkit.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE `t``abl\"e` (\n"+
			"  `c``olum\"n` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`c``olum\"n`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// ANSI_QUOTES will change the SHOW output
	tk.MustExec("set @old_sql_mode=@@sql_mode")
	tk.MustExec("set sql_mode=ansi_quotes")
	tk.MustQuery("show create table \"t`abl\"\"e\"").Check(testkit.RowsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE \"t`abl\"\"e\" (\n"+
			"  \"c`olum\"\"n\" int(11) NOT NULL,\n"+
			"  PRIMARY KEY (\"c`olum\"\"n\") /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("rename table \"t`abl\"\"e\" to t")
	tk.MustExec("set sql_mode=@old_sql_mode")
}

func TestShowBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	res := tk.MustQuery("show builtins;")
	require.NotNil(t, res)
	rows := res.Rows()
	const builtinFuncNum = 275
	require.Equal(t, len(rows), builtinFuncNum)
	require.Equal(t, rows[0][0].(string), "abs")
	require.Equal(t, rows[builtinFuncNum-1][0].(string), "yearweek")
}

func TestShowClusterConfig(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var confItems [][]types.Datum
	var confErr error
	var confFunc executor.TestShowClusterConfigFunc = func() ([][]types.Datum, error) {
		return confItems, confErr
	}
	tk.Session().SetValue(executor.TestShowClusterConfigKey, confFunc)
	strs2Items := func(strs ...string) []types.Datum {
		items := make([]types.Datum, 0, len(strs))
		for _, s := range strs {
			items = append(items, types.NewStringDatum(s))
		}
		return items
	}
	confItems = append(confItems, strs2Items("tidb", "127.0.0.1:1111", "log.level", "info"))
	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2222", "log.level", "info"))
	confItems = append(confItems, strs2Items("tikv", "127.0.0.1:3333", "log.level", "info"))
	tk.MustQuery("show config").Check(testkit.Rows(
		"tidb 127.0.0.1:1111 log.level info",
		"pd 127.0.0.1:2222 log.level info",
		"tikv 127.0.0.1:3333 log.level info"))
	tk.MustQuery("show config where type='tidb'").Check(testkit.Rows(
		"tidb 127.0.0.1:1111 log.level info"))
	tk.MustQuery("show config where type like '%ti%'").Check(testkit.Rows(
		"tidb 127.0.0.1:1111 log.level info",
		"tikv 127.0.0.1:3333 log.level info"))

	confErr = fmt.Errorf("something unknown error")
	require.EqualError(t, tk.QueryToErr("show config"), confErr.Error())
}

func TestInvisibleCoprCacheConfig(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show variables like '%config%'").Rows()
	require.Equal(t, 1, len(rows))
	configValue := rows[0][1].(string)
	coprCacheVal :=
		"\t\t\"copr-cache\": {\n" +
			"\t\t\t\"capacity-mb\": 1000\n" +
			"\t\t},\n"
	require.Equal(t, true, strings.Contains(configValue, coprCacheVal))
}

func TestEnableGlobalKillConfig(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show variables like '%config%'").Rows()
	require.Equal(t, 1, len(rows))
	configValue := rows[0][1].(string)
	globalKillVal := "\"enable-global-kill\": true"
	require.True(t, strings.Contains(configValue, globalKillVal))
}

func TestShowCreateTableWithIntegerDisplayLengthWarnings(t *testing.T) {
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() {
		parsertypes.TiDBStrictIntegerDisplayWidth = false
	}()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(2), b varchar(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `b` varchar(2) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(10), b bigint)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` bigint DEFAULT NULL,\n" +
		"  `b` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a tinyint(5), b tinyint(2), c tinyint)")
	// Here it will occur 2 warnings.
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release.",
		"Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` tinyint DEFAULT NULL,\n" +
		"  `b` tinyint DEFAULT NULL,\n" +
		"  `c` tinyint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a smallint(5), b smallint)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` smallint DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a mediumint(5), b mediumint)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` mediumint DEFAULT NULL,\n" +
		"  `b` mediumint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int1(1), b int2(2), c int3, d int4, e int8)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release.",
		"Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use [parser:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` tinyint DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL,\n" +
		"  `c` mediumint DEFAULT NULL,\n" +
		"  `d` int DEFAULT NULL,\n" +
		"  `e` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestShowVar(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	var showSQL string
	sessionVars := make([]string, 0, len(variable.GetSysVars()))
	globalVars := make([]string, 0, len(variable.GetSysVars()))
	for _, v := range variable.GetSysVars() {
		if v.Hidden {
			continue
		}

		if v.Scope == variable.ScopeSession {
			sessionVars = append(sessionVars, v.Name)
		} else {
			globalVars = append(globalVars, v.Name)
		}
	}

	// When ScopeSession only. `show global variables` must return empty.
	sessionVarsStr := strings.Join(sessionVars, "','")
	showSQL = "show variables where variable_name in('" + sessionVarsStr + "')"
	res := tk.MustQuery(showSQL)
	require.Len(t, res.Rows(), len(sessionVars))
	showSQL = "show global variables where variable_name in('" + sessionVarsStr + "')"
	res = tk.MustQuery(showSQL)
	require.Len(t, res.Rows(), 0)

	globalVarsStr := strings.Join(globalVars, "','")
	showSQL = "show variables where variable_name in('" + globalVarsStr + "')"
	res = tk.MustQuery(showSQL)
	require.Len(t, res.Rows(), len(globalVars))
	showSQL = "show global variables where variable_name in('" + globalVarsStr + "')"
	res = tk.MustQuery(showSQL)
	require.Len(t, res.Rows(), len(globalVars))

	// Test Hidden tx_read_ts
	res = tk.MustQuery("show variables like '%tx_read_ts'")
	require.Len(t, res.Rows(), 0)

	// Test versions' related variables
	res = tk.MustQuery("show variables like 'version%'")
	for _, row := range res.Rows() {
		line := fmt.Sprint(row)
		if strings.HasPrefix(line, "version ") {
			require.Equal(t, mysql.ServerVersion, line[len("version "):])
		} else if strings.HasPrefix(line, "version_comment ") {
			require.Equal(t, variable.GetSysVar(variable.VersionComment), line[len("version_comment "):])
		}
	}

	// Test case insensitive case for show session variables
	tk.MustExec("SET @@SQL_MODE='NO_BACKSLASH_ESCAPES'")
	tk.MustQuery("SHOW SESSION VARIABLES like 'sql_mode'").Check(
		testkit.RowsWithSep("|", "sql_mode|NO_BACKSLASH_ESCAPES"))
	tk.MustQuery("SHOW SESSION VARIABLES like 'SQL_MODE'").Check(
		testkit.RowsWithSep("|", "sql_mode|NO_BACKSLASH_ESCAPES"))
}

func TestIssue19507(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t2(a int primary key, b int unique, c int not null, unique index (c));")
	tk.MustQuery("SHOW INDEX IN t2;").Check(
		testkit.RowsWithSep("|", "t2|0|PRIMARY|1|a|A|0|<nil>|<nil>||BTREE|||YES|<nil>|YES",
			"t2|0|c|1|c|A|0|<nil>|<nil>||BTREE|||YES|<nil>|NO",
			"t2|0|b|1|b|A|0|<nil>|<nil>|YES|BTREE|||YES|<nil>|NO"))

	tk.MustExec("CREATE INDEX t2_b_c_index ON t2 (b, c);")
	tk.MustExec("CREATE INDEX t2_c_b_index ON t2 (c, b);")
	tk.MustQuery("SHOW INDEX IN t2;").Check(
		testkit.RowsWithSep("|", "t2|0|PRIMARY|1|a|A|0|<nil>|<nil>||BTREE|||YES|<nil>|YES",
			"t2|0|c|1|c|A|0|<nil>|<nil>||BTREE|||YES|<nil>|NO",
			"t2|0|b|1|b|A|0|<nil>|<nil>|YES|BTREE|||YES|<nil>|NO",
			"t2|1|t2_b_c_index|1|b|A|0|<nil>|<nil>|YES|BTREE|||YES|<nil>|NO",
			"t2|1|t2_b_c_index|2|c|A|0|<nil>|<nil>||BTREE|||YES|<nil>|NO",
			"t2|1|t2_c_b_index|1|c|A|0|<nil>|<nil>||BTREE|||YES|<nil>|NO",
			"t2|1|t2_c_b_index|2|b|A|0|<nil>|<nil>|YES|BTREE|||YES|<nil>|NO"))
}

// TestShowPerformanceSchema tests for Issue 19231
func TestShowPerformanceSchema(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// Ideally we should create a new performance_schema table here with indices that we run the tests on.
	// However, its not possible to create a new performance_schema table since its a special in memory table.
	// Instead the test below uses the default index on the table.
	tk.MustQuery("SHOW INDEX FROM performance_schema.events_statements_summary_by_digest").Check(
		testkit.Rows("events_statements_summary_by_digest 0 SCHEMA_NAME 1 SCHEMA_NAME A 0 <nil> <nil> YES BTREE   YES <nil> NO",
			"events_statements_summary_by_digest 0 SCHEMA_NAME 2 DIGEST A 0 <nil> <nil> YES BTREE   YES <nil> NO"))
}

func TestShowCreatePlacementPolicy(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE PLACEMENT POLICY xyz PRIMARY_REGION='us-east-1' REGIONS='us-east-1,us-east-2' FOLLOWERS=4")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz").Check(testkit.Rows("xyz CREATE PLACEMENT POLICY `xyz` PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" FOLLOWERS=4"))
	// non existent policy
	err := tk.QueryToErr("SHOW CREATE PLACEMENT POLICY doesnotexist")
	require.Equal(t, infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs("doesnotexist").Error(), err.Error())
	// alter and try second example
	tk.MustExec("ALTER PLACEMENT POLICY xyz FOLLOWERS=4")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz").Check(testkit.Rows("xyz CREATE PLACEMENT POLICY `xyz` FOLLOWERS=4"))
	tk.MustExec("DROP PLACEMENT POLICY xyz")
}

func TestShowTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create global temporary table t1 (id int) on commit delete rows")
	tk.MustExec("create global temporary table t3 (i int primary key, j int) on commit delete rows")
	// For issue https://github.com/pingcap/tidb/issues/24752
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE GLOBAL TEMPORARY TABLE `t1` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS"))
	// No panic, fix issue https://github.com/pingcap/tidb/issues/24788
	expect := "CREATE GLOBAL TEMPORARY TABLE `t3` (\n" +
		"  `i` int(11) NOT NULL,\n" +
		"  `j` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`i`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS"
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 " + expect))

	// Verify that the `show create table` result can be used to build the table.
	createTable := strings.ReplaceAll(expect, "t3", "t4")
	tk.MustExec(createTable)

	// Cover auto increment column.
	tk.MustExec(`CREATE GLOBAL TEMPORARY TABLE t5 (
	id int(11) NOT NULL AUTO_INCREMENT,
	b int(11) NOT NULL,
	pad varbinary(255) DEFAULT NULL,
	PRIMARY KEY (id),
	KEY b (b)) ON COMMIT DELETE ROWS`)
	expect = "CREATE GLOBAL TEMPORARY TABLE `t5` (\n" +
		"  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `b` int(11) NOT NULL,\n" +
		"  `pad` varbinary(255) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n" +
		"  KEY `b` (`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS"
	tk.MustQuery("show create table t5").Check(testkit.Rows("t5 " + expect))

	tk.MustExec("create temporary table t6 (i int primary key, j int)")
	expect = "CREATE TEMPORARY TABLE `t6` (\n" +
		"  `i` int(11) NOT NULL,\n" +
		"  `j` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`i`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	tk.MustQuery("show create table t6").Check(testkit.Rows("t6 " + expect))
	tk.MustExec("create temporary table t7 (i int primary key auto_increment, j int)")
	defer func() {
		tk.MustExec("commit;")
	}()
	tk.MustExec("begin;")
	tk.MustExec("insert into t7 (j) values (14)")
	tk.MustExec("insert into t7 (j) values (24)")
	tk.MustQuery("select * from t7").Check(testkit.Rows("1 14", "2 24"))
	expect = "CREATE TEMPORARY TABLE `t7` (\n" +
		"  `i` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `j` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`i`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=2"
	tk.MustQuery("show create table t7").Check(testkit.Rows("t7 " + expect))
}

func TestShowCachedTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("alter table t1 cache")
	tk.MustQuery("show create table t1").Check(
		testkit.Rows("t1 CREATE TABLE `t1` (\n" +
			"  `id` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /* CACHED ON */"))
	tk.MustQuery("select create_options from information_schema.tables where table_schema = 'test' and table_name = 't1'").Check(
		testkit.Rows("cached=on"))

	tk.MustExec("alter table t1 nocache")
	tk.MustQuery("show create table t1").Check(
		testkit.Rows("t1 CREATE TABLE `t1` (\n" +
			"  `id` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select create_options from information_schema.tables where table_schema = 'test' and table_name = 't1'").Check(
		testkit.Rows(""))
}

func TestShowBindingCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec(`set global tidb_mem_quota_binding_cache = 1`)
	tk.MustQuery("select @@global.tidb_mem_quota_binding_cache").Check(testkit.Rows("1"))
	tk.MustExec("admin reload bindings;")
	res := tk.MustQuery("show global bindings")
	require.Equal(t, 0, len(res.Rows()))

	tk.MustExec("create global binding for select * from t using select * from t")
	res = tk.MustQuery("show global bindings")
	require.Equal(t, 0, len(res.Rows()))

	tk.MustExec(`set global tidb_mem_quota_binding_cache = default`)
	tk.MustQuery("select @@global.tidb_mem_quota_binding_cache").Check(testkit.Rows("67108864"))
	tk.MustExec("admin reload bindings")
	res = tk.MustQuery("show global bindings")
	require.Equal(t, 1, len(res.Rows()))

	tk.MustExec("create global binding for select * from t where a > 1 using select * from t where a > 1")
	res = tk.MustQuery("show global bindings")
	require.Equal(t, 2, len(res.Rows()))
}

func TestShowBindingCacheStatus(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"0 0 0 Bytes 64 MB"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	result := tk.MustQuery("show global bindings")
	rows := result.Rows()
	require.Equal(t, len(rows), 0)
	tk.MustExec("create global binding for select * from t using select * from t")

	result = tk.MustQuery("show global bindings")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)

	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"1 1 159 Bytes 64 MB"))

	tk.MustExec(`set global tidb_mem_quota_binding_cache = 250`)
	tk.MustQuery(`select @@global.tidb_mem_quota_binding_cache`).Check(testkit.Rows("250"))
	tk.MustExec("admin reload bindings;")
	tk.MustExec("create global binding for select * from t where a > 1 using select * from t where a > 1")
	result = tk.MustQuery("show global bindings")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)
	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"1 2 187 Bytes 250 Bytes"))

	tk.MustExec("drop global binding for select * from t where a > 1")
	result = tk.MustQuery("show global bindings")
	rows = result.Rows()
	require.Equal(t, len(rows), 0)
	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"0 1 0 Bytes 250 Bytes"))

	tk.MustExec("admin reload bindings")
	result = tk.MustQuery("show global bindings")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)
	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"1 1 159 Bytes 250 Bytes"))

	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a)")

	result = tk.MustQuery("show global bindings")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)

	tk.MustQuery("show binding_cache status").Check(testkit.Rows(
		"1 1 198 Bytes 250 Bytes"))
}
