// Copyright 2020 PingCAP, Inc.
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

package showtest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/pkg/autoid_service"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestShowCreateTablePlacement(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		"(PARTITION `pLow` VALUES IN ('1','2','3','5','8') COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES IN ('10','11','12'))",
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
		"(PARTITION `pLow` VALUES IN ((1,'1'),(2,'2'),(3,'3'),(5,'5'),(8,'8')) COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES IN ((10,'10'),(11,'11'),(12,'12')))",
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
		"(PARTITION `pLow` VALUES LESS THAN ('1000000') COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n"+
		" PARTITION `pMax` VALUES LESS THAN (MAXVALUE))",
	))

	tk.MustExec(`DROP TABLE IF EXISTS t`)

	tk.MustExec("create table t(a int, b varchar(255))" +
		"/*T![placement] PLACEMENT POLICY=\"x\" */" +
		"PARTITION BY RANGE COLUMNS (a,b)\n" +
		"(PARTITION pLow VALUES less than (1000000,'1000000') COMMENT 'a comment' placement policy 'x'," +
		" PARTITION pMidLow VALUES less than (1000000,MAXVALUE) COMMENT 'another comment' placement policy 'x'," +
		" PARTITION pMadMax VALUES less than (9000000,'1000000') COMMENT ='Not a comment' placement policy 'x'," +
		"partition pMax values LESS THAN (MAXVALUE, 'Does not matter...'))")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec(`insert into t values (1,'1')`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk.MustQuery(`show create table t`).Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) DEFAULT NULL,\n" +
			"  `b` varchar(255) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![placement] PLACEMENT POLICY=`x` */\n" +
			"PARTITION BY RANGE COLUMNS(`a`,`b`)\n" +
			"(PARTITION `pLow` VALUES LESS THAN (1000000,'1000000') COMMENT 'a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n" +
			" PARTITION `pMidLow` VALUES LESS THAN (1000000,MAXVALUE) COMMENT 'another comment' /*T![placement] PLACEMENT POLICY=`x` */,\n" +
			" PARTITION `pMadMax` VALUES LESS THAN (9000000,'1000000') COMMENT 'Not a comment' /*T![placement] PLACEMENT POLICY=`x` */,\n" +
			" PARTITION `pMax` VALUES LESS THAN (MAXVALUE,'Does not matter...'))"))

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

func TestShowVisibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database showdatabase")
	tk.MustExec("use showdatabase")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec(`create user 'show'@'%'`)

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil, nil))

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

func TestShowWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `create table if not exists show_warnings (a int)`
	tk.MustExec(testSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1366|Incorrect int value: 'a' for column 'a' at row 1"))
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1366|Incorrect int value: 'a' for column 'a' at row 1"))
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

func TestShowWarningsForExprPushdown(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_cost_model_version=2`)
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	testSQL := `create table if not exists show_warnings_expr_pushdown (a int, value date)`
	tk.MustExec(testSQL)

	// create tiflash replica
	{
		is := dom.InfoSchema()
		db, exists := is.SchemaByName(model.NewCIStr("test"))
		require.True(t, exists)
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
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
	tk.MustExec("explain select /*+ read_from_storage(tiflash[show_warnings_expr_pushdown]) */ max(md5(value)) from show_warnings_expr_pushdown group by a")
	require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Scalar function 'md5'(signature: MD5, return type: var_string(32)) is not supported to push down to tiflash now.", "Warning|1105|Aggregation can not be pushed to tiflash because arguments of AggFunc `max` contains unsupported exprs"))
	tk.MustExec("explain select /*+ read_from_storage(tiflash[show_warnings_expr_pushdown]) */ max(a) from show_warnings_expr_pushdown group by md5(value)")
	require.Equal(t, uint16(2), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Scalar function 'md5'(signature: MD5, return type: var_string(32)) is not supported to push down to tiflash now.", "Warning|1105|Aggregation can not be pushed to tiflash because groupByItems contain unsupported exprs"))
	tk.MustExec("set tidb_opt_distinct_agg_push_down=0")
	tk.MustExec("explain select max(distinct a) from show_warnings_expr_pushdown group by value")
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	// tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1105|Aggregation can not be pushed to storage layer in non-mpp mode because it contains agg function with distinct"))
}

func TestShowGrantsPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user show_grants")
	tk.MustExec("show grants for show_grants")
	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "%"}, nil, nil, nil))
	err := tk1.QueryToErr("show grants for root")
	require.EqualError(t, exeerrors.ErrDBaccessDenied.GenWithStackByArgs("show_grants", "%", mysql.SystemDB), err.Error())
	// Test show grants for user with auth host name `%`.
	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "127.0.0.1", AuthUsername: "show_grants", AuthHostname: "%"}, nil, nil, nil))
	tk2.MustQuery("show grants")
}

func TestShowStatsPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user show_stats")
	tk1 := testkit.NewTestKit(t, store)

	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "show_stats", Hostname: "%"}, nil, nil, nil))
	e := "[planner:1142]SHOW command denied to user 'show_stats'@'%' for table"
	err := tk1.ExecToErr("show stats_meta")
	require.ErrorContains(t, err, e)
	err = tk1.ExecToErr("SHOW STATS_BUCKETS")
	require.ErrorContains(t, err, e)
	err = tk1.ExecToErr("SHOW STATS_HISTOGRAMS")
	require.ErrorContains(t, err, e)

	eqErr := plannererrors.ErrDBaccessDenied.GenWithStackByArgs("show_stats", "%", mysql.SystemDB)
	err = tk1.ExecToErr("SHOW STATS_HEALTHY")
	require.EqualError(t, err, eqErr.Error())
	tk.MustExec("grant select on mysql.* to show_stats")
	tk1.MustExec("show stats_meta")
	tk1.MustExec("SHOW STATS_BUCKETS")
	tk1.MustExec("SHOW STATS_HEALTHY")
	tk1.MustExec("SHOW STATS_HISTOGRAMS")

	tk.MustExec("create user a@'%' identified by '';")
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "a", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("grant select on mysql.stats_meta to a@'%';")
	tk.MustExec("grant select on mysql.stats_buckets to a@'%';")
	tk.MustExec("grant select on mysql.stats_histograms to a@'%';")
	tk1.MustExec("show stats_meta")
	tk1.MustExec("SHOW STATS_BUCKETS")
	tk1.MustExec("SHOW STATS_HISTOGRAMS")
}

func TestIssue18878(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER 'root'@'8.8.%'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "9.9.9.9", AuthHostname: "%"}, nil, nil, nil))

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "8.8.8.8", AuthHostname: "8.8.%"}, nil, nil, nil))
	tk.MustQuery("show grants").Check(testkit.Rows("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"))
	tk1.MustQuery("show grants").Check(testkit.Rows("GRANT USAGE ON *.* TO 'root'@'8.8.%'"))
}

func TestIssue10549(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE newdb;")
	tk.MustExec("CREATE ROLE 'app_developer';")
	tk.MustExec("GRANT ALL ON newdb.* TO 'app_developer';")
	tk.MustExec("CREATE USER 'dev';")
	tk.MustExec("GRANT 'app_developer' TO 'dev';")
	tk.MustExec("SET DEFAULT ROLE app_developer TO 'dev';")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "dev", Hostname: "%", AuthUsername: "dev", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery("SHOW DATABASES;").Check(testkit.Rows("INFORMATION_SCHEMA", "newdb"))
	tk.MustQuery("SHOW GRANTS;").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON `newdb`.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON `newdb`.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR dev").Check(testkit.Rows("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
}

func TestIssue11165(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE ROLE 'r_manager';")
	tk.MustExec("CREATE USER 'manager'@'localhost';")
	tk.MustExec("GRANT 'r_manager' TO 'manager'@'localhost';")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "manager", Hostname: "localhost", AuthUsername: "manager", AuthHostname: "localhost"}, nil, nil, nil))
	tk.MustExec("SET DEFAULT ROLE ALL TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE NONE TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE 'r_manager' TO 'manager'@'localhost';")
}

// TestShow2 is moved from session_test
func TestShow2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
					c_date_default date default current_date,
					c_date_default_2 date default (curdate()),
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
			"[c_date_default date <nil> YES  CURRENT_DATE  select,insert,update,references ]\n" +
			"[c_date_default_2 date <nil> YES  CURRENT_DATE  select,insert,update,references ]\n" +
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
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format(time.DateTime)

	// The Hostname is the actual host
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	r := tk.MustQuery("show table status from test like 't'")
	r.Check(testkit.Rows(fmt.Sprintf("t InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   注释", createTime)))

	tk.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
	tk.MustQuery("show grants for current_user").Check(testkit.Rows(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
}

func TestShowCreateUser(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// Create a new user.
	tk.MustExec(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED BY 'root';`)
	tk.MustQuery("show create user 'test_show_create_user'@'%'").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))

	tk.MustExec(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED BY 'test';`)
	tk.MustQuery("show create user 'test_show_create_user'@'localhost';").
		Check(testkit.Rows(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))

	// Case: the user exists but the host portion doesn't match
	err := tk.QueryToErr("show create user 'test_show_create_user'@'asdf';")
	require.Equal(t, exeerrors.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'test_show_create_user'@'asdf'").Error(), err.Error())

	// Case: a user that doesn't exist
	err = tk.QueryToErr("show create user 'aaa'@'localhost';")
	require.Equal(t, exeerrors.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'aaa'@'localhost'").Error(), err.Error())

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, nil, nil)
	tk.MustQuery("show create user current_user").
		Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT"))

	tk.MustQuery("show create user current_user()").
		Check(testkit.Rows("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT"))

	tk.MustExec("create user 'check_priv'")

	// "show create user" for other user requires the SELECT privilege on mysql database.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use mysql")
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "check_priv", Hostname: "127.0.0.1", AuthUsername: "test_show", AuthHostname: "asdf"}, nil, nil, nil))
	err = tk1.QueryToErr("show create user 'root'@'%'")
	require.Error(t, err)

	// "show create user" for current user doesn't check privileges.
	tk1.MustQuery("show create user current_user").
		Check(testkit.Rows("CREATE USER 'check_priv'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT"))

	// Creating users with `IDENTIFIED WITH 'caching_sha2_password'`.
	tk.MustExec("CREATE USER 'sha_test'@'%' IDENTIFIED WITH 'caching_sha2_password' BY 'temp_passwd'")

	// Compare only the start of the output as the salt changes every time.
	rows := tk.MustQuery("SHOW CREATE USER 'sha_test'@'%'")
	require.Equal(t, "CREATE USER 'sha_test'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$", rows.Rows()[0][0].(string)[:78])
	// Creating users with `IDENTIFIED WITH 'auth-socket'`
	tk.MustExec("CREATE USER 'sock'@'%' IDENTIFIED WITH 'auth_socket'")

	// Compare only the start of the output as the salt changes every time.
	rows = tk.MustQuery("SHOW CREATE USER 'sock'@'%'")
	require.Equal(t, "CREATE USER 'sock'@'%' IDENTIFIED WITH 'auth_socket' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT", rows.Rows()[0][0].(string))
	tk.MustExec("CREATE USER 'sock2'@'%' IDENTIFIED WITH 'auth_socket' AS 'sock3'")

	// Compare only the start of the output as the salt changes every time.
	rows = tk.MustQuery("SHOW CREATE USER 'sock2'@'%'")
	require.Equal(t, "CREATE USER 'sock2'@'%' IDENTIFIED WITH 'auth_socket' AS 'sock3' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT", rows.Rows()[0][0].(string))

	// Test ACCOUNT LOCK/UNLOCK.
	tk.MustExec("CREATE USER 'lockness'@'%' IDENTIFIED BY 'monster' ACCOUNT LOCK")
	rows = tk.MustQuery("SHOW CREATE USER 'lockness'@'%'")
	require.Equal(t, "CREATE USER 'lockness'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*BC05309E7FE12AFD4EBB9FFE7E488A6320F12FF3' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT LOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT", rows.Rows()[0][0].(string))

	// Test COMMENT and ATTRIBUTE.
	tk.MustExec("CREATE USER commentUser COMMENT '1234'")
	tk.MustQuery("SHOW CREATE USER commentUser").Check(testkit.Rows(`CREATE USER 'commentUser'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT ATTRIBUTE '{"comment": "1234"}'`))
	tk.MustExec(`CREATE USER attributeUser attribute '{"name": "Tom", "age": 19}'`)
	tk.MustQuery("SHOW CREATE USER attributeUser").Check(testkit.Rows(`CREATE USER 'attributeUser'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT ATTRIBUTE '{"age": 19, "name": "Tom"}'`))

	// Creating users with IDENTIFIED WITH 'tidb_auth_token'.
	tk.MustExec(`CREATE USER 'token_user'@'%' IDENTIFIED WITH 'tidb_auth_token' ATTRIBUTE '{"email": "user@pingcap.com"}'`)
	tk.MustQuery("SHOW CREATE USER token_user").Check(testkit.Rows(`CREATE USER 'token_user'@'%' IDENTIFIED WITH 'tidb_auth_token' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT ATTRIBUTE '{"email": "user@pingcap.com"}'`))
	tk.MustExec(`ALTER USER 'token_user'@'%' REQUIRE token_issuer 'issuer-ABC'`)
	tk.MustQuery("SHOW CREATE USER token_user").Check(testkit.Rows(`CREATE USER 'token_user'@'%' IDENTIFIED WITH 'tidb_auth_token' AS '' REQUIRE NONE token_issuer issuer-ABC PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT ATTRIBUTE '{"email": "user@pingcap.com"}'`))

	// create users with password reuse.
	tk.MustExec(`CREATE USER 'reuse_user'@'%' IDENTIFIED WITH 'tidb_auth_token' PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 3 DAY`)
	tk.MustQuery("SHOW CREATE USER reuse_user").Check(testkit.Rows(`CREATE USER 'reuse_user'@'%' IDENTIFIED WITH 'tidb_auth_token' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY 5 PASSWORD REUSE INTERVAL 3 DAY`))
	tk.MustExec(`ALTER USER 'reuse_user'@'%' PASSWORD HISTORY 50`)
	tk.MustQuery("SHOW CREATE USER reuse_user").Check(testkit.Rows(`CREATE USER 'reuse_user'@'%' IDENTIFIED WITH 'tidb_auth_token' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY 50 PASSWORD REUSE INTERVAL 3 DAY`))
	tk.MustExec(`ALTER USER 'reuse_user'@'%' PASSWORD REUSE INTERVAL 31 DAY`)
	tk.MustQuery("SHOW CREATE USER reuse_user").Check(testkit.Rows(`CREATE USER 'reuse_user'@'%' IDENTIFIED WITH 'tidb_auth_token' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY 50 PASSWORD REUSE INTERVAL 31 DAY`))

	tk.MustExec("CREATE USER 'jeffrey1'@'localhost' PASSWORD EXPIRE")
	tk.MustQuery("SHOW CREATE USER 'jeffrey1'@'localhost'").Check(testkit.Rows(`CREATE USER 'jeffrey1'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))
	tk.MustExec("CREATE USER 'jeffrey2'@'localhost' PASSWORD EXPIRE DEFAULT")
	tk.MustQuery("SHOW CREATE USER 'jeffrey2'@'localhost'").Check(testkit.Rows(`CREATE USER 'jeffrey2'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))
	tk.MustExec("CREATE USER 'jeffrey3'@'localhost' PASSWORD EXPIRE NEVER")
	tk.MustQuery("SHOW CREATE USER 'jeffrey3'@'localhost'").Check(testkit.Rows(`CREATE USER 'jeffrey3'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE NEVER ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))
	tk.MustExec("CREATE USER 'jeffrey4'@'localhost' PASSWORD EXPIRE INTERVAL 180 DAY")
	tk.MustQuery("SHOW CREATE USER 'jeffrey4'@'localhost'").Check(testkit.Rows(`CREATE USER 'jeffrey4'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE INTERVAL 180 DAY ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))

	tk.MustExec("CREATE USER failed_login_user")
	tk.MustQuery("SHOW CREATE USER failed_login_user").Check(testkit.Rows(`CREATE USER 'failed_login_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT`))
	tk.MustExec("ALTER USER failed_login_user FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 2")
	tk.MustQuery("SHOW CREATE USER failed_login_user").Check(testkit.Rows(`CREATE USER 'failed_login_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 2`))
	tk.MustExec("ALTER USER failed_login_user PASSWORD_LOCK_TIME UNBOUNDED")
	tk.MustQuery("SHOW CREATE USER failed_login_user").Check(testkit.Rows(`CREATE USER 'failed_login_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED`))
	tk.MustExec("ALTER USER failed_login_user comment 'testcomment'")
	tk.MustQuery("SHOW CREATE USER failed_login_user").Check(testkit.Rows(`CREATE USER 'failed_login_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED ATTRIBUTE '{"comment": "testcomment"}'`))
	tk.MustExec("ALTER USER failed_login_user  ATTRIBUTE '{\"attribute\": \"testattribute\"}'")
	tk.MustQuery("SHOW CREATE USER failed_login_user").Check(testkit.Rows(`CREATE USER 'failed_login_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED ATTRIBUTE '{"attribute": "testattribute", "comment": "testcomment"}'`))
}

func TestUnprivilegedShow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE testshow")
	tk.MustExec("USE testshow")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")

	tk.MustExec(`CREATE USER 'lowprivuser'`) // no grants

	tk.Session().Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	rs, err := tk.Exec("SHOW TABLE STATUS FROM testshow")
	require.NoError(t, err)
	require.NotNil(t, rs)

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tk.MustExec("GRANT ALL ON testshow.t1 TO 'lowprivuser'")
	tk.Session().Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	is := dom.InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("testshow"), model.NewCIStr("t1"))
	require.NoError(t, err)
	createTime := model.TSConvert2Time(tblInfo.Meta().UpdateTS).Format(time.DateTime)

	tk.MustQuery("show table status from testshow").Check(testkit.Rows(fmt.Sprintf("t1 InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   ", createTime)))
}

func TestCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint);`)

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

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

func TestAutoRandomBase(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange"))
	}()
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestShowClusterConfig(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func TestShowConfig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	rows := tk.MustQuery("show variables like '%config%'").Rows()
	require.Equal(t, 1, len(rows))
	configValue := rows[0][1].(string)

	// Test copr-cache
	coprCacheVal :=
		"\t\t\"copr-cache\": {\n" +
			"\t\t\t\"capacity-mb\": 1000\n" +
			"\t\t},\n"
	require.Equal(t, true, strings.Contains(configValue, coprCacheVal))

	// Test GlobalKill
	globalKillVal := "\"enable-global-kill\": true"
	require.True(t, strings.Contains(configValue, globalKillVal))
}

func TestShowCreateTableWithIntegerDisplayLengthWarnings(t *testing.T) {
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() {
		parsertypes.TiDBStrictIntegerDisplayWidth = false
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(2), b varchar(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `b` varchar(2) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(10), b bigint)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` bigint DEFAULT NULL,\n" +
		"  `b` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a tinyint(5), b tinyint(2), c tinyint)")
	// Here it will occur 2 warnings.
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` tinyint DEFAULT NULL,\n" +
		"  `b` tinyint DEFAULT NULL,\n" +
		"  `c` tinyint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a smallint(5), b smallint)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` smallint DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a mediumint(5), b mediumint)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` mediumint DEFAULT NULL,\n" +
		"  `b` mediumint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int1(1), b int2(2), c int3, d int4, e int8)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` tinyint(1) DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL,\n" +
		"  `c` mediumint DEFAULT NULL,\n" +
		"  `d` int DEFAULT NULL,\n" +
		"  `e` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c1 bool, c2 int(10) zerofill)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1681 Integer display width is deprecated and will be removed in a future release.",
		"Warning 1681 The ZEROFILL attribute is deprecated and will be removed in a future release. Use the LPAD function to zero-pad numbers, or store the formatted numbers in a CHAR column.",
	))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `c1` tinyint(1) DEFAULT NULL,\n" +
		"  `c2` int(10) unsigned zerofill DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestShowVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	var showSQL string
	sessionVars := make([]string, 0, len(variable.GetSysVars()))
	globalVars := make([]string, 0, len(variable.GetSysVars()))
	for _, v := range variable.GetSysVars() {
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

func TestShowCreatePlacementPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE PLACEMENT POLICY xyz PRIMARY_REGION='us-east-1' REGIONS='us-east-1,us-east-2' FOLLOWERS=4")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz").Check(testkit.Rows("xyz CREATE PLACEMENT POLICY `xyz` PRIMARY_REGION=\"us-east-1\" REGIONS=\"us-east-1,us-east-2\" FOLLOWERS=4"))
	tk.MustExec("CREATE PLACEMENT POLICY xyz2 FOLLOWERS=1 SURVIVAL_PREFERENCES=\"[zone, dc, host]\"")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz2").Check(testkit.Rows("xyz2 CREATE PLACEMENT POLICY `xyz2` FOLLOWERS=1 SURVIVAL_PREFERENCES=\"[zone, dc, host]\""))
	tk.MustExec("DROP PLACEMENT POLICY xyz2")
	// non existent policy
	err := tk.QueryToErr("SHOW CREATE PLACEMENT POLICY doesnotexist")
	require.Equal(t, infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs("doesnotexist").Error(), err.Error())
	// alter and try second example
	tk.MustExec("ALTER PLACEMENT POLICY xyz FOLLOWERS=4")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz").Check(testkit.Rows("xyz CREATE PLACEMENT POLICY `xyz` FOLLOWERS=4"))
	tk.MustExec("ALTER PLACEMENT POLICY xyz FOLLOWERS=4 SURVIVAL_PREFERENCES=\"[zone, dc, host]\"")
	tk.MustQuery("SHOW CREATE PLACEMENT POLICY xyz").Check(testkit.Rows("xyz CREATE PLACEMENT POLICY `xyz` FOLLOWERS=4 SURVIVAL_PREFERENCES=\"[zone, dc, host]\""))
	tk.MustExec("DROP PLACEMENT POLICY xyz")
}

func TestShowBindingCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)

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

func TestShowLimitReturnRow(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a int, b int, c int, d int, index idx_a(a), index idx_b(b))")
	tk.MustExec("create table t2(a int, b int, c int, d int, index idx_a(a), index idx_b(b))")
	tk.MustExec("INSERT INTO t1 VALUES(1,2,3,4)")
	tk.MustExec("INSERT INTO t1 VALUES(4,3,1,2)")
	tk.MustExec("SET @@sql_select_limit=1")
	tk.MustExec("PREPARE stmt FROM \"SHOW COLUMNS FROM t1\"")
	result := tk.MustQuery("EXECUTE stmt")
	rows := result.Rows()
	require.Equal(t, len(rows), 1)

	tk.MustExec("PREPARE stmt FROM \"select * FROM t1\"")
	result = tk.MustQuery("EXECUTE stmt")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)

	// Test case for other scenarios.
	result = tk.MustQuery("SHOW ENGINES")
	rows = result.Rows()
	require.Equal(t, len(rows), 1)

	tk.MustQuery("SHOW DATABASES like '%SCHEMA'").Check(testkit.RowsWithSep("|", "INFORMATION_SCHEMA"))

	tk.MustQuery("SHOW TABLES where tables_in_test='t2'").Check(testkit.RowsWithSep("|", "t2"))

	result = tk.MustQuery("SHOW TABLE STATUS where name='t2'")
	rows = result.Rows()
	require.Equal(t, rows[0][0], "t2")

	tk.MustQuery("SHOW COLUMNS FROM t1 where Field ='d'").Check(testkit.RowsWithSep("|", ""+
		"d int(11) YES  <nil> "))

	tk.MustQuery("Show Charset where charset='gbk'").Check(testkit.RowsWithSep("|", ""+
		"gbk Chinese Internal Code Specification gbk_chinese_ci 2"))

	tk.MustQuery("Show Variables where variable_name ='max_allowed_packet'").Check(testkit.RowsWithSep("|", ""+
		"max_allowed_packet 67108864"))

	result = tk.MustQuery("SHOW status where variable_name ='server_id'")
	rows = result.Rows()
	require.Equal(t, rows[0][0], "server_id")

	tk.MustQuery("Show Collation where collation='utf8_bin'").Check(testkit.RowsWithSep("|", ""+
		"utf8_bin utf8 83 Yes Yes 1"))

	result = tk.MustQuery("show index from t1 where key_name='idx_b'")
	rows = result.Rows()
	require.Equal(t, rows[0][2], "idx_b")
}

func TestShowBindingDigestField(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int, key(id))")
	tk.MustExec("create table t2(id int, key(id))")
	tk.MustExec("create binding for select * from t1, t2 where t1.id = t2.id using select /*+ merge_join(t1, t2)*/ * from t1, t2 where t1.id = t2.id")
	result := tk.MustQuery("show bindings;")
	rows := result.Rows()[0]
	require.Equal(t, len(rows), 11)
	require.Equal(t, rows[9], "ac1ceb4eb5c01f7c03e29b7d0d6ab567e563f4c93164184cde218f20d07fd77c")
	tk.MustExec("drop binding for select * from t1, t2 where t1.id = t2.id")
	result = tk.MustQuery("show bindings;")
	require.Equal(t, len(result.Rows()), 0)

	tk.MustExec("create global binding for select * from t1, t2 where t1.id = t2.id using select /*+ merge_join(t1, t2)*/ * from t1, t2 where t1.id = t2.id")
	result = tk.MustQuery("show global bindings;")
	rows = result.Rows()[0]
	require.Equal(t, len(rows), 11)
	require.Equal(t, rows[9], "ac1ceb4eb5c01f7c03e29b7d0d6ab567e563f4c93164184cde218f20d07fd77c")
	tk.MustExec("drop global binding for select * from t1, t2 where t1.id = t2.id")
	result = tk.MustQuery("show global bindings;")
	require.Equal(t, len(result.Rows()), 0)
}
