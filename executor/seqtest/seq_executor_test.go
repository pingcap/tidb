// Copyright 2015 PingCAP, Inc.
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

// Note: All the tests in this file will be executed sequentially.

package executor_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	ddltestutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestEarlyClose(t *testing.T) {
	var cluster testutils.Cluster
	store, dom, clean := testkit.CreateMockStoreAndDomain(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table earlyclose (id int primary key)")

	N := 100
	// Insert N rows.
	var values []string
	for i := 0; i < N; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert earlyclose values " + strings.Join(values, ","))

	// Get table ID for split.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("earlyclose"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), N/2)

	ctx := context.Background()
	for i := 0; i < N/2; i++ {
		rss, err := tk.Session().Execute(ctx, "select * from earlyclose order by id")
		require.NoError(t, err)
		rs := rss[0]
		req := rs.NewChunk(nil)
		require.NoError(t, rs.Next(ctx, req))
		require.NoError(t, rs.Close())
	}

	// Goroutine should not leak when error happen.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/handleTaskOnceError", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/handleTaskOnceError"))
	}()
	rss, err := tk.Session().Execute(ctx, "select * from earlyclose")
	require.NoError(t, err)
	rs := rss[0]
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.Error(t, err)
	require.NoError(t, rs.Close())
}

type stats struct {
}

func (s stats) GetScope(_ string) variable.ScopeFlag { return variable.DefaultStatusVarScopeFlag }

func (s stats) Stats(_ *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	var a, b interface{}
	b = "123"
	m["test_interface_nil"] = a
	m["test_interface"] = b
	m["test_interface_slice"] = []interface{}{"a", "b", "c"}
	return m, nil
}

func TestShow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testSQL := `drop table if exists show_test`
	tk.MustExec(testSQL)
	testSQL = `create table SHOW_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int comment "c1_comment", c2 int, c3 int default 1, c4 text, c5 boolean, key idx_wide_c4(c3, c4(10))) ENGINE=InnoDB AUTO_INCREMENT=28934 DEFAULT CHARSET=utf8 COMMENT "table_comment";`
	tk.MustExec(testSQL)

	testSQL = "show columns from show_test;"
	result := tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 6)

	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row := result.Rows()[0]
	// For issue https://github.com/pingcap/tidb/issues/1061
	expectedRow := []interface{}{
		"SHOW_test", "CREATE TABLE `SHOW_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `c1` int(11) DEFAULT NULL COMMENT 'c1_comment',\n  `c2` int(11) DEFAULT NULL,\n  `c3` int(11) DEFAULT '1',\n  `c4` text DEFAULT NULL,\n  `c5` tinyint(1) DEFAULT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  KEY `idx_wide_c4` (`c3`,`c4`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=28934 COMMENT='table_comment'"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// For issue https://github.com/pingcap/tidb/issues/1918
	testSQL = `create table ptest(
		a int primary key,
		b double NOT NULL DEFAULT 2.0,
		c varchar(10) NOT NULL,
		d time unique,
		e timestamp NULL,
		f timestamp
	);`
	tk.MustExec(testSQL)
	testSQL = "show create table ptest;"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"ptest", "CREATE TABLE `ptest` (\n  `a` int(11) NOT NULL,\n  `b` double NOT NULL DEFAULT '2.0',\n  `c` varchar(10) NOT NULL,\n  `d` time DEFAULT NULL,\n  `e` timestamp NULL DEFAULT NULL,\n  `f` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,\n  UNIQUE KEY `d` (`d`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// Issue #4684.
	tk.MustExec("drop table if exists `t1`")
	testSQL = "create table `t1` (" +
		"`c1` tinyint unsigned default null," +
		"`c2` smallint unsigned default null," +
		"`c3` mediumint unsigned default null," +
		"`c4` int unsigned default null," +
		"`c5` bigint unsigned default null);"

	tk.MustExec(testSQL)
	testSQL = "show create table t1"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"t1", "CREATE TABLE `t1` (\n" +
			"  `c1` tinyint(3) unsigned DEFAULT NULL,\n" +
			"  `c2` smallint(5) unsigned DEFAULT NULL,\n" +
			"  `c3` mediumint(8) unsigned DEFAULT NULL,\n" +
			"  `c4` int(10) unsigned DEFAULT NULL,\n" +
			"  `c5` bigint(20) unsigned DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// Issue #7665
	tk.MustExec("drop table if exists `decimalschema`")
	testSQL = "create table `decimalschema` (`c1` decimal);"
	tk.MustExec(testSQL)
	testSQL = "show create table decimalschema"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"decimalschema", "CREATE TABLE `decimalschema` (\n" +
			"  `c1` decimal(10,0) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	tk.MustExec("drop table if exists `decimalschema`")
	testSQL = "create table `decimalschema` (`c1` decimal(15));"
	tk.MustExec(testSQL)
	testSQL = "show create table decimalschema"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"decimalschema", "CREATE TABLE `decimalschema` (\n" +
			"  `c1` decimal(15,0) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// test SHOW CREATE TABLE with invisible index
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (
		a int,
		b int,
		c int UNIQUE KEY,
		d int UNIQUE KEY,
		index invisible_idx_b (b) invisible,
		index (d) invisible)`)
	expected :=
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) DEFAULT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  KEY `invisible_idx_b` (`b`) /*!80000 INVISIBLE */,\n" +
			"  KEY `d` (`d`) /*!80000 INVISIBLE */,\n" +
			"  UNIQUE KEY `c` (`c`),\n" +
			"  UNIQUE KEY `d_2` (`d`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	tk.MustQuery("show create table t").Check(testkit.Rows(expected))
	tk.MustExec("drop table t")

	testSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)

	// Test case for index type and comment
	tk.MustExec(`create table show_index (id int, c int, primary key (id), index cIdx using hash (c) comment "index_comment_for_cIdx");`)
	tk.MustExec(`create index idx1 on show_index (id) using hash;`)
	tk.MustExec(`create index idx2 on show_index (id) comment 'idx';`)
	tk.MustExec(`create index idx3 on show_index (id) using hash comment 'idx';`)
	tk.MustExec(`alter table show_index add index idx4 (id) using btree comment 'idx';`)
	tk.MustExec(`create index idx5 using hash on show_index (id) using btree comment 'idx';`)
	tk.MustExec(`create index idx6 using hash on show_index (id);`)
	tk.MustExec(`create index idx7 on show_index (id);`)
	tk.MustExec(`create index idx8 on show_index (id) visible;`)
	tk.MustExec(`create index idx9 on show_index (id) invisible;`)
	tk.MustExec(`create index expr_idx on show_index ((id*2+1))`)
	testSQL = "SHOW index from show_index;"
	tk.MustQuery(testSQL).Check(testkit.RowsWithSep("|",
		"show_index|0|PRIMARY|1|id|A|0|<nil>|<nil>||BTREE| |YES|<nil>|YES",
		"show_index|1|cIdx|1|c|A|0|<nil>|<nil>|YES|HASH||index_comment_for_cIdx|YES|<nil>|NO",
		"show_index|1|idx1|1|id|A|0|<nil>|<nil>||HASH| |YES|<nil>|NO",
		"show_index|1|idx2|1|id|A|0|<nil>|<nil>||BTREE||idx|YES|<nil>|NO",
		"show_index|1|idx3|1|id|A|0|<nil>|<nil>||HASH||idx|YES|<nil>|NO",
		"show_index|1|idx4|1|id|A|0|<nil>|<nil>||BTREE||idx|YES|<nil>|NO",
		"show_index|1|idx5|1|id|A|0|<nil>|<nil>||BTREE||idx|YES|<nil>|NO",
		"show_index|1|idx6|1|id|A|0|<nil>|<nil>||HASH| |YES|<nil>|NO",
		"show_index|1|idx7|1|id|A|0|<nil>|<nil>||BTREE| |YES|<nil>|NO",
		"show_index|1|idx8|1|id|A|0|<nil>|<nil>||BTREE| |YES|<nil>|NO",
		"show_index|1|idx9|1|id|A|0|<nil>|<nil>||BTREE| |NO|<nil>|NO",
		"show_index|1|expr_idx|1|NULL|A|0|<nil>|<nil>|YES|BTREE| |YES|`id` * 2 + 1|NO",
	))

	// For show like with escape
	testSQL = `show tables like 'SHOW\_test'`
	result = tk.MustQuery(testSQL)
	rows := result.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"SHOW_test"}, rows[0])

	var ss stats
	variable.RegisterStatistics(ss)
	testSQL = "show status like 'character_set_results';"
	result = tk.MustQuery(testSQL)
	require.NotNil(t, result.Rows())

	tk.MustQuery("SHOW PROCEDURE STATUS WHERE Db='test'").Check(testkit.Rows())
	tk.MustQuery("SHOW TRIGGERS WHERE `Trigger` ='test'").Check(testkit.Rows())
	tk.MustQuery("SHOW PROCESSLIST;").Check(testkit.Rows())
	tk.MustQuery("SHOW FULL PROCESSLIST;").Check(testkit.Rows())
	tk.MustQuery("SHOW EVENTS WHERE Db = 'test'").Check(testkit.Rows())
	tk.MustQuery("SHOW PLUGINS").Check(testkit.Rows())
	tk.MustQuery("SHOW PROFILES").Check(testkit.Rows())

	// +-------------+--------------------+--------------+------------------+-------------------+
	// | File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
	// +-------------+--------------------+--------------+------------------+-------------------+
	// | tidb-binlog | 400668057259474944 |              |                  |                   |
	// +-------------+--------------------+--------------+------------------+-------------------+
	result = tk.MustQuery("SHOW MASTER STATUS")
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	require.Len(t, row, 5)
	require.NotEqual(t, "0", row[1].(string))

	tk.MustQuery("SHOW PRIVILEGES")

	// Test show create database
	testSQL = `create database show_test_DB`
	tk.MustExec(testSQL)
	testSQL = "show create database show_test_DB;"
	tk.MustQuery(testSQL).Check(testkit.RowsWithSep("|",
		"show_test_DB|CREATE DATABASE `show_test_DB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	testSQL = "show create database if not exists show_test_DB;"
	tk.MustQuery(testSQL).Check(testkit.RowsWithSep("|",
		"show_test_DB|CREATE DATABASE /*!32312 IF NOT EXISTS*/ `show_test_DB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	tk.MustExec("use show_test_DB")
	result = tk.MustQuery("SHOW index from show_index from test where Column_name = 'c'")
	require.Len(t, result.Rows(), 1)

	// Test show full columns
	// for issue https://github.com/pingcap/tidb/issues/4224
	tk.MustExec(`drop table if exists show_test_comment`)
	tk.MustExec(`create table show_test_comment (id int not null default 0 comment "show_test_comment_id")`)
	tk.MustQuery(`show full columns from show_test_comment`).Check(testkit.RowsWithSep("|",
		"id|int(11)|<nil>|NO||0||select,insert,update,references|show_test_comment_id",
	))

	// Test show create table with AUTO_INCREMENT option
	// for issue https://github.com/pingcap/tidb/issues/3747
	tk.MustExec(`drop table if exists show_auto_increment`)
	tk.MustExec(`create table show_auto_increment (id int key auto_increment) auto_increment=4`)
	tk.MustQuery(`show create table show_auto_increment`).Check(testkit.RowsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=4",
	))
	// for issue https://github.com/pingcap/tidb/issues/4678
	autoIDStep := autoid.GetStep()
	tk.MustExec("insert into show_auto_increment values(20)")
	autoID := autoIDStep + 21
	tk.MustQuery(`show create table show_auto_increment`).Check(testkit.RowsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT="+strconv.Itoa(int(autoID)),
	))
	tk.MustExec(`drop table show_auto_increment`)
	tk.MustExec(`create table show_auto_increment (id int primary key auto_increment)`)
	tk.MustQuery(`show create table show_auto_increment`).Check(testkit.RowsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("insert into show_auto_increment values(10)")
	autoID = autoIDStep + 11
	tk.MustQuery(`show create table show_auto_increment`).Check(testkit.RowsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT="+strconv.Itoa(int(autoID)),
	))

	// Test show table with column's comment contain escape character
	// for issue https://github.com/pingcap/tidb/issues/4411
	tk.MustExec(`drop table if exists show_escape_character`)
	tk.MustExec(`create table show_escape_character(id int comment 'a\rb\nc\td\0ef')`)
	tk.MustQuery(`show create table show_escape_character`).Check(testkit.RowsWithSep("|",
		""+
			"show_escape_character CREATE TABLE `show_escape_character` (\n"+
			"  `id` int(11) DEFAULT NULL COMMENT 'a\\rb\\nc	d\\0ef'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// for issue https://github.com/pingcap/tidb/issues/4424
	tk.MustExec("drop table if exists show_test")
	testSQL = `create table show_test(
		a varchar(10) COMMENT 'a\nb\rc\td\0e'
	) COMMENT='a\nb\rc\td\0e';`
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` varchar(10) DEFAULT NULL COMMENT 'a\\nb\\rc	d\\0e'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='a\\nb\\rc	d\\0e'"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// for issue https://github.com/pingcap/tidb/issues/4425
	tk.MustExec("drop table if exists show_test")
	testSQL = `create table show_test(
		a varchar(10) DEFAULT 'a\nb\rc\td\0e'
	);`
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` varchar(10) DEFAULT 'a\\nb\\rc	d\\0e'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// for issue https://github.com/pingcap/tidb/issues/4426
	tk.MustExec("drop table if exists show_test")
	testSQL = `create table show_test(
		a bit(1),
		b bit(32) DEFAULT 0b0,
		c bit(1) DEFAULT 0b1,
		d bit(10) DEFAULT 0b1010
	);`
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` bit(1) DEFAULT NULL,\n  `b` bit(32) DEFAULT b'0',\n  `c` bit(1) DEFAULT b'1',\n  `d` bit(10) DEFAULT b'1010'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// for issue #4255
	result = tk.MustQuery(`show function status like '%'`)
	result.Check(result.Rows())
	result = tk.MustQuery(`show plugins like '%'`)
	result.Check(result.Rows())

	// for issue #4740
	testSQL = `drop table if exists t`
	tk.MustExec(testSQL)
	testSQL = `create table t (a int1, b int2, c int3, d int4, e int8)`
	tk.MustExec(testSQL)
	testSQL = `show create table t;`
	result = tk.MustQuery(testSQL)
	require.Len(t, result.Rows(), 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"t",
		"CREATE TABLE `t` (\n" +
			"  `a` tinyint(4) DEFAULT NULL,\n" +
			"  `b` smallint(6) DEFAULT NULL,\n" +
			"  `c` mediumint(9) DEFAULT NULL,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  `e` bigint(20) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	for i, r := range row {
		require.Equal(t, expectedRow[i], r)
	}

	// Test get default collate for a specified charset.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int) default charset=utf8mb4`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test range partition
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (a int) PARTITION BY RANGE(a) (
 	PARTITION p0 VALUES LESS THAN (10),
 	PARTITION p1 VALUES LESS THAN (20),
 	PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"+"\nPARTITION BY RANGE (`a`)\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n PARTITION `p2` VALUES LESS THAN (MAXVALUE))",
	))

	tk.MustExec(`drop table if exists t`)
	_, err := tk.Exec(`CREATE TABLE t (x int, y char) PARTITION BY RANGE(y) (
 	PARTITION p0 VALUES LESS THAN (10),
 	PARTITION p1 VALUES LESS THAN (20),
 	PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	require.Error(t, err)

	// Test range columns partition
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (a int, b int, c char, d int) PARTITION BY RANGE COLUMNS(a,d,c) (
 	PARTITION p0 VALUES LESS THAN (5,10,'ggg'),
 	PARTITION p1 VALUES LESS THAN (10,20,'mmm'),
 	PARTITION p2 VALUES LESS THAN (15,30,'sss'),
        PARTITION p3 VALUES LESS THAN (50,MAXVALUE,MAXVALUE))`)
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL,\n"+
			"  `c` char(1) DEFAULT NULL,\n"+
			"  `d` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test hash partition
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (a int) PARTITION BY HASH(a) PARTITIONS 4`)
	tk.MustQuery("show create table t").Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n"+
			"PARTITION BY HASH (`a`) PARTITIONS 4"))

	// Test show create table compression type.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`CREATE TABLE t1 (c1 INT) COMPRESSION="zlib";`)
	tk.MustQuery("show create table t1").Check(testkit.RowsWithSep("|",
		"t1 CREATE TABLE `t1` (\n"+
			"  `c1` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMPRESSION='zlib'",
	))

	// Test show create table year type
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(y year unsigned signed zerofill zerofill, x int, primary key(y));`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `y` year(4) NOT NULL,\n"+
			"  `x` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`y`) /*T![clustered_index] NONCLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show create table with zerofill flag
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(id int primary key, val tinyint(10) zerofill);`)
	tk.MustQuery(`show create table t`).Check(testkit.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  `val` tinyint(10) unsigned zerofill DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show columns with different types of default value
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(
		c0 int default 1,
		c1 int default b'010',
		c2 bigint default x'A7',
		c3 bit(8) default b'00110001',
		c4 varchar(6) default b'00110001',
		c5 varchar(6) default '\'C6\'',
		c6 enum('s', 'm', 'l', 'xl') default 'xl',
		c7 set('a', 'b', 'c', 'd') default 'a,c,c',
		c8 datetime default current_timestamp on update current_timestamp,
		c9 year default '2014',
		c10 enum('2', '3', '4') default 2
	);`)
	tk.MustQuery(`show columns from t`).Check(testkit.RowsWithSep("|",
		"c0|int(11)|YES||1|",
		"c1|int(11)|YES||2|",
		"c2|bigint(20)|YES||167|",
		"c3|bit(8)|YES||b'110001'|",
		"c4|varchar(6)|YES||1|",
		"c5|varchar(6)|YES||'C6'|",
		"c6|enum('s','m','l','xl')|YES||xl|",
		"c7|set('a','b','c','d')|YES||a,c|",
		"c8|datetime|YES||CURRENT_TIMESTAMP|DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
		"c9|year(4)|YES||2014|",
		"c10|enum('2','3','4')|YES||3|",
	))

	// Test if 'show [status|variables]' is sorted by Variable_name (#14542)
	sqls := []string{
		"show global status;",
		"show session status;",
		"show global variables",
		"show session variables"}

	for _, sql := range sqls {
		res := tk.MustQuery(sql)
		require.NotNil(t, res)
		sorted := res.Sort()
		require.Equal(t, sorted, res)
	}
}

func TestShowStatsHealthy(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create index idx on t(a)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("insert into t values (1), (2)")
	do, _ := session.GetDomain(store)
	err := do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	require.NoError(t, err)
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("insert into t values (3), (4), (5), (6), (7), (8), (9), (10)")
	err = do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	require.NoError(t, err)
	err = do.StatsHandle().Update(do.InfoSchema())
	require.NoError(t, err)
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  19"))
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  100"))
	tk.MustExec("delete from t")
	err = do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	require.NoError(t, err)
	err = do.StatsHandle().Update(do.InfoSchema())
	require.NoError(t, err)
	tk.MustQuery("show stats_healthy").Check(testkit.Rows("test t  0"))
}

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func TestIndexDoubleReadClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	if _, ok := store.GetClient().(*copr.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := atomic.LoadInt32(&executor.LookupTableTaskChannelSize)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, 1)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_index_lookup_size = '10'")
	tk.MustExec("use test")
	tk.MustExec("create table dist (id int primary key, c_idx int, c_col int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustExec("insert dist values " + strings.Join(values, ","))

	rs, err := tk.Exec("select * from dist where c_idx between 0 and 100")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, err)
	keyword := "pickAndExecTask"
	require.NoError(t, rs.Close())
	time.Sleep(time.Millisecond * 10)
	require.False(t, checkGoroutineExists(keyword))
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, originSize)
}

// TestIndexMergeReaderClose checks that when a partial index worker failed to start, the goroutine doesn't
// leak.
func TestIndexMergeReaderClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index idx1 on t(a)")
	tk.MustExec("create index idx2 on t(b)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/startPartialIndexWorkerErr", "return"))
	err := tk.QueryToErr("select /*+ USE_INDEX_MERGE(t, idx1, idx2) */ * from t where a > 10 or b < 100")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/startPartialIndexWorkerErr"))
	require.Error(t, err)
	require.False(t, checkGoroutineExists("fetchLoop"))
	require.False(t, checkGoroutineExists("fetchHandles"))
	require.False(t, checkGoroutineExists("waitPartialWorkersAndCloseFetchChan"))
}

func TestParallelHashAggClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	// desc select sum(a) from (select cast(t.a as signed) as a, b from t) t group by b
	// HashAgg_8                | 2.40  | root       | group by:t.b, funcs:sum(t.a)
	// └─Projection_9           | 3.00  | root       | cast(test.t.a), test.t.b
	//   └─TableReader_11       | 3.00  | root       | data:TableFullScan_10
	//     └─TableFullScan_10   | 3.00  | cop[tikv]  | table:t, keep order:fa$se, stats:pseudo |

	// Goroutine should not leak when error happen.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/parallelHashAggError", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/parallelHashAggError"))
	}()
	ctx := context.Background()
	rss, err := tk.Session().Execute(ctx, "select sum(a) from (select cast(t.a as signed) as a, b from t) t group by b;")
	require.NoError(t, err)
	rs := rss[0]
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.EqualError(t, err, "HashAggExec.parallelExec error")
}

func TestUnparallelHashAggClose(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2)")

	// Goroutine should not leak when error happen.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/unparallelHashAggError", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/unparallelHashAggError"))
	}()
	ctx := context.Background()
	rss, err := tk.Session().Execute(ctx, "select sum(distinct a) from (select cast(t.a as signed) as a, b from t) t group by b;")
	require.NoError(t, err)
	rs := rss[0]
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	require.EqualError(t, err, "HashAggExec.unparallelExec error")
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	err := profile.WriteTo(buf, 1)
	if err != nil {
		panic(err)
	}
	str := buf.String()
	return strings.Contains(str, keyword)
}

func TestAdminShowNextID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	HelperTestAdminShowNextID(t, store, `admin show `)
	HelperTestAdminShowNextID(t, store, `show table `)
}

func HelperTestAdminShowNextID(t *testing.T, store kv.Storage, str string) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()
	step := int64(10)
	autoIDStep := autoid.GetStep()
	autoid.SetStep(step)
	defer autoid.SetStep(autoIDStep)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,tt")
	tk.MustExec("create table t(id int, c int)")
	// Start handle is 1.
	r := tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 1 AUTO_INCREMENT"))
	// Row ID is step + 1.
	tk.MustExec("insert into t values(1, 1)")
	r = tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 11 AUTO_INCREMENT"))
	// Row ID is original + step.
	for i := 0; i < int(step); i++ {
		tk.MustExec("insert into t values(10000, 1)")
	}
	r = tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 21 AUTO_INCREMENT"))
	tk.MustExec("drop table t")

	// test for a table with the primary key
	tk.MustExec("create table tt(id int primary key auto_increment, c int)")
	// Start handle is 1.
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Rows("test tt id 1 AUTO_INCREMENT"))
	// After rebasing auto ID, row ID is 20 + step + 1.
	tk.MustExec("insert into tt values(20, 1)")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Rows("test tt id 31 AUTO_INCREMENT"))
	// test for renaming the table
	tk.MustExec("drop database if exists test1")
	tk.MustExec("create database test1")
	tk.MustExec("rename table test.tt to test1.tt")
	tk.MustExec("use test1")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Rows("test1 tt id 31 AUTO_INCREMENT"))
	tk.MustExec("insert test1.tt values ()")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Rows("test1 tt id 41 AUTO_INCREMENT"))
	tk.MustExec("drop table tt")

	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	// Test for a table with auto_random primary key.
	tk.MustExec("create table t3(id bigint primary key clustered auto_random(5), c int)")
	// Start handle is 1.
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Rows("test1 t3 id 1 AUTO_RANDOM"))
	// Insert some rows.
	tk.MustExec("insert into t3 (c) values (1), (2);")
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Rows("test1 t3 id 11 AUTO_RANDOM"))
	// Rebase.
	tk.MustExec("insert into t3 (id, c) values (103, 3);")
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Rows("test1 t3 id 114 AUTO_RANDOM"))

	// Test for a sequence.
	tk.MustExec("create sequence seq1 start 15 cache 57")
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Rows("test1 seq1 _tidb_rowid 1 AUTO_INCREMENT", "test1 seq1  15 SEQUENCE"))
	r = tk.MustQuery("select nextval(seq1)")
	r.Check(testkit.Rows("15"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Rows("test1 seq1 _tidb_rowid 1 AUTO_INCREMENT", "test1 seq1  72 SEQUENCE"))
	r = tk.MustQuery("select nextval(seq1)")
	r.Check(testkit.Rows("16"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Rows("test1 seq1 _tidb_rowid 1 AUTO_INCREMENT", "test1 seq1  72 SEQUENCE"))
	r = tk.MustQuery("select setval(seq1, 96)")
	r.Check(testkit.Rows("96"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Rows("test1 seq1 _tidb_rowid 1 AUTO_INCREMENT", "test1 seq1  97 SEQUENCE"))
}

func TestNoHistoryWhenDisableRetry(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists history")
	tk.MustExec("create table history (a int)")
	tk.MustExec("set @@autocommit = 0")

	// retry_limit = 0 will not add history.
	tk.MustExec("set @@tidb_retry_limit = 0")
	tk.MustExec("insert history values (1)")
	require.Equal(t, 0, session.GetHistory(tk.Session()).Count())

	// Disable auto_retry will add history for auto committed only
	tk.MustExec("set @@autocommit = 1")
	tk.MustExec("set @@tidb_retry_limit = 10")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/keepHistory", `return(true)`))
	tk.MustExec("insert history values (1)")
	require.Equal(t, 1, session.GetHistory(tk.Session()).Count())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/keepHistory"))
	tk.MustExec("begin")
	tk.MustExec("insert history values (1)")
	require.Equal(t, 0, session.GetHistory(tk.Session()).Count())
	tk.MustExec("commit")

	// Enable auto_retry will add history for both.
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/keepHistory", `return(true)`))
	tk.MustExec("insert history values (1)")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/keepHistory"))
	require.Equal(t, 1, session.GetHistory(tk.Session()).Count())
	tk.MustExec("begin")
	tk.MustExec("insert history values (1)")
	require.Equal(t, 2, session.GetHistory(tk.Session()).Count())
	tk.MustExec("commit")
}

func TestPrepareMaxParamCountCheck(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int)")
	normalSQL, normalParams := generateBatchSQL(math.MaxUint16)
	_, err := tk.Exec(normalSQL, normalParams...)
	require.NoError(t, err)

	bigSQL, bigParams := generateBatchSQL(math.MaxUint16 + 2)
	_, err = tk.Exec(bigSQL, bigParams...)
	require.Error(t, err)
	require.EqualError(t, err, "[executor:1390]Prepared statement contains too many placeholders")
}

func generateBatchSQL(paramCount int) (sql string, paramSlice []interface{}) {
	params := make([]interface{}, 0, paramCount)
	placeholders := make([]string, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, i)
		placeholders = append(placeholders, "(?)")
	}
	return "insert into t values " + strings.Join(placeholders, ","), params
}

func TestCartesianProduct(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int)")
	plannercore.AllowCartesianProduct.Store(false)
	err := tk.ExecToErr("select * from t t1, t t2")
	require.True(t, plannercore.ErrCartesianProductUnsupported.Equal(err))
	err = tk.ExecToErr("select * from t t1 left join t t2 on 1")
	require.True(t, plannercore.ErrCartesianProductUnsupported.Equal(err))
	err = tk.ExecToErr("select * from t t1 right join t t2 on 1")
	require.True(t, plannercore.ErrCartesianProductUnsupported.Equal(err))
	plannercore.AllowCartesianProduct.Store(true)
}

func TestBatchInsertDelete(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	originLimit := atomic.LoadUint64(&kv.TxnTotalSizeLimit)
	defer func() {
		atomic.StoreUint64(&kv.TxnTotalSizeLimit, originLimit)
	}()
	// Set the limitation to a small value, make it easier to reach the limitation.
	atomic.StoreUint64(&kv.TxnTotalSizeLimit, 5500)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists batch_insert")
	tk.MustExec("create table batch_insert (c int)")
	tk.MustExec("drop table if exists batch_insert_on_duplicate")
	tk.MustExec("create table batch_insert_on_duplicate (id int primary key, c int)")
	// Insert 10 rows.
	tk.MustExec("insert into batch_insert values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)")
	r := tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("10"))
	// Insert 10 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("20"))
	// Insert 20 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("40"))
	// Insert 40 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("80"))
	// Insert 80 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("160"))
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))
	// for on duplicate key
	for i := 0; i < 320; i++ {
		tk.MustExec(fmt.Sprintf("insert into batch_insert_on_duplicate values(%d, %d);", i, i))
	}
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Rows("320"))

	// This will meet txn too large error.
	_, err := tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	require.Error(t, err)
	require.True(t, kv.ErrTxnTooLarge.Equal(err))
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))

	// Test tidb_batch_insert could not work if enable-batch-dml is disabled.
	tk.MustExec("set @@session.tidb_batch_insert=1;")
	_, err = tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	require.Error(t, err)
	require.True(t, kv.ErrTxnTooLarge.Equal(err))
	tk.MustExec("set @@session.tidb_batch_insert=0;")

	// for on duplicate key
	_, err = tk.Exec(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key update batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	require.Error(t, err)
	require.Truef(t, kv.ErrTxnTooLarge.Equal(err), "%v", err)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))

	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 1")
	defer tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 0")

	// Change to batch insert mode and batch size to 50.
	tk.MustExec("set @@session.tidb_batch_insert=1;")
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("640"))

	// Enlarge the batch size to 150 which is larger than the txn limitation (100).
	// So the insert will meet error.
	tk.MustExec("set @@session.tidb_dml_batch_size=600;")
	_, err = tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	require.Error(t, err)
	require.True(t, kv.ErrTxnTooLarge.Equal(err))
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("640"))
	// Set it back to 50.
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")

	// for on duplicate key
	_, err = tk.Exec(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key update batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	require.NoError(t, err)
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Rows("320"))

	// Disable BachInsert mode in transition.
	tk.MustExec("begin;")
	_, err = tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	require.Error(t, err)
	require.True(t, kv.ErrTxnTooLarge.Equal(err))
	tk.MustExec("rollback;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("640"))

	tk.MustExec("drop table if exists com_batch_insert")
	tk.MustExec("create table com_batch_insert (c int)")
	sql := "insert into com_batch_insert values "
	values := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		values = append(values, "(1)")
	}
	sql = sql + strings.Join(values, ",")
	tk.MustExec(sql)
	tk.MustQuery("select count(*) from com_batch_insert;").Check(testkit.Rows("200"))

	// Test case for batch delete.
	// This will meet txn too large error.
	_, err = tk.Exec("delete from batch_insert;")
	require.Error(t, err)
	require.True(t, kv.ErrTxnTooLarge.Equal(err))
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("640"))
	// Enable batch delete and set batch size to 50.
	tk.MustExec("set @@session.tidb_batch_delete=on;")
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")
	tk.MustExec("delete from batch_insert;")
	// Make sure that all rows are gone.
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("0"))
}

type checkPrioClient struct {
	tikv.Client
	priority kvrpcpb.CommandPri
	mu       struct {
		sync.RWMutex
		checkPrio bool
	}
}

func (c *checkPrioClient) setCheckPriority(priority kvrpcpb.CommandPri) {
	atomic.StoreInt32((*int32)(&c.priority), int32(priority))
}

func (c *checkPrioClient) getCheckPriority() kvrpcpb.CommandPri {
	return (kvrpcpb.CommandPri)(atomic.LoadInt32((*int32)(&c.priority)))
}

func (c *checkPrioClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.mu.RLock()
	defer func() {
		c.mu.RUnlock()
	}()
	if c.mu.checkPrio {
		switch req.Type {
		case tikvrpc.CmdCop:
			if c.getCheckPriority() != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		}
	}
	return resp, err
}

func TestCoprocessorPriority(t *testing.T) {
	cli := &checkPrioClient{}
	store, clean := testkit.CreateMockStore(t, mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}))
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.MustExec("create table t1 (id int, v int, unique index i_id (id))")
	defer tk.MustExec("drop table t")
	defer tk.MustExec("drop table t1")
	tk.MustExec("insert into t values (1)")

	// Insert some data to make sure plan build IndexLookup for t1.
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}

	cli.mu.Lock()
	cli.mu.checkPrio = true
	cli.mu.Unlock()

	cli.setCheckPriority(kvrpcpb.CommandPri_High)
	tk.MustQuery("select id from t where id = 1")
	tk.MustQuery("select * from t1 where id = 1")

	cli.setCheckPriority(kvrpcpb.CommandPri_Normal)
	tk.MustQuery("select count(*) from t")
	tk.MustExec("update t set id = 3")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t select * from t limit 2")
	tk.MustExec("delete from t")

	// Insert some data to make sure plan build IndexLookup for t.
	tk.MustExec("insert into t values (1), (2)")

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.ExpensiveThreshold = 0
	})

	cli.setCheckPriority(kvrpcpb.CommandPri_High)
	tk.MustQuery("select id from t where id = 1")
	tk.MustQuery("select * from t1 where id = 1")
	tk.MustExec("delete from t where id = 2")
	tk.MustExec("update t set id = 2 where id = 1")

	cli.setCheckPriority(kvrpcpb.CommandPri_Low)
	tk.MustQuery("select count(*) from t")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values (3)")

	// Test priority specified by SQL statement.
	cli.setCheckPriority(kvrpcpb.CommandPri_High)
	tk.MustQuery("select HIGH_PRIORITY * from t")

	cli.setCheckPriority(kvrpcpb.CommandPri_Low)
	tk.MustQuery("select LOW_PRIORITY id from t where id = 1")

	cli.setCheckPriority(kvrpcpb.CommandPri_High)
	tk.MustExec("set tidb_force_priority = 'HIGH_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Rows("3"))
	tk.MustExec("update t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Rows("0", "1"))

	cli.setCheckPriority(kvrpcpb.CommandPri_Low)
	tk.MustExec("set tidb_force_priority = 'LOW_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Rows("4"))
	tk.MustExec("update t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Rows("0", "1"))

	cli.setCheckPriority(kvrpcpb.CommandPri_Normal)
	tk.MustExec("set tidb_force_priority = 'DELAYED'")
	tk.MustQuery("select * from t").Check(testkit.Rows("5"))
	tk.MustExec("update t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Rows("0", "1"))

	cli.setCheckPriority(kvrpcpb.CommandPri_Low)
	tk.MustExec("set tidb_force_priority = 'NO_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Rows("6"))
	tk.MustExec("update t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Rows("0", "1"))

	cli.mu.Lock()
	cli.mu.checkPrio = false
	cli.mu.Unlock()
}

func TestShowForNewCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	expectRows := testkit.Rows(
		"ascii_bin ascii 65 Yes Yes 1",
		"binary binary 63 Yes Yes 1",
		"gbk_bin gbk 87  Yes 1",
		"gbk_chinese_ci gbk 28 Yes Yes 1",
		"latin1_bin latin1 47 Yes Yes 1",
		"utf8_bin utf8 83 Yes Yes 1",
		"utf8_general_ci utf8 33  Yes 1",
		"utf8_unicode_ci utf8 192  Yes 1",
		"utf8mb4_bin utf8mb4 46 Yes Yes 1",
		"utf8mb4_general_ci utf8mb4 45  Yes 1",
		"utf8mb4_unicode_ci utf8mb4 224  Yes 1",
	)
	tk.MustQuery("show collation").Check(expectRows)
	tk.MustQuery("select * from information_schema.COLLATIONS").Check(expectRows)
}

func TestForbidUnsupportedCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	mustGetUnsupportedCollation := func(sql string, coll string) {
		tk.MustGetErrMsg(sql, fmt.Sprintf("[ddl:1273]Unsupported collation when new collation is enabled: '%s'", coll))
	}

	mustGetUnsupportedCollation("select 'a' collate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedCollation("select cast('a' as char) collate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedCollation("set names utf8 collate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedCollation("set session collation_server = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedCollation("set session collation_database = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedCollation("set session collation_connection = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedCollation("set global collation_server = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedCollation("set global collation_database = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedCollation("set global collation_connection = 'utf8_roman_ci'", "utf8_roman_ci")
}

func TestAutoIncIDInRetry(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int not null auto_increment primary key)")

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (),()")
	tk.MustExec("insert into t values ()")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockCommitRetryForAutoIncID", `return(true)`))
	tk.MustExec("commit")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockCommitRetryForAutoIncID"))

	tk.MustExec("insert into t values ()")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestPessimisticConflictRetryAutoID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int not null auto_increment unique key, idx int unique key, c int);")
	concurrency := 2
	var wg sync.WaitGroup
	var err []error
	wg.Add(concurrency)
	err = make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set tidb_txn_mode = 'pessimistic'")
		tk.MustExec("set autocommit = 1")
		go func(idx int) {
			for i := 0; i < 10; i++ {
				sql := fmt.Sprintf("insert into t(idx, c) values (1, %[1]d) on duplicate key update c = %[1]d", i)
				_, e := tk.Exec(sql)
				if e != nil {
					err[idx] = e
					wg.Done()
					return
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, e := range err {
		require.NoError(t, e)
	}
}

func TestInsertFromSelectConflictRetryAutoID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int not null auto_increment unique key, idx int unique key, c int);")
	tk.MustExec("create table src (a int);")
	concurrency := 2
	var wg sync.WaitGroup
	var err []error
	wgCount := concurrency + 1
	wg.Add(wgCount)
	err = make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		go func(idx int) {
			for i := 0; i < 10; i++ {
				sql := fmt.Sprintf("insert into t(idx, c) select 1 as idx, 1 as c from src on duplicate key update c = %[1]d", i)
				_, e := tk.Exec(sql)
				if e != nil {
					err[idx] = e
					wg.Done()
					return
				}
			}
			wg.Done()
		}(i)
	}
	var insertErr error
	go func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		for i := 0; i < 10; i++ {
			_, e := tk.Exec("insert into src values (null);")
			if e != nil {
				insertErr = e
				wg.Done()
				return
			}
		}
		wg.Done()
	}()
	wg.Wait()
	for _, e := range err {
		require.NoError(t, e)
	}
	require.NoError(t, insertErr)
}

func TestAutoRandIDRetry(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database if not exists auto_random_retry")
	tk.MustExec("use auto_random_retry")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id bigint auto_random(3) primary key clustered)")

	extractMaskedOrderedHandles := func() []int64 {
		handles, err := ddltestutil.ExtractAllTableHandles(tk.Session(), "auto_random_retry", "t")
		require.NoError(t, err)
		return testutil.MaskSortHandles(handles, 3, mysql.TypeLong)
	}

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("set @@tidb_retry_limit = 10")
	tk.MustExec("begin")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (),()")
	tk.MustExec("insert into t values ()")

	session.ResetMockAutoRandIDRetryCount(5)
	fpName := "github.com/pingcap/tidb/session/mockCommitRetryForAutoRandID"
	require.NoError(t, failpoint.Enable(fpName, `return(true)`))
	tk.MustExec("commit")
	require.NoError(t, failpoint.Disable(fpName))
	tk.MustExec("insert into t values ()")
	maskedHandles := extractMaskedOrderedHandles()
	require.Equal(t, []int64{1, 2, 3, 4, 5}, maskedHandles)

	session.ResetMockAutoRandIDRetryCount(11)
	tk.MustExec("begin")
	tk.MustExec("insert into t values ()")
	require.NoError(t, failpoint.Enable(fpName, `return(true)`))
	// Insertion failure will skip the 6 in retryInfo.
	tk.MustGetErrCode("commit", errno.ErrTxnRetryable)
	require.NoError(t, failpoint.Disable(fpName))

	tk.MustExec("insert into t values ()")
	maskedHandles = extractMaskedOrderedHandles()
	require.Equal(t, []int64{1, 2, 3, 4, 5, 7}, maskedHandles)
}

func TestAutoRandRecoverTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover_auto_rand")
	defer func(originGC bool) {
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise, emulator GC will delete table record as soon as possible after execute drop table ddl.
	util.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	// Set GC safe point.
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	err := gcutil.EnableGC(tk.Session())
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()
	const autoRandIDStep = 5000
	stp := autoid.GetStep()
	autoid.SetStep(autoRandIDStep)
	defer autoid.SetStep(stp)

	// Check rebase auto_random id.
	tk.MustExec("create table t_recover_auto_rand (a bigint auto_random(5) primary key clustered);")
	tk.MustExec("insert into t_recover_auto_rand values (),(),()")
	tk.MustExec("drop table t_recover_auto_rand")
	tk.MustExec("recover table t_recover_auto_rand")
	tk.MustExec("insert into t_recover_auto_rand values (),(),()")
	hs, err := ddltestutil.ExtractAllTableHandles(tk.Session(), "test_recover", "t_recover_auto_rand")
	require.NoError(t, err)
	ordered := testutil.MaskSortHandles(hs, 5, mysql.TypeLong)

	require.Equal(t, []int64{1, 2, 3, autoRandIDStep + 1, autoRandIDStep + 2, autoRandIDStep + 3}, ordered)
}

func TestMaxDeltaSchemaCount(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.Equal(t, int64(variable.DefTiDBMaxDeltaSchemaCount), variable.GetMaxDeltaSchemaCount())

	tk.MustExec("set @@global.tidb_max_delta_schema_count= -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_max_delta_schema_count value: '-1'"))
	// Make sure a new session will load global variables.
	tk.RefreshSession()
	tk.MustExec("use test")
	require.Equal(t, int64(100), variable.GetMaxDeltaSchemaCount())
	tk.MustExec(fmt.Sprintf("set @@global.tidb_max_delta_schema_count= %v", uint64(math.MaxInt64)))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_max_delta_schema_count value: '%d'", uint64(math.MaxInt64))))
	tk.RefreshSession()
	tk.MustExec("use test")
	require.Equal(t, int64(16384), variable.GetMaxDeltaSchemaCount())
	_, err := tk.Exec("set @@global.tidb_max_delta_schema_count= invalid_val")
	require.Truef(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), "err %v", err)

	tk.MustExec("set @@global.tidb_max_delta_schema_count= 2048")
	tk.RefreshSession()
	tk.MustExec("use test")
	require.Equal(t, int64(2048), variable.GetMaxDeltaSchemaCount())
	tk.MustQuery("select @@global.tidb_max_delta_schema_count").Check(testkit.Rows("2048"))
}

func TestOOMPanicInHashJoinWhenFetchBuildRows(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	fpName := "github.com/pingcap/tidb/executor/errorFetchBuildSideRowsMockOOMPanic"
	require.NoError(t, failpoint.Enable(fpName, `panic("ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")`))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()
	err := tk.QueryToErr("select * from t as t2  join t as t1 where t1.c1=t2.c1")
	require.EqualError(t, err, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")
}

func TestIssue18744(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t, t1;`)
	tk.MustExec(`CREATE TABLE t (
  id int(11) NOT NULL,
  a bigint(20) DEFAULT NULL,
  b char(20) DEFAULT NULL,
  c datetime DEFAULT NULL,
  d double DEFAULT NULL,
  e json DEFAULT NULL,
  f decimal(40,6) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY a (a),
  KEY b (b),
  KEY c (c),
  KEY d (d),
  KEY f (f)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`CREATE TABLE t1 (
  id int(11) NOT NULL,
  a bigint(20) DEFAULT NULL,
  b char(20) DEFAULT NULL,
  c datetime DEFAULT NULL,
  d double DEFAULT NULL,
  e json DEFAULT NULL,
  f decimal(40,6) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY a (a),
  KEY b (b),
  KEY c (c),
  KEY d (d),
  KEY f (f)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`insert into t1(id) values(0),(1),(2);`)
	tk.MustExec(`insert into t values(0, 2010,  "2010-01-01 01:01:00" , "2010-01-01 01:01:00" , 2010 , 2010 , 2010.000000);`)
	tk.MustExec(`insert into t values(1 , NULL , NULL                , NULL                , NULL , NULL ,        NULL);`)
	tk.MustExec(`insert into t values(2 , 2012 , "2012-01-01 01:01:00" , "2012-01-01 01:01:00" , 2012 , 2012 , 2012.000000);`)
	tk.MustExec(`set tidb_index_lookup_join_concurrency=1`)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexHashJoinOuterWorkerErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexHashJoinOuterWorkerErr"))
	}()
	err := tk.QueryToErr(`select /*+ inl_hash_join(t2) */ t1.id, t2.id from t1 join t t2 on t1.a = t2.a order by t1.a ASC limit 1;`)
	require.EqualError(t, err, "mockIndexHashJoinOuterWorkerErr")
}

func TestIssue19410(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b enum('A', 'B'));")
	tk.MustExec("create table t1(a1 int, b1 enum('B', 'A') NOT NULL, UNIQUE KEY (b1));")
	tk.MustExec("insert into t values (1, 'A');")
	tk.MustExec("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Rows("1 A 1 A"))
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Rows("1 A 1 A"))

	tk.MustExec("create table t2(a1 int, b1 enum('C', 'D') NOT NULL, UNIQUE KEY (b1));")
	tk.MustExec("insert into t2 values (1, 'C');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t2) */ * from t join t2 on t.b = t2.b1;").Check(testkit.Rows())
	tk.MustQuery("select /*+ INL_JOIN(t2) */ * from t join t2 on t.b = t2.b1;").Check(testkit.Rows())

	tk.MustExec("create table t3(a1 int, b1 enum('A', 'B') NOT NULL, UNIQUE KEY (b1));")
	tk.MustExec("insert into t3 values (1, 'A');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t3) */ * from t join t3 on t.b = t3.b1;").Check(testkit.Rows("1 A 1 A"))
	tk.MustQuery("select /*+ INL_JOIN(t3) */ * from t join t3 on t.b = t3.b1;").Check(testkit.Rows("1 A 1 A"))
}

func TestAnalyzeNextRawErrorNoLeak(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int, c varchar(32))")
	tk.MustExec("set @@session.tidb_analyze_version = 2")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/distsql/mockNextRawError", `return(true)`))
	err := tk.ExecToErr("analyze table t1")
	require.EqualError(t, err, "mockNextRawError")
}
