// Copyright 2022 PingCAP, Inc.
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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestSplitTableRegion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split table t index idx1 by (10000,"abcd"),(10000000);`)
	tk.MustGetErrCode(`split table t index idx1 by ("abcd");`, mysql.WarnDataTruncated)

	// Test for split index region.
	// Check min value is more than max value.
	tk.MustExec(`split table t index idx1 between (0) and (1000000000) regions 10`)
	tk.MustGetErrCode(`split table t index idx1 between (2,'a') and (1,'c') regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Check min value is invalid.
	tk.MustGetErrMsg(`split table t index idx1 between () and (1) regions 10`, "Split index `idx1` region lower value count should more than 0")

	// Check max value is invalid.
	tk.MustGetErrMsg(`split table t index idx1 between (1) and () regions 10`, "Split index `idx1` region upper value count should more than 0")

	// Check pre-split region num is too large.
	tk.MustGetErrMsg(`split table t index idx1 between (0) and (1000000000) regions 10000`, "Split index region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	tk.MustGetErrMsg(`split table t index idx1 between (0) and (1000000000) regions 0`, "Split index region num should more than 0")

	// Test truncate error msg.
	tk.MustGetErrMsg(`split table t index idx1 between ("aa") and (1000000000) regions 0`, "[types:1265]Incorrect value: 'aa' for column 'b'")

	// Test for split table region.
	tk.MustExec(`split table t between (0) and (1000000000) regions 10`)
	// Check the lower value is more than the upper value.
	tk.MustGetErrCode(`split table t between (2) and (1) regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Check the lower value is invalid.
	tk.MustGetErrMsg(`split table t between () and (1) regions 10`, "Split table region lower value count should be 1")

	// Check upper value is invalid.
	tk.MustGetErrMsg(`split table t between (1) and () regions 10`, "Split table region upper value count should be 1")

	// Check pre-split region num is too large.
	tk.MustGetErrMsg(`split table t between (0) and (1000000000) regions 10000`, "Split table region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	tk.MustGetErrMsg(`split table t between (0) and (1000000000) regions 0`, "Split table region num should more than 0")

	// Test truncate error msg.
	tk.MustGetErrMsg(`split table t between ("aa") and (1000000000) regions 10`, "[types:1265]Incorrect value: 'aa' for column '_tidb_rowid'")

	// Test split table region step is too small.
	tk.MustGetErrCode(`split table t between (0) and (100) regions 10`, errno.ErrInvalidSplitRegionRanges)

	// Test split region by syntax.
	tk.MustExec(`split table t by (0),(1000),(1000000)`)

	// Test split region twice to test for multiple batch split region requests.
	tk.MustExec("create table t1(a int, b int)")
	tk.MustQuery("split table t1 between(0) and (10000) regions 10;").Check(testkit.Rows("9 1"))
	tk.MustQuery("split table t1 between(10) and (10010) regions 5;").Check(testkit.Rows("4 1"))

	// Test split region for partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (1000000) regions 5;").Check(testkit.Rows("20 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition table t between (1000000) and (100000000) regions 10;").Check(testkit.Rows("45 1"))

	// Test split region for partition table with specified partition.
	tk.MustQuery("split table t partition (p1,p2) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition table t partition (p3,p4) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
}

func TestSplitRegionEdgeCase(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint(20) auto_increment primary key);")
	tk.MustExec("split table t between (-9223372036854775808) and (9223372036854775807) regions 16;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int(20) auto_increment primary key);")
	tk.MustGetErrCode("split table t between (-9223372036854775808) and (9223372036854775807) regions 16;", errno.ErrDataOutOfRange)
}

func TestClusterIndexSplitTableIntegration(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_cluster_index_index_split_table_integration;")
	tk.MustExec("create database test_cluster_index_index_split_table_integration;")
	tk.MustExec("use test_cluster_index_index_split_table_integration;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t (a varchar(255), b double, c int, primary key (a, b));")

	// Value list length not match.
	lowerMsg := "Split table region lower value count should be 2"
	upperMsg := "Split table region upper value count should be 2"
	tk.MustGetErrMsg("split table t between ('aaa') and ('aaa', 100.0) regions 10;", lowerMsg)
	tk.MustGetErrMsg("split table t between ('aaa', 1.0) and ('aaa', 100.0, 11) regions 10;", upperMsg)

	// Value type not match.
	errMsg := "[types:1265]Incorrect value: 'aaa' for column 'b'"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and (100.0, 'aaa') regions 10;", errMsg)

	// lower bound >= upper bound.
	errMsg = "[executor:8212]Failed to split region ranges: Split table `t` region lower value (aaa,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and ('aaa', 0.0) regions 10;", errMsg)
	errMsg = "[executor:8212]Failed to split region ranges: Split table `t` region lower value (bbb,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split table t between ('bbb', 0.0) and ('aaa', 0.0) regions 10;", errMsg)

	// Exceed limit 1000.
	errMsg = "Split table region num exceeded the limit 1000"
	tk.MustGetErrMsg("split table t between ('aaa', 0.0) and ('aaa', 0.1) regions 100000;", errMsg)

	// Split on null values.
	errMsg = "[planner:1048]Column 'a' cannot be null"
	tk.MustGetErrMsg("split table t between (null, null) and (null, null) regions 1000;", errMsg)
	tk.MustGetErrMsg("split table t by (null, null);", errMsg)

	// Success.
	tk.MustExec("split table t between ('aaa', 0.0) and ('aaa', 100.0) regions 10;")
	tk.MustExec("split table t by ('aaa', 0.0), ('aaa', 20.0), ('aaa', 100.0);")
	tk.MustExec("split table t by ('aaa', 100.0), ('qqq', 20.0), ('zzz', 100.0), ('zzz', 1000.0);")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key(a, c, d));")
	tk.MustQuery("split table t between (0, 0, 0) and (0, 0, 1) regions 1000;").Check(testkit.Rows("999 1"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key(d, a, c));")
	tk.MustQuery("split table t by (0, 0, 0), (1, 2, 3), (65535, 65535, 65535);").Check(testkit.Rows("3 1"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b decimal, c int, primary key (a, b));")
	errMsg = "[types:1265]Incorrect value: '' for column 'b'"
	tk.MustGetErrMsg("split table t by ('aaa', '')", errMsg)
}

func TestClusterIndexShowTableRegion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set global tidb_scatter_region = 1")
	tk.MustExec("drop database if exists cluster_index_regions;")
	tk.MustExec("create database cluster_index_regions;")
	tk.MustExec("use cluster_index_regions;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a int, b int, c int, primary key(a, b));")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2);")
	tk.MustQuery("split table t between (1, 0) and (2, 3) regions 2;").Check(testkit.Rows("1 1"))
	rows := tk.MustQuery("show table t regions").Rows()
	tbl := external.GetTableByName(t, tk, "cluster_index_regions", "t")
	// Check the region start key.
	require.Regexp(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_r_03800000000000000183800000000000", tbl.Meta().ID), rows[1][1])

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustQuery("split table t between (0) and (100000) regions 2;").Check(testkit.Rows("1 1"))
	rows = tk.MustQuery("show table t regions").Rows()
	tbl = external.GetTableByName(t, tk, "cluster_index_regions", "t")
	// Check the region start key is int64.
	require.Regexp(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_r_50000", tbl.Meta().ID), rows[1][1])
}

func TestShowTableRegion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_regions")
	tk.MustExec("set global tidb_scatter_region = 1")
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("create table t_regions (a int key, b int, c int, index idx(b), index idx2(c))")
	tk.MustGetErrMsg(
		"split partition table t_regions partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannercore.ErrPartitionClauseOnNonpartitioned.Error())

	// Test show table regions.
	tk.MustQuery(`split table t_regions between (-10000) and (10000) regions 4;`).Check(testkit.Rows("4 1"))
	re := tk.MustQuery("show table t_regions regions")

	// Test show table regions and split table on global temporary table.
	tk.MustExec("drop table if exists t_regions_temporary_table")
	tk.MustExec("create global temporary table t_regions_temporary_table (a int key, b int, c int, index idx(b), index idx2(c)) ON COMMIT DELETE ROWS;")
	// Test show table regions.
	tk.MustGetErrMsg(
		"show table t_regions_temporary_table regions",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions").Error())
	// Test split table.
	tk.MustGetErrMsg(
		"split table t_regions_temporary_table between (-10000) and (10000) regions 4;",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustGetErrMsg(
		"split partition table t_regions_temporary_table partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustExec("drop table if exists t_regions_temporary_table")
	// Test pre split regions
	tk.MustGetErrMsg(
		"create global temporary table temporary_table_pre_split(id int ) pre_split_regions=2 ON COMMIT DELETE ROWS;",
		dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions").Error())

	// Test show table regions and split table on local temporary table
	tk.MustExec("drop table if exists t_regions_local_temporary_table")
	tk.MustExec("create temporary table t_regions_local_temporary_table (a int key, b int, c int, index idx(b), index idx2(c));")
	// Test show table regions.
	tk.MustGetErrMsg(
		"show table t_regions_local_temporary_table regions",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions").Error())
	// Test split table.
	tk.MustGetErrMsg(
		"split table t_regions_local_temporary_table between (-10000) and (10000) regions 4;",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustGetErrMsg(
		"split partition table t_regions_local_temporary_table partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannercore.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustExec("drop table if exists t_regions_local_temporary_table")
	// Test pre split regions
	tk.MustGetErrMsg(
		"create temporary table local_temporary_table_pre_split(id int ) pre_split_regions=2;",
		dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions").Error())

	rows := re.Rows()
	// Table t_regions should have 5 regions now.
	// 4 regions to store record data.
	// 1 region to store index data.
	require.Len(t, rows, 5)
	require.Len(t, rows[0], 11)
	tbl := external.GetTableByName(t, tk, "test", "t_regions")
	// Check the region start key.
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID), rows[3][1])
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[4][2])

	// Test show table index regions.
	tk.MustQuery(`split table t_regions index idx between (-1000) and (1000) regions 4;`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	require.Len(t, rows, 4)
	// Check the region start key.
	require.Regexp(t, fmt.Sprintf("t_%d.*", tbl.Meta().ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[3][1])

	re = tk.MustQuery("show table t_regions regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 9 regions now.
	// 4 regions to store record data.
	// 4 region to store index idx data.
	// 1 region to store index idx2 data.
	require.Len(t, rows, 9)
	// Check the region start key.
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID), rows[3][1])
	require.Regexp(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[4][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[5][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[6][1])
	require.Equal(t, fmt.Sprintf("t_%d_i_2_", tbl.Meta().ID), rows[7][2])
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[8][2])

	// Test unsigned primary key and wait scatter finish.
	tk.MustExec("drop table if exists t_regions")
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("create table t_regions (a int unsigned key, b int, index idx(b))")

	// Test show table regions.
	tk.MustExec(`set @@session.tidb_wait_split_region_finish=1;`)
	tk.MustQuery(`split table t_regions by (2500),(5000),(7500);`).Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show table t_regions regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	require.Len(t, rows, 4)
	tbl = external.GetTableByName(t, tk, "test", "t_regions")
	// Check the region start key.
	require.Regexp(t, "t_.*", rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_2500", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_7500", tbl.Meta().ID), rows[3][1])

	// Test show table index regions.
	tk.MustQuery(`split table t_regions index idx by (250),(500),(750);`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	require.Len(t, rows, 4)
	// Check the region start key.
	require.Equal(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[3][1])

	// Test show table regions for partition table when disable split region when create table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	require.Len(t, rows, 1)
	require.Regexp(t, "t_.*", rows[0][1])

	// Test show table regions for partition table when enable split region when create table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	require.Len(t, rows, 3)
	tbl = external.GetTableByName(t, tk, "test", "partition_t")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[0].ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[1].ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[2].ID), rows[2][1])

	// Test split partition region when add new partition.
	tk.MustExec("drop table if exists partition_t;")
	tk.MustExec(`create table partition_t (a int, b int,index(a)) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20),
		PARTITION p2 VALUES LESS THAN (30));`)
	tk.MustExec(`alter table partition_t add partition ( partition p3 values less than (40), partition p4 values less than (50) );`)
	re = tk.MustQuery("show table partition_t regions")
	rows = re.Rows()
	require.Len(t, rows, 5)
	tbl = external.GetTableByName(t, tk, "test", "partition_t")
	partitionDef = tbl.Meta().GetPartitionInfo().Definitions
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[0].ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[1].ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[2].ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[3].ID), rows[3][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[4].ID), rows[4][1])

	// Test pre-split table region when create table.
	tk.MustExec("drop table if exists t_pre")
	tk.MustExec("create table t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2;")
	re = tk.MustQuery("show table t_pre regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	require.Len(t, rows, 4)
	tbl = external.GetTableByName(t, tk, "test", "t_pre")
	require.Equal(t, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID), rows[3][1])

	// Test pre-split table region when create table.
	tk.MustExec("drop table if exists pt_pre")
	tk.MustExec("create table pt_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2 partition by hash(a) partitions 3;")
	re = tk.MustQuery("show table pt_pre regions")
	rows = re.Rows()
	// Table t_regions should have 4 regions now.
	require.Len(t, rows, 12)
	tbl = external.GetTableByName(t, tk, "test", "pt_pre")
	pi := tbl.Meta().GetPartitionInfo().Definitions
	require.Len(t, pi, 3)
	for i, p := range pi {
		require.Equal(t, fmt.Sprintf("t_%d_r_2305843009213693952", p.ID), rows[1+4*i][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_4611686018427387904", p.ID), rows[2+4*i][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_6917529027641081856", p.ID), rows[3+4*i][1])
	}

	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)

	// Test split partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (4000000) regions 4;").Check(testkit.Rows("15 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	require.Len(t, rows, 20)
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().GetPartitionInfo().Definitions, 5)
	for i, p := range tbl.Meta().GetPartitionInfo().Definitions {
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*4+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*4+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*4+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*4+3][1])
	}

	// Test split region for partition table with specified partition.
	tk.MustQuery("split table t partition (p4) between (1000000) and (2000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	require.Len(t, rows, 24)
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().GetPartitionInfo().Definitions, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*4+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*4+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*4+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*4+3][1])
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*4+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*4+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1200000", p.ID), rows[i*4+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1400000", p.ID), rows[i*4+3][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1600000", p.ID), rows[i*4+4][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1800000", p.ID), rows[i*4+5][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*4+6][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*4+7][1])
	}

	// Test for show table partition regions.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show table t partition (p%v) regions", i))
		rows = re.Rows()
		require.Len(t, rows, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[3][1])
	}
	re = tk.MustQuery("show table t partition (p0, p4) regions")
	rows = re.Rows()
	require.Len(t, rows, 12)
	p := tbl.Meta().GetPartitionInfo().Definitions[0]
	require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[3][1])
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[4][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[5][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1200000", p.ID), rows[6][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1400000", p.ID), rows[7][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1600000", p.ID), rows[8][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1800000", p.ID), rows[9][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[10][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[11][1])
	// Test for duplicate partition names.
	re = tk.MustQuery("show table t partition (p0, p0, p0) regions")
	rows = re.Rows()
	require.Len(t, rows, 4)
	p = tbl.Meta().GetPartitionInfo().Definitions[0]
	require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[3][1])

	// Test split partition table index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int,index idx(a)) partition by hash(a) partitions 5;")
	tk.MustQuery("split table t between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	tk.MustQuery("split table t index idx between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	require.Len(t, rows, 40)
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().GetPartitionInfo().Definitions, 5)
	for i := 0; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_r", p.ID), rows[i*8+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*8+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*8+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*8+3][1])
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*8+4][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+5][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+6][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+7][1])
	}

	// Test split index region for partition table with specified partition.
	tk.MustQuery("split table t partition (p4) index idx between (0) and (1000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	require.Len(t, rows, 44)
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().GetPartitionInfo().Definitions, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_r", p.ID), rows[i*8+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*8+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*8+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*8+3][1])
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*8+4][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+5][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+6][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+7][1])
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_r", p.ID), rows[i*8+0][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_1000000", p.ID), rows[i*8+1][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_2000000", p.ID), rows[i*8+2][1])
		require.Equal(t, fmt.Sprintf("t_%d_r_3000000", p.ID), rows[i*8+3][1])
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[i*8+4][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+5][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+6][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+7][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+8][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+9][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+10][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[i*8+11][1])
	}

	// Test show table partition region on unknown-partition.
	err := tk.QueryToErr("show table t partition (p_unknown) index idx regions")
	require.True(t, terror.ErrorEqual(err, table.ErrUnknownPartition))

	// Test show table partition index.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show table t partition (p%v) index idx regions", i))
		rows = re.Rows()
		require.Len(t, rows, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[0][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[1][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[2][1])
		require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[3][1])
	}
	re = tk.MustQuery("show table t partition (p3,p4) index idx regions")
	rows = re.Rows()
	require.Len(t, rows, 12)
	p = tbl.Meta().GetPartitionInfo().Definitions[3]
	require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[3][1])
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	require.Equal(t, fmt.Sprintf("t_%d_", p.ID), rows[4][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[5][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[6][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[7][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[8][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[9][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[10][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", p.ID), rows[11][1])

	// Test split for the second index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int,index idx(a), index idx2(b))")
	tk.MustQuery("split table t index idx2 between (0) and (4000000) regions 2;").Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show table t regions")
	rows = re.Rows()
	require.Len(t, rows, 4)
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, fmt.Sprintf("t_%d_i_3_", tbl.Meta().ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID), rows[3][1])

	// Test show table partition region on non-partition table.
	err = tk.QueryToErr("show table t partition (p3,p4) index idx regions")
	require.True(t, terror.ErrorEqual(err, plannercore.ErrPartitionClauseOnNonpartitioned))
}
