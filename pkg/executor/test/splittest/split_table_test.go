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

package splittest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestClusterIndexShowTableRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

	// test split regions boundary, it's too slow in TiKV env, move it here.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key(a, c, d));")
	tk.MustQuery("split table t between (0, 0, 0) and (0, 0, 1) regions 1000;").Check(testkit.Rows("999 1"))
}

func TestShowTableRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_regions")
	tk.MustExec("set global tidb_scatter_region = 1")
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("create table t_regions (a int key, b int, c int, index idx(b), index idx2(c))")
	tk.MustGetErrMsg(
		"split partition table t_regions partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannererrors.ErrPartitionClauseOnNonpartitioned.Error())

	// Test show table regions.
	tk.MustQuery(`split table t_regions between (-10000) and (10000) regions 4;`).Check(testkit.Rows("4 1"))
	re := tk.MustQuery("show table t_regions regions")

	// Test show table regions and split table on global temporary table.
	tk.MustExec("drop table if exists t_regions_temporary_table")
	tk.MustExec("create global temporary table t_regions_temporary_table (a int key, b int, c int, index idx(b), index idx2(c)) ON COMMIT DELETE ROWS;")
	// Test show table regions.
	tk.MustGetErrMsg(
		"show table t_regions_temporary_table regions",
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions").Error())
	// Test split table.
	tk.MustGetErrMsg(
		"split table t_regions_temporary_table between (-10000) and (10000) regions 4;",
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustGetErrMsg(
		"split partition table t_regions_temporary_table partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
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
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("show table regions").Error())
	// Test split table.
	tk.MustGetErrMsg(
		"split table t_regions_local_temporary_table between (-10000) and (10000) regions 4;",
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
	tk.MustGetErrMsg(
		"split partition table t_regions_local_temporary_table partition (p1,p2) index idx between (0) and (20000) regions 2;",
		plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("split table").Error())
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
	require.Len(t, rows[0], 13)
	tbl := external.GetTableByName(t, tk, "test", "t_regions")
	// Check the region start key.
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[0][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID), rows[3][1])
	require.Equal(t, fmt.Sprintf("t_%d_r", tbl.Meta().ID), rows[4][2])
	// Check scheduling constraint and scheduling state default value
	for i := range rows {
		require.Equal(t, "", rows[i][11])
		require.Equal(t, "", rows[i][12])
	}

	// Test show table index regions.
	tk.MustQuery(`split table t_regions index idx between (-1000) and (1000) regions 4;`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show table t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of table t_regions should have 4 regions now.
	require.Len(t, rows, 4)
	require.Len(t, rows[0], 13)
	// Check the region start key.
	require.Regexp(t, fmt.Sprintf("t_%d.*", tbl.Meta().ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[2][1])
	require.Regexp(t, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID), rows[3][1])
	// Check scheduling constraint and scheduling state default value
	for i := range rows {
		require.Equal(t, "", rows[i][11])
		require.Equal(t, "", rows[i][12])
	}

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
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPartitionClauseOnNonpartitioned))

	// Test scheduling info for un-partitioned table with placement policy
	tk.MustExec("drop table if exists t1_scheduling")
	tk.MustExec("drop placement policy if exists pa1")
	tk.MustExec("create placement policy p1 " +
		"PRIMARY_REGION=\"cn-east-1\" " +
		"REGIONS=\"cn-east-1,cn-east-2\"" +
		"SCHEDULE=\"EVEN\"")
	tk.MustExec("create table t1_scheduling (id int) placement policy p1")
	re = tk.MustQuery("show table t1_scheduling regions")
	rows = re.Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 13)
	tbl = external.GetTableByName(t, tk, "test", "t1_scheduling")
	require.Equal(t, fmt.Sprintf("t_%d_", tbl.Meta().ID), rows[0][1])
	require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[0][11])
	require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[0][12])

	// Test scheduling info for partitioned table with placement policy
	tk.MustExec("drop table if exists t2_scheduling")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p2 " +
		"LEADER_CONSTRAINTS=\"[+region=us-east-1]\" " +
		"FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" " +
		"FOLLOWERS=3")
	tk.MustExec("create table t2_scheduling (id INT) placement policy p1 partition by range (id) (" +
		"partition p0 values less than (100) placement policy p2," +
		"partition p1 values less than (1000)," +
		"partition p2 values less than (10000)" +
		")")
	re = tk.MustQuery("show table t2_scheduling regions")
	rows = re.Rows()
	require.Len(t, rows, 3)
	require.Len(t, rows[0], 13)
	require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"", rows[0][11])
	require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[1][11])
	require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[2][11])
	require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[0][12])
	require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[1][12])
	require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[2][12])

	// Test scheduling info for partitioned table after split to regions
	tk.MustExec("drop table if exists t3_scheduling")
	tk.MustExec("create table t3_scheduling (id INT) placement policy p1 partition by range (id) (" +
		"partition p0 values less than (100) placement policy p2," +
		"partition p1 values less than (1000)," +
		"partition p2 values less than (10000)" +
		")")
	tk.MustQuery("split partition table t3_scheduling between (0) and (10000) regions 4")
	re = tk.MustQuery("show table t3_scheduling regions")
	rows = re.Rows()
	require.Len(t, rows, 12)
	require.Len(t, rows[0], 13)
	for i := range rows {
		if i < 4 {
			require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"", rows[i][11])
		} else {
			require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[i][11])
		}
		require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[i][12])
	}

	// Test scheduling info for un-partitioned table after split index to regions
	tk.MustExec("drop table if exists t4_scheduling")
	tk.MustExec("create table t4_scheduling (id INT, val INT, index idx1(val)) placement policy p1")
	tk.MustQuery("split table t4_scheduling index idx1 between (0) and (12345) regions 3")
	re = tk.MustQuery("show table t4_scheduling regions")
	rows = re.Rows()
	require.Len(t, rows, 4)
	require.Len(t, rows[0], 13)
	for i := range rows {
		require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[i][11])
		require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[i][12])
	}

	// Test scheduling info for partitioned table after split index to regions
	tk.MustExec("drop table if exists t5_scheduling")
	tk.MustExec("create table t5_scheduling (id INT, val INT, index idx1(val)) placement policy p1 partition by range (id) (" +
		"partition p0 values less than (100) placement policy p2," +
		"partition p1 values less than (1000)," +
		"partition p2 values less than (10000)" +
		")")
	tk.MustQuery("split table t5_scheduling index idx1 between (0) and (12345) regions 3")
	re = tk.MustQuery("show table t5_scheduling regions")
	rows = re.Rows()
	require.Len(t, rows, 12)
	require.Len(t, rows[0], 13)
	for i := range rows {
		if i < 4 {
			require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"", rows[i][11])
		} else {
			require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[i][11])
		}
		require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[i][12])
	}
	re = tk.MustQuery("show table t5_scheduling index idx1 regions")
	rows = re.Rows()
	require.Len(t, rows, 9)
	require.Len(t, rows[0], 13)
	for i := range rows {
		if i < 3 {
			require.Equal(t, "LEADER_CONSTRAINTS=\"[+region=us-east-1]\" FOLLOWERS=3 FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\"", rows[i][11])
		} else {
			require.Equal(t, "PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\" SCHEDULE=\"EVEN\"", rows[i][11])
		}
		require.Equal(t, infosync.PlacementScheduleStatePending.String(), rows[i][12])
	}
}

func BenchmarkLocateRegion(t *testing.B) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t (a varchar(100), b int, index idx1(b,a))`)
	tk.MustExec("set @@tidb_wait_split_region_finish = 1")
	tk.MustQuery("split table t between (0) and (1000000000) regions 1000").Check(testkit.Rows("1000 1"))
	tk.MustQuery("split table t between (1000000000) and (2000000000) regions 1000").Check(testkit.Rows("999 1"))
	tk.MustQuery("split table t between (2000000000) and (3000000000) regions 1000").Check(testkit.Rows("999 1"))

	bo := backoff.NewBackoffer(context.Background(), 10)
	tmp := store.(helper.Storage).GetRegionCache()
	cache := copr.RegionCache{RegionCache: tmp}
	ranges := copr.NewKeyRanges([]kv.KeyRange{
		{
			StartKey: []byte("t"),
			EndKey:   []byte("u"),
		},
	})

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		_, err := cache.SplitKeyRangesByBuckets(bo, ranges)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.StopTimer()
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(BenchmarkLocateRegion)
}
