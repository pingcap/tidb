// Copyright 2026 PingCAP, Inc.
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

package options

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
	"github.com/stretchr/testify/require"
)

func TestSavedAnalyzeOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")
	originalVal2 := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.01")
	originalVal3 := statistics.AutoAnalyzeMinCnt
	defer func() {
		statistics.AutoAnalyzeMinCnt = originalVal3
	}()
	statistics.AutoAnalyzeMinCnt = 0

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "c")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	tk.MustExec("analyze table t with 1 topn, 2 buckets")
	is := dom.InfoSchema()
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	lastVersion := tbl.Version
	col0 := tbl.GetCol(tableInfo.Columns[0].ID)
	require.Equal(t, 2, len(col0.Buckets))
	col1 := tbl.GetCol(tableInfo.Columns[1].ID)
	require.Equal(t, 1, len(col1.TopN.TopN))
	require.Equal(t, 2, len(col1.Buckets))
	col2 := tbl.GetCol(tableInfo.Columns[2].ID)
	require.Equal(t, 2, len(col2.Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])

	// auto-analyze uses the table-level options
	tk.MustExec("insert into t values (10,10,10)")
	tk.MustExec("flush stats_delta")
	require.Nil(t, h.Update(context.Background(), is))
	h.HandleAutoAnalyze()
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	col0 = tbl.GetCol(tableInfo.Columns[0].ID)
	require.Equal(t, 2, len(col0.Buckets))

	// manual analyze uses the table-level persisted options by merging the new options
	tk.MustExec("analyze table t columns a,b with 1 samplerate, 3 buckets")
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	col0 = tbl.GetCol(tableInfo.Columns[0].ID)
	require.Equal(t, 3, len(col0.Buckets))
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	col1 = tbl.GetCol(tableInfo.Columns[1].ID)
	require.Equal(t, 1, len(col1.TopN.TopN))
	col2 = tbl.GetCol(tableInfo.Columns[2].ID)
	require.Less(t, col2.LastUpdateVersion, col0.LastUpdateVersion) // not updated since removed from list
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "1", rs.Rows()[0][0])
	require.Equal(t, "3", rs.Rows()[0][1])
	require.Equal(t, "1", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	colIDStrs := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10)}, ",")
	require.Equal(t, colIDStrs, rs.Rows()[0][4])

	// disable option persistence
	tk.MustExec("set global tidb_persist_analyze_options = false")
	// manual analyze will neither use the pre-persisted options nor persist new options
	tk.MustExec("analyze table t with 2 topn")
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	col0 = tbl.GetCol(tableInfo.Columns[0].ID)
	require.NotEqual(t, 3, len(col0.Buckets))
	rs = tk.MustQuery("select topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.NotEqual(t, "2", rs.Rows()[0][0])
}

func TestSavedPartitionAnalyzeOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(11,11,11),(12,12,12),(13,13,13),(14,14,14)")

	h := dom.StatsHandle()

	// analyze partition only sets options of partition
	tk.MustExec("analyze table t partition p0 with 1 topn, 3 buckets")
	is := dom.InfoSchema()
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)
	p0 := h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	lastVersion := p0.Version
	require.Equal(t, 3, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "-1", rs.Rows()[0][1])

	// merge partition & table level options
	tk.MustExec("analyze table t columns a,b with 0 topn, 2 buckets")
	p0 = h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	p1 := h.GetPhysicalTableStats(pi.Definitions[1].ID, tableInfo)
	require.Greater(t, p0.Version, lastVersion)
	lastVersion = p0.Version
	require.Equal(t, 2, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 2, len(p1.GetCol(tableInfo.Columns[0].ID).Buckets))
	// check column c is not analyzed
	require.Less(t, p0.GetCol(tableInfo.Columns[2].ID).LastUpdateVersion, p0.GetCol(tableInfo.Columns[0].ID).LastUpdateVersion)
	require.Less(t, p1.GetCol(tableInfo.Columns[2].ID).LastUpdateVersion, p1.GetCol(tableInfo.Columns[0].ID).LastUpdateVersion)
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	colIDStrsAB := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10)}, ",")
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])

	// analyze partition only updates this partition, and set different collist
	tk.MustExec("analyze table t partition p1 columns a,c with 1 buckets")
	p0 = h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	p1 = h.GetPhysicalTableStats(pi.Definitions[1].ID, tableInfo)
	require.Equal(t, p0.Version, lastVersion)
	require.Greater(t, p1.Version, lastVersion)
	lastVersion = p1.Version
	require.Equal(t, 1, len(p1.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 2, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	// only column c of p1 is re-analyzed
	require.Equal(t, 1, len(p1.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.NotEqual(t, 1, len(p0.GetCol(tableInfo.Columns[2].ID).Buckets))
	colIDStrsABC := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10), strconv.FormatInt(tableInfo.Columns[2].ID, 10)}, ",")
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "1", rs.Rows()[0][0])
	require.Equal(t, colIDStrsABC, rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][1])

	// analyze partition without options uses saved partition options
	tk.MustExec("analyze table t partition p0")
	p0 = h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	require.Greater(t, p0.Version, lastVersion)
	lastVersion = p0.Version
	require.Equal(t, 2, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	rs = tk.MustQuery("select buckets from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	rs = tk.MustQuery("select buckets from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])

	// merge options of statement's, partition's and table's
	tk.MustExec("analyze table t partition p0 with 3 buckets")
	p0 = h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	require.Greater(t, p0.Version, lastVersion)
	require.Equal(t, 3, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "3", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])

	// add new partitions, use table options as default
	tk.MustExec("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")
	tk.MustExec("insert into t values (21,21,21),(22,22,22),(23,23,23),(24,24,24)")
	tk.MustExec("analyze table t partition p2")
	is = dom.InfoSchema()
	table, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = table.Meta()
	pi = tableInfo.GetPartitionInfo()
	p2 := h.GetPhysicalTableStats(pi.Definitions[2].ID, tableInfo)
	require.Equal(t, 2, len(p2.GetCol(tableInfo.Columns[0].ID).Buckets))
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p2.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	if kerneltype.IsNextGen() {
		t.Log("analyze V1 cannot support in the next gen")
		return
	}
	// set analyze version back to 1, will not use persisted
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t partition p2")
	pi = tableInfo.GetPartitionInfo()
	p2 = h.GetPhysicalTableStats(pi.Definitions[2].ID, tableInfo)
	require.NotEqual(t, 2, len(p2.GetCol(tableInfo.Columns[0].ID).Buckets))

	// drop column
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("alter table t drop column b")
	tk.MustExec("analyze table t")
	colIDStrsA := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10)}, ",")
	colIDStrsAC := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[2].ID, 10)}, ",")
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsA, rs.Rows()[0][0])
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsA, rs.Rows()[0][0])
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsAC, rs.Rows()[0][0])

	// drop partition
	tk.MustExec("alter table t drop partition p1")
	is = dom.InfoSchema() // refresh infoschema
	require.Nil(t, h.GCStats(is, time.Duration(0)))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 0, len(rs.Rows()))

	// drop table
	tk.MustExec("drop table t")
	is = dom.InfoSchema() // refresh infoschema
	require.Nil(t, h.GCStats(is, time.Duration(0)))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	//require.Equal(t, len(rs.Rows()), 0) TODO
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 0, len(rs.Rows()))
}

func TestSavedAnalyzeOptionsForMultipleTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("create table t1(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t1 values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t2 values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")

	h := dom.StatsHandle()

	tk.MustExec("analyze table t1 with 1 topn, 3 buckets")
	tk.MustExec("analyze table t2 with 0 topn, 2 buckets")
	tk.MustExec("analyze table t1,t2 with 2 topn")
	is := dom.InfoSchema()
	table1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	table2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo1 := table1.Meta()
	tableInfo2 := table2.Meta()
	tblStats1 := h.GetPhysicalTableStats(tableInfo1.ID, tableInfo1)
	tblStats2 := h.GetPhysicalTableStats(tableInfo2.ID, tableInfo2)
	tbl1Col0 := tblStats1.GetCol(tableInfo1.Columns[0].ID)
	tbl2Col0 := tblStats2.GetCol(tableInfo2.Columns[0].ID)
	require.Equal(t, 3, len(tbl1Col0.Buckets))
	require.Equal(t, 2, len(tbl2Col0.Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo1.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo2.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
}
func TestSavedAnalyzeColumnOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")
	originalVal2 := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.01")
	originalVal3 := statistics.AutoAnalyzeMinCnt
	defer func() {
		statistics.AutoAnalyzeMinCnt = originalVal3
	}()
	statistics.AutoAnalyzeMinCnt = 0
	originalVal4 := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal4))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4)")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(3 * time.Second)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tk.MustExec("select * from t where b > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	tk.MustExec("analyze table t predicate columns")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tblStats := h.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	lastVersion := tblStats.Version
	// column b is analyzed
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[0].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[1].ID).LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[2].ID).LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows("PREDICATE "))

	tk.MustExec("select * from t where c > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// manually analyze uses the saved option(predicate columns).
	tk.MustExec("analyze table t")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tblStats = h.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column b, c are analyzed
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[0].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[1].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[2].ID).LastUpdateVersion)

	tk.MustExec("insert into t values (5,5,5),(6,6,6)")
	tk.MustExec("flush stats_delta")
	require.Nil(t, h.Update(context.Background(), is))
	// auto analyze uses the saved option(predicate columns).
	h.HandleAutoAnalyze()
	tblStats = h.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column b, c are analyzed
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[0].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[1].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[2].ID).LastUpdateVersion)

	tk.MustExec("analyze table t columns a")
	// TODO: the a's meta should be keep. Or the previous a's meta should be clear.
	tblStats, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	require.NoError(t, err)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column a is analyzed
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[0].ID).LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[1].ID).LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.GetCol(tblInfo.Columns[2].ID).LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows(fmt.Sprintf("LIST %v", tblInfo.Columns[0].ID)))

	tk.MustExec("analyze table t all columns")
	// TODO: the a's meta should be keep. Or the previous a's meta should be clear.
	tblStats, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	require.NoError(t, err)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column a, b, c are analyzed
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[0].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[1].ID).LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.GetCol(tblInfo.Columns[2].ID).LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows("ALL "))
}
