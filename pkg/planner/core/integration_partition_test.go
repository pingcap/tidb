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

package core_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
)

func TestListPartitionOrderLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_order_limit")
	tk.MustExec("use list_partition_order_limit")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tcollist (a int, b int) partition by list columns(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	vals := ""
	for i := 0; i < 50; i++ {
		if vals != "" {
			vals += ", "
		}
		vals += fmt.Sprintf("(%v, %v)", i*2+rand.Intn(2), i*2+rand.Intn(2))
	}
	tk.MustExec(`insert into tlist values ` + vals)
	tk.MustExec(`insert into tcollist values ` + vals)
	tk.MustExec(`insert into tnormal values ` + vals)

	for _, orderCol := range []string{"a", "b"} {
		for _, limitNum := range []string{"1", "5", "20", "100"} {
			randCond := fmt.Sprintf("where %v > %v", []string{"a", "b"}[rand.Intn(2)], rand.Intn(100))
			rs := tk.MustQuery(fmt.Sprintf(`select * from tnormal %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsDynamic := tk.MustQuery(fmt.Sprintf(`select * from tlist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsStatic := tk.MustQuery(fmt.Sprintf(`select * from tlist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsColDynamic := tk.MustQuery(fmt.Sprintf(`select * from tcollist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsColStatic := tk.MustQuery(fmt.Sprintf(`select * from tcollist %v order by %v limit %v`, randCond, orderCol, limitNum)).Sort()

			rs.Check(rsDynamic.Rows())
			rs.Check(rsStatic.Rows())
			rs.Check(rsColDynamic.Rows())
			rs.Check(rsColStatic.Rows())
		}
	}
}

func TestListPartitionAgg(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_agg")
	tk.MustExec("use list_partition_agg")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tcollist (a int, b int) partition by list columns(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	vals := ""
	for i := 0; i < 50; i++ {
		if vals != "" {
			vals += ", "
		}
		vals += fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100))
	}
	tk.MustExec(`insert into tlist values ` + vals)
	tk.MustExec(`insert into tcollist values ` + vals)
	tk.MustExec(`insert into tnormal values ` + vals)

	for _, aggFunc := range []string{"min", "max", "sum", "count"} {
		c1, c2 := "a", "b"
		for i := 0; i < 2; i++ {
			rs := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tnormal group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsDynamic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsStatic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tlist group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			rsColDynamic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tcollist group by %v`, c1, aggFunc, c2, c1)).Sort()

			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			rsColStatic := tk.MustQuery(fmt.Sprintf(`select %v, %v(%v) from tcollist group by %v`, c1, aggFunc, c2, c1)).Sort()

			rs.Check(rsDynamic.Rows())
			rs.Check(rsStatic.Rows())
			rs.Check(rsColDynamic.Rows())
			rs.Check(rsColStatic.Rows())
		}
	}
}

func TestListPartitionView(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_view")
	tk.MustExec("use list_partition_view")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int, b int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec(`create definer='root'@'localhost' view vlist as select a*2 as a2, a+b as ab from tlist`)
	tk.MustExec(`create table tnormal (a int, b int)`)
	tk.MustExec(`create definer='root'@'localhost' view vnormal as select a*2 as a2, a+b as ab from tnormal`)
	for i := 0; i < 10; i++ {
		a, b := rand.Intn(15), rand.Intn(100)
		tk.MustExec(fmt.Sprintf(`insert into tlist values (%v, %v)`, a, b))
		tk.MustExec(fmt.Sprintf(`insert into tnormal values (%v, %v)`, a, b))
	}

	r1 := tk.MustQuery(`select * from vlist`).Sort()
	r2 := tk.MustQuery(`select * from vnormal`).Sort()
	r1.Check(r2.Rows())

	tk.MustExec(`create table tcollist (a int, b int) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec(`create definer='root'@'localhost' view vcollist as select a*2 as a2, a+b as ab from tcollist`)
	tk.MustExec(`truncate tnormal`)
	for i := 0; i < 10; i++ {
		a, b := rand.Intn(15), rand.Intn(100)
		tk.MustExec(fmt.Sprintf(`insert into tcollist values (%v, %v)`, a, b))
		tk.MustExec(fmt.Sprintf(`insert into tnormal values (%v, %v)`, a, b))
	}

	r1 = tk.MustQuery(`select * from vcollist`).Sort()
	r2 = tk.MustQuery(`select * from vnormal`).Sort()
	r1.Check(r2.Rows())
}

func TestListPartitionRandomTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_random_tran")
	tk.MustExec("use list_partition_random_tran")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int, b int) partition by list(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tcollist (a int, b int) partition by list columns(a) (` +
		` partition p0 values in ` + genListPartition(0, 20) +
		`, partition p1 values in ` + genListPartition(20, 40) +
		`, partition p2 values in ` + genListPartition(40, 60) +
		`, partition p3 values in ` + genListPartition(60, 80) +
		`, partition p4 values in ` + genListPartition(80, 100) + `)`)
	tk.MustExec(`create table tnormal (a int, b int)`)

	inTrans := false
	for i := 0; i < 50; i++ {
		switch rand.Intn(4) {
		case 0: // begin
			if inTrans {
				continue
			}
			tk.MustExec(`begin`)
			inTrans = true
		case 1: // sel
			cond := fmt.Sprintf("where a>=%v and a<=%v", rand.Intn(50), 50+rand.Intn(50))
			rnormal := tk.MustQuery(`select * from tnormal ` + cond).Sort().Rows()
			tk.MustQuery(`select * from tlist ` + cond).Sort().Check(rnormal)
			tk.MustQuery(`select * from tcollist ` + cond).Sort().Check(rnormal)
		case 2: // insert
			values := fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100))
			tk.MustExec(`insert into tnormal values ` + values)
			tk.MustExec(`insert into tlist values ` + values)
			tk.MustExec(`insert into tcollist values ` + values)
		case 3: // commit or rollback
			if !inTrans {
				continue
			}
			tk.MustExec([]string{"commit", "rollback"}[rand.Intn(2)])
			inTrans = false
		}
	}
}

func genListPartition(begin, end int) string {
	buf := &bytes.Buffer{}
	buf.WriteString("(")
	for i := begin; i < end-1; i++ {
		buf.WriteString(fmt.Sprintf("%v, ", i))
	}
	buf.WriteString(fmt.Sprintf("%v)", end-1))
	return buf.String()
}

func BenchmarkPartitionRangeColumns(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("create schema rcb")
	tk.MustExec("use rcb")
	tk.MustExec(`create table t (` +
		`c1 int primary key clustered,` +
		`c2 varchar(255))` +
		` partition by range columns (c1)` +
		` interval (10000) first partition less than (10000) last partition less than (5120000)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := strconv.FormatInt(int64(rand.Intn(5120000)), 10)
		tk.MustExec("select * from t where c1 = " + val)
		//tk.MustExec("insert ignore into t values (" + val + ",'" + val + "')")
	}
	b.StopTimer()
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkPartitionRangeColumns,
	)
}
