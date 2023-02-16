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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/benchdaily"
	"github.com/stretchr/testify/require"
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

func TestListPartitionDML(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_dml")
	tk.MustExec("use list_partition_dml")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	tk.MustExec("insert into tlist partition(p0) values (0), (1)")
	tk.MustExec("insert into tlist partition(p0, p1) values (2), (3), (8), (9)")

	err := tk.ExecToErr("insert into tlist partition(p0) values (9)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Found a row not matching the given partition set")

	err = tk.ExecToErr("insert into tlist partition(p3) values (20)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")

	tk.MustExec("update tlist partition(p0) set a=a+1")
	tk.MustQuery("select a from tlist order by a").Check(testkit.Rows("1", "2", "3", "4", "8", "9"))
	tk.MustExec("update tlist partition(p0, p1) set a=a-1")
	tk.MustQuery("select a from tlist order by a").Check(testkit.Rows("0", "1", "2", "3", "7", "8"))

	tk.MustExec("delete from tlist partition(p1)")
	tk.MustQuery("select a from tlist order by a").Check(testkit.Rows("0", "1", "2", "3"))
	tk.MustExec("delete from tlist partition(p0, p2)")
	tk.MustQuery("select a from tlist order by a").Check(testkit.Rows())

	tk.MustExec(`create table tcollist (a int) partition by list columns(a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec("insert into tcollist partition(p0) values (0), (1)")
	tk.MustExec("insert into tcollist partition(p0, p1) values (2), (3), (8), (9)")

	err = tk.ExecToErr("insert into tcollist partition(p0) values (9)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Found a row not matching the given partition set")

	err = tk.ExecToErr("insert into tcollist partition(p3) values (20)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")

	tk.MustExec("update tcollist partition(p0) set a=a+1")
	tk.MustQuery("select a from tcollist order by a").Check(testkit.Rows("1", "2", "3", "4", "8", "9"))
	tk.MustExec("update tcollist partition(p0, p1) set a=a-1")
	tk.MustQuery("select a from tcollist order by a").Check(testkit.Rows("0", "1", "2", "3", "7", "8"))

	tk.MustExec("delete from tcollist partition(p1)")
	tk.MustQuery("select a from tcollist order by a").Check(testkit.Rows("0", "1", "2", "3"))
	tk.MustExec("delete from tcollist partition(p0, p2)")
	tk.MustQuery("select a from tcollist order by a").Check(testkit.Rows())
}

func TestListPartitionCreation(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_cre")
	tk.MustExec("use list_partition_cre")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	// with UK
	tk.MustExec("create table tuk1 (a int, b int, unique key(a)) partition by list (a) (partition p0 values in (0))")

	err := tk.ExecToErr("create table tuk2 (a int, b int, unique key(a)) partition by list (b) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNIQUE INDEX must include all columns")

	err = tk.ExecToErr("create table tuk2 (a int, b int, unique key(a), unique key(b)) partition by list (a) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNIQUE INDEX must include all columns")

	tk.MustExec("create table tcoluk1 (a int, b int, unique key(a)) partition by list columns(a) (partition p0 values in (0))")

	err = tk.ExecToErr("create table tcoluk2 (a int, b int, unique key(a)) partition by list columns(b) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNIQUE INDEX must include all columns")

	err = tk.ExecToErr("create table tcoluk2 (a int, b int, unique key(a), unique key(b)) partition by list columns(a) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNIQUE INDEX must include all columns")

	// with PK
	tk.MustExec("create table tpk1 (a int, b int, primary key(a)) partition by list (a) (partition p0 values in (0))")
	tk.MustExec("create table tpk2 (a int, b int, primary key(a, b)) partition by list (a) (partition p0 values in (0))")

	tk.MustExec("create table tcolpk1 (a int, b int, primary key(a)) partition by list columns(a) (partition p0 values in (0))")
	tk.MustExec("create table tcolpk2 (a int, b int, primary key(a, b)) partition by list columns(a) (partition p0 values in (0))")

	// with IDX
	tk.MustExec("create table tidx1 (a int, b int, key(a), key(b)) partition by list (a) (partition p0 values in (0))")
	tk.MustExec("create table tidx2 (a int, b int, key(a, b), key(b)) partition by list (a) (partition p0 values in (0))")

	tk.MustExec("create table tcolidx1 (a int, b int, key(a), key(b)) partition by list columns(a) (partition p0 values in (0))")
	tk.MustExec("create table tcolidx2 (a int, b int, key(a, b), key(b)) partition by list columns(a) (partition p0 values in (0))")

	// with expression
	tk.MustExec("create table texp1 (a int, b int) partition by list(a-10000) (partition p0 values in (0))")
	tk.MustExec("create table texp2 (a int, b int) partition by list(a%b) (partition p0 values in (0))")
	tk.MustExec("create table texp3 (a int, b int) partition by list(a*b) (partition p0 values in (0))")

	err = tk.ExecToErr("create table texp4 (a int, b int) partition by list(a|b) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "This partition function is not allowed")

	err = tk.ExecToErr("create table texp4 (a int, b int) partition by list(a^b) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "This partition function is not allowed")

	err = tk.ExecToErr("create table texp4 (a int, b int) partition by list(a&b) (partition p0 values in (0))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "This partition function is not allowed")
}

func TestListPartitionDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_ddl")
	tk.MustExec("use list_partition_ddl")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	// index
	tk.MustExec(`create table tlist (a int, b int) partition by list (a) (partition p0 values in (0))`)
	err := tk.ExecToErr(`alter table tlist add primary key (b)`) // add pk
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all")
	tk.MustExec(`alter table tlist add primary key (a)`)
	err = tk.ExecToErr(`alter table tlist add unique key (b)`) // add uk
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all")
	tk.MustExec(`alter table tlist add key (b)`) // add index
	tk.MustExec(`alter table tlist rename index b to bb`)
	tk.MustExec(`alter table tlist drop index bb`)

	tk.MustExec(`create table tcollist (a int, b int) partition by list columns (a) (partition p0 values in (0))`)
	err = tk.ExecToErr(`alter table tcollist add primary key (b)`) // add pk
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all")
	tk.MustExec(`alter table tcollist add primary key (a)`)
	err = tk.ExecToErr(`alter table tcollist add unique key (b)`) // add uk
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all")
	tk.MustExec(`alter table tcollist add key (b)`) // add index
	tk.MustExec(`alter table tcollist rename index b to bb`)
	tk.MustExec(`alter table tcollist drop index bb`)

	// column
	tk.MustExec(`alter table tlist add column c varchar(8)`)
	tk.MustExec(`alter table tlist rename column c to cc`)
	tk.MustExec(`alter table tlist drop column cc`)

	tk.MustExec(`alter table tcollist add column c varchar(8)`)
	tk.MustExec(`alter table tcollist rename column c to cc`)
	tk.MustExec(`alter table tcollist drop column cc`)

	// table
	tk.MustExec(`alter table tlist rename to tlistxx`)
	tk.MustExec(`truncate tlistxx`)
	tk.MustExec(`drop table tlistxx`)

	tk.MustExec(`alter table tcollist rename to tcollistxx`)
	tk.MustExec(`truncate tcollistxx`)
	tk.MustExec(`drop table tcollistxx`)
}

func TestListPartitionOperations(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_op")
	tk.MustExec("use list_partition_op")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14),
    partition p3 values in (15, 16, 17, 18, 19))`)
	tk.MustExec(`create table tcollist (a int) partition by list columns(a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14),
    partition p3 values in (15, 16, 17, 18, 19))`)

	// truncate
	tk.MustExec("insert into tlist values (0), (5), (10), (15)")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	tk.MustExec("alter table tlist truncate partition p0")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("10", "15", "5"))
	tk.MustExec("alter table tlist truncate partition p1, p2")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("15"))

	tk.MustExec("insert into tcollist values (0), (5), (10), (15)")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	tk.MustExec("alter table tcollist truncate partition p0")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("10", "15", "5"))
	tk.MustExec("alter table tcollist truncate partition p1, p2")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("15"))

	// drop partition
	tk.MustExec("insert into tlist values (0), (5), (10)")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	tk.MustExec("alter table tlist drop partition p0")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("10", "15", "5"))
	err := tk.ExecToErr("select * from tlist partition (p0)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")
	tk.MustExec("alter table tlist drop partition p1, p2")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("15"))
	err = tk.ExecToErr("select * from tlist partition (p1)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")
	err = tk.ExecToErr("alter table tlist drop partition p3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot remove all partitions")

	tk.MustExec("insert into tcollist values (0), (5), (10)")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	tk.MustExec("alter table tcollist drop partition p0")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("10", "15", "5"))
	err = tk.ExecToErr("select * from tcollist partition (p0)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")
	tk.MustExec("alter table tcollist drop partition p1, p2")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("15"))
	err = tk.ExecToErr("select * from tcollist partition (p1)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown partition")
	err = tk.ExecToErr("alter table tcollist drop partition p3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot remove all partitions")

	// add partition
	tk.MustExec("alter table tlist add partition (partition p0 values in (0, 1, 2, 3, 4))")
	tk.MustExec("alter table tlist add partition (partition p1 values in (5, 6, 7, 8, 9), partition p2 values in (10, 11, 12, 13, 14))")
	tk.MustExec("insert into tlist values (0), (5), (10)")
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	err = tk.ExecToErr("alter table tlist add partition (partition pxxx values in (4))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multiple definition")

	tk.MustExec("alter table tcollist add partition (partition p0 values in (0, 1, 2, 3, 4))")
	tk.MustExec("alter table tcollist add partition (partition p1 values in (5, 6, 7, 8, 9), partition p2 values in (10, 11, 12, 13, 14))")
	tk.MustExec("insert into tcollist values (0), (5), (10)")
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("0", "10", "15", "5"))
	err = tk.ExecToErr("alter table tcollist add partition (partition pxxx values in (4))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multiple definition")
}

func TestListPartitionPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.SetSession(se)
	tk.MustExec("create database list_partition_pri")
	tk.MustExec("use list_partition_pri")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int) partition by list (a) (partition p0 values in (0), partition p1 values in (1))`)

	tk.MustExec(`create user 'priv_test'@'%'`)
	tk.MustExec(`grant select on list_partition_pri.tlist to 'priv_test'`)

	tk1 := testkit.NewTestKit(t, store)
	se, err = session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "priv_test", Hostname: "%"}, nil, nil))
	tk1.SetSession(se)
	tk1.MustExec(`use list_partition_pri`)
	err = tk1.ExecToErr(`alter table tlist truncate partition p0`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "denied")
	err = tk1.ExecToErr(`alter table tlist drop partition p0`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "denied")
	err = tk1.ExecToErr(`alter table tlist add partition (partition p2 values in (2))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "denied")
	err = tk1.ExecToErr(`insert into tlist values (1)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "denied")
}

func TestListPartitionShardBits(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_shard_bits")
	tk.MustExec("use list_partition_shard_bits")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec("insert into tlist values (0), (1), (5), (6), (10), (12)")

	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("0", "1", "10", "12", "5", "6"))
	tk.MustQuery("select * from tlist partition (p0)").Sort().Check(testkit.Rows("0", "1"))
	tk.MustQuery("select * from tlist partition (p1, p2)").Sort().Check(testkit.Rows("10", "12", "5", "6"))

	tk.MustExec(`create table tcollist (a int) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec("insert into tcollist values (0), (1), (5), (6), (10), (12)")

	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("0", "1", "10", "12", "5", "6"))
	tk.MustQuery("select * from tcollist partition (p0)").Sort().Check(testkit.Rows("0", "1"))
	tk.MustQuery("select * from tcollist partition (p1, p2)").Sort().Check(testkit.Rows("10", "12", "5", "6"))
}

func TestListPartitionSplitRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_split_region")
	tk.MustExec("use list_partition_split_region")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int, key(a)) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec("insert into tlist values (0), (1), (5), (6), (10), (12)")

	tk.MustExec(`split table tlist index a between (2) and (15) regions 10`)
	tk.MustQuery("select * from tlist").Sort().Check(testkit.Rows("0", "1", "10", "12", "5", "6"))
	tk.MustQuery("select * from tlist partition (p0)").Sort().Check(testkit.Rows("0", "1"))
	tk.MustQuery("select * from tlist partition (p1, p2)").Sort().Check(testkit.Rows("10", "12", "5", "6"))

	tk.MustExec(`create table tcollist (a int, key(a)) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec("insert into tcollist values (0), (1), (5), (6), (10), (12)")

	tk.MustExec(`split table tcollist index a between (2) and (15) regions 10`)
	tk.MustQuery("select * from tcollist").Sort().Check(testkit.Rows("0", "1", "10", "12", "5", "6"))
	tk.MustQuery("select * from tcollist partition (p0)").Sort().Check(testkit.Rows("0", "1"))
	tk.MustQuery("select * from tcollist partition (p1, p2)").Sort().Check(testkit.Rows("10", "12", "5", "6"))
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

func TestListPartitionAutoIncre(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_auto_incre")
	tk.MustExec("use list_partition_auto_incre")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	err := tk.ExecToErr(`create table tlist (a int, b int AUTO_INCREMENT) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "it must be defined as a key")

	tk.MustExec(`create table tlist (a int, b int AUTO_INCREMENT, key(b)) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	tk.MustExec(`insert into tlist (a) values (0)`)
	tk.MustExec(`insert into tlist (a) values (5)`)
	tk.MustExec(`insert into tlist (a) values (10)`)
	tk.MustExec(`insert into tlist (a) values (1)`)

	err = tk.ExecToErr(`create table tcollist (a int, b int AUTO_INCREMENT) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "it must be defined as a key")

	tk.MustExec(`create table tcollist (a int, b int AUTO_INCREMENT, key(b)) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	tk.MustExec(`insert into tcollist (a) values (0)`)
	tk.MustExec(`insert into tcollist (a) values (5)`)
	tk.MustExec(`insert into tcollist (a) values (10)`)
	tk.MustExec(`insert into tcollist (a) values (1)`)
}

func TestListPartitionAutoRandom(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_auto_rand")
	tk.MustExec("use list_partition_auto_rand")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	err := tk.ExecToErr(`create table tlist (a int, b bigint AUTO_RANDOM) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid auto random")

	tk.MustExec(`create table tlist (a bigint auto_random, primary key(a)) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	err = tk.ExecToErr(`create table tcollist (a int, b bigint AUTO_RANDOM) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid auto random")

	tk.MustExec(`create table tcollist (a bigint auto_random, primary key(a)) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
}

func TestListPartitionInvisibleIdx(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_invisible_idx")
	tk.MustExec("use list_partition_invisible_idx")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int, b int, key(a)) partition by list (a) (partition p0 values in (0, 1, 2), partition p1 values in (3, 4, 5))`)
	tk.MustExec(`alter table tlist alter index a invisible`)
	tk.HasPlan(`select a from tlist where a>=0 and a<=5`, "TableFullScan")

	tk.MustExec(`create table tcollist (a int, b int, key(a)) partition by list columns (a) (partition p0 values in (0, 1, 2), partition p1 values in (3, 4, 5))`)
	tk.MustExec(`alter table tcollist alter index a invisible`)
	tk.HasPlan(`select a from tcollist where a>=0 and a<=5`, "TableFullScan")
}

func TestListPartitionCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_cte")
	tk.MustExec("use list_partition_cte")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)

	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	tk.MustExec(`insert into tlist values (0), (1), (5), (6), (10)`)
	tk.MustQuery(`with tmp as (select a+1 as a from tlist) select * from tmp`).Sort().Check(testkit.Rows("1", "11", "2", "6", "7"))

	tk.MustExec(`create table tcollist (a int) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)

	tk.MustExec(`insert into tcollist values (0), (1), (5), (6), (10)`)
	tk.MustQuery(`with tmp as (select a+1 as a from tcollist) select * from tmp`).Sort().Check(testkit.Rows("1", "11", "2", "6", "7"))
}

func TestListPartitionTempTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_temp_table")
	tk.MustExec("use list_partition_temp_table")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	err := tk.ExecToErr("create global temporary table t(a int, b int) partition by list(a) (partition p0 values in (0)) on commit delete rows")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot create temporary table with partitions")
	err = tk.ExecToErr("create global temporary table t(a int, b int) partition by list columns (a) (partition p0 values in (0)) on commit delete rows")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot create temporary table with partitions")
}

func TestListPartitionAlterPK(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database list_partition_alter_pk")
	tk.MustExec("use list_partition_alter_pk")
	tk.MustExec("drop table if exists tlist")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table tlist (a int, b int) partition by list (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec(`alter table tlist add primary key(a)`)
	tk.MustExec(`alter table tlist drop primary key`)
	err := tk.ExecToErr(`alter table tlist add primary key(b)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all columns")

	tk.MustExec(`create table tcollist (a int, b int) partition by list columns (a) (
    partition p0 values in (0, 1, 2, 3, 4),
    partition p1 values in (5, 6, 7, 8, 9),
    partition p2 values in (10, 11, 12, 13, 14))`)
	tk.MustExec(`alter table tcollist add primary key(a)`)
	tk.MustExec(`alter table tcollist drop primary key`)
	err = tk.ExecToErr(`alter table tcollist add primary key(b)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include all columns")
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

func TestIssue27018(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27018")
	tk.MustExec("use issue_27018")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE PK_LP9326 (
  COL1 tinyint(45) NOT NULL DEFAULT '30' COMMENT 'NUMERIC PK',
  PRIMARY KEY (COL1) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(col1) (
  PARTITION P0 VALUES IN (56,127,-128,-125,-40,-18,-10,-5,49,51),
  PARTITION P1 VALUES IN (-107,-97,-57,-37,4,43,99,-9,-6,45),
  PARTITION P2 VALUES IN (108,114,-85,-72,-38,-11,29,97,40,107),
  PARTITION P3 VALUES IN (-112,-95,-42,24,28,47,-103,-94,7,64),
  PARTITION P4 VALUES IN (-115,-101,-76,-47,1,19,-114,-23,-19,11),
  PARTITION P5 VALUES IN (44,95,-92,-89,-26,-21,25,-117,-116,27),
  PARTITION P6 VALUES IN (50,61,118,-110,-32,-1,111,125,-90,74),
  PARTITION P7 VALUES IN (75,121,-96,-87,-14,-13,37,-68,-58,81),
  PARTITION P8 VALUES IN (126,30,48,68)
)`)
	tk.MustExec(`insert into PK_LP9326 values(30),(48),(56)`)
	tk.MustQuery(`SELECT COL1 FROM PK_LP9326 WHERE COL1 NOT IN (621579514938,-17333745845828,2777039147338)`).Sort().Check(testkit.Rows("30", "48", "56"))
}

func TestIssue27017(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27017")
	tk.MustExec("use issue_27017")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE PK_LP9465 (
  COL1 mediumint(45) NOT NULL DEFAULT '77' COMMENT 'NUMERIC PK',
  PRIMARY KEY (COL1) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(col1) (
  PARTITION P0 VALUES IN (-5237720,2949267,6047247,-8317208,-6854239,-6612749,-6578207,-5649321,2450483,2953765),
  PARTITION P1 VALUES IN (5884439,-7816703,-6716210,-6050369,-5691207,6836620,5769359,-8237127,-1294367,-1228621),
  PARTITION P2 VALUES IN (-976130,-8351227,-8294140,-4800605,1370685,-7351802,-6447779,77,1367409,5965199),
  PARTITION P3 VALUES IN (7347944,7397124,8013414,-5737292,-3938813,-3687304,1307396,444598,1216072,1603451),
  PARTITION P4 VALUES IN (2518402,-8388608,-5291256,-3796824,121011,8388607,39191,2323510,3386861,4886727),
  PARTITION P5 VALUES IN (-6512367,-5922779,-3272589,-1313463,5751641,-3974640,2605656,3336269,4416436,-7975238),
  PARTITION P6 VALUES IN (-6693544,-6023586,-4201506,6416586,-3254125,-205332,1072201,2679754,1963191,2077718),
  PARTITION P7 VALUES IN (4205081,5170051,-8087893,-5805143,-1202286,1657202,8330979,5042855,7578575,-5830439),
  PARTITION P8 VALUES IN (-5244013,3837781,4246485,670906,5644986,5843443,7794811,7831812,-7704740,-2222984),
  PARTITION P9 VALUES IN (764108,3406142,8263677,248997,6129417,7556305,7939455,3526998,8239485,-5195482),
  PARTITION P10 VALUES IN (-3625794,69270,377245)
)`)
	tk.MustExec(`insert into PK_LP9465 values(8263677)`)
	tk.MustQuery(`SELECT COL1 FROM PK_LP9465 HAVING COL1>=-12354348921530`).Sort().Check(testkit.Rows("8263677"))
}

func TestIssue27544(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27544")
	tk.MustExec("use issue_27544")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table t3 (a datetime) partition by list (mod( year(a) - abs(weekday(a) + dayofweek(a)), 4) + 1) (
		partition p0 values in (2),
		partition p1 values in (3),
		partition p3 values in (4))`)
	tk.MustExec(`insert into t3 values ('1921-05-10 15:20:10')`) // success without any error
	tk.MustExec(`insert into t3 values ('1921-05-10 15:20:20')`)
	tk.MustExec(`insert into t3 values ('1921-05-10 15:20:30')`)
}

func TestIssue27012(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27012")
	tk.MustExec("use issue_27012")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE IDT_LP24306 (
  COL1 tinyint(16) DEFAULT '41' COMMENT 'NUMERIC UNIQUE INDEX',
  KEY UK_COL1 (COL1) /*!80000 INVISIBLE */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(col1) (
  PARTITION P0 VALUES IN (-126,-36,-96,-6,-83,-123,-5,-52,-98,-124),
  PARTITION P1 VALUES IN (-2,-22,-88,-100,-60,-39,-69,-38,-11,-30),
  PARTITION P2 VALUES IN (-119,-13,-67,-91,-65,-16,0,-128,-73,-118),
  PARTITION P3 VALUES IN (-99,-56,-76,-110,-93,-114,-78,NULL)
)`)
	tk.MustExec(`insert into IDT_LP24306 values(-128)`)
	tk.MustQuery(`select * from IDT_LP24306 where col1 not between 12021 and 99 and col1 <= -128`).Sort().Check(testkit.Rows("-128"))

	tk.MustExec(`drop table if exists IDT_LP24306`)
	tk.MustExec(`CREATE TABLE IDT_LP24306 (
  COL1 tinyint(16) DEFAULT '41' COMMENT 'NUMERIC UNIQUE INDEX',
  KEY UK_COL1 (COL1) /*!80000 INVISIBLE */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`insert into IDT_LP24306 values(-128)`)
	tk.MustQuery(`select * from IDT_LP24306 where col1 not between 12021 and 99 and col1 <= -128`).Sort().Check(testkit.Rows("-128"))
}

func TestIssue27030(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27030")
	tk.MustExec("use issue_27030")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE PK_LCP9290 (
  COL1 varbinary(10) NOT NULL,
  PRIMARY KEY (COL1) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(col1) (
  PARTITION P5 VALUES IN (x'32d8fb9da8b63508a6b8'),
  PARTITION P6 VALUES IN (x'ffacadeb424179bc4b5c'),
  PARTITION P8 VALUES IN (x'ae9f733168669fa900be')
)`)
	tk.MustExec(`insert into PK_LCP9290 values(0xffacadeb424179bc4b5c),(0xae9f733168669fa900be),(0x32d8fb9da8b63508a6b8)`)
	tk.MustQuery(`SELECT COL1 FROM PK_LCP9290 WHERE COL1!=x'9f7ebdc957a36f2531b5' AND COL1 IN (x'ffacadeb424179bc4b5c',x'ae9f733168669fa900be',x'32d8fb9da8b63508a6b8')`).Sort().Check(testkit.Rows("2\xd8\xfb\x9d\xa8\xb65\b\xa6\xb8", "\xae\x9fs1hf\x9f\xa9\x00\xbe", "\xff\xac\xad\xebBAy\xbcK\\"))
	tk.MustQuery(`SELECT COL1 FROM PK_LCP9290 WHERE COL1 IN (x'ffacadeb424179bc4b5c',x'ae9f733168669fa900be',x'32d8fb9da8b63508a6b8')`).Sort().Check(testkit.Rows("2\xd8\xfb\x9d\xa8\xb65\b\xa6\xb8", "\xae\x9fs1hf\x9f\xa9\x00\xbe", "\xff\xac\xad\xebBAy\xbcK\\"))
}

func TestIssue27070(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27070")
	tk.MustExec("use issue_27070")
	tk.MustExec("set @@tidb_enable_list_partition = OFF")
	tk.MustExec(`create table if not exists t (id int,   create_date date NOT NULL DEFAULT '2000-01-01',   PRIMARY KEY (id,create_date)  ) PARTITION BY list COLUMNS(create_date) (   PARTITION p20210506 VALUES IN ("20210507"),   PARTITION p20210507 VALUES IN ("20210508") )`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type LIST, treat as normal table"))
}

func TestIssue27031(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27031")
	tk.MustExec("use issue_27031")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE NT_LP27390 (
  COL1 mediumint(28) DEFAULT '114' COMMENT 'NUMERIC NO INDEX'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(col1) (
  PARTITION P9 VALUES IN (3376825,-7753310,-4123498,6483048,6953968,-996842,-7542484,320451,-8322717,-2426029)
)`)
	tk.MustExec(`insert into NT_LP27390 values(-4123498)`)
	tk.MustQuery(`SELECT COL1 FROM NT_LP27390 WHERE COL1 IN (46015556,-4123498,54419751)`).Sort().Check(testkit.Rows("-4123498"))
}

func TestIssue27493(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27493")
	tk.MustExec("use issue_27493")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`CREATE TABLE UK_LP17321 (
  COL1 mediumint(16) DEFAULT '82' COMMENT 'NUMERIC UNIQUE INDEX',
  COL3 bigint(20) DEFAULT NULL,
  UNIQUE KEY UM_COL (COL1,COL3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST (COL1 DIV COL3) (
  PARTITION P0 VALUES IN (NULL,0)
)`)
	tk.MustQuery(`select * from UK_LP17321 where col1 is null`).Check(testkit.Rows()) // without any error
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

func TestIssue27532(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database issue_27532")
	defer tk.MustExec(`drop database issue_27532`)
	tk.MustExec("use issue_27532")
	tk.MustExec(`set tidb_enable_list_partition = 1`)
	tk.MustExec(`create table t2 (c1 int primary key, c2 int, c3 int, c4 int, key k2 (c2), key k3 (c3)) partition by hash(c1) partitions 10`)
	tk.MustExec(`insert into t2 values (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4)`)
	tk.MustExec(`set @@tidb_partition_prune_mode="dynamic"`)
	tk.MustExec(`set autocommit = 0`)
	tk.MustQuery(`select * from t2`).Sort().Check(testkit.Rows("1 1 1 1", "2 2 2 2", "3 3 3 3", "4 4 4 4"))
	tk.MustQuery(`select * from t2`).Sort().Check(testkit.Rows("1 1 1 1", "2 2 2 2", "3 3 3 3", "4 4 4 4"))
	tk.MustExec(`drop table t2`)
}

func TestIssue37508(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("select @@tidb_partition_prune_mode").Check(testkit.Rows("dynamic"))
	tk.MustExec(`create table t1 (id int, c date) partition by range (to_days(c))
(partition p0 values less than (to_days('2022-01-11')),
partition p1 values less than (to_days('2022-02-11')),
partition p2 values less than (to_days('2022-03-11')));`)
	tk.MustExec("analyze table t1")

	tk.MustPartition("select * from t1 where c in ('2022-01-23', '2022-01-22');", "p1").Sort().Check(testkit.Rows())
	tk.MustPartition("select * from t1 where c in (NULL, '2022-01-23');", "p0,p1").Sort().Check(testkit.Rows())
	tk.MustExec(`drop table t1`)
}

func TestRangeColumnsMultiColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database RangeColumnsMulti")
	tk.MustExec("use RangeColumnsMulti")

	tk.MustGetErrCode(`create table t (a int, b datetime, c varchar(255)) partition by range columns (a,b,c)`+
		`(partition p0 values less than (NULL,NULL,NULL))`,
		errno.ErrWrongTypeColumnValue)
	tk.MustGetErrCode(`create table t (a int, b datetime, c varchar(255)) partition by range columns (a,b,c)`+
		`(partition p1 values less than (`+strconv.FormatInt(math.MinInt32-1, 10)+`,'0000-00-00',""))`,
		errno.ErrWrongTypeColumnValue)
	tk.MustExec(`create table t (a int, b datetime, c varchar(255)) partition by range columns (a,b,c)` +
		`(partition p1 values less than (` + strconv.FormatInt(math.MinInt32, 10) + `,'0000-00-00',""),` +
		`partition p2 values less than (10,'2022-01-01',"Wow"),` +
		`partition p3 values less than (11,'2022-01-01',MAXVALUE),` +
		`partition p4 values less than (MAXVALUE,'2022-01-01',"Wow"))`)
	tk.MustGetErrCode(`insert into t values (`+strconv.FormatInt(math.MinInt32, 10)+`,'0000-00-00',null)`, errno.ErrTruncatedWrongValue)
	tk.MustExec(`insert into t values (NULL,NULL,NULL)`)
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustExec(`insert into t values (` + strconv.FormatInt(math.MinInt32, 10) + `,'0000-00-00',null)`)
	tk.MustExec(`insert into t values (` + strconv.FormatInt(math.MinInt32, 10) + `,'0000-00-00',"")`)
	tk.MustExec(`insert into t values (5,'0000-00-00',null)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`insert into t values (5,'0000-00-00',"Hi")`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`set @@sql_mode = DEFAULT`)
	tk.MustExec(`insert into t values (10,'2022-01-01',"Hi")`)
	tk.MustExec(`insert into t values (10,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (10,'2022-01-01',"Wowe")`)
	tk.MustExec(`insert into t values (11,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (1,null,"Wow")`)
	tk.MustExec(`insert into t values (NULL,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (11,null,"Wow")`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`select a,b,c from t partition(p1)`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 <nil>",
		"<nil> 2022-01-01 00:00:00 Wow",
		"<nil> <nil> <nil>"))
	tk.MustQuery(`select a,b,c from t partition(p2)`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 ",
		"1 <nil> Wow",
		"10 2022-01-01 00:00:00 Hi",
		"5 0000-00-00 00:00:00 <nil>",
		"5 0000-00-00 00:00:00 Hi"))
	tk.MustQuery(`select a,b,c from t partition(p3)`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wow",
		"10 2022-01-01 00:00:00 Wowe",
		"11 2022-01-01 00:00:00 Wow",
		"11 <nil> Wow"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c = "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wow"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c <= "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Hi",
		"10 2022-01-01 00:00:00 Wow"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c < "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Hi"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c > "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wowe"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c >= "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wow",
		"10 2022-01-01 00:00:00 Wowe"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 10 and b = "2022-01-01" and c = "Wow"`).Check(testkit.Rows(
		"TableReader 0.52 root partition:p3 data:Selection",
		`└─Selection 0.52 cop[tikv]  eq(rangecolumnsmulti.t.a, 10), eq(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), eq(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 10 and b = "2022-01-01" and c <= "Wow"`).Check(testkit.Rows(
		`TableReader 0.83 root partition:p2,p3 data:Selection`,
		`└─Selection 0.83 cop[tikv]  eq(rangecolumnsmulti.t.a, 10), eq(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), le(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 10 and b = "2022-01-01" and c < "Wow"`).Check(testkit.Rows(
		`TableReader 0.31 root partition:p2 data:Selection`,
		`└─Selection 0.31 cop[tikv]  eq(rangecolumnsmulti.t.a, 10), eq(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), lt(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 10 and b = "2022-01-01" and c > "Wow"`).Check(testkit.Rows(
		`TableReader 0.10 root partition:p3 data:Selection`,
		`└─Selection 0.10 cop[tikv]  eq(rangecolumnsmulti.t.a, 10), eq(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), gt(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 10 and b = "2022-01-01" and c >= "Wow"`).Check(testkit.Rows(
		`TableReader 0.62 root partition:p3 data:Selection`,
		`└─Selection 0.62 cop[tikv]  eq(rangecolumnsmulti.t.a, 10), eq(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), ge(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`select * from t where a <= 10 and b <= '2022-01-01' and c < "Wow"`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 ",
		"10 2022-01-01 00:00:00 Hi",
		"5 0000-00-00 00:00:00 Hi"))
	tk.MustQuery(`select * from t where a = 10 and b = "2022-01-01" and c = "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wow"))
	tk.MustQuery(`select * from t where a <= 10 and b <= '2022-01-01' and c <= "Wow"`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 ",
		"10 2022-01-01 00:00:00 Hi",
		"10 2022-01-01 00:00:00 Wow",
		"5 0000-00-00 00:00:00 Hi"))
	tk.MustQuery(`explain format = 'brief' select * from t where a <= 10 and b <= '2022-01-01' and c < "Wow"`).Check(testkit.Rows(
		`TableReader 1.50 root partition:p1,p2,p3 data:Selection`,
		`└─Selection 1.50 cop[tikv]  le(rangecolumnsmulti.t.a, 10), le(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), lt(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`select * from t where a <= 11 and b <= '2022-01-01' and c < "Wow"`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 ",
		"10 2022-01-01 00:00:00 Hi",
		"5 0000-00-00 00:00:00 Hi"))
	// Possible optimization: p3 should not be included!!! The range optimizer will just use a <= 10 here
	// But same with non-partitioned index, so the range optimizer needs to be improved.
	tk.MustQuery(`explain format = 'brief' select * from t where a <= 10 and b <= '2022-01-01' and c < "Wow"`).Check(testkit.Rows(
		`TableReader 1.50 root partition:p1,p2,p3 data:Selection`,
		`└─Selection 1.50 cop[tikv]  le(rangecolumnsmulti.t.a, 10), le(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), lt(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustExec(`create table tref (a int, b datetime, c varchar(255), key (a,b,c))`)
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustExec(`insert into tref select * from t`)
	tk.MustExec(`set @@sql_mode = DEFAULT`)
	tk.MustQuery(`explain format = 'brief' select * from tref where a <= 10 and b <= '2022-01-01' and c < "Wow"`).Check(testkit.Rows(
		`IndexReader 367.05 root  index:Selection`,
		`└─Selection 367.05 cop[tikv]  le(rangecolumnsmulti.tref.b, 2022-01-01 00:00:00.000000), lt(rangecolumnsmulti.tref.c, "Wow")`,
		`  └─IndexRangeScan 3323.33 cop[tikv] table:tref, index:a(a, b, c) range:[-inf,10], keep order:false, stats:pseudo`))
	tk.MustQuery(`explain format = 'brief' select * from t where a <= 10 and b <= '2022-01-01' and c <= "Wow"`).Check(testkit.Rows(
		`TableReader 4.00 root partition:p1,p2,p3 data:Selection`,
		`└─Selection 4.00 cop[tikv]  le(rangecolumnsmulti.t.a, 10), le(rangecolumnsmulti.t.b, 2022-01-01 00:00:00.000000), le(rangecolumnsmulti.t.c, "Wow")`,
		`  └─TableFullScan 12.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`select * from t where a = 2 and b = "2022-01-02" and c = "Hi" or b = '2022-01-01' and c = "Wow"`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00 Wow",
		"11 2022-01-01 00:00:00 Wow",
		"<nil> 2022-01-01 00:00:00 Wow"))
	tk.MustQuery(`select * from t where a = 2 and b = "2022-01-02" and c = "Hi" or a = 10 and b = '2022-01-01' and c = "Wow"`).Sort().Check(testkit.Rows("10 2022-01-01 00:00:00 Wow"))
	tk.MustQuery(`select * from t where a = 2 and b = "2022-01-02" and c = "Hi"`).Sort().Check(testkit.Rows())
	tk.MustQuery(`select * from t where a = 2 and b = "2022-01-02" and c < "Hi"`).Sort().Check(testkit.Rows())
	tk.MustQuery(`select * from t where a < 2`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 ",
		"-2147483648 0000-00-00 00:00:00 <nil>",
		"1 <nil> Wow"))
	tk.MustQuery(`select * from t where a <= 2 and b <= "2022-01-02" and c < "Hi"`).Sort().Check(testkit.Rows(
		"-2147483648 0000-00-00 00:00:00 "))
	tk.MustQuery(`explain format = 'brief' select * from t where a < 2`).Check(testkit.Rows(
		"TableReader 3.00 root partition:p1,p2 data:Selection",
		"└─Selection 3.00 cop[tikv]  lt(rangecolumnsmulti.t.a, 2)",
		"  └─TableFullScan 12.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`select * from t where a < 2 and a > -22`).Sort().Check(testkit.Rows(
		"1 <nil> Wow"))
	tk.MustQuery(`explain format = 'brief' select * from t where a < 2 and a > -22`).Check(testkit.Rows(
		"TableReader 1.00 root partition:p2 data:Selection",
		"└─Selection 1.00 cop[tikv]  gt(rangecolumnsmulti.t.a, -22), lt(rangecolumnsmulti.t.a, 2)",
		"  └─TableFullScan 12.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`select * from t where c = ""`).Sort().Check(testkit.Rows("-2147483648 0000-00-00 00:00:00 "))
	tk.MustQuery(`explain format = 'brief' select * from t where c = ""`).Check(testkit.Rows(
		"TableReader 1.00 root partition:all data:Selection",
		`└─Selection 1.00 cop[tikv]  eq(rangecolumnsmulti.t.c, "")`,
		"  └─TableFullScan 12.00 cop[tikv] table:t keep order:false"))
}

func TestRangeMultiColumnsPruning(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database RColumnsMulti")
	tk.MustExec("use RColumnsMulti")
	tk.MustExec(`create table t (a int, b datetime, c varchar(255), key (a,b,c))` +
		` partition by range columns (a,b,c) ` +
		`(partition p0 values less than (-2147483648, '0000-01-01', ""),` +
		` partition p1 values less than (-2147483648, '0001-01-01', ""),` +
		` partition p2 values less than (-2, '0001-01-01', ""),` +
		` partition p3 values less than (0, '0001-01-01', ""),` +
		` partition p4 values less than (0, '2031-01-01', ""),` +
		` partition p5 values less than (0, '2031-01-01', "Wow"),` +
		` partition p6 values less than (0, '2031-01-01', MAXVALUE),` +
		` partition p7 values less than (0, MAXVALUE, MAXVALUE),` +
		` partition p8 values less than (MAXVALUE, MAXVALUE, MAXVALUE))`)
	tk.MustGetErrCode(`insert into t values (`+strconv.FormatInt(math.MinInt32, 10)+`,'0000-00-00',null)`, errno.ErrTruncatedWrongValue)
	tk.MustExec(`insert into t values (NULL,NULL,NULL)`)
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustExec(`insert into t values (` + strconv.FormatInt(math.MinInt32, 10) + `,'0000-00-00',null)`)
	tk.MustExec(`insert into t values (` + strconv.FormatInt(math.MinInt32, 10) + `,'0000-00-00',"")`)
	tk.MustExec(`insert into t values (5,'0000-00-00',null)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`insert into t values (5,'0000-00-00',"Hi")`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`set @@sql_mode = DEFAULT`)
	tk.MustExec(`insert into t values (10,'2022-01-01',"Hi")`)
	tk.MustExec(`insert into t values (10,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (11,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (0,'2020-01-01',"Wow")`)
	tk.MustExec(`insert into t values (1,null,"Wow")`)
	tk.MustExec(`insert into t values (NULL,'2022-01-01',"Wow")`)
	tk.MustExec(`insert into t values (11,null,"Wow")`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`select a,b from t where b = '2022-01-01'`).Sort().Check(testkit.Rows(
		"10 2022-01-01 00:00:00",
		"10 2022-01-01 00:00:00",
		"11 2022-01-01 00:00:00",
		"<nil> 2022-01-01 00:00:00"))
	tk.MustQuery(`select a,b,c from t where a = 1`).Check(testkit.Rows("1 <nil> Wow"))
	tk.MustQuery(`select a,b,c from t where a = 1 AND c = "Wow"`).Check(testkit.Rows("1 <nil> Wow"))
	tk.MustQuery(`explain format = 'brief' select a,b,c from t where a = 1 AND c = "Wow"`).Check(testkit.Rows(
		`IndexReader 0.50 root partition:p8 index:Selection`,
		`└─Selection 0.50 cop[tikv]  eq(rcolumnsmulti.t.c, "Wow")`,
		`  └─IndexRangeScan 1.00 cop[tikv] table:t, index:a(a, b, c) range:[1,1], keep order:false`))
	// WAS HERE, Why is the start return TRUE making this to work and FALSE disapear?
	tk.MustQuery(`select a,b,c from t where a = 0 AND c = "Wow"`).Check(testkit.Rows("0 2020-01-01 00:00:00 Wow"))
	tk.MustQuery(`explain format = 'brief' select a,b,c from t where a = 0 AND c = "Wow"`).Check(testkit.Rows(
		`IndexReader 0.50 root partition:p3,p4,p5,p6,p7,p8 index:Selection`,
		`└─Selection 0.50 cop[tikv]  eq(rcolumnsmulti.t.c, "Wow")`,
		`  └─IndexRangeScan 1.00 cop[tikv] table:t, index:a(a, b, c) range:[0,0], keep order:false`))
}

func TestRangeColumnsExpr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database rce`)
	tk.MustExec(`use rce`)
	tk.MustExec(`create table tref (a int unsigned, b int, c int)`)
	tk.MustExec(`create table t (a int unsigned, b int, c int) partition by range columns (a,b) ` +
		`(partition p0 values less than (3, MAXVALUE), ` +
		` partition p1 values less than (4, -2147483648), ` +
		` partition p2 values less than (4, 1), ` +
		` partition p3 values less than (4, 4), ` +
		` partition p4 values less than (4, 7), ` +
		` partition p5 values less than (4, 11), ` +
		` partition p6 values less than (4, 14), ` +
		` partition p7 values less than (4, 17), ` +
		` partition p8 values less than (4, MAXVALUE), ` +
		` partition p9 values less than (7, 0), ` +
		` partition p10 values less than (11, MAXVALUE), ` +
		` partition p11 values less than (14, -2147483648), ` +
		` partition p12 values less than (17, 17), ` +
		` partition p13 values less than (MAXVALUE, -2147483648))`)
	allRows := []string{
		"0 0 0",
		"11 2147483647 2147483647",
		"14 10 4",
		"14 <nil> 2",
		"14 <nil> <nil>",
		"17 16 16",
		"17 17 17",
		"3 2147483647 9",
		"4 -2147483648 -2147483648",
		"4 1 1",
		"4 10 3",
		"4 13 1",
		"4 14 2",
		"4 2147483647 2147483647",
		"4 4 4",
		"4 5 6",
		"4 <nil> 4",
		"5 0 0",
		"7 0 0",
		"<nil> -2147483648 <nil>",
		"<nil> <nil> <nil>",
	}
	insertStr := []string{}
	for _, row := range allRows {
		s := strings.ReplaceAll(row, " ", ",")
		s = strings.ReplaceAll(s, "<nil>", "NULL")
		insertStr = append(insertStr, "("+s+")")
	}
	tk.MustExec(`insert into t values ` + strings.Join(insertStr, ","))
	tk.MustExec(`insert into tref select * from t`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`select * from tref`).Sort().Check(testkit.Rows(allRows...))
	tk.MustQuery(`select * from t`).Sort().Check(testkit.Rows(allRows...))
	tk.MustQuery(`select * from t partition (p0)`).Sort().Check(testkit.Rows(
		"0 0 0",
		"3 2147483647 9",
		"<nil> -2147483648 <nil>",
		"<nil> <nil> <nil>"))
	tk.MustQuery(`select * from t partition (p1)`).Sort().Check(testkit.Rows(
		"4 <nil> 4"))
	tk.MustQuery(`select * from t partition (p2)`).Sort().Check(testkit.Rows(
		"4 -2147483648 -2147483648"))
	tk.MustQuery(`select * from t partition (p3)`).Sort().Check(testkit.Rows(
		"4 1 1"))
	tk.MustQuery(`select * from t partition (p4)`).Sort().Check(testkit.Rows(
		"4 4 4",
		"4 5 6"))
	tk.MustQuery(`select * from t partition (p5)`).Sort().Check(testkit.Rows(
		"4 10 3"))
	tk.MustQuery(`select * from t partition (p6)`).Sort().Check(testkit.Rows(
		"4 13 1"))
	tk.MustQuery(`select * from t partition (p7)`).Sort().Check(testkit.Rows(
		"4 14 2"))
	tk.MustQuery(`select * from t partition (p8)`).Sort().Check(testkit.Rows(
		"4 2147483647 2147483647"))
	tk.MustQuery(`select * from t partition (p9)`).Sort().Check(testkit.Rows(
		"5 0 0"))
	tk.MustQuery(`select * from t partition (p10)`).Sort().Check(testkit.Rows(
		"11 2147483647 2147483647",
		"7 0 0"))
	tk.MustQuery(`select * from t partition (p11)`).Sort().Check(testkit.Rows(
		"14 <nil> 2",
		"14 <nil> <nil>"))
	tk.MustQuery(`select * from t partition (p12)`).Sort().Check(testkit.Rows(
		"14 10 4",
		"17 16 16"))
	tk.MustQuery(`select * from t partition (p13)`).Sort().Check(testkit.Rows(
		"17 17 17"))
	tk.MustQuery(`explain format = 'brief' select * from t where c = 3`).Check(testkit.Rows(
		"TableReader 1.00 root partition:all data:Selection",
		"└─Selection 1.00 cop[tikv]  eq(rce.t.c, 3)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b > 3 and c = 3`).Check(testkit.Rows(
		"TableReader 0.52 root partition:all data:Selection",
		"└─Selection 0.52 cop[tikv]  eq(rce.t.c, 3), gt(rce.t.b, 3)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 5 and c = 3`).Check(testkit.Rows(
		"TableReader 0.05 root partition:p9 data:Selection",
		"└─Selection 0.05 cop[tikv]  eq(rce.t.a, 5), eq(rce.t.c, 3)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 4 and c = 3`).Check(testkit.Rows(
		"TableReader 0.43 root partition:p1,p2,p3,p4,p5,p6,p7,p8,p9 data:Selection",
		"└─Selection 0.43 cop[tikv]  eq(rce.t.a, 4), eq(rce.t.c, 3)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a in (4,14) and c = 3`).Check(testkit.Rows(
		"TableReader 0.57 root partition:p1,p2,p3,p4,p5,p6,p7,p8,p9,p11,p12 data:Selection",
		"└─Selection 0.57 cop[tikv]  eq(rce.t.c, 3), in(rce.t.a, 4, 14)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a in (4,14) and b in (null,10)`).Check(testkit.Rows(
		"TableReader 1.14 root partition:p5,p12 data:Selection",
		"└─Selection 1.14 cop[tikv]  in(rce.t.a, 4, 14), in(rce.t.b, NULL, 10)",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`select * from tref where a in (4,14) and b in (null,10)`).Sort().Check(testkit.Rows(
		"14 10 4",
		"4 10 3"))
	tk.MustQuery(`select * from t where a in (4,14) and b in (null,10)`).Sort().Check(testkit.Rows(
		"14 10 4",
		"4 10 3"))
	tk.MustQuery(`explain format = 'brief' select * from t where a in (4,14) and (b in (11,10) OR b is null)`).Check(testkit.Rows(
		"TableReader 3.43 root partition:p1,p5,p6,p11,p12 data:Selection",
		"└─Selection 3.43 cop[tikv]  in(rce.t.a, 4, 14), or(in(rce.t.b, 11, 10), isnull(rce.t.b))",
		"  └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`select * from tref where a in (4,14) and (b in (11,10) OR b is null)`).Sort().Check(testkit.Rows(
		"14 10 4",
		"14 <nil> 2",
		"14 <nil> <nil>",
		"4 10 3",
		"4 <nil> 4"))
	tk.MustQuery(`select * from t where a in (4,14) and (b in (11,10) OR b is null)`).Sort().Check(testkit.Rows(
		"14 10 4",
		"14 <nil> 2",
		"14 <nil> <nil>",
		"4 10 3",
		"4 <nil> 4"))
}

func TestPartitionRangePrunerCharWithCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database cwc`)
	tk.MustExec(`use cwc`)
	// "'c'", "'F'", "'h'", "'L'", "'t'", "MAXVALUE"
	tk.MustExec(
		`create table t (a char(32) collate utf8mb4_unicode_ci) ` +
			`partition by range columns (a) ` +
			`(partition p0 values less than ('c'),` +
			` partition p1 values less than ('F'),` +
			` partition p2 values less than ('h'),` +
			` partition p3 values less than ('L'),` +
			` partition p4 values less than ('t'),` +
			` partition p5 values less than (MAXVALUE))`)

	tk.MustExec(`insert into t values ('a'),('A'),('c'),('C'),('f'),('F'),('h'),('H'),('l'),('L'),('t'),('T'),('z'),('Z')`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`select * from t partition(p0)`).Sort().Check(testkit.Rows("A", "a"))
	tk.MustQuery(`select * from t partition(p1)`).Sort().Check(testkit.Rows("C", "c"))
	tk.MustQuery(`select * from t partition(p2)`).Sort().Check(testkit.Rows("F", "f"))
	tk.MustQuery(`select * from t partition(p3)`).Sort().Check(testkit.Rows("H", "h"))
	tk.MustQuery(`select * from t partition(p4)`).Sort().Check(testkit.Rows("L", "l"))
	tk.MustQuery(`select * from t partition(p5)`).Sort().Check(testkit.Rows("T", "Z", "t", "z"))
	tk.MustQuery(`select * from t where a > 'C' and a < 'q'`).Sort().Check(testkit.Rows("F", "H", "L", "f", "h", "l"))
	tk.MustQuery(`select * from t where a > 'c' and a < 'Q'`).Sort().Check(testkit.Rows("F", "H", "L", "f", "h", "l"))
	tk.MustQuery(`explain format = 'brief' select * from t where a > 'C' and a < 'q'`).Check(testkit.Rows(
		`TableReader 6.00 root partition:p1,p2,p3,p4 data:Selection`,
		`└─Selection 6.00 cop[tikv]  gt(cwc.t.a, "C"), lt(cwc.t.a, "q")`,
		`  └─TableFullScan 14.00 cop[tikv] table:t keep order:false`))
	tk.MustQuery(`explain format = 'brief' select * from t where a > 'c' and a < 'Q'`).Check(testkit.Rows(
		`TableReader 6.00 root partition:p1,p2,p3,p4 data:Selection`,
		`└─Selection 6.00 cop[tikv]  gt(cwc.t.a, "c"), lt(cwc.t.a, "Q")`,
		`  └─TableFullScan 14.00 cop[tikv] table:t keep order:false`))
}

func TestPartitionRangePrunerDate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database rcd`)
	tk.MustExec(`use rcd`)
	tk.MustExec(`set @@tidb_partition_prune_mode = 'dynamic'`)
	tk.MustExec(`create table i (a int, b int, key (a,b))`)
	tk.MustQuery(`select * from i where a < 1 and a > 2`).Check(testkit.Rows())
	tk.MustQuery(`explain format = 'brief' select * from i where a < 1 and a > 2`).Check(testkit.Rows("TableDual 0.00 root  rows:0"))
	tk.MustExec(
		`create table t (a date) ` +
			`partition by range columns (a) ` +
			`(partition p0 values less than ('19990601'),` +
			` partition p1 values less than ('2000-05-01'),` +
			` partition p2 values less than ('20080401'),` +
			` partition p3 values less than ('2010-03-01'),` +
			` partition p4 values less than ('20160201'),` +
			` partition p5 values less than ('2020-01-01'),` +
			` partition p6 values less than (MAXVALUE))`)
	tk.MustQuery(`show create table t`).Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` date DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE COLUMNS(`a`)\n" +
			"(PARTITION `p0` VALUES LESS THAN ('19990601'),\n" +
			" PARTITION `p1` VALUES LESS THAN ('2000-05-01'),\n" +
			" PARTITION `p2` VALUES LESS THAN ('20080401'),\n" +
			" PARTITION `p3` VALUES LESS THAN ('2010-03-01'),\n" +
			" PARTITION `p4` VALUES LESS THAN ('20160201'),\n" +
			" PARTITION `p5` VALUES LESS THAN ('2020-01-01'),\n" +
			" PARTITION `p6` VALUES LESS THAN (MAXVALUE))"))
	tk.MustExec(`insert into t values ('19990101'),('1999-06-01'),('2000-05-01'),('20080401'),('2010-03-01'),('2016-02-01'),('2020-01-01')`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`select * from t partition(p0)`).Sort().Check(testkit.Rows("1999-01-01"))
	tk.MustQuery(`select * from t partition(p1)`).Sort().Check(testkit.Rows("1999-06-01"))
	tk.MustQuery(`select * from t partition(p2)`).Sort().Check(testkit.Rows("2000-05-01"))
	tk.MustQuery(`select * from t partition(p3)`).Sort().Check(testkit.Rows("2008-04-01"))
	tk.MustQuery(`select * from t partition(p4)`).Sort().Check(testkit.Rows("2010-03-01"))
	tk.MustQuery(`select * from t partition(p5)`).Sort().Check(testkit.Rows("2016-02-01"))
	tk.MustQuery(`select * from t partition(p6)`).Sort().Check(testkit.Rows("2020-01-01"))
	tk.UsedPartitions(`select * from t where a < '1943-02-12'`).Check(testkit.Rows("p0"))
	tk.UsedPartitions(`select * from t where a >= '19690213'`).Check(testkit.Rows("all"))
	tk.UsedPartitions(`select * from t where a > '2003-03-13'`).Check(testkit.Rows("p2 p3 p4 p5 p6"))
	tk.UsedPartitions(`select * from t where a < '2006-02-03'`).Check(testkit.Rows("p0 p1 p2"))
	tk.UsedPartitions(`select * from t where a = '20070707'`).Check(testkit.Rows("p2"))
	tk.UsedPartitions(`select * from t where a > '1949-10-10'`).Check(testkit.Rows("all"))
	tk.UsedPartitions(`select * from t where a > '2016-02-01' AND a < '20000103'`).Check(testkit.Rows("dual"))
	tk.UsedPartitions(`select * from t where a < '19691112' or a >= '2019-09-18'`).Check(testkit.Rows("p0 p5 p6"))
	tk.UsedPartitions(`select * from t where a is null`).Check(testkit.Rows("p0"))
	tk.UsedPartitions(`select * from t where '2003-02-27' >= a`).Check(testkit.Rows("p0 p1 p2"))
	tk.UsedPartitions(`select * from t where '20141024' < a`).Check(testkit.Rows("p4 p5 p6"))
	tk.UsedPartitions(`select * from t where '2003-03-30' > a`).Check(testkit.Rows("p0 p1 p2"))
	tk.UsedPartitions(`select * from t where a between '2003-03-30' AND '2014-01-01'`).Check(testkit.Rows("p2 p3 p4"))
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

func TestPartitionRangeColumnPruning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database rcd`)
	tk.MustExec(`use rcd`)
	tk.MustExec(`create table t1 (a char, b char, c char) ` +
		`partition by range columns (a,b,c) ` +
		`( partition p0 values less than ('a','b','c'),` +
		` partition p1 values less than ('b','c','d'),` +
		` partition p2 values less than ('d','e','f'))`)
	tk.MustExec(`insert into t1 values ('a', NULL, 'd')`)
	tk.MustExec(`analyze table t1`)
	tk.MustQuery(`explain format=brief select * from t1 where a = 'a' AND c = 'd'`).Check(testkit.Rows(
		`TableReader 1.00 root partition:p0,p1 data:Selection`,
		`└─Selection 1.00 cop[tikv]  eq(rcd.t1.a, "a"), eq(rcd.t1.c, "d")`,
		`  └─TableFullScan 1.00 cop[tikv] table:t1 keep order:false`))
	tk.MustQuery(`select * from t1 where a = 'a' AND c = 'd'`).Check(testkit.Rows("a <nil> d"))
	tk.MustExec(`drop table t1`)
}

func TestPartitionProcessorWithUninitializedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(" create table q1(a int, b int, key (a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")
	tk.MustExec(" create table q2(a int, b int, key (a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")

	rows := [][]interface{}{
		{"HashJoin"},
		{"├─PartitionUnion(Build)"},
		{"│ ├─TableReader"},
		{"│ │ └─TableFullScan"},
		{"│ └─TableReader"},
		{"│   └─TableFullScan"},
		{"└─PartitionUnion(Probe)"},
		{"  ├─TableReader"},
		{"  │ └─TableFullScan"},
		{"  └─TableReader"},
		{"    └─TableFullScan"},
	}
	tk.MustQuery("explain format=brief select * from q1,q2").CheckAt([]int{0}, rows)

	tk.MustExec("analyze table q1")
	tk.MustQuery("explain format=brief select * from q1,q2").CheckAt([]int{0}, rows)

	tk.MustExec("analyze table q2")
	rows = [][]interface{}{
		{"HashJoin"},
		{"├─TableReader(Build)"},
		{"│ └─TableFullScan"},
		{"└─TableReader(Probe)"},
		{"  └─TableFullScan"},
	}
	tk.MustQuery("explain format=brief select * from q1,q2").CheckAt([]int{0}, rows)
}
