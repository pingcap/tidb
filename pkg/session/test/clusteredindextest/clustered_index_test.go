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

package clusteredindextest

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func createTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeOn
	return tk
}

type SnapCacheSizeGetter interface {
	SnapCacheSize() int
}

func TestClusteredInsertIgnoreBatchGetKeyCount(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(10) primary key, b int)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert ignore t values ('a', 1)")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	snapSize := 0
	if snap, ok := txn.GetSnapshot().(SnapCacheSizeGetter); ok {
		snapSize = snap.SnapCacheSize()
	}
	require.Equal(t, 1, snapSize)
	tk.MustExec("rollback")
}

func TestClusteredWithOldRowFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := createTestKit(t, store)
	tk.Session().GetSessionVars().RowEncoder.Enable = false
	tk.MustExec("create table t_base(id varchar(255) primary key, a int, b int, unique index idx(b));")
	tk.MustExec("insert into t_base values ('b568004d-afad-11ea-8e4d-d651e3a981b7', 1, -1);")
	tk.MustQuery("select * from t_base use index(primary);").Check(testkit.Rows("b568004d-afad-11ea-8e4d-d651e3a981b7 1 -1"))

	// Test for issue https://github.com/pingcap/tidb/issues/21568
	tk.MustExec("create table t_21568 (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key(c_str));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t_21568 (c_int, c_str) values (13, 'dazzling torvalds'), (3, 'happy rhodes');")
	tk.MustExec("delete from t_21568 where c_decimal <= 3.024 or (c_int, c_str) in ((5, 'happy saha'));")
	tk.MustExec("commit;")

	// Test for issue https://github.com/pingcap/tidb/issues/21502.
	tk.MustExec("create table t_21502 (c_int int, c_double double, c_decimal decimal(12, 6), primary key(c_decimal, c_double), unique key(c_int));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t_21502 values (5, 55.068712, 8.256);")
	tk.MustExec("delete from t_21502 where c_int = 5;")
	tk.MustExec("commit;")

	// Test for issue https://github.com/pingcap/tidb/issues/21568#issuecomment-741601887
	tk.MustExec("create table t_21568_comment (c_int int, c_str varchar(40), c_timestamp timestamp, c_decimal decimal(12, 6), primary key(c_int, c_str), key(c_decimal));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t_21568_comment values (11, 'abc', null, null);")
	tk.MustExec("update t_21568_comment set c_str = upper(c_str) where c_decimal is null;")
	tk.MustQuery("select * from t_21568_comment where c_decimal is null;").Check(testkit.Rows("11 ABC <nil> <nil>"))
	tk.MustExec("commit;")

	// Test for issue https://github.com/pingcap/tidb/issues/22193
	tk.MustExec("create table t_22193 (col_0 blob(20), col_1 int, primary key(col_0(1)), unique key idx(col_0(2)));")
	tk.MustExec("insert into t_22193 values('aaa', 1);")
	tk.MustExec("begin;")
	tk.MustExec("update t_22193 set col_0 = 'ccc';")
	tk.MustExec("update t_22193 set col_0 = 'ddd';")
	tk.MustExec("commit;")
	tk.MustQuery("select cast(col_0 as char(20)) from t_22193 use index (`primary`);").Check(testkit.Rows("ddd"))
	tk.MustQuery("select cast(col_0 as char(20)) from t_22193 use index (idx);").Check(testkit.Rows("ddd"))
	tk.MustExec("admin check table t_22193")

	// Test for issue https://github.com/pingcap/tidb/issues/23646
	tk.MustExec("create table t_23646(c1 varchar(100), c2 set('dav', 'aaa'), c3 varchar(100), primary key(c1(2), c2) clustered, unique key uk1(c2), index idx1(c2, c1, c3))")
	tk.MustExec("insert into t_23646 select 'AarTrNoAL', 'dav', '1'")
	tk.MustExec("update t_23646 set c3 = '10', c1 = 'BxTXbyKRFBGbcPmPR' where c2 in ('dav', 'dav')")
	tk.MustExec("admin check table t_23646")

	// TestClusteredIndexNewCollationWithOldRowFormat
	// This case maybe not useful, because newCollation isn't convenience to run on TiKV(it's required serialSuit)
	// but unistore doesn't support old row format.
	tk.MustExec("create table t_collation(col_1 varchar(132) CHARACTER SET utf8 COLLATE utf8_unicode_ci, primary key(col_1) clustered)")
	tk.MustExec("insert into t_collation select 'aBc'")
	tk.MustQuery("select col_1 from t_collation where col_1 = 'aBc'").Check(testkit.Rows("aBc"))

	// TestClusteredUnionScan with old row format.
	tk.MustExec("CREATE TABLE t_union_scan (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustExec("insert t_union_scan (a, b) values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update t_union_scan set c = 1")
	tk.MustQuery("select * from t_union_scan").Check(testkit.Rows("1 1 1"))
	tk.MustExec("rollback")
}

func TestPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_view")
	tk.MustExec("use test_view")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table thash (a int, b int, c varchar(32), primary key(a, b) clustered) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, c varchar(32), primary key(a, b) clustered) partition by range columns(a) (
						partition p0 values less than (3000),
						partition p1 values less than (6000),
						partition p2 values less than (9000),
						partition p3 values less than (10000))`)
	tk.MustExec(`create table tnormal (a int, b int, c varchar(32), primary key(a, b))`)

	vals := []string{
		"(0, 5, 'p0_hash_0')",
		"(1, 6, 'p0_hash_1')",
		"(2, 7, 'p0_hash_2')",
		"(3, 8, 'p0_hash_3')",
		"(2999, 20, 'p0_edge')",
		"(3000, 30, 'p1_start')",
		"(4500, 90, 'p1_mid')",
		"(5999, 40, 'p1_edge')",
		"(6000, 50, 'p2_start')",
		"(8999, 60, 'p2_edge')",
		"(9000, 70, 'p3_start')",
		"(9999, 80, 'p3_edge')",
	}

	tk.MustExec("insert into thash values " + strings.Join(vals, ", "))
	tk.MustExec("insert into trange values " + strings.Join(vals, ", "))
	tk.MustExec("insert into tnormal values " + strings.Join(vals, ", "))

	conditions := []string{
		"where a in (0, 1, 2, 3) and b < 10",
		"where a in (2999, 3000, 5999, 6000) and b < 55",
		"where a in (8999, 9000, 9999) and b < 75",
		"where a in (42, 4500, 9999) and b < 100",
		"where a in (0, 3000, 9000) and b < 1",
	}
	for _, cond := range conditions {
		result := tk.MustQuery("select * from tnormal " + cond).Sort().Rows()
		tk.MustQuery("select * from thash use index(primary) " + cond).Sort().Check(result)
		tk.MustQuery("select * from trange use index(primary) " + cond).Sort().Check(result)
	}
}
