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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func createTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
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
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id varchar(255) primary key, a int, b int, unique index idx(b));")
	tk.MustExec("insert into t values ('b568004d-afad-11ea-8e4d-d651e3a981b7', 1, -1);")
	tk.MustQuery("select * from t use index(primary);").Check(testkit.Rows("b568004d-afad-11ea-8e4d-d651e3a981b7 1 -1"))

	// Test for issue https://github.com/pingcap/tidb/issues/21568
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key(c_str));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t (c_int, c_str) values (13, 'dazzling torvalds'), (3, 'happy rhodes');")
	tk.MustExec("delete from t where c_decimal <= 3.024 or (c_int, c_str) in ((5, 'happy saha'));")

	// Test for issue https://github.com/pingcap/tidb/issues/21502.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_double double, c_decimal decimal(12, 6), primary key(c_decimal, c_double), unique key(c_int));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (5, 55.068712, 8.256);")
	tk.MustExec("delete from t where c_int = 5;")

	// Test for issue https://github.com/pingcap/tidb/issues/21568#issuecomment-741601887
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_str varchar(40), c_timestamp timestamp, c_decimal decimal(12, 6), primary key(c_int, c_str), key(c_decimal));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (11, 'abc', null, null);")
	tk.MustExec("update t set c_str = upper(c_str) where c_decimal is null;")
	tk.MustQuery("select * from t where c_decimal is null;").Check(testkit.Rows("11 ABC <nil> <nil>"))

	// Test for issue https://github.com/pingcap/tidb/issues/22193
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (col_0 blob(20), col_1 int, primary key(col_0(1)), unique key idx(col_0(2)));")
	tk.MustExec("insert into t values('aaa', 1);")
	tk.MustExec("begin;")
	tk.MustExec("update t set col_0 = 'ccc';")
	tk.MustExec("update t set col_0 = 'ddd';")
	tk.MustExec("commit;")
	tk.MustQuery("select cast(col_0 as char(20)) from t use index (`primary`);").Check(testkit.Rows("ddd"))
	tk.MustQuery("select cast(col_0 as char(20)) from t use index (idx);").Check(testkit.Rows("ddd"))
	tk.MustExec("admin check table t")

	// Test for issue https://github.com/pingcap/tidb/issues/23646
	tk.MustExec("drop table if exists txx")
	tk.MustExec("create table txx(c1 varchar(100), c2 set('dav', 'aaa'), c3 varchar(100), primary key(c1(2), c2) clustered, unique key uk1(c2), index idx1(c2, c1, c3))")
	tk.MustExec("insert into txx select 'AarTrNoAL', 'dav', '1'")
	tk.MustExec("update txx set c3 = '10', c1 = 'BxTXbyKRFBGbcPmPR' where c2 in ('dav', 'dav')")
	tk.MustExec("admin check table txx")

	// TestClusteredIndexNewCollationWithOldRowFormat
	// This case maybe not useful, because newCollation isn't convenience to run on TiKV(it's required serialSuit)
	// but unistore doesn't support old row format.
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(col_1 varchar(132) CHARACTER SET utf8 COLLATE utf8_unicode_ci, primary key(col_1) clustered)")
	tk.MustExec("insert into t2 select 'aBc'")
	tk.MustQuery("select col_1 from t2 where col_1 = 'aBc'").Check(testkit.Rows("aBc"))

	// TestClusteredUnionScan with old row format.
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustExec("insert t (a, b) values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1"))
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

	vals := make([]string, 0, 400)
	existedPK := make(map[string]struct{}, 400)
	for i := 0; i < 400; {
		a := rand.Intn(10000)
		b := rand.Intn(10000)
		pk := fmt.Sprintf("%v, %v", a, b)
		if _, ok := existedPK[pk]; ok {
			continue
		}
		existedPK[pk] = struct{}{}
		i++
		vals = append(vals, fmt.Sprintf(`(%v, %v, '%v')`, a, b, rand.Intn(10000)))
	}

	tk.MustExec("insert into thash values " + strings.Join(vals, ", "))
	tk.MustExec("insert into trange values " + strings.Join(vals, ", "))
	tk.MustExec("insert into tnormal values " + strings.Join(vals, ", "))

	for i := 0; i < 20; i++ {
		cond := fmt.Sprintf("where a in (%v, %v, %v) and b < %v", rand.Intn(10000), rand.Intn(10000), rand.Intn(10000), rand.Intn(10000))
		result := tk.MustQuery("select * from tnormal " + cond).Sort().Rows()
		tk.MustQuery("select * from thash use index(primary) " + cond).Sort().Check(result)
		tk.MustQuery("select * from trange use index(primary) " + cond).Sort().Check(result)
	}
}
