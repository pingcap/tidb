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

package session_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/israce"
)

func TestCreateClusteredTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("set @@tidb_enable_clustered_index = 'int_only';")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6, t7, t8")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("create table t2(id varchar(10) primary key, v int)")
	tk.MustExec("create table t3(id int primary key clustered, v int)")
	tk.MustExec("create table t4(id varchar(10) primary key clustered, v int)")
	tk.MustExec("create table t5(id int primary key nonclustered, v int)")
	tk.MustExec("create table t6(id varchar(10) primary key nonclustered, v int)")
	tk.MustExec("create table t7(id varchar(10), v int, primary key (id) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("create table t8(id varchar(10), v int, primary key (id) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustQuery("show index from t1").Check(testkit.Rows("t1 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t2").Check(testkit.Rows("t2 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t3").Check(testkit.Rows("t3 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t4").Check(testkit.Rows("t4 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t5").Check(testkit.Rows("t5 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t6").Check(testkit.Rows("t6 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t7").Check(testkit.Rows("t7 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t8").Check(testkit.Rows("t8 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))

	tk.MustExec("set @@tidb_enable_clustered_index = 'off';")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6, t7, t8")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("create table t2(id varchar(10) primary key, v int)")
	tk.MustExec("create table t3(id int primary key clustered, v int)")
	tk.MustExec("create table t4(id varchar(10) primary key clustered, v int)")
	tk.MustExec("create table t5(id int primary key nonclustered, v int)")
	tk.MustExec("create table t6(id varchar(10) primary key nonclustered, v int)")
	tk.MustExec("create table t7(id varchar(10), v int, primary key (id) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("create table t8(id varchar(10), v int, primary key (id) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustQuery("show index from t1").Check(testkit.Rows("t1 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t2").Check(testkit.Rows("t2 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t3").Check(testkit.Rows("t3 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t4").Check(testkit.Rows("t4 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t5").Check(testkit.Rows("t5 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t6").Check(testkit.Rows("t6 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t7").Check(testkit.Rows("t7 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t8").Check(testkit.Rows("t8 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))

	tk.MustExec("set @@tidb_enable_clustered_index = 'on';")
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6, t7, t8")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("create table t2(id varchar(10) primary key, v int)")
	tk.MustExec("create table t3(id int primary key clustered, v int)")
	tk.MustExec("create table t4(id varchar(10) primary key clustered, v int)")
	tk.MustExec("create table t5(id int primary key nonclustered, v int)")
	tk.MustExec("create table t6(id varchar(10) primary key nonclustered, v int)")
	tk.MustExec("create table t7(id varchar(10), v int, primary key (id) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("create table t8(id varchar(10), v int, primary key (id) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustQuery("show index from t1").Check(testkit.Rows("t1 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t2").Check(testkit.Rows("t2 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t3").Check(testkit.Rows("t3 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t4").Check(testkit.Rows("t4 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t5").Check(testkit.Rows("t5 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t6").Check(testkit.Rows("t6 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t7").Check(testkit.Rows("t7 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t8").Check(testkit.Rows("t8 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))

	tk.MustExec("set @@tidb_enable_clustered_index = 'int_only';")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	tk.MustExec("drop table if exists t1, t2, t3, t4, t5, t6, t7, t8")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("create table t2(id varchar(10) primary key, v int)")
	tk.MustExec("create table t3(id int primary key clustered, v int)")
	tk.MustExec("create table t4(id varchar(10) primary key clustered, v int)")
	tk.MustExec("create table t5(id int primary key nonclustered, v int)")
	tk.MustExec("create table t6(id varchar(10) primary key nonclustered, v int)")
	tk.MustExec("create table t7(id varchar(10), v int, primary key (id) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("create table t8(id varchar(10), v int, primary key (id) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustQuery("show index from t1").Check(testkit.Rows("t1 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t2").Check(testkit.Rows("t2 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t3").Check(testkit.Rows("t3 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t4").Check(testkit.Rows("t4 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t5").Check(testkit.Rows("t5 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t6").Check(testkit.Rows("t6 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("show index from t7").Check(testkit.Rows("t7 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("show index from t8").Check(testkit.Rows("t8 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
}

// Test for union scan in prefixed clustered index table.
// See https://github.com/pingcap/tidb/issues/22069.
func TestClusteredUnionScanOnPrefixingPrimaryKey(t *testing.T) {
	originCollate := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(originCollate)
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (col_1 varchar(255), col_2 tinyint, primary key idx_1 (col_1(1)));")
	tk.MustExec("insert into t values ('aaaaa', -38);")
	tk.MustExec("insert into t values ('bbbbb', -48);")

	tk.MustExec("begin PESSIMISTIC;")
	tk.MustExec("update t set col_2 = 47 where col_1 in ('aaaaa') order by col_1,col_2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("aaaaa 47", "bbbbb -48"))
	tk.MustGetErrCode("insert into t values ('bb', 0);", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('aa', 0);", errno.ErrDupEntry)
	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("aaaaa 47", "bbbbb -48"))
	tk.MustExec("admin check table t;")
}

// https://github.com/pingcap/tidb/issues/22453
func TestClusteredIndexSplitAndAddIndex2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b enum('Alice'), c int, primary key (c, b));")
	tk.MustExec("insert into t values (-1,'Alice',100);")
	tk.MustExec("insert into t values (-1,'Alice',7000);")
	tk.MustQuery("split table t between (0,'Alice') and (10000,'Alice') regions 2;").Check(testkit.Rows("1 1"))
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 3;")
	tk.MustExec("alter table t add index idx (c);")
	tk.MustExec("admin check table t;")
}

func TestClusteredIndexSyntax(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	const showPKType = `select tidb_pk_type from information_schema.tables where table_schema = 'test' and table_name = 't';`
	const nonClustered, clustered = `NONCLUSTERED`, `CLUSTERED`
	assertPkType := func(sql string, pkType string) {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(sql)
		tk.MustQuery(showPKType).Check(testkit.Rows(pkType))
	}

	// Test single integer column as the primary key.
	clusteredDefault := clustered
	assertPkType("create table t (a int primary key, b int);", clusteredDefault)
	assertPkType("create table t (a int, b int, primary key(a) clustered);", clustered)
	assertPkType("create table t (a int, b int, primary key(a) /*T![clustered_index] clustered */);", clustered)
	assertPkType("create table t (a int, b int, primary key(a) nonclustered);", nonClustered)
	assertPkType("create table t (a int, b int, primary key(a) /*T![clustered_index] nonclustered */);", nonClustered)

	// Test for clustered index.
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	assertPkType("create table t (a int, b varchar(255), primary key(b, a));", nonClustered)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) nonclustered);", nonClustered)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) clustered);", clustered)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	assertPkType("create table t (a int, b varchar(255), primary key(b, a));", clusteredDefault)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) nonclustered);", nonClustered)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) /*T![clustered_index] nonclustered */);", nonClustered)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) clustered);", clustered)
	assertPkType("create table t (a int, b varchar(255), primary key(b, a) /*T![clustered_index] clustered */);", clustered)

	tk.MustGetErrCode("create table t (a varchar(255) unique key clustered);", errno.ErrParse)
	tk.MustGetErrCode("create table t (a varchar(255), foreign key (a) reference t1(a) clustered);", errno.ErrParse)
	tk.MustGetErrCode("create table t (a varchar(255), foreign key (a) clustered reference t1(a));", errno.ErrParse)
	tk.MustGetErrCode("create table t (a varchar(255) clustered);", errno.ErrParse)

	errMsg := "[ddl:8200]CLUSTERED/NONCLUSTERED keyword is only supported for primary key"
	tk.MustGetErrMsg("create table t (a varchar(255), unique key(a) clustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), unique key(a) nonclustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), unique index(a) clustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), unique index(a) nonclustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), key(a) clustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), key(a) nonclustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), index(a) clustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), index(a) nonclustered);", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), b decimal(5, 4), primary key (a, b) clustered, key (b) clustered)", errMsg)
	tk.MustGetErrMsg("create table t (a varchar(255), b decimal(5, 4), primary key (a, b) clustered, key (b) nonclustered)", errMsg)
}

func TestPrefixClusteredIndexAddIndexAndRecover(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test;")
	tk1.MustExec("drop table if exists t;")
	defer func() {
		tk1.MustExec("drop table if exists t;")
	}()

	tk1.MustExec("create table t(a char(3), b char(3), primary key(a(1)) clustered)")
	tk1.MustExec("insert into t values ('aaa', 'bbb')")
	tk1.MustExec("alter table t add index idx(b)")
	tk1.MustQuery("select * from t use index(idx)").Check(testkit.Rows("aaa bbb"))
	tk1.MustExec("admin check table t")
	tk1.MustExec("admin recover index t idx")
	tk1.MustQuery("select * from t use index(idx)").Check(testkit.Rows("aaa bbb"))
	tk1.MustExec("admin check table t")
}

func TestPartitionTable(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("exhaustive types test, skip race test")
	}

	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

	vals := make([]string, 0, 4000)
	existedPK := make(map[string]struct{}, 4000)
	for i := 0; i < 4000; {
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

	for i := 0; i < 200; i++ {
		cond := fmt.Sprintf("where a in (%v, %v, %v) and b < %v", rand.Intn(10000), rand.Intn(10000), rand.Intn(10000), rand.Intn(10000))
		result := tk.MustQuery("select * from tnormal " + cond).Sort().Rows()
		tk.MustQuery("select * from thash use index(primary) " + cond).Sort().Check(result)
		tk.MustQuery("select * from trange use index(primary) " + cond).Sort().Check(result)
	}
}

// https://github.com/pingcap/tidb/issues/23106
func TestClusteredIndexDecodeRestoredDataV5(t *testing.T) {
	defer collate.SetNewCollationEnabledForTest(false)
	collate.SetNewCollationEnabledForTest(true)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id1 int, id2 varchar(10), a1 int, primary key(id1, id2) clustered) collate utf8mb4_general_ci;")
	tk.MustExec("insert into t values (1, 'asd', 1), (1, 'dsa', 1);")
	tk.MustGetErrCode("alter table t add unique index t_idx(id1, a1);", errno.ErrDupEntry)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id1 int, id2 varchar(10), a1 int, primary key(id1, id2) clustered, unique key t_idx(id1, a1)) collate utf8mb4_general_ci;")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, 'asd', 1);")
	tk.MustQuery("select * from t use index (t_idx);").Check(testkit.Rows("1 asd 1"))
	tk.MustExec("commit;")
	tk.MustExec("admin check table t;")
	tk.MustExec("drop table t;")
}

// https://github.com/pingcap/tidb/issues/23178
func TestPrefixedClusteredIndexUniqueKeyWithNewCollation(t *testing.T) {
	defer collate.SetNewCollationEnabledForTest(false)
	collate.SetNewCollationEnabledForTest(true)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (a text collate utf8mb4_general_ci not null, b int(11) not null, " +
		"primary key (a(10), b) clustered, key idx(a(2)) ) default charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustExec("insert into t values ('aaa', 2);")
	// Key-value content: sk = sortKey, p = prefixed
	// row record:     sk(aaa), 2              -> aaa
	// index record:   sk(p(aa)), {sk(aaa), 2} -> restore data(aaa)
	tk.MustExec("admin check table t;")
	tk.MustExec("drop table t;")
}

func TestClusteredIndexNewCollationWithOldRowFormat(t *testing.T) {
	// This case maybe not useful, because newCollation isn't convenience to run on TiKV(it's required serialSuit)
	// but unistore doesn't support old row format.
	defer collate.SetNewCollationEnabledForTest(false)
	collate.SetNewCollationEnabledForTest(true)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.Session().GetSessionVars().RowEncoder.Enable = false
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(col_1 varchar(132) CHARACTER SET utf8 COLLATE utf8_unicode_ci, primary key(col_1) clustered)")
	tk.MustExec("insert into t2 select 'aBc'")
	tk.MustQuery("select col_1 from t2 where col_1 = 'aBc'").Check(testkit.Rows("aBc"))
}
