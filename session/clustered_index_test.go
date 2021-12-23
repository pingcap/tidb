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

package session_test

import (
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func createTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	return tk
}

func TestClusteredUnionScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustExec("insert t (a, b) values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("rollback")

	// cover old row format.
	tk = testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().RowEncoder.Enable = false
	tk.MustExec("use test")
	tk.MustExec("begin")
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("rollback")
}

func TestClusteredPrefixColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(cb varchar(12), ci int, v int, primary key(cb(1)), key idx_1(cb))")
	tk.MustExec("insert into t1 values('PvtYW2', 1, 1)")
	tk.MustQuery("select cb from t1").Check(testkit.Rows("PvtYW2"))
	tk.MustQuery("select * from t1").Check(testkit.Rows("PvtYW2 1 1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2), key idx1 (c2(1)))")
	tk.MustExec("insert into t1 select 'a', 'cd', 'ef'")
	tk.MustExec("create table t2(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2(1)), key idx1 (c1,c2))")
	tk.MustExec("insert into t2 select 'a', 'cd', 'ef'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	testData := session.GetClusteredIndexSuiteData()
	testData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}

	tk.MustExec("drop table if exists test1")
	tk.MustExec("create table test1(c1 varchar(100) not null default 'xyza', c2 int, primary key(c1(3)) clustered)")
	tk.MustExec("replace into test1(c2) values(1)")
	tk.MustExec("replace into test1(c2) values(2)")
	tk.MustQuery("select * from test1").Check(testkit.Rows("xyza 2"))

	tk.MustExec("drop table if exists test3")
	tk.MustExec("create table test3(c1 varchar(100), c2 int, primary key(c1(1)) clustered)")
	tk.MustExec("insert into test3 values('ab', 1) on duplicate key update c2 = 100")
	tk.MustExec("insert into test3 values('ab', 1) on duplicate key update c2 = 100")
	tk.MustQuery("select * from test3").Check(testkit.Rows("ab 100"))
	tk.MustExec("insert into test3 values('ab', 1) on duplicate key update c1 = 'cc', c2 = '200'")
	tk.MustQuery("select * from test3").Check(testkit.Rows("cc 200"))

	tk.MustExec("drop table if exists tbl_3")
	tk.MustExec(`create table tbl_3 ( col_15 text(138) , col_16 varchar(37) default 'yGdboyZqIGDQhwRRc' not null , col_17 text(39) not null , col_18 char(58) default 'vBahOai' , col_19 varchar(470) , primary key idx_12 ( col_16(3),col_17(6),col_15(4)) clustered, key idx_13 ( col_19(2) ) , key idx_14 ( col_18(3),col_15(2) ) , unique key idx_15 ( col_16(4),col_18(6) ) , unique key idx_16 ( col_17(1) ) )`)
	tk.MustExec("insert into tbl_3 values ( 'XJUDeSZplXx','TfZhIWnJPygn','HlZjQffSh','VDsepqNPkx','xqtMHHOqnLvcxDpL')")
	tk.MustExec("insert into tbl_3 (col_15,col_17,col_19) values ( 'aeMrIjbfCxErg','HTZmtykzIkFMF','' ) on duplicate key update col_18 = values( col_18 )")
	tk.MustQuery("select col_17 from tbl_3").Check(testkit.Rows("HlZjQffSh"))

	tk.MustExec("drop table if exists tbl_1")
	tk.MustExec("CREATE TABLE `tbl_1`(`col_5` char(84) NOT NULL DEFAULT 'BnHWZQY',   `col_6` char(138) DEFAULT NULL,   `col_7` tinytext NOT NULL,   `col_8` char(231) DEFAULT NULL,   `col_9` varchar(393) NOT NULL DEFAULT 'lizgVQd',   PRIMARY KEY (`col_5`(4),`col_7`(3)) clustered ,   KEY `idx_2` (`col_5`(6),`col_8`(5)),   UNIQUE KEY `idx_3` (`col_7`(2)),   UNIQUE KEY `idx_4` (`col_9`(6),`col_7`(4),`col_6`(3)),   UNIQUE KEY `idx_5` (`col_9`(3)) );")
	tk.MustExec("insert into tbl_1 values('BsXhVuVvPRcSOlkzuM','QXIEA','IHeTDzJJyfOhIOY','ddxnmRcIjVfosRVC','lizgVQd')")
	tk.MustExec("replace into tbl_1 (col_6,col_7,col_8) values ( 'WzdD','S','UrQhNEUZy' )")
	tk.MustExec("admin check table tbl_1")

	tk.MustExec("drop table if exists tbl_3")
	tk.MustExec("create table tbl_3 ( col_15 char(167) not null , col_16 varchar(56) not null , col_17 text(25) not null , col_18 char , col_19 char(12) not null , primary key idx_21 ( col_16(5) ) clustered, key idx_22 ( col_19(2),col_16(4) ) , unique key idx_23 ( col_19(6),col_16(4) ) , unique key idx_24 ( col_19(1),col_18(1) ) , key idx_25 ( col_17(3),col_16(2),col_19(4) ) , key idx_26 ( col_18(1),col_17(3) ) , key idx_27 ( col_18(1) ) , unique key idx_28 ( col_16(4),col_15(3) ) , unique key idx_29 ( col_16(2) ) , key idx_30 ( col_18(1),col_16(2),col_19(4),col_17(6) ) , key idx_31 ( col_19(2) ) , key idx_32 ( col_16(6) ) , unique key idx_33 ( col_18(1) ) , unique key idx_34 ( col_15(4) ) , key idx_35 ( col_19(6) ) , key idx_36 ( col_19(4),col_17(4),col_18(1) ) )")
	tk.MustExec("insert into tbl_3 values('auZELjkOUG','yhFUdsZphsWDFG','mNbCXHOWlIMQvXhY','        ','NpQwmX');")
	tk.MustExec("insert into tbl_3 (col_15,col_16,col_17,col_18,col_19) values ( 'PboEJsnVPBknRhpEC','PwqzUThyDHhxhXAdJ','szolY','','pzZfZeOa' ) on duplicate key update col_16 = values( col_16 ) , col_19 = 'zgLlCUA'")
	tk.MustExec("admin check table tbl_3")

	tk.MustExec("create table t (c_int int, c_str varchar(40), primary key(c_str(8)) clustered, unique key(c_int), key(c_str))")
	tk.MustExec("insert into t values (1, 'determined varahamihira')")
	tk.MustExec("insert into t values (1, 'pensive mendeleev') on duplicate key update c_int=values(c_int), c_str=values(c_str)")
	tk.MustExec("admin check table t")
}

func TestClusteredUnionScanIndexLookup(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, pk char(10), c int, primary key(pk), key(a));")
	tk.MustExec("insert into t values (1, '111', 3);")

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1, pk = '222' where a = 1;")
	sql := "select pk, c from t where a = 2;"
	tk.HasPlan(sql, "IndexLookUp")
	tk.MustQuery(sql).Check(testkit.Rows("222 3"))

	tk.MustExec("commit")
	tk.MustQuery(sql).Check(testkit.Rows("222 3"))
}

func TestClusteredIndexLookUp(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a, b))")
	tk.MustExec("create index idx on t(c)")
	tk.MustExec("insert t values (1, 1, 1, 1)")
	tk.MustQuery("select d from t use index (idx)").Check(testkit.Rows("1"))
}

func TestClusteredIndexLookUp2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists c3")
	createTable := `
CREATE TABLE c3 (
  c_id int(11) NOT NULL,
  c_d_id int(11) NOT NULL,
  c_w_id int(11) NOT NULL,
  c_first varchar(16) DEFAULT NULL,
  c_middle char(2) DEFAULT NULL,
  c_last varchar(16) DEFAULT NULL,
  c_balance decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id),
  KEY idx (c_w_id,c_d_id,c_last,c_first)
);`
	tk.MustExec(createTable)
	tk.MustExec("insert c3 values (772,1,1,'aaa','OE','CALL',0),(1905,1,1,'bbb','OE','CALL',0);")
	query := `
SELECT c_balance, c_first, c_middle, c_id FROM c3 use index (idx) WHERE c_w_id = 1 AND c_d_id = 1 and c_last = 'CALL' ORDER BY c_first
`
	tk.MustQuery(query).Check(testkit.Rows("0.00 aaa OE 772", "0.00 bbb OE 1905"))
}

func TestClusteredTopN(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists o3")
	createTables := `
	CREATE TABLE o3 (
	o_id int NOT NULL,
	o_d_id int,
	o_w_id int,
	o_c_id int,
	PRIMARY KEY (o_w_id,o_d_id,o_id),
	KEY idx_order (o_w_id,o_d_id,o_c_id,o_id)
);`
	tk.MustExec(createTables)
	tk.MustExec("insert o3 values (1, 6, 9, 3), (2, 6, 9, 5), (3, 6, 9, 7)")
	tk.MustQuery("SELECT max(o_id) max_order FROM o3 use index (idx_order)").Check(testkit.Rows("3"))
}

func TestClusteredHint(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists ht")
	tk.MustExec("create table ht (a varchar(64) primary key, b int)")
	tk.MustQuery("select * from ht use index (`PRIMARY`)")
}

func TestClusteredBatchPointGet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b)) PARTITION BY HASH(a) PARTITIONS 3")
	tk.MustExec("insert t values (1, 1, 1), (3, 3, 3), (5, 5, 5)")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (3, 3), (5, 5))").Check(
		testkit.Rows("1 1 1", "3 3 3", "5 5 5"))
}

type SnapCacheSizeGetter interface {
	SnapCacheSize() int
}

func TestClusteredInsertIgnoreBatchGetKeyCount(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestClusteredPrefixingPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, c int, primary key(name(2)), index idx(b));")
	tk.MustExec("insert into t(name, b) values('aaaaa', 1), ('bbbbb', 2);")
	tk.MustExec("admin check table t;")

	tk.MustGetErrCode("insert into t(name, b) values('aaa', 3);", errno.ErrDupEntry)
	sql := "select * from t use index(primary) where name = 'aaaaa';"
	tk.HasPlan(sql, "TableReader")
	tk.HasPlan(sql, "TableRangeScan")
	tk.MustQuery(sql).Check(testkit.Rows("aaaaa 1 <nil>"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, c char(10), primary key(c(2), name(2)), index idx(b));")
	tk.MustExec("insert into t values ('aaa', 1, 'aaa'), ('bbb', 1, 'bbb');")
	tk.MustExec("insert into t values ('aa', 1, 'bbb'), ('bbb', 1, 'ccc');")
	tk.MustGetErrCode("insert into t values ('aa', 1, 'aa');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('aac', 1, 'aac');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('bb', 1, 'bb');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('bbc', 1, 'bbc');", errno.ErrDupEntry)
	tk.MustGetErrCode("update t set name = 'aa', c = 'aa' where c = 'ccc'", errno.ErrDupEntry)
	tk.MustExec("update t set name = 'ccc' where name = 'aa'")
	tk.MustQuery("select group_concat(name order by name separator '.') from t use index(idx);").
		Check(testkit.Rows("aaa.bbb.bbb.ccc"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, primary key(name(2)), index idx(b));")
	tk.MustExec("insert into t values ('aaa', 1), ('bbb', 1);")
	tk.MustQuery("select group_concat(name order by name separator '.') from t use index(idx);").
		Check(testkit.Rows("aaa.bbb"))

	tk.MustGetErrCode("update t set name = 'aaaaa' where name = 'bbb'", errno.ErrDupEntry)
	tk.MustExec("update ignore t set name = 'aaaaa' where name = 'bbb'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry 'aaaaa' for key 'PRIMARY'"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_str varchar(40), c_decimal decimal(12, 6) , primary key(c_str(8)))")
	tk.MustExec("create table t2  like t1")
	tk.MustExec("insert into t1 values ('serene ramanujan', 6.383), ('frosty hodgkin', 3.504), ('stupefied spence', 5.869)")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */ * from t1 right join t2 on t1.c_str = t2.c_str").Check(testkit.Rows(
		"frosty hodgkin 3.504000 frosty hodgkin 3.504000",
		"serene ramanujan 6.383000 serene ramanujan 6.383000",
		"stupefied spence 5.869000 stupefied spence 5.869000"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */ * from t1 right join t2 on t1.c_str = t2.c_str").Check(testkit.Rows(
		"frosty hodgkin 3.504000 frosty hodgkin 3.504000",
		"serene ramanujan 6.383000 serene ramanujan 6.383000",
		"stupefied spence 5.869000 stupefied spence 5.869000"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1 right join t2 on t1.c_str = t2.c_str").Check(testkit.Rows(
		"frosty hodgkin 3.504000 frosty hodgkin 3.504000",
		"serene ramanujan 6.383000 serene ramanujan 6.383000",
		"stupefied spence 5.869000 stupefied spence 5.869000"))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1  (c_int int, c_str varchar(40), primary key(c_int, c_str) clustered, key(c_int), key(c_str));`)
	tk.MustExec(`create table t2  like t1;`)
	tk.MustExec(`insert into t1 values (1, 'nifty elion');`)
	tk.MustExec(`insert into t2 values (1, 'funny shaw');`)
	tk.MustQuery(`select /*+ INL_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1  (c_int int, c_str varchar(40), primary key(c_int, c_str(4)) clustered, key(c_int), key(c_str));`)
	tk.MustExec(`create table t2  like t1;`)
	tk.MustExec(`insert into t1 values (1, 'nifty elion');`)
	tk.MustExec(`insert into t2 values (1, 'funny shaw');`)
	tk.MustQuery(`select /*+ INL_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1,t2) */  * from t1, t2 where t1.c_int = t2.c_int and t1.c_str >= t2.c_str;`).Check(testkit.Rows("1 nifty elion 1 funny shaw"))
}

func TestClusteredWithOldRowFormat(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
}

func TestIssue20002(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t ( c_int int, c_str varchar(40), c_datetime datetime, primary key(c_str), unique key(c_datetime));")
	tk.MustExec("insert into t values (1, 'laughing hertz', '2020-04-27 20:29:30'), (2, 'sharp yalow', '2020-04-01 05:53:36'), (3, 'pedantic hoover', '2020-03-10 11:49:00');")
	tk.MustExec("begin;")
	tk.MustExec("update t set c_str = 'amazing herschel' where c_int = 3;")
	tk.MustExec("select c_int, c_str, c_datetime from t where c_datetime between '2020-01-09 22:00:28' and '2020-04-08 15:12:37';")
	tk.MustExec("commit;")
	tk.MustExec("admin check index t `c_datetime`;")
}

// https://github.com/pingcap/tidb/issues/20727
func TestClusteredIndexSplitAndAddIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, primary key(a));")
	tk.MustExec("insert into t values ('a', 1), ('b', 2), ('c', 3), ('u', 1);")
	tk.MustQuery("split table t between ('a') and ('z') regions 5;").Check(testkit.Rows("4 1"))
	tk.MustExec("create index idx on t (b);")
	tk.MustQuery("select a from t order by a;").Check(testkit.Rows("a", "b", "c", "u"))
	tk.MustQuery("select a from t use index (idx) order by a;").Check(testkit.Rows("a", "b", "c", "u"))
}

func TestClusteredIndexSelectWhereInNull(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := createTestKit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a datetime, b bigint, primary key (a));")
	tk.MustQuery("select * from t where a in (null);").Check(testkit.Rows( /* empty result */ ))
}
