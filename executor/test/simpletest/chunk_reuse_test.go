// Copyright 2023 PingCAP, Inc.
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

package simpletest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestLongBlobReuse(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id1 int ,id2 char(10) ,id3 text,id4 blob,id5 json,id6 varchar(1000),id7 varchar(1001), PRIMARY KEY (`id1`) clustered,key id2(id2))")
	tk.MustExec("insert into t1 (id1,id2)values(1,1);")
	tk.MustExec("insert into t1 (id1,id2)values(2,2),(3,3);")
	tk.MustExec("create table t2 (id1 int ,id2 char(10) ,id3 text,id4 blob,id5 json,id6 varchar(1000),PRIMARY KEY (`id1`) clustered,key id2(id2))")
	tk.MustExec("insert into t2 (id1,id2)values(1,1);")
	tk.MustExec("insert into t2 (id1,id2)values(2,2),(3,3);")
	//IndexRangeScan
	res := tk.MustQuery("explain select t1.id1 from t1,t2 where t1.id2 > '1' and t2.id2 > '1'")
	require.Regexp(t, ".*IndexRangeScan*", res.Rows()[4][0])
	tk.MustQuery("select t1.id1 from t1,t2 where t1.id2 > '1' and t2.id2 > '1'").Check(testkit.Rows("2", "2", "3", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id2 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 2", "2 2", "3 3", "3 3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id3 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 <nil>", "2 <nil>", "3 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id4 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 <nil>", "2 <nil>", "3 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id5 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 <nil>", "2 <nil>", "3 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id6 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 <nil>", "2 <nil>", "3 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id7 from t1,t2 where t1.id2 > '1' and t2.id2 > '1' ").Check(testkit.Rows("2 <nil>", "2 <nil>", "3 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))

	//TableFullScan
	res = tk.MustQuery("explain select t1.id1 from t1,t2 where t1.id2 > '1'and t1.id1 = t2.id1")
	require.Regexp(t, ".*TableFullScan*", res.Rows()[2][0])
	tk.MustQuery("select t1.id1 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1 ,t1.id3 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1 ,t1.id4 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1 ,t1.id5 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1 ,t1.id6 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1 ,t1.id7 from t1,t2 where t1.id2 > '1' and t1.id1 = t2.id1").Check(testkit.Rows("2 <nil>", "3 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))

	//Point_Get
	res = tk.MustQuery("explain select t1.id1 from t1,t2 where t1.id1 = 1 and t2.id1 = 1")
	require.Regexp(t, ".*Point_Get*", res.Rows()[1][0])
	tk.MustQuery("select t1.id1 from t1,t2 where t1.id1 = 1 and t2.id1 = 1").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id2 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 1"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id3 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id4 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id5 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id6 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id7 from t1,t2 where t1.id1 = 1 and t2.id1 = 1 ").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))

	//IndexLookUp
	res = tk.MustQuery("explain select t1.id1,t1.id6 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2' ;")
	require.Regexp(t, ".*IndexLookUp*", res.Rows()[1][0])
	tk.MustQuery("select t1.id1,t1.id6 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select t1.id1,t1.id3 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id4 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id5 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id7 ,t2.id6  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select t1.id1,t1.id6 ,t2.id3  from t1 join t2 on t1.id2 = '1' and t2.id2 = '2'").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))

	// IndexMerge
	tk.MustExec("create table t3 (id1 int ,id2 char(10),id8 int ,id3 text,id4 blob,id5 json,id6 varchar(1000),id7 varchar(1001), PRIMARY KEY (`id1`) clustered,key id2(id2),key id8(id8))")
	tk.MustExec("insert into t3 (id1,id2,id8)values(1,1,1),(2,2,2),(3,3,3);")
	res = tk.MustQuery("explain select id1 from t3 where id2 > '3' or id8 < 10 union (select id1 from t3 where id2 > '4' or id8 < 7);")
	require.Regexp(t, ".*IndexMerge*", res.Rows()[3][0])
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id1 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id3 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id4 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id5 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id6 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id7 from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))

	//IndexReader
	tk.MustExec("set tidb_enable_clustered_index = OFF")
	tk.MustExec("create table t4 (id1 int ,id2 char(10),id8 int ,id3 text,id4 blob,id5 json,id6 varchar(1000),id7 varchar(1001), PRIMARY KEY (`id1`),key id2(id2),key id8(id8,id2))")
	tk.MustExec("insert into t4 (id1,id2,id8)values(1,1,1),(2,2,2),(3,3,3);")
	res = tk.MustQuery("explain select id2 from t4 where id2 > '3' union (select id2 from t4 where id2 > '4');")
	require.Regexp(t, ".*IndexReader*", res.Rows()[2][0])
	tk.MustQuery("select id2 from t4 where id2 > '3' union (select id2 from t4 where id2 > '4');").Sort().Check(testkit.Rows())
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))

	//function
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select CHAR_LENGTH(id3) from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3", "<nil>"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select CHAR_LENGTH(id2) from t3 where id2 > '4' or id8 < 7);").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("1"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id2 from t3 where id2 > '4' or id8 < 7 and id3 is null);").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
	tk.MustQuery("select id1 from t3 where id2 > '3' or id8 < 10 union (select id2 from t3 where id2 > '4' or id8 < 7 and char_length(id3) > 0);").Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select @@last_sql_use_alloc").Check(testkit.Rows("0"))
}
