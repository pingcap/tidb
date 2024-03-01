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

package core_test

import (
	"regexp"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheForIntersectionIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, e int, index ia(a), index ib(b), index ic(c), index id(d), index ie(e))")
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t, ia, ib, ic, id, ie) */ * from t where a = 10 and b = ? and c > ? and d is null and e in (0, 100)'")
	tk.MustExec("set @a=1, @b=3")
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @a=100, @b=500")
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	require.True(t, tk.HasPlanForLastExecution("IndexMerge"))
}

func TestIndexMergeWithOrderProperty(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, e int, key a(a), key b(b), key c(c), key ac(a, c), key bc(b, c), key ae(a, e), key be(b, e)," +
		" key abd(a, b, d), key cd(c, d))")
	tk.MustExec("create table t2 (a int, b int, c int, key a(a), key b(b), key ac(a, c))")

	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		// Expect no warnings.
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}

func TestHintForIntersectionIndexMerge(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	handle := domain.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a int, b int, c int, d int, e int, index ia(a), index ibc(b, c),index ic(c), index id(d), index ie(e))" +
		"partition by range(c) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (20)," +
		"partition p2 values less than (30)," +
		"partition p3 values less than (maxvalue))")
	tk.MustExec("insert into t1 values (10, 20, 5, 5, 3), (20, 20, 50, 5, 200), (20, 20, 10, 5, 5), (10, 30, 5, 3, 1)")
	tk.MustExec("create definer='root'@'localhost' view vh as " +
		"select /*+ use_index_merge(t1, ia, ibc, id) */ * from t1 where a = 10 and b = 20 and c < 30 and d in (2,5)")
	tk.MustExec("create definer='root'@'localhost' view v as " +
		"select * from t1 where a = 10 and b = 20 and c < 30 and d in (2,5)")
	tk.MustExec("create definer='root'@'localhost' view v1 as " +
		"select * from t1 where a = 10 and b = 20")
	tk.MustExec("create table t2(a int, b int, c int, d int, e int, index ia(a), index ibc(b, c), index id(d), index ie(e))" +
		"partition by range columns (c, d) (" +
		"partition p0 values less than (10, 20)," +
		"partition p1 values less than (30, 40)," +
		"partition p2 values less than (50, 60)," +
		"partition p3 values less than (maxvalue, maxvalue))")
	tk.MustExec("insert into t2 values (10, 20, 5, 5, 3), (20, 20, 20, 5, 100), (100, 30, 5, 3, 100)")
	tk.MustExec("create table t3(a int, b int, c int, d int, e int, index ia(a), index ibc(b, c), index id(d), index ie(e))" +
		"partition by hash (e) partitions 5")
	tk.MustExec("insert into t3 values (10, 20, 5, 5, 3), (20, 20, 20, 5, 100), (10, 30, 5, 3, 100)")
	tk.MustExec("create table t4(a int, b int, c int, d int, e int, index ia(a), index ibc(b, c), index id(d), index ie(e))" +
		"partition by list (d) (" +
		"partition p0 values in (1,2,3,4,5)," +
		"partition p1 values in (6,7,8,9,10)," +
		"partition p2 values in (11,12,13,14,15)," +
		"partition p3 values in (16,17,18,19,20))")
	tk.MustExec("insert into t4 values (30, 20, 5, 8, 100), (20, 20, 20, 3, 2), (10, 30, 5, 3, 100)")
	tk.MustExec("create table t5(" +
		"s1 varchar(20) collate utf8mb4_bin," +
		"s2 varchar(30) collate ascii_bin," +
		"s3 varchar(50) collate utf8_unicode_ci," +
		"s4 varchar(20) collate gbk_chinese_ci," +
		"index is1(s1), index is2(s2), index is3(s3), index is4(s4))")
	tk.MustExec("insert into t5 values ('Abc', 'zzzz', 'aa', 'ccc'), ('abc', 'zzzz', 'CCC', 'ccc')")
	tk.MustExec("create table t6(" +
		"s1 varchar(20) collate utf8mb4_bin," +
		"s2 varchar(30) collate ascii_bin," +
		"s3 varchar(50) collate utf8_unicode_ci," +
		"s4 varchar(20) collate gbk_chinese_ci," +
		"primary key (s1, s2(10)) nonclustered," +
		"index is1(s1), index is2(s2), index is3(s3), index is4(s4))")
	tk.MustExec("insert into t6 values ('Abc', 'zzzz', 'A啊A', 'Cdaa'), ('Abc', 'zczz', 'A啊', 'Cda')")
	tk.MustExec("create table t7(" +
		"a tinyint unsigned," +
		"b bit(3)," +
		"c float," +
		"d decimal(10,3)," +
		"e datetime," +
		"f timestamp(5)," +
		"g year," +
		"primary key (d) nonclustered," +
		"index ia(a), unique index ib(b), index ic(c), index ie(e), index iff(f), index ig(g))")
	tk.MustExec("insert into t7 values (100, 6, 12.2, 56, '2022-11-22 17:00', '2022-12-21 00:00', 2021)," +
		"(20, 7, 12.4, 30, '2022-12-22 17:00', '2016-12-21 00:00', 2021)")
	tk.MustExec("create table t8(" +
		"s1 mediumtext collate utf8mb4_general_ci," +
		"s2 varbinary(20)," +
		"s3 tinyblob," +
		"s4 enum('测试', 'aA', '??') collate gbk_chinese_ci," +
		"s5 set('^^^', 'tEsT', '2') collate utf8_general_ci," +
		"primary key (s1(10)) nonclustered," +
		"unique index is2(s2(20)), index is3(s3(20)), index is4(s4), index is5(s5))")
	tk.MustExec("insert into t8 values('啊aabbccdd', 'abcc', 'cccc', 'aa', '2,test')," +
		"('啊aabb', 'abcdc', 'aaaa', '??', '2')")

	require.NoError(t, handle.HandleDDLEvent(<-handle.DDLEventCh()))
	require.Nil(t, handle.Update(domain.InfoSchema()))
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t1,t2,t3,t4")
	require.Nil(t, handle.Update(domain.InfoSchema()))

	var (
		input  []string
		output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
	)
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	matchSetStmt, err := regexp.Compile("^set")
	require.NoError(t, err)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
		})
		ok := matchSetStmt.MatchString(ts)
		if ok {
			tk.MustExec(ts)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
		// Expect no warnings.
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}
