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

package scalarsubquery

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestExplainNonEvaledSubquery(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		var (
			input []struct {
				SQL              string
				IsExplainAnalyze bool
				HasErr           bool
			}
			output []struct {
				SQL   string
				Plan  []string
				Error string
			}
		)
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		testKit.MustExec("use test")
		testKit.MustExec("create table t1(a int, b int, c int)")
		testKit.MustExec("create table t2(a int, b int, c int)")
		testKit.MustExec("create table t3(a varchar(5), b varchar(5), c varchar(5))")
		testKit.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery=true")

		cutExecutionInfoFromExplainAnalyzeOutput := func(rows [][]any) [][]any {
			// The columns are id, estRows, actRows, task type, access object, execution info, operator info, memory, disk
			// We need to cut the unstable output of execution info, memory and disk.
			for i := range rows {
				rows[i] = rows[i][:6] // cut the final memory and disk.
				rows[i] = slices.Delete(rows[i], 5, 6)
			}
			return rows
		}

		for i, ts := range input {
			testdata.OnRecord(func() {
				output[i].SQL = ts.SQL
				if ts.HasErr {
					err := testKit.ExecToErr(ts.SQL)
					require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
					output[i].Error = err.Error()
					output[i].Plan = nil
				} else {
					rows := testKit.MustQuery(ts.SQL).Rows()
					if ts.IsExplainAnalyze {
						rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
					}
					output[i].Plan = testdata.ConvertRowsToStrings(rows)
					output[i].Error = ""
				}
			})
			if ts.HasErr {
				err := testKit.ExecToErr(ts.SQL)
				require.NotNil(t, err, fmt.Sprintf("Failed at case #%d", i))
			} else {
				rows := testKit.MustQuery(ts.SQL).Rows()
				if ts.IsExplainAnalyze {
					rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
				}
				require.Equal(t,
					testdata.ConvertRowsToStrings(testkit.Rows(output[i].Plan...)),
					testdata.ConvertRowsToStrings(rows),
					fmt.Sprintf("Failed at case #%d, SQL: %v", i, ts.SQL),
				)
			}
		}
	})
}

func TestSubqueryInExplainAnalyze(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		// Setup test tables with more comprehensive data
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1, t2, t3, t4")
		testKit.MustExec("create table t1(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t2(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t3(a int, b int, c int, primary key(a))")
		testKit.MustExec("create table t4(a int, b int, c int, primary key(a))")

		// Insert test data for better execution plan analysis
		testKit.MustExec("insert into t1 values (1,10,100), (2,20,200), (3,30,300), (4,40,400), (5,50,500)")
		testKit.MustExec("insert into t2 values (1,11,111), (2,22,222), (3,33,333), (4,44,444), (5,55,555)")
		testKit.MustExec("insert into t3 values (1,12,121), (2,23,232), (3,34,343), (4,45,454), (5,56,565)")
		testKit.MustExec("insert into t4 values (1,13,131), (2,24,242), (3,35,353), (4,46,464), (5,57,575)")

		// Test 1: Subquery in WHERE clause
		sql := "explain analyze select *,(select a from t2 limit 1) from t1 where a = (select a from t2 limit 1)"
		rows := [][]any{
			{"Projection_43", "root", "test.t1.a, test.t1.b, test.t1.c, (ScalarQueryCol#14)->Column#15"},
			{"└─Point_Get_45", "root", "handle:1"},
			{"ScalarSubQuery_22", "root", "Output: ScalarQueryCol#10"},
			{"└─MaxOneRow_10", "root", ""},
			{"  └─Limit_13", "root", "offset:0, count:1"},
			{"    └─TableReader_19", "root", "data:Limit_18"},
			{"      └─Limit_18", "cop[tikv]", "offset:0, count:1"},
			{"        └─TableFullScan_17", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_42", "root", "Output: ScalarQueryCol#14"},
			{"└─MaxOneRow_30", "root", ""},
			{"  └─Limit_33", "root", "offset:0, count:1"},
			{"    └─TableReader_39", "root", "data:Limit_38"},
			{"      └─Limit_38", "cop[tikv]", "offset:0, count:1"},
			{"        └─TableFullScan_37", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 2: Subquery in projection columns
		sql = "explain analyze select a, (select max(b) from t2 where t2.a ) as max_b, (select count(*) from t3 where t3.a) as count_t3 from t1"
		rows = [][]any{
			{"Projection_74", "root", "test.t1.a, (ScalarQueryCol#14)->Column#15, (ScalarQueryCol#22)->Column#23"},
			{"└─TableReader_77", "root", "data:TableFullScan_76"},
			{"  └─TableFullScan_76", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_47", "root", "Output: ScalarQueryCol#14"},
			{"└─MaxOneRow_14", "root", ""},
			{"  └─StreamAgg_22", "root", "funcs:max(test.t2.b)->Column#13"},
			{"    └─TopN_37", "root", "test.t2.b:desc, offset:0, count:1"},
			{"      └─TableReader_45", "root", "data:TopN_44"},
			{"        └─TopN_44", "cop[tikv]", "test.t2.b:desc, offset:0, count:1"},
			{"          └─Selection_31", "cop[tikv]", "not(isnull(test.t2.b))"},
			{"            └─TableRangeScan_30", "cop[tikv]", "range:[-inf,0), (0,+inf], keep order:false, stats:pseudo"},
			{"ScalarSubQuery_73", "root", "Output: ScalarQueryCol#22"},
			{"└─MaxOneRow_53", "root", ""},
			{"  └─HashAgg_63", "root", "funcs:count(Column#20)->Column#19"},
			{"    └─TableReader_64", "root", "data:HashAgg_56"},
			{"      └─HashAgg_56", "cop[tikv]", "funcs:count(1)->Column#20"},
			{"        └─TableRangeScan_62", "cop[tikv]", "range:[-inf,0), (0,+inf], keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 3: Nested subqueries
		sql = "explain analyze select * from t1 where a = (select a from t2 where b = (select b from t3 where c = (select c from t4 where a = 1)))"
		rows = [][]any{
			{"TableDual_44", "root", "rows:0"},
			{"ScalarSubQuery_17", "root", "Output: ScalarQueryCol#13"},
			{"└─MaxOneRow_11", "root", ""},
			{"  └─Projection_12", "root", "test.t4.c"},
			{"    └─Point_Get_15", "root", "handle:1"},
			{"ScalarSubQuery_30", "root", "Output: ScalarQueryCol#14"},
			{"└─MaxOneRow_20", "root", ""},
			{"  └─TableReader_29", "root", "data:Projection_23"},
			{"    └─Projection_23", "cop[tikv]", "test.t3.b"},
			{"      └─Selection_28", "cop[tikv]", "eq(test.t3.c, 131<-(ScalarQueryCol#13))"},
			{"        └─TableFullScan_27", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_39", "root", "Output: ScalarQueryCol#15"},
			{"└─MaxOneRow_34", "root", ""},
			{"  └─TableDual_37", "root", "rows:0"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 4: CTE with nested subqueries
		sql = "explain analyze with cte1 as (select a, b from t1 where c > (select avg(c) from t2)), cte2 as (select a, b from t2 where a in (select a from t3 where b > 20)) select cte1.*, cte2.* from cte1 join cte2 on cte1.a = cte2.a"
		rows = [][]any{
			{"MergeJoin_84", "root", "inner join, left key:test.t2.a, right key:test.t3.a"},
			{"├─TableReader_117(Build)", "root", "data:Selection_116"},
			{"│ └─Selection_116", "cop[tikv]", "gt(test.t3.b, 20)"},
			{"│   └─TableFullScan_115", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"└─MergeJoin_100(Probe)", "root", "inner join, left key:test.t1.a, right key:test.t2.a"},
			{"  ├─TableReader_111(Build)", "root", "data:TableFullScan_110"},
			{"  │ └─TableFullScan_110", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"  └─TableReader_109(Probe)", "root", "data:Selection_108"},
			{"    └─Selection_108", "cop[tikv]", "gt(test.t1.c, 333<-(ScalarQueryCol#32))"},
			{"      └─TableFullScan_107", "cop[tikv]", "keep order:true, stats:pseudo"},
			{"ScalarSubQuery_27", "root", "Output: ScalarQueryCol#12"},
			{"└─MaxOneRow_7", "root", ""},
			{"  └─HashAgg_17", "root", "funcs:avg(Column#8, Column#9)->Column#7"},
			{"    └─TableReader_18", "root", "data:HashAgg_10"},
			{"      └─HashAgg_10", "cop[tikv]", "funcs:count(test.t2.c)->Column#8, funcs:sum(test.t2.c)->Column#9"},
			{"        └─TableFullScan_16", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_64", "root", "Output: ScalarQueryCol#32"},
			{"└─MaxOneRow_44", "root", ""},
			{"  └─HashAgg_54", "root", "funcs:avg(Column#28, Column#29)->Column#27"},
			{"    └─TableReader_55", "root", "data:HashAgg_47"},
			{"      └─HashAgg_47", "cop[tikv]", "funcs:count(test.t2.c)->Column#28, funcs:sum(test.t2.c)->Column#29"},
			{"        └─TableFullScan_53", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

		// Test 5: UNION with subqueries
		sql = "explain analyze select * from t1 where a in (select a from t2 union all select a from t3 where b > (select avg(b) from t4))"
		rows = [][]any{
			{"HashJoin_55", "root", "inner join, equal:[eq(test.t1.a, Column#19)]"},
			{"├─HashAgg_69(Build)", "root", "group by:Column#19, funcs:firstrow(Column#19)->Column#19"},
			{"│ └─Union_73", "root", ""},
			{"│   ├─TableReader_78", "root", "data:TableFullScan_77"},
			{"│   │ └─TableFullScan_77", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"│   └─TableReader_87", "root", "data:Projection_81"},
			{"│     └─Projection_81", "cop[tikv]", "test.t3.a->Column#19"},
			{"│       └─Selection_86", "cop[tikv]", "gt(test.t3.b, 35<-(ScalarQueryCol#18))"},
			{"│         └─TableFullScan_85", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"└─TableReader_58(Probe)", "root", "data:TableFullScan_57"},
			{"  └─TableFullScan_57", "cop[tikv]", "keep order:false, stats:pseudo"},
			{"ScalarSubQuery_31", "root", "Output: ScalarQueryCol#18"},
			{"└─MaxOneRow_11", "root", ""},
			{"  └─HashAgg_21", "root", "funcs:avg(Column#14, Column#15)->Column#13"},
			{"    └─TableReader_22", "root", "data:HashAgg_14"},
			{"      └─HashAgg_14", "cop[tikv]", "funcs:count(test.t4.b)->Column#14, funcs:sum(test.t4.b)->Column#15"},
			{"        └─TableFullScan_20", "cop[tikv]", "keep order:false, stats:pseudo"},
		}
		testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)
	})
}

func TestSubqueryWithDifferentDataTypesInExplainAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set tidb_cost_model_version=1")
	testKit.MustExec("drop table if exists t1, t2, t3, t4")

	// Create tables with comprehensive data types
	testKit.MustExec("create table t1 (a int, b bigint, c decimal(10,2), d double, e float, f char(20), g varchar(100), h datetime, i timestamp, j time, k year, l json, m bit(8), n enum('a','b','c'), o set('x','y','z'), p binary(10), q varbinary(20), r tinyint, s smallint, t mediumint, u tinyint unsigned, v smallint unsigned, w mediumint unsigned, x int unsigned, y bigint unsigned)")
	testKit.MustExec("create table t2 (a tinyint, b smallint, c mediumint, d int, e bigint, f decimal(15,3), g numeric(12,4), h float, i double, j real, k char(30), l varchar(80), m tinytext, n text, o mediumtext, p longtext, q tinyblob, r blob, s mediumblob, t longblob, u binary(15), v varbinary(25), w date, x datetime(3), y timestamp(6), z time(3), aa year, bb json, cc bit(16), dd enum('red','green','blue'), ee set('apple','banana','cherry'))")
	testKit.MustExec("create table t3 (a float, b double, c decimal(8,2), d numeric(10,1), e char(25), f varchar(50), g tinytext, h text, i mediumtext, j longtext, k tinyblob, l blob, m mediumblob, n longblob, o binary(12), p varbinary(18), q date, r datetime(2), s timestamp(4), t time(2), u year, v json, w bit(12), x enum('one','two','three'), y set('cat','dog','bird'), z tinyint, aa smallint, bb mediumint, cc int, dd bigint)")
	testKit.MustExec("create table t4 (a decimal(20,5), b numeric(18,6), c float, d double, e real, f char(40), g varchar(120), h tinytext, i text, j mediumtext, k longtext, l tinyblob, m blob, n mediumblob, o longblob, p binary(20), q varbinary(30), r date, s datetime(6), t timestamp(3), u time(6), v year, w json, x bit(24), y enum('jan','feb','mar'), z set('monday','tuesday','wednesday'), aa tinyint unsigned, bb smallint unsigned, cc mediumint unsigned, dd int unsigned, ee bigint unsigned, ff tinyint, gg smallint, hh mediumint, ii int, jj bigint)")

	// Insert comprehensive test data
	testKit.MustExec("insert into t1 values (1, 100, 10.50, 123.456, 789.012, 'char_val', 'varchar_val', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '10:00:00', 2023, '{\"key\":\"value\"}', b'10101010', 'a', 'x,y', x'0A0B0C0D0E', x'0F1011121314', 127, 32767, 8388607, 255, 65535, 16777215, 4294967295, 18446744073709551615)")
	testKit.MustExec("insert into t2 values (127, 32767, 8388607, 2147483647, 9223372036854775807, 123.456, 987.6543, 456.789, 789.123, 321.654, 'char_30_chars_long', 'varchar_80_chars', 'tiny_text', 'medium_text_content', 'long_text_content_here', 'very_long_text_content', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-02-15', '2023-02-15 15:30:45.123', '2023-02-15 15:30:45.123456', '15:30:45.123', 2024, '{\"color\":\"red\"}', b'1111000011110000', 'red', 'apple,cherry')")
	testKit.MustExec("insert into t3 values (123.456, 789.123, 456.78, 987.6, 'char_25_chars_long', 'varchar_50_chars', 'tiny_text', 'medium_text', 'long_text_content', 'very_long_text', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-03-20', '2023-03-20 20:45:30.12', '2023-03-20 20:45:30.1234', '20:45:30.12', 25, '{\"number\":42}', b'101010101010', 'one', 'cat,dog', 127, 32767, 8388607, 2147483647, 9223372036854775807)")
	testKit.MustExec("insert into t4 values (123456.78901, 987654.321098, 456.789, 789.123, 321.654, 'char_40_chars_very_long_string', 'varchar_120_chars_very_long_string_content_here', 'tiny_text_content', 'medium_text_content_here', 'long_text_content_here', 'very_long_text_content_here', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-04-25', '2023-04-25 12:15:30.123456', '2023-04-25 12:15:30.123', '12:15:30.123456', 2025, '{\"month\":\"april\"}', b'111100001111000011110000', 'jan', 'monday,tuesday', 255, 65535, 16777215, 4294967295, 18446744073709551615, 127, 32767, 8388607, 2147483647, 9223372036854775807)")

	// Test 1: Nested scalar subqueries with different data types and type conversions
	sql := "explain analyze select * from t1 where a = (select a from t2 where b = (select b from t3 where c = (select c from t4 where a = 1)))"
	rows := [][]any{
		{"TableDual_43", "root", "rows:0"},
		{"ScalarSubQuery_21", "root", "Output: ScalarQueryCol#127"},
		{"└─MaxOneRow_11", "root", ""},
		{"  └─TableReader_20", "root", "data:Projection_14"},
		{"    └─Projection_14", "cop[tikv]", "test.t4.c"},
		{"      └─Selection_19", "cop[tikv]", "eq(test.t4.a, 1)"},
		{"        └─TableFullScan_18", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_30", "root", "Output: ScalarQueryCol#128"},
		{"└─MaxOneRow_25", "root", ""},
		{"  └─TableDual_28", "root", "rows:0"},
		{"ScalarSubQuery_39", "root", "Output: ScalarQueryCol#129"},
		{"└─MaxOneRow_34", "root", ""},
		{"  └─TableDual_37", "root", "rows:0"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

	// Test 2: Multiple scalar subqueries with different return types
	sql = "explain analyze select * from t1 where a > (select count(*) from t2) and b < (select max(b) from t3) and c = (select avg(c) from t4)"
	rows = [][]any{
		{"TableReader_99", "root", "data:Selection_98"},
		{"└─Selection_98", "cop[tikv]", "eq(cast(test.t1.c, double BINARY), 456.78900146484375<-(ScalarQueryCol#138)), gt(test.t1.a, 1<-(ScalarQueryCol#62)), lt(test.t1.b, 790<-(ScalarQueryCol#95))"},
		{"  └─TableFullScan_97", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_27", "root", "Output: ScalarQueryCol#62"},
		{"└─MaxOneRow_7", "root", ""},
		{"  └─StreamAgg_22", "root", "funcs:count(Column#61)->Column#59"},
		{"    └─TableReader_23", "root", "data:StreamAgg_14"},
		{"      └─StreamAgg_14", "cop[tikv]", "funcs:count(1)->Column#61"},
		{"        └─TableFullScan_21", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_69", "root", "Output: ScalarQueryCol#95"},
		{"└─MaxOneRow_36", "root", ""},
		{"  └─StreamAgg_44", "root", "funcs:max(test.t3.b)->Column#94"},
		{"    └─TopN_59", "root", "test.t3.b:desc, offset:0, count:1"},
		{"      └─TableReader_67", "root", "data:TopN_66"},
		{"        └─TopN_66", "cop[tikv]", "test.t3.b:desc, offset:0, count:1"},
		{"          └─Selection_53", "cop[tikv]", "not(isnull(test.t3.b))"},
		{"            └─TableFullScan_52", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_94", "root", "Output: ScalarQueryCol#138"},
		{"└─MaxOneRow_74", "root", ""},
		{"  └─StreamAgg_89", "root", "funcs:avg(Column#136, Column#137)->Column#133"},
		{"    └─TableReader_90", "root", "data:StreamAgg_81"},
		{"      └─StreamAgg_81", "cop[tikv]", "funcs:count(test.t4.c)->Column#136, funcs:sum(test.t4.c)->Column#137"},
		{"        └─TableFullScan_88", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

	// Test 3: Scalar subquery with type conversion in arithmetic operations
	sql = "explain analyze select * from t1 where a + (select max(a) from t2) > (select avg(b) from t3)"
	rows = [][]any{
		{"TableReader_74", "root", "data:Selection_73"},
		{"└─Selection_73", "cop[tikv]", "gt(plus(test.t1.a, 127<-(ScalarQueryCol#60)), 789<-(ScalarQueryCol#97))"},
		{"  └─TableFullScan_72", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_44", "root", "Output: ScalarQueryCol#60"},
		{"└─MaxOneRow_11", "root", ""},
		{"  └─StreamAgg_19", "root", "funcs:max(test.t2.a)->Column#59"},
		{"    └─TopN_34", "root", "test.t2.a:desc, offset:0, count:1"},
		{"      └─TableReader_42", "root", "data:TopN_41"},
		{"        └─TopN_41", "cop[tikv]", "test.t2.a:desc, offset:0, count:1"},
		{"          └─Selection_28", "cop[tikv]", "not(isnull(test.t2.a))"},
		{"            └─TableFullScan_27", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_69", "root", "Output: ScalarQueryCol#97"},
		{"└─MaxOneRow_49", "root", ""},
		{"  └─StreamAgg_64", "root", "funcs:avg(Column#95, Column#96)->Column#92"},
		{"    └─TableReader_65", "root", "data:StreamAgg_56"},
		{"      └─StreamAgg_56", "cop[tikv]", "funcs:count(test.t3.b)->Column#95, funcs:sum(test.t3.b)->Column#96"},
		{"        └─TableFullScan_63", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

	// Test 4: Complex data type conversions with multiple scalar subqueries
	sql = "explain analyze select * from t1 where a > (select a from t2 where b = (select b from t3 where c = (select c from t4 where aa = 255))) and b < (select bb from t2 where cc = (select dd from t3 where ee = (select ff from t4 where gg = 32767)))"
	rows = [][]any{
		{"TableDual_72", "root", "rows:0"},
		{"ScalarSubQuery_21", "root", "Output: ScalarQueryCol#127"},
		{"└─MaxOneRow_11", "root", ""},
		{"  └─TableReader_20", "root", "data:Projection_14"},
		{"    └─Projection_14", "cop[tikv]", "test.t4.c"},
		{"      └─Selection_19", "cop[tikv]", "eq(test.t4.aa, 255)"},
		{"        └─TableFullScan_18", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_34", "root", "Output: ScalarQueryCol#128"},
		{"└─MaxOneRow_24", "root", ""},
		{"  └─TableReader_33", "root", "data:Projection_27"},
		{"    └─Projection_27", "cop[tikv]", "test.t3.b"},
		{"      └─Selection_32", "cop[tikv]", "eq(cast(test.t3.c, double BINARY), 456.789<-(ScalarQueryCol#127))"},
		{"        └─TableFullScan_31", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_43", "root", "Output: ScalarQueryCol#129"},
		{"└─MaxOneRow_38", "root", ""},
		{"  └─TableDual_41", "root", "rows:0"},
		{"ScalarSubQuery_62", "root", "Output: ScalarQueryCol#230"},
		{"└─MaxOneRow_52", "root", ""},
		{"  └─TableReader_61", "root", "data:Projection_55"},
		{"    └─Projection_55", "cop[tikv]", "test.t4.ff"},
		{"      └─Selection_60", "cop[tikv]", "eq(test.t4.gg, 32767)"},
		{"        └─TableFullScan_59", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

	// Test 5: String and binary data type conversions
	sql = "explain analyze select * from t1 where f = (select k from t2 where l = (select g from t3 where h = (select i from t4 where j = 'very_long_text_content_here'))) and g = (select l from t2 where m = (select h from t3 where i = (select j from t4 where f = 'char_40_chars_very_long_string')))"
	rows = [][]any{
		{"TableDual_84", "root", "rows:0"},
		{"ScalarSubQuery_21", "root", "Output: ScalarQueryCol#127"},
		{"└─MaxOneRow_11", "root", ""},
		{"  └─TableReader_20", "root", "data:Projection_14"},
		{"    └─Projection_14", "cop[tikv]", "test.t4.i"},
		{"      └─Selection_19", "cop[tikv]", "eq(test.t4.j, \"very_long_text_content_here\")"},
		{"        └─TableFullScan_18", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_30", "root", "Output: ScalarQueryCol#128"},
		{"└─MaxOneRow_25", "root", ""},
		{"  └─TableDual_28", "root", "rows:0"},
		{"ScalarSubQuery_39", "root", "Output: ScalarQueryCol#129"},
		{"└─MaxOneRow_34", "root", ""},
		{"  └─TableDual_37", "root", "rows:0"},
		{"ScalarSubQuery_58", "root", "Output: ScalarQueryCol#230"},
		{"└─MaxOneRow_48", "root", ""},
		{"  └─TableReader_57", "root", "data:Projection_51"},
		{"    └─Projection_51", "cop[tikv]", "test.t4.j"},
		{"      └─Selection_56", "cop[tikv]", "eq(test.t4.f, \"char_40_chars_very_long_string\")"},
		{"        └─TableFullScan_55", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_71", "root", "Output: ScalarQueryCol#231"},
		{"└─MaxOneRow_61", "root", ""},
		{"  └─TableReader_70", "root", "data:Projection_64"},
		{"    └─Projection_64", "cop[tikv]", "test.t3.h"},
		{"      └─Selection_69", "cop[tikv]", "eq(test.t3.i, \"long_text_content_here\"<-(ScalarQueryCol#230))"},
		{"        └─TableFullScan_68", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_80", "root", "Output: ScalarQueryCol#232"},
		{"└─MaxOneRow_75", "root", ""},
		{"  └─TableDual_78", "root", "rows:0"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

	// Test 6: Date, time and JSON data type conversions
	sql = "explain analyze select * from t1 where h = (select x from t2 where w = (select s from t3 where t = (select v from t4 where w = '{\"month\":\"april\"}'))) and i = (select y from t2 where z = (select u from t3 where v = (select w from t4 where y = 'jan')))"
	rows = [][]any{
		{"TableDual_82", "root", "rows:0"},
		{"ScalarSubQuery_21", "root", "Output: ScalarQueryCol#127"},
		{"└─MaxOneRow_11", "root", ""},
		{"  └─TableReader_20", "root", "data:Projection_14"},
		{"    └─Projection_14", "cop[tikv]", "test.t4.v"},
		{"      └─Selection_19", "cop[tikv]", "eq(test.t4.w, cast(\"{\"month\":\"april\"}\", json BINARY))"},
		{"        └─TableFullScan_18", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_30", "root", "Output: ScalarQueryCol#128"},
		{"└─MaxOneRow_25", "root", ""},
		{"  └─TableDual_28", "root", "rows:0"},
		{"ScalarSubQuery_39", "root", "Output: ScalarQueryCol#129"},
		{"└─MaxOneRow_34", "root", ""},
		{"  └─TableDual_37", "root", "rows:0"},
		{"ScalarSubQuery_58", "root", "Output: ScalarQueryCol#230"},
		{"└─MaxOneRow_48", "root", ""},
		{"  └─TableReader_57", "root", "data:Projection_51"},
		{"    └─Projection_51", "cop[tikv]", "test.t4.w"},
		{"      └─Selection_56", "cop[tikv]", "eq(test.t4.y, \"jan\")"},
		{"        └─TableFullScan_55", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_69", "root", "Output: ScalarQueryCol#231"},
		{"└─MaxOneRow_61", "root", ""},
		{"  └─Projection_62", "root", "test.t3.u"},
		{"    └─Selection_65", "root", "eq(test.t3.v, \"{\"month\": \"april\"}\"<-(ScalarQueryCol#230))"},
		{"      └─TableReader_67", "root", "data:TableFullScan_66"},
		{"        └─TableFullScan_66", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"ScalarSubQuery_78", "root", "Output: ScalarQueryCol#232"},
		{"└─MaxOneRow_73", "root", ""},
		{"  └─TableDual_76", "root", "rows:0"},
	}
	testKit.MustQuery(sql).CheckAt([]int{0, 3, 6}, rows)

}
