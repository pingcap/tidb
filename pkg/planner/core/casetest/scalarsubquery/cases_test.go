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
		var (
			input []struct {
				SQL              string
				IsExplainAnalyze bool
			}
			output []struct {
				SQL  string
				Plan []string
			}
		)
		planSuiteData := GetPlanSuiteData()
		planSuiteData.LoadTestCases(t, &input, &output, cascades, caller)

		// Setup test tables with comprehensive data types
		testKit.MustExec("use test")
		testKit.MustExec("set tidb_cost_model_version=1")
		testKit.MustExec("drop table if exists t1, t2, t3, t4")

		// Create tables with comprehensive data types for better testing coverage
		testKit.MustExec("create table t1 (a int, b bigint, c decimal(10,2), d double, e float, f char(20), g varchar(100), h datetime, i timestamp, j time, k year, l json, m bit(8), n enum('a','b','c'), o set('x','y','z'), p binary(10), q varbinary(20), r tinyint, s smallint, t mediumint, u tinyint unsigned, v smallint unsigned, w mediumint unsigned, x int unsigned, y bigint unsigned)")
		testKit.MustExec("create table t2 (a tinyint, b smallint, c mediumint, d int, e bigint, f decimal(15,3), g numeric(12,4), h float, i double, j real, k char(30), l varchar(80), m tinytext, n text, o mediumtext, p longtext, q tinyblob, r blob, s mediumblob, t longblob, u binary(15), v varbinary(25), w date, x datetime(3), y timestamp(6), z time(3), aa year, bb json, cc bit(16), dd enum('red','green','blue'), ee set('apple','banana','cherry'))")
		testKit.MustExec("create table t3 (a float, b double, c decimal(8,2), d numeric(10,1), e char(25), f varchar(50), g tinytext, h text, i mediumtext, j longtext, k tinyblob, l blob, m mediumblob, n longblob, o binary(12), p varbinary(18), q date, r datetime(2), s timestamp(4), t time(2), u year, v json, w bit(12), x enum('one','two','three'), y set('cat','dog','bird'), z tinyint, aa smallint, bb mediumint, cc int, dd bigint)")
		testKit.MustExec("create table t4 (a decimal(20,5), b numeric(18,6), c float, d double, e real, f char(40), g varchar(120), h tinytext, i text, j mediumtext, k longtext, l tinyblob, m blob, n mediumblob, o longblob, p binary(20), q varbinary(30), r date, s datetime(6), t timestamp(3), u time(6), v year, w json, x bit(24), y enum('jan','feb','mar'), z set('monday','tuesday','wednesday'), aa tinyint unsigned, bb smallint unsigned, cc mediumint unsigned, dd int unsigned, ee bigint unsigned, ff tinyint, gg smallint, hh mediumint, ii int, jj bigint)")

		// Insert comprehensive test data
		testKit.MustExec("insert into t1 values (1, 100, 10.50, 123.456, 789.012, 'char_val', 'varchar_val', '2023-01-01 10:00:00', '2023-01-01 10:00:00', '10:00:00', 2023, '{\"key\":\"value\"}', b'10101010', 'a', 'x,y', x'0A0B0C0D0E', x'0F1011121314', 127, 32767, 8388607, 255, 65535, 16777215, 4294967295, 18446744073709551615)")
		testKit.MustExec("insert into t2 values (127, 32767, 8388607, 2147483647, 9223372036854775807, 123.456, 987.6543, 456.789, 789.123, 321.654, 'char_30_chars_long', 'varchar_80_chars', 'tiny_text', 'medium_text_content', 'long_text_content_here', 'very_long_text_content', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-02-15', '2023-02-15 15:30:45.123', '2023-02-15 15:30:45.123456', '15:30:45.123', 2024, '{\"color\":\"red\"}', b'1111000011110000', 'red', 'apple,cherry')")
		testKit.MustExec("insert into t3 values (123.456, 789.123, 456.78, 987.6, 'char_25_chars_long', 'varchar_50_chars', 'tiny_text', 'medium_text', 'long_text_content', 'very_long_text', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-03-20', '2023-03-20 20:45:30.12', '2023-03-20 20:45:30.1234', '20:45:30.12', 25, '{\"number\":42}', b'101010101010', 'one', 'cat,dog', 127, 32767, 8388607, 2147483647, 9223372036854775807)")
		testKit.MustExec("insert into t4 values (123456.78901, 987654.321098, 456.789, 789.123, 321.654, 'char_40_chars_very_long_string', 'varchar_120_chars_very_long_string_content_here', 'tiny_text_content', 'medium_text_content_here', 'long_text_content_here', 'very_long_text_content_here', x'0102030405', x'060708090A0B0C0D0E0F', x'10111213141516171819', x'1A1B1C1D1E1F2021222324', x'25262728292A2B2C2D2E2F', x'30313233343536373839', '2023-04-25', '2023-04-25 12:15:30.123456', '2023-04-25 12:15:30.123', '12:15:30.123456', 2025, '{\"month\":\"april\"}', b'111100001111000011110000', 'jan', 'monday,tuesday', 255, 65535, 16777215, 4294967295, 18446744073709551615, 127, 32767, 8388607, 2147483647, 9223372036854775807)")

		// Define the same cutting function used in other tests
		cutExecutionInfoFromExplainAnalyzeOutput := func(rows [][]any) [][]any {
			// The columns are id, estRows, actRows, task type, access object, execution info, operator info, memory, disk
			// We only want columns 0, 3, 6 (id, task, operator info)
			for i := range rows {
				rows[i] = []any{rows[i][0], rows[i][3], rows[i][6]}
			}
			return rows
		}

		for i, ts := range input {
			testdata.OnRecord(func() {
				output[i].SQL = ts.SQL
				rows := testKit.MustQuery(ts.SQL).Rows()
				if ts.IsExplainAnalyze {
					rows = cutExecutionInfoFromExplainAnalyzeOutput(rows)
				}
				output[i].Plan = testdata.ConvertRowsToStrings(rows)
			})

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
	})
}
