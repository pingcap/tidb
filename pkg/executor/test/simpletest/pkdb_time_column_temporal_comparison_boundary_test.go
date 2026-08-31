// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func newTimeColumnTemporalComparisonTestKit(t *testing.T) *testkit.TestKit {
	t.Helper()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_time (id int primary key, trx_time time, dt datetime, key(trx_time))")
	tk.MustExec(`insert into t_time values
		(1, '-01:00:00', '1970-01-01 00:00:00'),
		(2, '00:00:00', '1970-01-01 00:00:00'),
		(3, '18:00:00', '1970-01-01 18:00:00'),
		(4, '19:10:06', '1970-01-01 19:10:06'),
		(5, '20:00:00', '1970-01-01 20:00:00'),
		(6, '25:00:00', '1970-01-02 01:00:00'),
		(7, null, '1970-01-01 19:10:06')`)
	return tk
}

func TestTimeColumnTemporalConstantBinaryComparison(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	for _, test := range []struct {
		name     string
		sql      string
		expected string
	}{
		{"equal", "select count(*) from t_time where trx_time = cast('1970-01-01 19:10:06' as datetime)", "1"},
		{"notEqual", "select count(*) from t_time where trx_time != cast('1970-01-01 19:10:06' as datetime)", "5"},
		{"notEqualSQLStandard", "select count(*) from t_time where trx_time <> cast('1970-01-01 19:10:06' as datetime)", "5"},
		{"greaterThan", "select count(*) from t_time where trx_time > cast('1970-01-01 19:10:06' as datetime)", "2"},
		{"greaterOrEqual", "select count(*) from t_time where trx_time >= cast('1970-01-01 19:10:06' as datetime)", "3"},
		{"lessThan", "select count(*) from t_time where trx_time < cast('1970-01-01 19:10:06' as datetime)", "3"},
		{"lessOrEqual", "select count(*) from t_time where trx_time <= cast('1970-01-01 19:10:06' as datetime)", "4"},
		{"constantOnLeftEqual", "select count(*) from t_time where cast('1970-01-01 19:10:06' as datetime) = trx_time", "1"},
		{"constantOnLeftRange", "select count(*) from t_time where cast('1970-01-01 19:10:06' as datetime) <= trx_time", "3"},
		{"timestampLiteral", "select count(*) from t_time where trx_time = timestamp '1970-01-01 19:10:06'", "1"},
		{"constantExpression", "select count(*) from t_time where trx_time = cast(concat('1970-01-01 ', '19:10:06') as datetime)", "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery(test.sql).Check(testkit.Rows(test.expected))
		})
	}
}

func TestTimeColumnTemporalComparisonFixControl70751(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	query := "select count(*) from t_time where trx_time = cast('1000-01-01 19:10:06' as datetime)"

	tk.MustQuery(query).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_opt_fix_control = '70751:OFF'")
	tk.MustQuery(query).Check(testkit.Rows("0"))
	tk.MustExec("set tidb_opt_fix_control = '70751:ON'")
	tk.MustQuery(query).Check(testkit.Rows("1"))
}

func TestTimeColumnTemporalComparisonOutsideScope(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("set timestamp = 69006")

	for _, test := range []struct {
		name     string
		sql      string
		expected string
	}{
		{"date", "select count(*) from t_time where trx_time = cast('1970-01-01' as date)", "1"},
		{"nullSafeEqual", "select count(*) from t_time where trx_time <=> cast('1970-01-01 19:10:06' as datetime)", "1"},
		{"nullif", "select nullif(trx_time, cast('1970-01-01 19:10:06' as datetime)) from t_time where id = 4", "<nil>"},
		{"joinedTimeColumn", "select count(*) from t_time t1 join t_time t2 on t1.trx_time = t2.trx_time where t1.trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		{"joinedBaseField", "select count(*) from t_time t1 join t_time t2 on t1.id = t2.id where t1.trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		// Scalar subqueries are not direct constants at rewrite time.
		{"scalarSubquery", "select count(*) from t_time where trx_time = (select cast('1000-01-01 19:10:06' as datetime))", "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery(test.sql).Check(testkit.Rows(test.expected))
		})
	}
}

func TestTimeColumnTemporalComparisonThroughDirectProjection(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("set timestamp = 69006")

	for _, test := range []struct {
		name     string
		sql      string
		expected string
	}{
		{"derivedField", "select count(*) from (select trx_time from t_time) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		{"derivedAlias", "select count(*) from (select trx_time as projected_time from t_time) d where projected_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		{"derivedFieldWithLimit", "select count(*) from (select trx_time from t_time limit 100) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		{"derivedFieldWithOrderBy", "select count(*) from (select trx_time from t_time order by id) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
		{"nestedDerivedField", "select count(*) from (select trx_time from (select trx_time from t_time) e) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery(test.sql).Check(testkit.Rows(test.expected))
		})
	}
}

func TestTimeColumnTemporalPreparedParameter(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("prepare time_cmp from 'select count(*) from t_time where trx_time = ?'")

	for _, test := range []struct {
		name     string
		value    string
		expected string
	}{
		{"zeroDateSameClock", "1970-00-01 19:10:06", "0"},
		{"matchingDatetime", "1970-01-01 19:10:06", "1"},
		{"differentClock", "1970-01-01 19:10:07", "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustExec("set @time_cmp = cast('" + test.value + "' as datetime)")
			tk.MustQuery("execute time_cmp using @time_cmp").Check(testkit.Rows(test.expected))
			if test.name == "zeroDateSameClock" {
				require.Equal(t, "invalid-zero temporal parameter", tk.Session().GetSessionVars().StmtCtx.PlanCacheUnqualified())
			}
		})
	}

	tk.MustExec("deallocate prepare time_cmp")
}

func TestTimeColumnTemporalComparisonUnderUnionScan(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("begin")
	tk.MustExec("insert into t_time values (8, '18:00:00', '1970-01-01 18:00:00')")

	// A write in the current transaction wraps the data source in LogicalUnionScan.
	// The direct base TIME column must still use the temporal comparison rewrite.
	tk.MustQuery("select count(*) from t_time where trx_time = cast('1000-01-01 19:10:06' as datetime)").Check(testkit.Rows("1"))
	tk.MustExec("rollback")
}

func TestTimeColumnTemporalComparisonTODO(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("set timestamp = 69006")
	tk.MustExec("set @temporal = cast('1970-01-01 19:10:06' as datetime)")

	// These cases are intentionally tracked without requiring MySQL-compatible
	// results yet. Keep the expected TiDB behavior here until the corresponding
	// expression paths are implemented consistently.
	for _, test := range []struct {
		name     string
		sql      string
		expected string
	}{
		// IN-list elements are not direct binary-comparison constants.
		{"multiElementIn", "select count(*) from t_time where trx_time in (cast('1000-01-01 19:10:06' as datetime), cast('9999-12-31 19:10:06' as datetime))", "0"},
		{"materializedDerivedExpression", "select count(*) from (select cast(trx_time as time) as trx_time from t_time limit 100) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "0"},
		// The mergeable derived expression remains on the existing path for now.
		{"derivedExpression", "select count(*) from (select cast(trx_time as time) as trx_time from t_time) d where trx_time = cast('1000-01-01 19:10:06' as datetime)", "0"},
		// MySQL returns 1.
		{"correlatedColumn", "select count(*) from t_time t1 where exists (select 1 from t_time t2 where t2.id = t1.id and t1.trx_time = cast('1000-01-01 19:10:06' as datetime))", "0"},
		{"singleElementIn", "select count(*) from t_time where trx_time in (cast('1000-01-01 19:10:06' as datetime))", "0"},
		{"between", "select count(*) from t_time where trx_time between cast('1000-01-01 00:00:00' as datetime) and cast('1000-12-31 23:59:59' as datetime)", "4"},
		{"userVariable", "select count(*) from t_time where trx_time = @temporal", "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery(test.sql).Check(testkit.Rows(test.expected))
		})
	}
}

func TestTimeColumnTemporalInvalidConstant(t *testing.T) {
	tk := newTimeColumnTemporalComparisonTestKit(t)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")

	for _, test := range []struct {
		name  string
		value string
	}{
		{"invalid", "not-a-datetime"},
		{"outOfRange", "10000-01-01 19:10:06"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery("select count(*) from t_time where trx_time = cast('" + test.value + "' as datetime)").Check(testkit.Rows("0"))
			// MySQL may report the same conversion warning more than once after
			// expression optimization. The warning code and query result are the
			// stable compatibility contract here.
			tk.MustQuery("show warnings").CheckContain("1292")
		})
	}
	tk.MustQuery("select count(*) from t_time where trx_time = cast('1970-00-01 19:10:06' as datetime)").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t_time where trx_time = cast('0000-00-00 19:10:06' as datetime)").Check(testkit.Rows("0"))

	tk.MustExec("set sql_mode = ''")
	tk.MustQuery("select count(*) from t_time where trx_time = cast('1970-00-01 19:10:06' as datetime)").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t_time where trx_time = cast('0000-00-00 19:10:06' as datetime)").Check(testkit.Rows("0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestTimeColumnFSPUsedForDatetimeConstant(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_time_fsp (id int primary key, t0 time(0), t3 time(3), key(t0), key(t3))")
	tk.MustExec("insert into t_time_fsp values (1, '16:11:01', '16:11:00.124')")

	tk.MustQuery("select id from t_time_fsp where t0 = cast('1970-01-01 16:11:00.600000' as datetime(6))").Check(testkit.Rows("1"))
	tk.MustQuery("select id from t_time_fsp where t3 = cast('1970-01-01 16:11:00.123500' as datetime(6))").Check(testkit.Rows("1"))
	tk.MustQuery("explain format='brief' select /*+ use_index(t_time_fsp, t0) */ * from t_time_fsp where t0 = cast('1970-01-01 16:11:00.600000' as datetime(6))").MultiCheckContain(
		[]string{"IndexRangeScan", "range:[16:11:01,16:11:01]"})
}
