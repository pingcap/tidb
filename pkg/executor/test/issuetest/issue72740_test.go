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

package issuetest_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// SUM/AVG over a BIT column must be exact DECIMAL arithmetic: MySQL treats BIT
// as an unsigned integer, while TiDB used to infer DOUBLE and silently round
// values above 2^53. The rounded value was persisted by INSERT ... SELECT and
// dropped groups in HAVING. See https://github.com/pingcap/tidb/issues/72740.
func TestIssue72740(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (g int, b bit(64))")
	tk.MustExec("insert into t values (1, 9007199254740993), (1, 1)")
	tk.MustQuery("select g, sum(b), avg(b), sum(distinct b), sum(b+0) from t group by g").Check(testkit.Rows(
		"1 9007199254740994 4503599627370497.0000 9007199254740994 9007199254740994"))

	// The aggregate pushed to the coprocessor must carry an explicit decimal
	// cast on the BIT argument, not the raw column.
	explain := tk.MustQuery("explain select sum(b) from t").Rows()
	var pushedAgg string
	for _, row := range explain {
		cells := make([]string, 0, len(row))
		for _, cell := range row {
			cells = append(cells, cell.(string))
		}
		line := strings.Join(cells, " ")
		if strings.Contains(line, "cop[tikv]") && strings.Contains(line, "sum(") {
			pushedAgg = line
		}
	}
	if pushedAgg == "" {
		t.Fatal("expected sum(b) to be pushed down to cop[tikv]")
	}
	if !strings.Contains(pushedAgg, "cast(test.t.b") {
		t.Fatalf("pushed aggregate should cast the BIT argument, got: %s", pushedAgg)
	}

	// The exact sum survives INSERT ... SELECT.
	tk.MustExec("create table dst (v decimal(30,0))")
	tk.MustExec("insert into dst select sum(b) from t where g = 1")
	tk.MustQuery("select v from dst").Check(testkit.Rows("9007199254740994"))

	// HAVING against the exact sum keeps the group.
	tk.MustQuery("select g from t group by g having sum(b) = 9007199254740994").Check(testkit.Rows("1"))
	tk.MustQuery("select g from t group by g having sum(b) > 9007199254740993").Check(testkit.Rows("1"))
	tk.MustQuery("select g from t group by g having sum(b) < 9007199254740995").Check(testkit.Rows("1"))

	// Values around 2^64 stay exact, including the carry past BIT(64) max.
	tk.MustExec("create table t64 (g int, b bit(64))")
	tk.MustExec("insert into t64 values (1, 18446744073709551615), (1, 1)")
	tk.MustQuery("select sum(b), avg(b) from t64").Check(testkit.Rows(
		"18446744073709551616 9223372036854775808.0000"))

	// Accumulating ten BIT(64) maxima produces a 21-digit sum, wider than any
	// single input value.
	tk.MustExec("create table acc (b bit(64))")
	tk.MustExec("insert into acc values (18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615),(18446744073709551615)")
	tk.MustQuery("select sum(b), avg(b) from acc").Check(testkit.Rows(
		"184467440737095516150 18446744073709551615.0000"))

	// Small values come back as exact decimals, not floats.
	tk.MustExec("create table small (b bit(16))")
	tk.MustExec("insert into small values (256)")
	tk.MustQuery("select sum(b), avg(b) from small").Check(testkit.Rows("256 256.0000"))

	tk.MustExec("create table flags (b bit(1))")
	tk.MustExec("insert into flags values (1), (0), (1), (1)")
	tk.MustQuery("select sum(b), avg(b) from flags").Check(testkit.Rows("3 0.7500"))

	// BIT(63) above 2^53.
	tk.MustExec("create table b63 (b bit(63))")
	tk.MustExec("insert into b63 values (9223372036854775807), (1)")
	tk.MustQuery("select sum(b), avg(b) from b63").Check(testkit.Rows(
		"9223372036854775808 4611686018427387904.0000"))

	// NULL rows and empty input.
	tk.MustExec("create table nul (g int, b bit(64))")
	tk.MustExec("insert into nul values (1, null), (2, 5)")
	tk.MustQuery("select g, sum(b), avg(b) from nul group by g order by g").Check(testkit.Rows(
		"1 <nil> <nil>", "2 5 5.0000"))
	tk.MustExec("create table empty (b bit(64))")
	tk.MustQuery("select sum(b), avg(b) from empty").Check(testkit.Rows("<nil> <nil>"))

	// DISTINCT deduplicates before summing.
	tk.MustExec("create table dup (b bit(64))")
	tk.MustExec("insert into dup values (9007199254740993), (9007199254740993), (1)")
	tk.MustQuery("select sum(distinct b), sum(b) from dup").Check(testkit.Rows(
		"9007199254740994 18014398509481987"))

	// Several groups exercise accumulator reset between groups.
	tk.MustExec("create table grp (g int, b bit(64))")
	tk.MustExec("insert into grp values (1, 9007199254740993), (1, 1), (2, 9007199254740993), (2, 2)")
	tk.MustQuery("select g, sum(b), avg(b) from grp group by g order by g").Check(testkit.Rows(
		"1 9007199254740994 4503599627370497.0000",
		"2 9007199254740995 4503599627370497.5000"))

	// Window aggregates.
	tk.MustExec("create table win (g int, b bit(64))")
	tk.MustExec("insert into win values (1, 9007199254740993), (1, 1), (2, 9007199254740993)")
	tk.MustQuery("select g, sum(b) over (partition by g), avg(b) over (partition by g) from win order by g").Check(testkit.Rows(
		"1 9007199254740994 4503599627370497.0000",
		"1 9007199254740994 4503599627370497.0000",
		"2 9007199254740993 9007199254740993.0000"))

	// Aggregate over a join (self join on g produces four rows).
	tk.MustQuery("select sum(t1.b) from t t1 join t t2 on t1.g = t2.g").Check(testkit.Rows("18014398509481988"))

	// Binary literals keep their existing behavior.
	tk.MustQuery("select sum(b'1010')").Check(testkit.Rows("10"))
}