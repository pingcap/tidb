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

package tables_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

type rangeColumnsCase struct {
	ddl  string
	vals [][]string // one value per column, "NULL" for NULL
}

var rangeColumnsConvCtx = types.NewContext(types.DefaultStmtFlags.WithIgnoreTruncateErr(true).
	WithIgnoreZeroInDate(true).WithIgnoreInvalidDateErr(true), time.UTC, contextutil.IgnoreWarn)

// rangeColumnsRow converts the values to the column types, as an INSERT does.
func rangeColumnsRow(tbl table.Table, vals []string) ([]types.Datum, bool) {
	cols := tbl.Cols()
	row := make([]types.Datum, len(cols))
	for i, c := range cols {
		if vals[i] == "NULL" {
			row[i].SetNull()
			continue
		}
		src := types.NewStringDatum(vals[i])
		d, err := src.ConvertTo(rangeColumnsConvCtx, &c.FieldType)
		if err != nil {
			return nil, false
		}
		row[i] = d
	}
	return row, true
}

func singleValues(vals ...string) [][]string {
	res := make([][]string, 0, len(vals))
	for _, v := range vals {
		res = append(res, []string{v})
	}
	return res
}

func rangeColumnsDay(n int) string {
	return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, n).Format(time.DateOnly)
}

// checkRangeColumnsLocate checks that comparing datums locates every row in the same
// partition, with the same error and warnings, as evaluating the bound expressions.
func checkRangeColumnsLocate(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cases []rangeColumnsCase) {
	evalCtx := tk.Session().GetExprCtx().GetEvalCtx()
	sc := tk.Session().GetSessionVars().StmtCtx
	for _, c := range cases {
		tk.MustExec("drop table if exists t")
		tk.MustExec(c.ddl)
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		pt := tbl.GetPartitionedTable()
		for _, v := range c.vals {
			row, ok := rangeColumnsRow(tbl, v)
			if !ok {
				continue
			}
			comment := fmt.Sprintf("%s %q", c.ddl, v)
			_, ok = tables.LocateRangeColumnsByDatumsForTest(evalCtx, pt, row)
			require.True(t, ok, comment)
			w0 := sc.WarningCount()
			exprIdx, exprErr := tables.LocateRangeColumnsByExprForTest(evalCtx, pt, row)
			w1 := sc.WarningCount()
			idx, err := pt.GetPartitionIdxByRow(evalCtx, row)
			require.Equal(t, w1-w0, sc.WarningCount()-w1, comment)
			if exprErr != nil {
				require.Error(t, err, comment)
				require.Equal(t, errors.Cause(exprErr).(*terror.Error).Code(), errors.Cause(err).(*terror.Error).Code(), comment)
				continue
			}
			require.NoError(t, err, comment)
			require.Equal(t, exprIdx, idx, comment)
		}
	}
}

func TestLocateRangeColumnsPartitionByDatums(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var cases []rangeColumnsCase
	add := func(ddl string, vals [][]string) { cases = append(cases, rangeColumnsCase{ddl, vals}) }

	// DATE, every bound and its neighbours.
	defs := make([]string, 0, 100)
	for i := range 100 {
		defs = append(defs, fmt.Sprintf("partition p%d values less than ('%s')", i, rangeColumnsDay(i+1)))
	}
	var dates []string
	for i := -3; i < 104; i++ {
		dates = append(dates, rangeColumnsDay(i))
	}
	dates = append(dates, "NULL", "0000-00-00", "2024-01-00", "2024-02-30", "9999-12-31", "2024-01-01 23:59:59", "2024-1-2")
	add("create table t (d date) partition by range columns (d) ("+strings.Join(defs, ",")+")", singleValues(dates...))
	add("create table t (d date) partition by range columns (d) ("+strings.Join(defs, ",")+", partition pmax values less than (maxvalue))", singleValues(dates...))
	for _, bound := range []string{"'2024-01-01 12:00:00'", "'20240101'", "'2024-01-01 00:00:00.000001'"} {
		add("create table t (d date) partition by range columns (d) (partition p0 values less than ("+bound+"), partition p1 values less than (maxvalue))",
			singleValues("NULL", "2023-12-31", "2024-01-01", "2024-01-02", "0000-00-00"))
	}

	// DATETIME with fractional seconds, in the column and in the bound.
	for _, tp := range []string{"datetime", "datetime(3)", "datetime(6)"} {
		for _, bound := range []string{"'2024-01-01 00:00:00'", "'2024-01-01 00:00:00.5'", "'2024-01-01 00:00:00.0004'", "'2024-01-01'"} {
			add(fmt.Sprintf("create table t (d %s) partition by range columns (d) (partition p0 values less than (%s), partition p1 values less than (maxvalue))", tp, bound),
				singleValues("NULL", "2023-12-31 23:59:59.999999", "2024-01-01", "2024-01-01 00:00:00.0004", "2024-01-01 00:00:00.0005",
					"2024-01-01 00:00:00.499", "2024-01-01 00:00:00.5", "2024-01-01 00:00:00.501", "2024-01-02", "0000-00-00 00:00:00"))
		}
	}

	// Strings, with case, trailing spaces and accents, and values above the last bound.
	strs := singleValues("NULL", "", " ", "a", "A", "a ", "b", "B", "b ", "B ", "ba", "c", "C", "c ", "C  ", "c\t", "j", "k", "K", "k ", "kk", "KK", "kk ", "y", "z", "Z", "z ", "zz", "é", "Ö", "ß")
	for _, coll := range []string{"utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci", "latin1_bin"} {
		cs := strings.SplitN(coll, "_", 2)[0]
		for _, tp := range []string{"varchar(20)", "char(10)"} {
			add(fmt.Sprintf("create table t (s %s charset %s collate %s) partition by range columns (s) (partition p0 values less than ('B'), partition p1 values less than ('c '), partition p2 values less than ('k'), partition p3 values less than ('kk'), partition p4 values less than ('z'))", tp, cs, coll), strs)
		}
	}
	add("create table t (s varbinary(20)) partition by range columns (s) (partition p0 values less than ('B'), partition p1 values less than ('b '), partition p2 values less than ('k'), partition p3 values less than (maxvalue))",
		singleValues("NULL", "", "A", "B", "a", "b", "b ", "b  ", "k", "k\x00", "z"))
	add("create table t (s binary(4)) partition by range columns (s) (partition p0 values less than ('B'), partition p1 values less than ('b'), partition p2 values less than ('k'))",
		singleValues("NULL", "", "A", "B", "a", "b", "b ", "j", "k", "z"))

	// Integers near the limits of their types.
	ints := []string{"NULL", "-129", "-128", "-1", "0", "1", "99", "100", "126", "127", "128", "255", "2147483647", "4294967295", "9223372036854775807", "18446744073709551615"}
	for _, tp := range []string{"tinyint", "int", "bigint"} {
		add(fmt.Sprintf("create table t (a %s) partition by range columns (a) (partition p0 values less than (0), partition p1 values less than (1), partition p2 values less than (127))", tp), singleValues(ints...))
		add(fmt.Sprintf("create table t (a %s unsigned) partition by range columns (a) (partition p1 values less than (1), partition p2 values less than (128), partition p3 values less than (maxvalue))", tp), singleValues(ints...))
	}
	add("create table t (a bigint unsigned) partition by range columns (a) (partition p0 values less than (9223372036854775807), partition p1 values less than (9223372036854775808), partition p2 values less than (18446744073709551615))",
		singleValues("NULL", "0", "9223372036854775806", "9223372036854775807", "9223372036854775808", "18446744073709551614", "18446744073709551615"))

	// Several columns, with ties and MAXVALUE in the middle of a bound.
	var multi [][]string
	for _, a := range []string{"NULL", "0", "1", "2", "3"} {
		for _, b := range []string{"NULL", "2023-12-31", "2024-01-01", "2024-01-02"} {
			for _, c := range []string{"NULL", "", "a", "A", "m", "M", "m ", "ma", "z"} {
				multi = append(multi, []string{a, b, c})
			}
		}
	}
	add("create table t (a int, b date, c varchar(5) collate utf8mb4_general_ci) partition by range columns (a, b, c) (partition p0 values less than (1, '2024-01-01', 'm'), partition p1 values less than (1, '2024-01-01', maxvalue), partition p2 values less than (1, maxvalue, maxvalue), partition p3 values less than (2, '2024-01-01', 'a'))", multi)

	checkRangeColumnsLocate(t, tk, dom, cases)
}

func TestLocateRangeColumnsPartitionByDatumsOldCollation(t *testing.T) {
	originCfg := *config.GetGlobalConfig()
	defer config.StoreGlobalConfig(&originCfg)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.NewCollationsEnabledOnFirstBootstrap = false
	})
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(true)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.False(t, collate.NewCollationEnabled())
	checkRangeColumnsLocate(t, tk, dom, []rangeColumnsCase{{
		"create table t (s varchar(20) collate utf8mb4_general_ci) partition by range columns (s) (partition p0 values less than ('B'), partition p1 values less than ('k'), partition p2 values less than (maxvalue))",
		singleValues("NULL", "", "a", "A", "b", "B", "b ", "j", "k", "K", "z"),
	}})

	// Collators built before NewCollationEnabled changed are not used.
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	row, ok := rangeColumnsRow(tbl, []string{"b"})
	require.True(t, ok)
	collate.SetNewCollationEnabledForTest(true)
	_, ok = tables.LocateRangeColumnsByDatumsForTest(tk.Session().GetExprCtx().GetEvalCtx(), tbl.GetPartitionedTable(), row)
	require.False(t, ok)
}

func BenchmarkLocateRangeColumnsPartition(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	evalCtx := tk.Session().GetExprCtx().GetEvalCtx()
	for _, c := range []struct {
		name, colDef string
		value        func(i int) string
	}{
		{"date", "d date", rangeColumnsDay},
		{"varchar_general_ci", "d varchar(20) collate utf8mb4_general_ci", func(i int) string { return fmt.Sprintf("k%05d", i) }},
	} {
		defs := make([]string, 0, 1000)
		for i := range 1000 {
			defs = append(defs, fmt.Sprintf("partition p%d values less than ('%s')", i, c.value(i+1)))
		}
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a int, " + c.colDef + ") partition by range columns (d) (" + strings.Join(defs, ",") + ", partition pmax values less than (maxvalue))")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(b, err)
		pt := tbl.GetPartitionedTable()
		rows := make([][]types.Datum, 0, 1100)
		for i := range 1100 {
			row, ok := rangeColumnsRow(tbl, []string{"1", c.value(i)})
			require.True(b, ok)
			rows = append(rows, row)
		}
		b.Run(c.name+"/datums", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if _, err := pt.GetPartitionIdxByRow(evalCtx, rows[i%len(rows)]); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(c.name+"/expressions", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if _, err := tables.LocateRangeColumnsByExprForTest(evalCtx, pt, rows[i%len(rows)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
