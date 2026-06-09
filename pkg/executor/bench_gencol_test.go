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

package executor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// BenchmarkInsertWideTableNoGC is the baseline: 150-column table, no generated columns.
func BenchmarkInsertWideTableNoGC(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	cols := make([]string, 150)
	vals := make([]string, 150)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d INT", i)
		vals[i] = "1"
	}
	tk.MustExec("CREATE TABLE t_no_gc (" + strings.Join(cols, ", ") + ")")

	insertSQL := "INSERT INTO t_no_gc VALUES (" + strings.Join(vals, ", ") + ")"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec(insertSQL)
	}
}

// BenchmarkInsertWideTableWithGC benchmarks INSERT into a table with 150 virtual
// generated columns (GENERATED ALWAYS AS (NULL) VIRTUAL) — the GTOC-8384 pattern.
func BenchmarkInsertWideTableWithGC(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	// 1 real base column + 150 virtual GCs.
	defs := []string{"id INT"}
	for i := 0; i < 150; i++ {
		defs = append(defs, fmt.Sprintf("g%d INT GENERATED ALWAYS AS (NULL) VIRTUAL", i))
	}
	tk.MustExec("CREATE TABLE t_gc (" + strings.Join(defs, ", ") + ")")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("INSERT INTO t_gc (id) VALUES (1)")
	}
}

// BenchmarkInsertYCSBLike benchmarks INSERT into an 11-column table with no generated columns,
// modeling the YCSB usertable schema (1 VARCHAR PK + 10 VARCHAR value fields).
// This is the regression scenario from issue #68129: MutRowFromDatums is called
// unconditionally even when gCols is empty, adding unnecessary allocation per row.
func BenchmarkInsertYCSBLike(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	defs := []string{"YCSB_KEY VARCHAR(255) PRIMARY KEY"}
	vals := []string{"'user1000'"}
	for i := 0; i < 10; i++ {
		defs = append(defs, fmt.Sprintf("FIELD%d VARCHAR(100)", i))
		vals = append(vals, fmt.Sprintf("'value%d'", i))
	}
	tk.MustExec("CREATE TABLE usertable (" + strings.Join(defs, ", ") + ")")

	// Use INSERT IGNORE so repeated runs don't fail on the duplicate PK.
	insertSQL := "INSERT IGNORE INTO usertable VALUES (" + strings.Join(vals, ", ") + ")"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec(insertSQL)
	}
}

// BenchmarkInsertWideTableWithNonLiteralGC benchmarks INSERT into a table with 150 virtual
// generated columns that reference a real column (GENERATED ALWAYS AS (LOWER(name)) VIRTUAL).
// This exercises the non-constant GC path where each GC must read the base column value.
func BenchmarkInsertWideTableWithNonLiteralGC(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	// 1 real VARCHAR base column + 150 virtual GCs derived from it.
	defs := []string{"name VARCHAR(30)"}
	for i := 0; i < 150; i++ {
		defs = append(defs, fmt.Sprintf("g%d VARCHAR(30) GENERATED ALWAYS AS (LOWER(name)) VIRTUAL", i))
	}
	tk.MustExec("CREATE TABLE t_gc_nonlit (" + strings.Join(defs, ", ") + ")")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("INSERT INTO t_gc_nonlit (name) VALUES ('HelloWorld')")
	}
}
