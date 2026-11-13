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
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// BenchmarkParallelApplyCheapInner compares equally frequent repeated correlated
// keys in two physical orders. Scattered keys recur only after 1024 other keys,
// making simultaneous misses unlikely; adjacent keys exercise inflight sharing.
// Both use the normal optimizer decision to enable the cache, with no failpoint
// delay or forced cache setting. The inner side returns one indexed row per key.
func BenchmarkParallelApplyCheapInner(b *testing.B) {
	store := testkit.CreateMockStore(b)
	for _, concurrency := range []int{4, 32} {
		for _, ordered := range []bool{false, true} {
			for _, ordering := range []string{"scattered", "adjacent"} {
				name := fmt.Sprintf("c%d/ordered=%t/%s", concurrency, ordered, ordering)
				b.Run(name, func(b *testing.B) {
					tk := testkit.NewTestKit(b, store)
					tk.MustExec("use test")
					tk.MustExec("drop table if exists bench_apply_outer, bench_apply_inner")
					tk.MustExec("create table bench_apply_outer (id int primary key, k int not null)")
					tk.MustExec("create table bench_apply_inner (k int primary key, v int not null)")
					const distinctKeys, repetitions = 1024, 4
					const outerRows = distinctKeys * repetitions
					for start := 0; start < outerRows; start += 256 {
						values := make([]string, 0, 256)
						for i := start; i < start+256; i++ {
							k := i % distinctKeys
							if ordering == "adjacent" {
								k = i / repetitions
							}
							values = append(values, fmt.Sprintf("(%d,%d)", i, k))
						}
						tk.MustExec("insert into bench_apply_outer values " + strings.Join(values, ","))
					}
					for start := 0; start < distinctKeys; start += 256 {
						values := make([]string, 0, 256)
						for k := start; k < start+256; k++ {
							values = append(values, fmt.Sprintf("(%d,%d)", k, k+1))
						}
						tk.MustExec("insert into bench_apply_inner values " + strings.Join(values, ","))
					}
					tk.MustExec("analyze table bench_apply_outer, bench_apply_inner all columns")
					tk.MustExec("set tidb_enable_parallel_apply = true")
					tk.MustExec(fmt.Sprintf("set tidb_executor_concurrency = %d", concurrency))
					tk.MustExec("set tidb_mem_quota_apply_cache = 67108864")
					tk.MustExec("set tidb_enable_non_prepared_plan_cache = false")
					query := "select o.id, (select /*+ NO_DECORRELATE() */ max(i.v) from bench_apply_inner i where i.k = o.k) from bench_apply_outer o"
					if ordered {
						query += " order by o.id"
					}
					foundApply := false
					for _, row := range tk.MustQuery("explain analyze " + query).Rows() {
						line := fmt.Sprint(row)
						b.Log(line)
						if strings.Contains(line, "Apply") {
							require.Contains(b, line, fmt.Sprintf("Concurrency:%d", concurrency))
							require.Contains(b, line, "cache:ON")
							foundApply = true
						}
					}
					require.True(b, foundApply)
					// Check the result before timing without depending on output order.
					result := tk.MustQuery(query).Rows()
					require.Len(b, result, outerRows)
					for position, row := range result {
						var id, value int
						_, err := fmt.Sscan(fmt.Sprint(row[0]), &id)
						require.NoError(b, err)
						if ordered {
							require.Equal(b, position, id)
						}
						_, err = fmt.Sscan(fmt.Sprint(row[1]), &value)
						require.NoError(b, err)
						k := id % distinctKeys
						if ordering == "adjacent" {
							k = id / repetitions
						}
						require.Equal(b, k+1, value)
					}
					ctx := context.Background()
					run := func() {
						rs, err := tk.Exec(query)
						if err != nil {
							b.Fatal(err)
						}
						chk := rs.NewChunk(nil)
						rows := 0
						for {
							if err := rs.Next(ctx, chk); err != nil {
								_ = rs.Close()
								b.Fatal(err)
							}
							if chk.NumRows() == 0 {
								break
							}
							rows += chk.NumRows()
							chk.Reset()
						}
						if err := rs.Close(); err != nil {
							b.Fatal(err)
						}
						if rows != outerRows {
							b.Fatalf("got %d rows, expected %d", rows, outerRows)
						}
					}
					run()
					cpuNanoseconds := func() int64 {
						var usage syscall.Rusage
						require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
						return (int64(usage.Utime.Sec)+int64(usage.Stime.Sec))*1e9 + (int64(usage.Utime.Usec)+int64(usage.Stime.Usec))*1e3
					}
					b.ReportAllocs()
					beforeCPU := cpuNanoseconds()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						run()
					}
					b.StopTimer()
					elapsedCPU := cpuNanoseconds() - beforeCPU
					// Process CPU includes the mockstore's background goroutines.
					b.ReportMetric(float64(elapsedCPU)/float64(b.N), "cpu-ns/op")
				})
			}
		}
	}
}
