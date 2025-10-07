// Copyright 2025 PingCAP, Inc.
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

package tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func BenchmarkOptimization(b *testing.B) {
	// Create in-memory buffers for profiling
	var cpuBuf bytes.Buffer

	store, _ := testkit.CreateMockStoreAndDomain(b)
	testKit := testkit.NewTestKit(b, store)

	// Create the table with many indexes as specified
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec(`
		CREATE TABLE t1 (
			a int,
			b int,
			c int,
			d int,
			e int,
			f int,
			KEY ia (a),
			KEY iab (a, b),
			KEY iac (a, c),
			KEY iad (a, d),
			KEY iae (a, e),
			KEY iaf (a, f),
			KEY iabc (a, b, c),
			KEY iabd (a, b, d),
			KEY iabe (a, b, e),
			KEY iabf (a, b, f),
			KEY iacb (a, c, b),
			KEY iacd (a, c, d),
			KEY iace (a, c, e),
			KEY iacf (a, c, f),
			KEY iade (a, d, e),
			KEY iadf (a, d, f),
			KEY iaeb (a, e, b),
			KEY iaec (a, e, c),
			KEY iaed (a, e, d),
			KEY iaef (a, e, f),
			KEY iafb (a, f, b),
			KEY iafc (a, f, c),
			KEY iafd (a, f, d),
			KEY iafe (a, f, e),
			KEY iabcd (a, b, c, d),
			KEY iabce (a, b, c, e),
			KEY iabcf (a, b, c, f),
			KEY iabdc (a, b, d, c),
			KEY iabfc (a, b, f, c),
			KEY iabfd (a, b, f, d),
			KEY iabfe (a, b, f, e),
			KEY iacbd (a, c, b, d),
			KEY iacbe (a, c, b, e),
			KEY iacbf (a, c, b, f),
			KEY iacdb (a, c, d, b),
			KEY iacde (a, c, d, e),
			KEY iadbe (a, d, b, e),
			KEY iadcb (a, d, c, b),
			KEY iadcf (a, d, c, f),
			KEY ib (b),
			KEY iba (b, a),
			KEY ibc (b, c),
			KEY ibd (b, d),
			KEY ibe (b, e),
			KEY ibf (b, f),
			KEY ibac (b, a, c),
			KEY ibad (b, a, d),
			KEY ibae (b, a, e),
			KEY ibaf (b, a, f),
			KEY ibca (b, c, a),
			KEY ibcd (b, c, d),
			KEY ibce (b, c, e),
			KEY ibcf (b, c, f),
			KEY ibda (b, d, a),
			KEY ibdc (b, d, c),
			KEY ibde (b, d, e),
			KEY ibdf (b, d, f),
			KEY ibea (b, e, a),
			KEY ibec (b, e, c),
			KEY ibfa (b, f, a),
			KEY ibfc (b, f, c),
			KEY ibfd (b, f, d),
			KEY ibfe (b, f, e)
		)
	`)

	sctx := testKit.Session().(sessionctx.Context)

	/* Other queries to add later:
	"SELECT * FROM t1 WHERE a = 1",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6 ORDER BY a",
	"SELECT * FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6 GROUP BY a",
	"SELECT a, COUNT(*) FROM t1 WHERE a = 1 AND b = 2 AND c = 3 GROUP BY a",
	"SELECT a, b, c FROM t1 WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6",
	"SELECT a, b, c FROM t1 WHERE b = 2 AND c = 3 AND d = 4 AND e = 5 AND f = 6",
	"SELECT b, c FROM t1 WHERE b = 2 AND c = 3",
	*/
	// Test queries with various predicate combinations
	queries := []string{
		"select a from t1 where (a = 1 and f = 1) or (a = 2 and f = 1) or (a = 3 and f = 1) or (a = 4 and f = 1) or (a = 5 and f = 1) or (a = 6 and f = 1) or (a = 7 and f = 1) or (a = 8 and f = 1) or (a = 9 and f = 1) or (a = 1 and f = 2) or (a = 2 and f = 2) or (a = 3 and f = 2) or (a = 4 and f = 2) or (a = 5 and f = 2) or (a = 6 and f = 2) or (a = 7 and f = 2) or (a = 8 and f = 2) or (a = 9 and f = 2)",
	}

	// Benchmark with different index pruning thresholds
	thresholds := []int{20, 100}

	// Start CPU profiling before running benchmarks
	if err := pprof.StartCPUProfile(&cpuBuf); err != nil {
		b.Fatalf("could not start CPU profile: %v", err)
	}

	defer func() {
		pprof.StopCPUProfile()

		// Write CPU profile to a file in the current directory
		cpuFile, err := os.Create("optimizer_cpu.prof")
		if err == nil {
			cpuFile.Write(cpuBuf.Bytes())
			cpuFile.Close()

			b.Log("\n=== CPU Profile - Optimizer/Planner Functions ===")
			b.Logf("Full profile saved to: optimizer_cpu.prof")
			analyzePlannerProfile(b, "optimizer_cpu.prof")
		}

		// Collect and write memory profile
		runtime.GC() // get up-to-date statistics
		if memProf := pprof.Lookup("heap"); memProf != nil {
			memFile, err := os.Create("optimizer_mem.prof")
			if err == nil {
				memProf.WriteTo(memFile, 0)
				memFile.Close()

				b.Log("\n=== Memory Profile - Optimizer/Planner Functions ===")
				b.Logf("Full profile saved to: optimizer_mem.prof")
				analyzePlannerProfile(b, "optimizer_mem.prof")
			}
		}

		b.Log("\n=== Interactive Analysis Commands ===")
		b.Log("CPU:    go tool pprof -http=:8080 optimizer_cpu.prof")
		b.Log("Memory: go tool pprof -http=:8080 optimizer_mem.prof")
	}()

	for _, threshold := range thresholds {
		b.Run(fmt.Sprintf("Threshold_%d", threshold), func(b *testing.B) {
			// Set the index pruning threshold
			testKit.MustExec(fmt.Sprintf("SET SESSION tidb_opt_index_prune_threshold = %d", threshold))

			for _, query := range queries {
				queryName := strings.ReplaceAll(query, " ", "_")
				if len(queryName) > 30 {
					queryName = queryName[:30]
				}
				b.Run(fmt.Sprintf("Query_%s", queryName), func(b *testing.B) {
					// Parse and preprocess the query
					stmts, err := session.Parse(sctx, query)
					require.NoError(b, err)
					require.Len(b, stmts, 1)

					ret := &plannercore.PreprocessorReturn{}
					nodeW := resolve.NewNodeW(stmts[0])
					err = plannercore.Preprocess(context.Background(), sctx, nodeW, plannercore.WithPreprocessorReturn(ret))
					require.NoError(b, err)

					// Build logical plan (this includes our optimization)
					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						_, err := plannercore.BuildLogicalPlanForTest(context.Background(), sctx, nodeW, ret.InfoSchema)
						require.NoError(b, err)
					}
				})
			}
		})
	}
}

// analyzePlannerProfile shows the call tree from BuildLogicalPlanForTest
func analyzePlannerProfile(b *testing.B, profilePath string) {
	// Use -tree view focused on the planner entry point to see optimizer rule breakdown
	cmd := exec.Command("go", "tool", "pprof",
		"-tree",
		"-nodecount=15",
		"-focus=BuildLogicalPlanForTest|doOptimize|logicalOptimize|physicalOptimize",
		profilePath)
	output, err := cmd.CombinedOutput()
	if err != nil || len(output) == 0 {
		// Fallback: try showing just planner package functions
		b.Log("Showing top planner/optimizer functions:")
		cmd = exec.Command("go", "tool", "pprof",
			"-top",
			"-nodecount=20",
			profilePath)
		output, err = cmd.CombinedOutput()
		if err != nil {
			b.Logf("Error analyzing profile: %v", err)
			return
		}
	}

	// Filter output to only show TiDB planner/optimizer functions
	lines := strings.Split(string(output), "\n")
	plannerLines := 0

	for _, line := range lines {
		if line == "" {
			continue
		}

		// Show header lines
		if strings.Contains(line, "Showing") ||
			strings.Contains(line, "flat") ||
			strings.Contains(line, "Total:") {
			b.Log(line)
			continue
		}

		// Show only lines with planner/optimizer/expression packages
		if strings.Contains(line, "github.com/pingcap/tidb/pkg/planner") ||
			strings.Contains(line, "github.com/pingcap/tidb/pkg/expression") {
			// Skip some noise
			if !strings.Contains(line, "chunk.") &&
				!strings.Contains(line, "types.") {
				b.Log(line)
				plannerLines++
			}
		}

		if plannerLines > 30 {
			b.Log("... (use interactive analysis for full details)")
			break
		}
	}

	if plannerLines == 0 {
		b.Log("No planner functions found. Showing all top functions:")
		for i, line := range lines {
			if i < 15 && line != "" {
				b.Log(line)
			}
		}
	}
}
