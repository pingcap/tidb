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
	"context"
	"fmt"
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

	// Test queries with various predicate combinations
	queries := []string{
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
		"select a from t1 where (a = 1 and f = 1) or (a = 2 and f = 1) or (a = 3 and f = 1) or (a = 4 and f = 1) or (a = 5 and f = 1) or (a = 6 and f = 1) or (a = 7 and f = 1) or (a = 8 and f = 1) or (a = 9 and f = 1) or (a = 1 and f = 2) or (a = 2 and f = 2) or (a = 3 and f = 2) or (a = 4 and f = 2) or (a = 5 and f = 2) or (a = 6 and f = 2) or (a = 7 and f = 2) or (a = 8 and f = 2) or (a = 9 and f = 2)",
	}

	// Benchmark with different index pruning thresholds
	thresholds := []int{20, 100}

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
