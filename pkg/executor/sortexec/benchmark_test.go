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

package sortexec_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

func benchmarkSortExec(b *testing.B, cas *testutil.SortCase) {
	opt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cas.Columns()...),
		Rows:       cas.Rows,
		Ctx:        cas.Ctx,
		Ndvs:       cas.Ndvs,
	}
	dataSource := testutil.BuildMockDataSource(opt)
	executor := &sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(cas.Ctx, dataSource.Schema(), 4, dataSource),
		ByItems:      make([]*util.ByItems, 0, len(cas.OrderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}
	for _, idx := range cas.OrderByIdx {
		executor.ByItems = append(executor.ByItems, &util.ByItems{Expr: cas.Columns()[idx]})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(executor)
		dataSource.PrepareChunks()

		b.StartTimer()
		if err := executor.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := executor.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := executor.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func benchmarkSortExecDerivateCases(b *testing.B, cas *testutil.SortCase) {
	cas.Ndvs = []int{0, 0}
	cas.OrderByIdx = []int{0, 1}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkSortExec(b, cas)
	})

	ndvs := []int{1, 10000}
	for _, ndv := range ndvs {
		cas.Ndvs = []int{ndv, 0}
		cas.OrderByIdx = []int{0, 1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.Ndvs = []int{ndv, 0}
		cas.OrderByIdx = []int{0}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.Ndvs = []int{ndv, 0}
		cas.OrderByIdx = []int{1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})
	}
}

func BenchmarkSortExec(b *testing.B) {
	b.ReportAllocs()
	cas := testutil.DefaultSortTestCase()
	benchmarkSortExecDerivateCases(b, cas)
}

func BenchmarkSortExecSpillToDisk(b *testing.B) {
	enableTmpStorageOnOOMCurrentVal := variable.EnableTmpStorageOnOOM.Load()
	variable.EnableTmpStorageOnOOM.Store(true)
	defer variable.EnableTmpStorageOnOOM.Store(enableTmpStorageOnOOMCurrentVal)

	b.ReportAllocs()
	cas := testutil.SortTestCaseWithMemoryLimit(1)
	benchmarkSortExecDerivateCases(b, cas)
}
