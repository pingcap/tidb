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

package stmtsummary

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/util/stmtsummary"
)

func BenchmarkStmtSummaryAddSingleWorkload(b *testing.B) {
	info := GenerateStmtExecInfo4Test("digest_test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Add(info)
	}
}

func BenchmarkStmtSummaryAddParallelSingleWorkload(b *testing.B) {
	const infoCount = 1000
	infos := make([]*stmtsummary.StmtExecInfo, infoCount)
	for i := range infoCount {
		infos[i] = GenerateStmtExecInfo4Test("digest_test")
	}

	var id atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := id.Add(1)
		for pb.Next() {
			Add(infos[idx])
		}
	})
}

func BenchmarkStmtSummaryAddParallelMultiWorkload(b *testing.B) {
	infoCount := 1000
	infos := make([]*stmtsummary.StmtExecInfo, infoCount)
	for i := range infoCount {
		infos[i] = GenerateStmtExecInfo4Test(fmt.Sprintf("digest_test_%d", i))
	}

	var id atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := id.Add(1)
		for pb.Next() {
			Add(infos[idx])
		}
	})
}
