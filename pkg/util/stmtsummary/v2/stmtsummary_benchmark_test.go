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

package stmtsummary

import (
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/util/stmtsummary"
)

// BenchmarkStmtSummaryAdd 测试单线程下 StmtSummary.Add 的性能
func BenchmarkStmtSummaryAdd(b *testing.B) {
	// 使用函数完整名称，避免被错误解析

	// 准备一个固定的 StmtExecInfo 进行测试
	info := GenerateStmtExecInfo4Test("digest_test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Add(info)
	}
}

func BenchmarkStmtSummaryAddParallelRealistic(b *testing.B) {
	// allocate different infos for each thread
    const infoCount = 1000
    infos := make([]*stmtsummary.StmtExecInfo, infoCount)
    for i := 0; i < infoCount; i++ {
        infos[i] = GenerateStmtExecInfo4Test("digest_test")
    }
    
	var id atomic.Int64
	// b.SetParallelism(10)
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
		idx := id.Add(1)
        for pb.Next() {
            Add(infos[idx])
        }
    })
}
