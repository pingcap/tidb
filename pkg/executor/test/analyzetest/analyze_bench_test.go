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

package analyzetest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

const (
	partitionCount    = 1000
	partitioninterval = 100
)

func BenchmarkAnalyzePartition(b *testing.B) {
	if testing.Short() {
		b.Skip("it takes too much time to run")
	}
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	sql := "create table t(a int,b varchar(100),c int,INDEX idx_c(c)) PARTITION BY RANGE ( a ) ("
	for n := partitioninterval; n < partitionCount*partitioninterval; n = n + partitioninterval {
		sql += "PARTITION p" + fmt.Sprint(n) + " VALUES LESS THAN (" + fmt.Sprint(n) + "),"
	}
	sql += "PARTITION p" + fmt.Sprint(partitionCount*partitioninterval) + " VALUES LESS THAN MAXVALUE)"
	tk.MustExec(sql)
	// insert random data into table t
	insertStr := "insert into t (a,b,c) values(0, 'abc', 0)"
	for i := 1; i < 100000; i++ {
		insertStr += fmt.Sprintf(" ,(%d, '%s', %d)", i, "abc", i)
	}
	insertStr += ";"
	tk.MustExec(insertStr)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("analyze table t")
	}
}
