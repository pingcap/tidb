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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/testkit"
)

func BenchmarkInfoschemaTables(b *testing.B) {
	log.InitLogger(&log.Config{Level: "error"}, "")
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	prepareData(tk)

	cases := []struct {
		caseName string
		sql      string
	}{
		{"select all", "select * from information_schema.tables where table_schema = 'test';"},
		{"select basic", "select table_name from information_schema.tables where table_schema = 'test';"},
		{"select without table rows", "select TABLE_NAME, CREATE_TIME from information_schema.tables where table_schema = 'test';"},
		{"select with table rows", "select TABLE_NAME, TABLE_ROWS from information_schema.tables where table_schema = 'test';"},
	}

	for _, c := range cases {
		b.Run(c.caseName, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tk.MustQuery(c.sql)
			}
			b.StopTimer()
		})
	}
}

func prepareData(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c int);`)
	tk.MustExec(`create table t2 (a int, b int, c int);`)
	tk.MustExec(`create table t3 (a int, b int, c int);`)
	tk.MustExec(`insert into t1 values (1, 2, 3);`)
	tk.MustExec(`insert into t2 values (4, 5, 6);`)
	tk.MustExec(`insert into t3 values (7, 8, 9);`)
	tk.MustExec(`analyze table t1 all columns;`)
	tk.MustExec(`analyze table t2 all columns;`)
	tk.MustExec(`analyze table t3 all columns;`)
}
