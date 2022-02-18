// Copyright 2017-present PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestStmtLabel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table label (c1 int primary key, c2 int, c3 int, index (c2))")
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("insert into label values (%d, %d, %d)", i, i, i)
		tk.MustExec(sql)
	}
	tk.MustExec("analyze table label")

	tests := []struct {
		sql   string
		label string
	}{
		{"select 1", "Select"},
		{"select * from label t1, label t2", "Select"},
		{"select * from label t1 where t1.c3 > (select count(t1.c1 = t2.c1) = 0 from label t2)", "Select"},
		{"select count(*) from label", "Select"},
		{"select * from label where c2 = 1", "Select"},
		{"select c1, c2 from label where c2 = 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label order by c3 limit 1", "Select"},
		{"delete from label", "Delete"},
		{"delete from label where c1 = 1", "Delete"},
		{"delete from label where c2 = 1", "Delete"},
		{"delete from label where c2 = 1 order by c3 limit 1", "Delete"},
		{"update label set c3 = 3", "Update"},
		{"update label set c3 = 3 where c1 = 1", "Update"},
		{"update label set c3 = 3 where c2 = 1", "Update"},
		{"update label set c3 = 3 where c2 = 1 order by c3 limit 1", "Update"},
	}
	for _, tt := range tests {
		stmtNode, err := parser.New().ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(tk.Session(), stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err)
		require.Equal(t, tt.label, executor.GetStmtLabel(stmtNode))
	}
}
