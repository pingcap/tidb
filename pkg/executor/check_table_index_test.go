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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestExtractCastArrayExpr(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tc := []struct {
		sql            string
		arrayExprIndex int
	}{
		{
			sql:            "CREATE TABLE t (a int, b JSON, KEY mv_idx_binary(a, (( ( CAST(b->'$[*]' AS BINARY(12) ARRAY ) ) ) )))",
			arrayExprIndex: 1,
		},
		{
			sql:            "CREATE TABLE t (a int, b JSON, KEY mv_idx_binary((a * 2), ( ( CAST(b->'$[*]' AS BINARY(12) ARRAY ) ) ) ))",
			arrayExprIndex: 1,
		},
		{
			sql:            "CREATE TABLE t (a int, CAST_AS_ARRAY int as (a * 2), case_array JSON, KEY mv_idx_binary(CAST_AS_ARRAY, (a * 2), (( ( CAST(case_array->'$[*]' AS BINARY(12) ARRAY ) ) ) )))",
			arrayExprIndex: 2,
		},
		{
			sql:            "CREATE TABLE t (a int, b int as (a * 2), CAST_AS_ARRAY JSON, KEY mv_idx_binary(b, (a * 2), (( ( CAST(`CAST_AS_ARRAY` AS CHAR(16)) ) ) )))",
			arrayExprIndex: -1,
		},
		{
			sql:            "CREATE TABLE t (a int, b int as (a * 2), CAST_AS_ARRAY JSON, KEY mv_idx_binary(b, (a * 2), (( ( CAST(CAST_AS_ARRAY AS CHAR(16)  ARRAY ) ) ) )))",
			arrayExprIndex: 2,
		},
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")

	for _, tt := range tc {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tt.sql)
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		meta := tbl.Meta()

		for i, col := range meta.Indices[0].Columns {
			tblCol := meta.Columns[col.Offset]
			extracted := executor.ExtractCastArrayExpr(tblCol)
			if i == tt.arrayExprIndex {
				require.True(t, len(extracted) > 0, "Expected to extract cast array expression, got empty result")
			} else {
				require.True(t, len(extracted) == 0, "Expected to extract nothing, got non-empty result")
			}
		}
	}
}
