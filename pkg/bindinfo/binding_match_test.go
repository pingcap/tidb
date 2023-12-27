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

package bindinfo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func getTableName(n []*ast.Node) []string {
	var result []string
	for _, v := range n {
		var sb strings.Builder
		restoreFlags := format.RestoreKeyWordLowercase
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		switch node := (*v).(type) {
		case *ast.TableName:
			node.Restore(restoreCtx)
			result = append(result, sb.String())
		case *ast.ColumnName:
			if node.Schema.String() != "" {
				result = append(result, fmt.Sprintf("%s.%s", node.Schema.String(), node.Table.String()))
			} else {
				result = append(result, node.Table.String())
			}
		}
	}
	return result
}

func TestExtractTableName(t *testing.T) {
	tc := []struct {
		sql    string
		tables []string
	}{
		{
			"select /*+ HASH_JOIN(t1, t2) */ * from t1 t1 join t1 t2 on t1.a=t2.a where t1.b is not null;",
			[]string{"t1", "t1", "t1", "t2", "t1"},
		},
	}
	for _, tt := range tc {
		stmt, err := parser.New().ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		rs := ExtractTableName(stmt)
		require.Equal(t, tt.tables, getTableName(rs))
	}
}
