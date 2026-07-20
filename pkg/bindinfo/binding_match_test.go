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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func getTableName(n []*ast.TableName) []string {
	result := make([]string, 0, len(n))
	for _, v := range n {
		var sb strings.Builder
		restoreFlags := format.RestoreKeyWordLowercase
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		v.Restore(restoreCtx)
		result = append(result, sb.String())
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
			[]string{"t1", "t1"},
		},
		{
			"select * from t",
			[]string{"t"},
		},
		{
			"select * from t1, t2, t3;",
			[]string{"t1", "t2", "t3"},
		},
		{
			"select * from t1 where t1.a > (select max(a) from t2);",
			[]string{"t1", "t2"},
		},
		{
			"select * from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
			[]string{"t1", "t2", "t3"},
		},
		{
			"select a,b,c,d,* from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
			[]string{"t1", "t2", "t3"},
		},
	}
	for _, tt := range tc {
		stmt, err := parser.New().ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		rs := CollectTableNames(stmt)
		result := getTableName(rs)
		require.Equal(t, tt.tables, result)
	}
}

func TestMayHaveSQLBinding(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"insert into t values (1)", false},
		{"insert into t values (1) on duplicate key update a = values(a)", false},
		{"replace into t values (1)", false},
		{"explain insert into t values (1)", false},
		{"insert into t select * from s", true},
		{"replace into t select * from s", true},
		{"explain insert into t select * from s", true},
		{"select * from t", true},
		{"update t set a = 1", true},
		{"delete from t where a = 1", true},
	}

	p := parser.New()
	for _, tt := range tests {
		stmt, err := p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		require.Equal(t, tt.want, mayHaveSQLBinding(stmt), tt.sql)
	}
}

func TestMatchSQLBindingSkipsInsertValues(t *testing.T) {
	tests := []string{
		"insert into t values (1)",
		"insert into t values (1) on duplicate key update a = values(a)",
		"replace into t values (1)",
		"explain insert into t values (1)",
	}

	sctx := mock.NewContext()
	sctx.GetSessionVars().UsePlanBaselines = true
	sctx.SetValue(SessionBindInfoKeyType, NewSessionBindingHandle())

	p := parser.New()
	for _, sql := range tests {
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		info := &BindingMatchInfo{}
		bindingSQL, ignoreBinding := MatchSQLBindingForPlanCache(sctx, stmt, info)
		require.Empty(t, bindingSQL, sql)
		require.False(t, ignoreBinding, sql)
		require.Empty(t, info.FuzzyDigest, sql)
		require.Nil(t, info.TableNames, sql)
	}
}
