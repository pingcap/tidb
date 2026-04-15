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
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func TestRefreshStatsStmt(t *testing.T) {
	tests := []struct {
		sql     string
		want    string
		modeSet bool
		mode    ast.RefreshStatsMode
	}{
		{
			sql:  "REFRESH STATS *.*",
			want: "REFRESH STATS *.*",
		},
		{
			sql:  "refresh stats *.*",
			want: "REFRESH STATS *.*",
		},
		{
			sql:  "REFRESH STATS db1.*",
			want: "REFRESH STATS `db1`.*",
		},
		{
			sql:  "REFRESH STATS db1.t1",
			want: "REFRESH STATS `db1`.`t1`",
		},
		{
			sql:  "REFRESH STATS table1",
			want: "REFRESH STATS `table1`",
		},
		{
			sql:  "REFRESH STATS table1, table2",
			want: "REFRESH STATS `table1`, `table2`",
		},
		{
			sql:  "REFRESH STATS *.*, db1.*, db2.t1, table1, table2",
			want: "REFRESH STATS *.*, `db1`.*, `db2`.`t1`, `table1`, `table2`",
		},
		{
			sql:     "REFRESH STATS table1 full",
			want:    "REFRESH STATS `table1` FULL",
			modeSet: true,
			mode:    ast.RefreshStatsModeFull,
		},
		{
			sql:  "REFRESH STATS table1 cluster",
			want: "REFRESH STATS `table1` CLUSTER",
		},
		{
			sql:     "REFRESH STATS db1.* lite cluster",
			want:    "REFRESH STATS `db1`.* LITE CLUSTER",
			modeSet: true,
			mode:    ast.RefreshStatsModeLite,
		},
	}

	p := parser.New()
	for _, test := range tests {
		stmt, err := p.ParseOneStmt(test.sql, "", "")
		require.NoError(t, err)
		rs := stmt.(*ast.RefreshStatsStmt)
		if test.modeSet {
			require.NotNil(t, rs.RefreshMode)
			require.Equal(t, test.mode, *rs.RefreshMode)
		} else {
			require.Nil(t, rs.RefreshMode)
		}
		var sb strings.Builder
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		require.NoError(t, err)
		require.Equal(t, test.want, sb.String())
	}
}

func TestRefreshStatsStmtDedup(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "global overrides all",
			sql:  "REFRESH STATS table1, db1.t1, *.*, db2.t2",
			want: "REFRESH STATS *.*",
		},
		{
			name: "database removes prior tables",
			sql:  "REFRESH STATS db1.t1, db2.t1, db1.*, db2.t2",
			want: "REFRESH STATS `db2`.`t1`, `db1`.*, `db2`.`t2`",
		},
		{
			name: "table duplicates case insensitive",
			sql:  "REFRESH STATS db1.t1, db1.T1, db2.t1",
			want: "REFRESH STATS `db1`.`t1`, `db2`.`t1`",
		},
		{
			name: "table duplicates without database",
			sql:  "REFRESH STATS table1, table1, table2",
			want: "REFRESH STATS `table1`, `table2`",
		},
		{
			name: "database duplicates case insensitive",
			sql:  "REFRESH STATS db1.*, DB1.*, db2.t1",
			want: "REFRESH STATS `db1`.*, `db2`.`t1`",
		},
	}

	p := parser.New()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := p.ParseOneStmt(test.sql, "", "")
			require.NoError(t, err)
			rs := stmt.(*ast.RefreshStatsStmt)
			rs.Dedup()
			var sb strings.Builder
			err = rs.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			require.NoError(t, err)
			require.Equal(t, test.want, sb.String())
		})
	}
}
