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
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func TestRefreshStatsStmt(t *testing.T) {
	tests := []struct {
		sql  string
		want string
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
	}

	p := parser.New()
	for _, test := range tests {
		stmt, err := p.ParseOneStmt(test.sql, "", "")
		require.NoError(t, err)
		var sb strings.Builder
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		require.NoError(t, err)
		require.Equal(t, test.want, sb.String())
	}
}
