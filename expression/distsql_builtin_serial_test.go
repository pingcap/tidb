// Copyright 2021 PingCAP, Inc.
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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPBToExprWithNewCollation(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)

	cases := []struct {
		name    string
		expName string
		id      int32
		pbID    int32
	}{
		{"utf8_general_ci", "utf8_general_ci", 33, 33},
		{"UTF8MB4_BIN", "utf8mb4_bin", 46, 46},
		{"utf8mb4_bin", "utf8mb4_bin", 46, 46},
		{"utf8mb4_general_ci", "utf8mb4_general_ci", 45, 45},
		{"", "utf8mb4_bin", 46, 46},
		{"some_error_collation", "utf8mb4_bin", 46, 46},
		{"utf8_unicode_ci", "utf8_unicode_ci", 192, 192},
		{"utf8mb4_unicode_ci", "utf8mb4_unicode_ci", 224, 224},
		{"utf8mb4_zh_pinyin_tidb_as_cs", "utf8mb4_zh_pinyin_tidb_as_cs", 2048, 2048},
	}

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		require.Equal(t, cs.pbID, expr.FieldType.Collate)

		e, err := PBToExpr(expr, fieldTps, sc)
		require.NoError(t, err)
		cons, ok := e.(*Constant)
		require.True(t, ok)
		require.Equal(t, cs.expName, cons.Value.Collation())
	}

	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	for _, cs := range cases {
		ft := types.NewFieldType(mysql.TypeString)
		ft.Collate = cs.name
		expr := new(tipb.Expr)
		expr.Tp = tipb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		require.Equal(t, -cs.pbID, expr.FieldType.Collate)

		e, err := PBToExpr(expr, fieldTps, sc)
		require.NoError(t, err)
		cons, ok := e.(*Constant)
		require.True(t, ok)
		require.Equal(t, cs.expName, cons.Value.Collation())
	}
}
