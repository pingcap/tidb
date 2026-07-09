// Copyright 2026 PingCAP, Inc.
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

package copr_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/planner/core" // for initializing expression.BuildSimpleExpr
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCopContextConditionUsesFixedCollation(t *testing.T) {
	origin := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(origin)

	colTp := types.NewFieldTypeWithCollation(mysql.TypeVarchar, "utf8mb4_general_ci", 16)
	colInfo := &model.ColumnInfo{
		ID:        1,
		Offset:    0,
		Name:      ast.NewCIStr("c0"),
		FieldType: *colTp,
		State:     model.StatePublic,
	}
	idxInfo := &model.IndexInfo{
		ID:      1,
		Name:    ast.NewCIStr("idx"),
		Columns: []*model.IndexColumn{{Name: colInfo.Name, Offset: colInfo.Offset}},
		State:   model.StatePublic,
		// This collate clause is accepted only when the builder uses the old
		// collation mode, because new collation validates charset compatibility.
		ConditionExprString: "c0 collate utf8_general_ci = 'A'",
	}
	tblInfo := &model.TableInfo{
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{colInfo},
		Indices: []*model.IndexInfo{idxInfo},
	}

	sctx := mock.NewContext()
	for _, tt := range []struct {
		useNewCollate bool
		expectErr     bool
	}{
		{useNewCollate: false, expectErr: false},
		{useNewCollate: true, expectErr: true},
	} {
		copCtx, err := copr.NewCopContextSingleIndex(
			sctx.GetExprCtx(),
			sctx.GetSessionVars().StmtCtx.PushDownFlags(),
			tblInfo,
			idxInfo,
			"",
			tt.useNewCollate,
		)
		require.NoError(t, err)
		condition, err := copCtx.GetCondition()
		if tt.expectErr {
			require.Error(t, err)
			require.Nil(t, condition)
			continue
		}
		require.NoError(t, err)
		require.NotNil(t, condition)
	}
}
