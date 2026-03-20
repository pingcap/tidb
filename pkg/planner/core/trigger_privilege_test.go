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

package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestTriggerDDLVisitInfoUsesTargetTable(t *testing.T) {
	sctx := MockContext()
	defer domain.GetDomain(sctx).StatsHandle().Close()
	sctx.GetSessionVars().User = &auth.UserIdentity{}

	tbl := MockSignedTable()
	tbl.Triggers = []*model.TriggerInfo{
		{
			Name:  pmodel.NewCIStr("trig1"),
			State: model.StatePublic,
		},
	}
	is := infoschema.MockInfoSchema([]*model.TableInfo{tbl})
	sctx.SetInfoSchema(is)
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(is)

	tests := []struct {
		sql string
		ans []visitInfo
	}{
		{
			sql: "CREATE TRIGGER trig1 BEFORE INSERT ON t FOR EACH ROW SELECT 1",
			ans: []visitInfo{
				{mysql.TriggerPriv, "test", "t", "", plannererrors.ErrTableaccessDenied.GenWithStackByArgs("TRIGGER", "", "", "t"), false, nil, false},
			},
		},
		{
			sql: "DROP TRIGGER trig1",
			ans: []visitInfo{
				{mysql.TriggerPriv, "test", "t", "", plannererrors.ErrTableaccessDenied.GenWithStackByArgs("TRIGGER", "", "", "t"), false, nil, false},
			},
		},
	}

	p := parser.New()
	for _, tt := range tests {
		comment := tt.sql
		stmt, _, err := p.Parse(tt.sql, "", "")
		require.NoError(t, err, comment)

		nodeW := resolve.NewNodeW(stmt[0])
		require.NoError(t, Preprocess(context.Background(), sctx, nodeW, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: is})), comment)

		builder, _ := NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil))
		_, err = builder.Build(context.Background(), nodeW)
		require.NoError(t, err, comment)

		checkVisitInfo(t, builder.visitInfo, tt.ans, comment)
	}
}

func TestTriggerPseudoColumnRewriteInvalidContextNoPanic(t *testing.T) {
	sctx := MockContext()
	defer domain.GetDomain(sctx).StatsHandle().Close()

	tbl := MockSignedTable()
	triggerCtx := &sctx.GetSessionVars().StmtCtx.TriggerCtx
	triggerCtx.InTrigger = true
	triggerCtx.TableInfo = tbl
	triggerCtx.NewData = nil
	triggerCtx.OldData = nil

	builder := &PlanBuilder{ctx: sctx, curClause: expressionClause}

	tests := []struct {
		name    string
		table   string
		wantErr error
	}{
		{name: "NewDataMissing", table: "new", wantErr: dbterror.ErrTrgNoSuchRowInTrg},
		{name: "OldDataMissing", table: "old", wantErr: dbterror.ErrTrgNoSuchRowInTrg},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rewriter := &expressionRewriter{
				planCtx: &exprRewriterPlanCtx{builder: builder},
			}
			col := &ast.ColumnName{
				Table: pmodel.NewCIStr(tt.table),
				Name:  pmodel.NewCIStr("a"),
			}

			var ok bool
			require.NotPanics(t, func() {
				ok = tryRewriteTriggerPseudoColumn(rewriter, col)
			})
			require.True(t, ok)
			require.True(t, terror.ErrorEqual(rewriter.err, tt.wantErr))
		})
	}
}
