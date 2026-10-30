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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/mview"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mlogStatsTestContext struct {
	*mock.Context
	estimation *planctx.MLogCommitTSEstimation
}

func (c *mlogStatsTestContext) GetMLogCommitTSEstimation() *planctx.MLogCommitTSEstimation {
	return c.estimation
}

func (c *mlogStatsTestContext) WithMLogCommitTSEstimation(
	estimation *planctx.MLogCommitTSEstimation,
	fn func() error,
) error {
	original := c.estimation
	c.estimation = estimation
	defer func() { c.estimation = original }()
	return fn()
}

func TestDeriveStatsByFilterUsesMLogCommitTSSelectivity(t *testing.T) {
	ctx := &mlogStatsTestContext{Context: mock.NewContext()}
	tableInfo := &model.TableInfo{
		ID:                  100,
		Name:                ast.NewCIStr("mlog"),
		MaterializedViewLog: &model.MaterializedViewLogInfo{},
	}
	commitTSCol := &expression.Column{
		ID:       model.ExtraCommitTSID,
		UniqueID: 1,
		RetType:  model.NewExtraCommitTSColInfo().FieldType.Clone(),
	}
	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = tableInfo
	ds.SetSchema(expression.NewSchema(commitTSCol))
	ds.TableStats = &property.StatsInfo{
		RowCount: 100,
		HistColl: statistics.NewHistColl(tableInfo.ID, 100, 0, 0, 0),
	}
	ctx.estimation = &planctx.MLogCommitTSEstimation{
		MLogTableID:      tableInfo.ID,
		RetainedLowerTSO: oracle.ComposeTS(10, 0),
		RetainedUpperTSO: oracle.ComposeTS(110, 0),
	}

	commitTSType := types.NewFieldType(mysql.TypeLonglong)
	lowerFilterTSO := oracle.ComposeTS(20, 0)
	upperFilterTSO := oracle.ComposeTS(50, 0)
	conds := expression.CNFExprs{
		expression.NewFunctionInternal(
			ctx.GetExprCtx(), ast.GT, types.NewFieldType(mysql.TypeTiny),
			commitTSCol,
			&expression.Constant{Value: types.NewUintDatum(lowerFilterTSO), RetType: commitTSType},
		),
		expression.NewFunctionInternal(
			ctx.GetExprCtx(), ast.LE, types.NewFieldType(mysql.TypeTiny),
			commitTSCol,
			&expression.Constant{Value: types.NewUintDatum(upperFilterTSO), RetType: commitTSType},
		),
	}
	selectivityConds, mlogSelectivity, hasMLogSelectivity :=
		mview.SplitMLogCommitTSFilterSelectivity(ctx, tableInfo, ds.Schema(), conds)
	require.True(t, hasMLogSelectivity)
	require.Empty(t, selectivityConds)
	require.InDelta(t, 0.3, mlogSelectivity, 0.0001)

	stats := deriveStatsByFilter(ds, conds, nil)
	require.InDelta(t, 30, stats.RowCount, 0.0001)
}
