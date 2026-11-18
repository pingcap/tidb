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

package pushdown

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPBAggregationGroupByColumnIdentity(t *testing.T) {
	data := infoschema.NewData()
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	isBuilder := infoschema.NewBuilder(nil, schemaCacheSize, nil, data, schemaCacheSize > 0)
	require.NoError(t, isBuilder.InitWithDBInfos(nil, nil, nil, nil, 0))
	is := isBuilder.Build(math.MaxUint64)
	table, err := is.TableByName(context.Background(), metadef.InformationSchemaName, ast.NewCIStr(infoschema.ClusterTableTiDBStatementsStats))
	require.NoError(t, err)
	columns := make([]*model.ColumnInfo, 0, 4)
	for _, col := range table.Meta().Columns {
		switch col.Name.L {
		case "digest", "index_names", "exec_count", "last_seen":
			columns = append(columns, col)
		}
	}
	require.Len(t, columns, 4)
	cutoff := types.NewTime(types.FromDate(2000, 1, 1, 0, 0, 0, 0), mysql.TypeTimestamp, 0)
	packedCutoff, err := cutoff.ToPackedUint()
	require.NoError(t, err)
	for _, aggType := range []tipb.ExecType{tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg} {
		for _, filtered := range []bool{false, true} {
			name := aggType.String()
			if filtered {
				name += "/selection"
			}
			t.Run(name, func(t *testing.T) {
				ctx := mock.NewContext()
				// The input schema's identities need not equal its column offsets.
				ctx.GetSessionVars().PlanColumnID.Store(10)
				columnRef := func(index int) *tipb.Expr {
					return &tipb.Expr{Tp: tipb.ExprType_ColumnRef, Val: codec.EncodeInt(nil, int64(index))}
				}
				executors := []*tipb.Executor{{
					Tp: tipb.ExecType_TypeTableScan,
					TblScan: &tipb.TableScan{
						TableId: table.Meta().ID,
						Columns: util.ColumnsToProto(columns, false, false, false),
					},
				}}
				if filtered {
					executors = append(executors, &tipb.Executor{
						Tp: tipb.ExecType_TypeSelection,
						Selection: &tipb.Selection{Conditions: []*tipb.Expr{{
							Tp:        tipb.ExprType_ScalarFunc,
							Sig:       tipb.ScalarFuncSig_GETime,
							FieldType: expression.ToPBFieldType(types.NewFieldType(mysql.TypeTiny)),
							Children: []*tipb.Expr{
								columnRef(3),
								{Tp: tipb.ExprType_MysqlTime, Val: codec.EncodeUint(nil, packedCutoff), FieldType: expression.ToPBFieldType(&columns[3].FieldType)},
							},
						}}},
					})
				}
				sumType := types.NewFieldType(mysql.TypeNewDecimal)
				sumType.SetFlen(mysql.MaxDecimalWidth)
				sumType.SetDecimal(0)
				agg := &tipb.Executor{
					Tp: aggType,
					Aggregation: &tipb.Aggregation{
						AggFunc: []*tipb.Expr{
							{Tp: tipb.ExprType_Sum, Children: []*tipb.Expr{columnRef(2)}, FieldType: expression.ToPBFieldType(sumType)},
							{Tp: tipb.ExprType_Max, Children: []*tipb.Expr{columnRef(1)}, FieldType: expression.ToPBFieldType(&columns[1].FieldType)},
							{Tp: tipb.ExprType_First, Children: []*tipb.Expr{columnRef(0)}, FieldType: expression.ToPBFieldType(&columns[0].FieldType)},
						},
						GroupBy: []*tipb.Expr{columnRef(0)},
					},
				}
				executors = append(executors, agg)
				plan, err := core.NewPBPlanBuilder(ctx, is, nil).Build(executors)
				require.NoError(t, err)
				plan = core.InjectExtraProjection(plan)
				var groupBy []expression.Expression
				switch p := plan.(type) {
				case *physicalop.PhysicalHashAgg:
					groupBy = p.GroupByItems
				case *physicalop.PhysicalStreamAgg:
					groupBy = p.GroupByItems
				}
				projection, ok := plan.Children()[0].(*physicalop.PhysicalProjection)
				require.True(t, ok, "SUM's decimal cast must inject a projection")
				// Different digests sharing one index must still yield different group
				// keys after PB decoding and projection injection (issue #71943).
				for _, digest := range []string{"digest1", "digest2"} {
					row := chunk.MutRowFromValues(digest, "t:ia", uint64(1), cutoff).ToRow()
					values := make([]types.Datum, len(projection.Exprs))
					for i, expr := range projection.Exprs {
						values[i], err = expr.Eval(ctx.GetExprCtx().GetEvalCtx(), row)
						require.NoError(t, err)
					}
					key, err := groupBy[0].Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(values).ToRow())
					require.NoError(t, err)
					require.Equal(t, digest, key.GetString())
				}
			})
		}
	}
}
