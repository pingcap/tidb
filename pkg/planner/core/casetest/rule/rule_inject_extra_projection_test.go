// Copyright 2019 PingCAP, Inc.
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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestOrderByHashCollision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_order (id int primary key, dt datetime, marker int default 0)")
	for _, order := range []string{
		"cast(dt as date), cast(dt as datetime), id",
		"to_seconds(cast(dt as date)), to_seconds(cast(dt as datetime)), id",
		"cast(dt as date), cast(dt as datetime), cast(dt as datetime) desc, id",
	} {
		t.Run(order, func(t *testing.T) {
			for _, operation := range []string{"sort", "topn", "aggregate", "delete", "update"} {
				t.Run(operation, func(t *testing.T) {
					tk := testkit.NewTestKit(t, store)
					tk.MustExec("use test")
					tk.MustExec("delete from t_order")
					tk.MustExec("insert into t_order(id,dt) values (1,'2024-01-15 10:00:00'),(2,'2024-01-15 09:00:00'),(3,'2024-01-16 08:00:00')")
					switch operation {
					case "sort":
						tk.MustQuery("select id from t_order order by " + order).Check(testkit.Rows("2", "1", "3"))
					case "topn":
						tk.MustQuery("select id from t_order order by " + order + " limit 1").Check(testkit.Rows("2"))
					case "aggregate":
						tk.MustQuery("select group_concat(id order by " + order + ") from t_order").Check(testkit.Rows("2,1,3"))
					case "delete":
						tk.MustExec("delete from t_order order by " + order + " limit 1")
						tk.MustQuery("select id from t_order order by id").Check(testkit.Rows("1", "3"))
					case "update":
						tk.MustExec("update t_order set marker=1 order by " + order + " limit 1")
						tk.MustQuery("select id from t_order where marker=1").Check(testkit.Rows("2"))
					}
				})
			}
		})
	}
}

func TestWrapCastForAggFuncs(t *testing.T) {
	ctx := exprstatic.NewEvalContext()

	aggNames := []string{ast.AggFuncSum}
	modes := []aggregation.AggFunctionMode{aggregation.CompleteMode,
		aggregation.FinalMode, aggregation.Partial1Mode, aggregation.Partial1Mode}
	retTypes := []byte{mysql.TypeLong, mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24}
	hasDistincts := []bool{true, false}

	aggFuncs := make([]*aggregation.AggFuncDesc, 0, 32)
	for _, hasDistinct := range hasDistincts {
		for _, name := range aggNames {
			for _, mode := range modes {
				for _, retType := range retTypes {
					sctx := mock.NewContext()
					aggFunc, err := aggregation.NewAggFuncDesc(sctx, name,
						[]expression.Expression{&expression.Constant{Value: types.Datum{}, RetType: types.NewFieldType(retType)}},
						hasDistinct)
					require.NoError(t, err)
					aggFunc.Mode = mode
					aggFuncs = append(aggFuncs, aggFunc)
				}
			}
		}
	}

	orgAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(aggFuncs))
	for _, agg := range aggFuncs {
		orgAggFuncs = append(orgAggFuncs, agg.Clone())
	}

	coreusage.WrapCastForAggFuncs(mock.NewContext(), aggFuncs)
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			require.Equal(t, aggFuncs[i].Args[0].GetType(ctx).GetType(), aggFuncs[i].RetTp.GetType())
		} else {
			require.Equal(t, orgAggFuncs[i].Args[0].GetType(ctx).GetType(), aggFuncs[i].Args[0].GetType(ctx).GetType())
		}
	}
}
