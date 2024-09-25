// Copyright 2024 PingCAP, Inc.
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

package metabuild_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/stretchr/testify/require"
)

func TestMetaBuildContext(t *testing.T) {
	defVars := variable.NewSessionVars(nil)
	fields := []struct {
		name         string
		getter       func(ctx *metabuild.Context) any
		checkDefault any
		option       func(val any) metabuild.Option
		testVals     []any
	}{
		{
			name: "exprCtx",
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetExprCtx()
			},
			checkDefault: func(ctx *metabuild.Context) {
				require.NotNil(t, ctx.GetExprCtx())
				cs, col := ctx.GetExprCtx().GetCharsetInfo()
				defCs, defCol := charset.GetDefaultCharsetAndCollate()
				require.Equal(t, defCs, cs)
				require.Equal(t, defCol, col)
				defSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
				require.NoError(t, err)
				require.Equal(t, defSQLMode, ctx.GetSQLMode())
				require.Equal(t, ctx.GetExprCtx().GetEvalCtx().SQLMode(), ctx.GetSQLMode())
				require.Equal(t, defVars.DefaultCollationForUTF8MB4, ctx.GetDefaultCollationForUTF8MB4())
				require.Equal(t, ctx.GetExprCtx().GetDefaultCollationForUTF8MB4(), ctx.GetDefaultCollationForUTF8MB4())
			},
			option: func(val any) metabuild.Option {
				return metabuild.WithExprCtx(val.(exprctx.ExprContext))
			},
			testVals: []any{exprstatic.NewExprContext()},
		},
		{
			name: "enableAutoIncrementInGenerated",
			getter: func(ctx *metabuild.Context) any {
				return ctx.EnableAutoIncrementInGenerated()
			},
			checkDefault: defVars.EnableAutoIncrementInGenerated,
			option: func(val any) metabuild.Option {
				return metabuild.WithEnableAutoIncrementInGenerated(val.(bool))
			},
			testVals: []any{true, false},
		},
		{
			name: "primaryKeyRequired",
			getter: func(ctx *metabuild.Context) any {
				return ctx.PrimaryKeyRequired()
			},
			checkDefault: defVars.PrimaryKeyRequired,
			option: func(val any) metabuild.Option {
				return metabuild.WithPrimaryKeyRequired(val.(bool))
			},
			testVals: []any{true, false},
		},
		{
			name: "clusteredIndexDefMode",
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetClusteredIndexDefMode()
			},
			checkDefault: defVars.EnableClusteredIndex,
			option: func(val any) metabuild.Option {
				return metabuild.WithClusteredIndexDefMode(val.(variable.ClusteredIndexDefMode))
			},
			testVals: []any{variable.ClusteredIndexDefModeOn, variable.ClusteredIndexDefModeOff},
		},
		{
			name: "shardRowIDBits",
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetShardRowIDBits()
			},
			checkDefault: defVars.ShardRowIDBits,
			option: func(val any) metabuild.Option {
				return metabuild.WithShardRowIDBits(val.(uint64))
			},
			testVals: []any{uint64(6), uint64(8)},
		},
		{
			name: "preSplitRegions",
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetPreSplitRegions()
			},
			checkDefault: defVars.PreSplitRegions,
			option: func(val any) metabuild.Option {
				return metabuild.WithPreSplitRegions(val.(uint64))
			},
			testVals: []any{uint64(123), uint64(456)},
		},
		{
			name: "suppressTooLongIndexErr",
			getter: func(ctx *metabuild.Context) any {
				return ctx.SuppressTooLongIndexErr()
			},
			checkDefault: false,
			option: func(val any) metabuild.Option {
				return metabuild.WithSuppressTooLongIndexErr(val.(bool))
			},
			testVals: []any{true, false},
		},
		{
			name: "is",
			getter: func(ctx *metabuild.Context) any {
				is, ok := ctx.GetInfoSchema()
				require.Equal(t, ok, is != nil)
				return is
			},
			checkDefault: nil,
			option: func(val any) metabuild.Option {
				if val == nil {
					return metabuild.WithInfoSchema(nil)
				}
				return metabuild.WithInfoSchema(val.(infoschemactx.MetaOnlyInfoSchema))
			},
			testVals: []any{infoschema.MockInfoSchema(nil), nil},
		},
	}
	defCtx := metabuild.NewContext()
	allFields := make([]string, 0, len(fields))
	for _, field := range fields {
		t.Run("default_of_"+field.name, func(t *testing.T) {
			switch val := field.checkDefault.(type) {
			case func(*metabuild.Context):
				val(defCtx)
			default:
				require.Equal(t, field.checkDefault, field.getter(defCtx), field.name)
			}
		})
		allFields = append(allFields, "$."+field.name)
	}

	for _, field := range fields {
		t.Run("option_of_"+field.name, func(t *testing.T) {
			for _, val := range field.testVals {
				ctx := metabuild.NewContext(field.option(val))
				require.Equal(t, val, field.getter(ctx), "%s %v", field.name, val)
			}
		})
	}

	// test allFields are tested
	deeptest.AssertRecursivelyNotEqual(t, metabuild.Context{}, metabuild.Context{}, deeptest.WithIgnorePath(allFields))
}
