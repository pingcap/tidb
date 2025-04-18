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

package ddl

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNewMetaBuildContextWithSctx(t *testing.T) {
	sqlMode := mysql.ModeStrictAllTables | mysql.ModeNoZeroDate
	sctx := mock.NewContext()
	sctx.GetSessionVars().SQLMode = sqlMode
	sessVars := sctx.GetSessionVars()
	cases := []struct {
		field    string
		setSctx  func(val any)
		testVals []any
		getter   func(*metabuild.Context) any
		check    func(*metabuild.Context)
		extra    func()
	}{
		{
			field: "exprCtx",
			check: func(ctx *metabuild.Context) {
				require.Same(t, sctx.GetExprCtx(), ctx.GetExprCtx())
				require.Equal(t, sqlMode, ctx.GetSQLMode())
				require.Equal(t, sctx.GetSessionVars().DefaultCollationForUTF8MB4, ctx.GetDefaultCollationForUTF8MB4())
				require.Equal(t, "utf8mb4_bin", ctx.GetDefaultCollationForUTF8MB4())
				warn := errors.New("warn1")
				note := errors.New("note1")
				ctx.AppendWarning(warn)
				ctx.AppendNote(note)
				require.Equal(t, []contextutil.SQLWarn{
					{Level: contextutil.WarnLevelWarning, Err: warn},
					{Level: contextutil.WarnLevelNote, Err: note},
				}, ctx.GetExprCtx().GetEvalCtx().CopyWarnings(nil))
			},
		},
		{
			field: "enableAutoIncrementInGenerated",
			setSctx: func(val any) {
				sessVars.EnableAutoIncrementInGenerated = val.(bool)
			},
			testVals: []any{true, false},
			getter: func(ctx *metabuild.Context) any {
				return ctx.EnableAutoIncrementInGenerated()
			},
		},
		{
			field: "primaryKeyRequired",
			setSctx: func(val any) {
				sessVars.PrimaryKeyRequired = val.(bool)
			},
			testVals: []any{true, false},
			getter: func(ctx *metabuild.Context) any {
				return ctx.PrimaryKeyRequired()
			},
			extra: func() {
				// `PrimaryKeyRequired` should always return false if `InRestrictedSQL` is true.
				sessVars.PrimaryKeyRequired = true
				sessVars.InRestrictedSQL = true
				require.False(t, NewMetaBuildContextWithSctx(sctx).PrimaryKeyRequired())
			},
		},
		{
			field: "clusteredIndexDefMode",
			setSctx: func(val any) {
				sessVars.EnableClusteredIndex = val.(vardef.ClusteredIndexDefMode)
			},
			testVals: []any{
				vardef.ClusteredIndexDefModeIntOnly,
				vardef.ClusteredIndexDefModeOff,
				vardef.ClusteredIndexDefModeOn,
			},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetClusteredIndexDefMode()
			},
		},
		{
			field: "shardRowIDBits",
			setSctx: func(val any) {
				sessVars.ShardRowIDBits = val.(uint64)
			},
			testVals: []any{uint64(vardef.DefShardRowIDBits), uint64(6)},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetShardRowIDBits()
			},
		},
		{
			field: "preSplitRegions",
			setSctx: func(val any) {
				sessVars.PreSplitRegions = val.(uint64)
			},
			testVals: []any{uint64(vardef.DefPreSplitRegions), uint64(123)},
			getter: func(ctx *metabuild.Context) any {
				return ctx.GetPreSplitRegions()
			},
		},
		{
			field: "suppressTooLongIndexErr",
			extra: func() {
				require.True(t,
					NewMetaBuildContextWithSctx(sctx, metabuild.WithSuppressTooLongIndexErr(true)).
						SuppressTooLongIndexErr(),
				)
				require.False(t,
					NewMetaBuildContextWithSctx(sctx, metabuild.WithSuppressTooLongIndexErr(false)).
						SuppressTooLongIndexErr(),
				)
			},
		},
		{
			field: "is",
			check: func(ctx *metabuild.Context) {
				sctxInfoSchema := sctx.GetDomainInfoSchema()
				require.NotNil(t, sctxInfoSchema)
				is, ok := ctx.GetInfoSchema()
				require.True(t, ok)
				require.Same(t, sctxInfoSchema, is)
			},
		},
	}

	allFields := make([]string, 0, len(cases))
	for _, f := range cases {
		t.Run(f.field, func(t *testing.T) {
			require.NotEmpty(t, f.field)
			allFields = append(allFields, "$."+f.field)
			if f.check != nil {
				ctx := NewMetaBuildContextWithSctx(sctx)
				f.check(ctx)
			}
			for _, testVal := range f.testVals {
				f.setSctx(testVal)
				ctx := NewMetaBuildContextWithSctx(sctx)
				require.Equal(t, testVal, f.getter(ctx), "field: %s, v: %v", f.field, testVal)
				if f.check != nil {
					f.check(ctx)
				}
			}
			if f.extra != nil {
				f.extra()
			}
		})
	}

	// make sure all fields are tested (WithIgnorePath contains all fields that the below asserting will pass).
	deeptest.AssertRecursivelyNotEqual(t, &metabuild.Context{}, &metabuild.Context{}, deeptest.WithIgnorePath(allFields))
}
