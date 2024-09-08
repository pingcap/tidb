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

package contextstatic

import (
	"strings"
	"testing"
	"time"

	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestNewStaticExprCtx(t *testing.T) {
	prevID := contextutil.GenContextID()
	ctx := NewStaticExprContext()
	require.Equal(t, ctx.GetEvalCtx().CtxID(), prevID+1)
	checkDefaultStaticExprCtx(t, ctx)

	opts, s := getExprCtxOptionsForTest()
	ctx = NewStaticExprContext(opts...)
	checkOptionsStaticExprCtx(t, ctx, s)
}

func TestStaticExprCtxApplyOptions(t *testing.T) {
	ctx := NewStaticExprContext()
	oldEvalCtx := ctx.evalCtx
	oldColumnIDAllocator := ctx.columnIDAllocator

	// apply with options
	opts, s := getExprCtxOptionsForTest()
	ctx2 := ctx.Apply(opts...)
	require.Equal(t, oldEvalCtx, ctx.evalCtx)
	require.Same(t, oldColumnIDAllocator, ctx.columnIDAllocator)
	checkDefaultStaticExprCtx(t, ctx)
	checkOptionsStaticExprCtx(t, ctx2, s)

	// apply with empty options
	ctx3 := ctx2.Apply()
	s.skipCacheArgs = nil
	checkOptionsStaticExprCtx(t, ctx3, s)
}

func checkDefaultStaticExprCtx(t *testing.T, ctx *StaticExprContext) {
	checkDefaultStaticEvalCtx(t, ctx.GetEvalCtx().(*StaticEvalContext))
	charsetName, collation := ctx.GetCharsetInfo()
	require.Equal(t, mysql.DefaultCharset, charsetName)
	cs, err := charset.GetCharsetInfo(charsetName)
	require.NoError(t, err)
	require.Equal(t, charsetName, cs.Name)
	require.Equal(t, cs.DefaultCollation, collation)
	require.Equal(t, mysql.DefaultCollationName, ctx.GetDefaultCollationForUTF8MB4())
	require.Equal(t, variable.DefBlockEncryptionMode, ctx.GetBlockEncryptionMode())
	require.Equal(t, variable.DefSysdateIsNow, ctx.GetSysdateIsNow())
	require.Equal(t, variable.TiDBOptOnOffWarn(variable.DefTiDBEnableNoopFuncs), ctx.GetNoopFuncsMode())
	require.NotNil(t, ctx.Rng())
	require.True(t, ctx.IsUseCache())
	require.NotNil(t, ctx.columnIDAllocator)
	_, ok := ctx.columnIDAllocator.(*exprctx.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, uint64(0), ctx.ConnectionID())
	require.Equal(t, true, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, variable.DefGroupConcatMaxLen, ctx.GetGroupConcatMaxLen())
}

type exprCtxOptionsTestState struct {
	evalCtx       *StaticEvalContext
	colIDAlloc    exprctx.PlanColumnIDAllocator
	rng           *mathutil.MysqlRng
	skipCacheArgs []any
}

func getExprCtxOptionsForTest() ([]StaticExprCtxOption, *exprCtxOptionsTestState) {
	s := &exprCtxOptionsTestState{
		evalCtx:    NewStaticEvalContext(WithLocation(time.FixedZone("UTC+11", 11*3600))),
		colIDAlloc: exprctx.NewSimplePlanColumnIDAllocator(1024),
		rng:        mathutil.NewWithSeed(12345678),
	}
	planCacheTracker := contextutil.NewPlanCacheTracker(s.evalCtx)

	return []StaticExprCtxOption{
		WithEvalCtx(s.evalCtx),
		WithCharset("gbk", "gbk_bin"),
		WithDefaultCollationForUTF8MB4("utf8mb4_0900_ai_ci"),
		WithBlockEncryptionMode("aes-256-cbc"),
		WithSysDateIsNow(true),
		WithNoopFuncsMode(variable.WarnInt),
		WithRng(s.rng),
		WithPlanCacheTracker(&planCacheTracker),
		WithColumnIDAllocator(s.colIDAlloc),
		WithConnectionID(778899),
		WithWindowingUseHighPrecision(false),
		WithGroupConcatMaxLen(2233445566),
	}, s
}

func checkOptionsStaticExprCtx(t *testing.T, ctx *StaticExprContext, s *exprCtxOptionsTestState) {
	require.Same(t, s.evalCtx, ctx.GetEvalCtx())
	cs, collation := ctx.GetCharsetInfo()
	require.Equal(t, "gbk", cs)
	require.Equal(t, "gbk_bin", collation)
	require.Equal(t, "utf8mb4_0900_ai_ci", ctx.GetDefaultCollationForUTF8MB4())
	require.Equal(t, "aes-256-cbc", ctx.GetBlockEncryptionMode())
	require.Equal(t, true, ctx.GetSysdateIsNow())
	require.Equal(t, variable.WarnInt, ctx.GetNoopFuncsMode())
	require.Same(t, s.rng, ctx.Rng())
	require.False(t, ctx.IsUseCache())
	require.Nil(t, s.skipCacheArgs)
	ctx.SetSkipPlanCache("reason")
	require.Same(t, s.colIDAlloc, ctx.columnIDAllocator)
	require.Equal(t, uint64(778899), ctx.ConnectionID())
	require.False(t, ctx.GetWindowingUseHighPrecision())
	require.Equal(t, uint64(2233445566), ctx.GetGroupConcatMaxLen())
}

func TestExprCtxColumnIDAllocator(t *testing.T) {
	// default
	ctx := NewStaticExprContext()
	alloc := ctx.columnIDAllocator
	require.NotNil(t, alloc)
	_, ok := ctx.columnIDAllocator.(*exprctx.SimplePlanColumnIDAllocator)
	require.True(t, ok)
	require.Equal(t, int64(1), ctx.AllocPlanColumnID())

	// Apply without an allocator
	ctx2 := ctx.Apply()
	require.Same(t, ctx2.columnIDAllocator, ctx.columnIDAllocator)
	require.Equal(t, int64(2), ctx2.AllocPlanColumnID())
	require.Equal(t, int64(3), ctx.AllocPlanColumnID())

	// Apply with new allocator
	alloc = exprctx.NewSimplePlanColumnIDAllocator(1024)
	ctx3 := ctx.Apply(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx3.columnIDAllocator)
	require.NotSame(t, ctx.columnIDAllocator, ctx3.columnIDAllocator)
	require.Equal(t, int64(1025), ctx3.AllocPlanColumnID())
	require.Equal(t, int64(4), ctx.AllocPlanColumnID())

	// New context with allocator
	alloc = exprctx.NewSimplePlanColumnIDAllocator(2048)
	ctx4 := NewStaticExprContext(WithColumnIDAllocator(alloc))
	require.Same(t, alloc, ctx4.columnIDAllocator)
	require.Equal(t, int64(2049), ctx4.AllocPlanColumnID())
}

func TestMakeExprContextStatic(t *testing.T) {
	evalCtx := NewStaticEvalContext()
	planCacheTracker := contextutil.NewPlanCacheTracker(evalCtx.warnHandler)
	obj := NewStaticExprContext(
		WithEvalCtx(evalCtx),
		WithCharset("a", "b"),
		WithDefaultCollationForUTF8MB4("c"),
		WithBlockEncryptionMode("d"),
		WithSysDateIsNow(true),
		WithNoopFuncsMode(1),
		WithRng(mathutil.NewWithSeed(12345678)),
		WithPlanCacheTracker(&planCacheTracker),
		WithColumnIDAllocator(exprctx.NewSimplePlanColumnIDAllocator(1)),
		WithConnectionID(1),
		WithWindowingUseHighPrecision(false),
		WithGroupConcatMaxLen(1),
	)

	ignorePath := []string{
		"$.staticExprCtxState.evalCtx**",
	}
	deeptest.AssertRecursivelyNotEqual(t, obj, NewStaticExprContext(),
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.staticExprCtxState.rng",
			"$.staticExprCtxState.planCacheTracker",
		}),
	)

	staticObj := MakeExprContextStatic(obj)
	deeptest.AssertDeepClonedEqual(t, obj, staticObj,
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.staticExprCtxState.rng",
			"$.staticExprCtxState.planCacheTracker",
		}))

	require.NotSame(t, obj.GetEvalCtx(), staticObj.GetEvalCtx())
}

func TestExprCtxLoadSystemVars(t *testing.T) {
	vars := []struct {
		name   string
		val    string
		field  string
		assert func(ctx *StaticExprContext, vars *variable.SessionVars)
	}{
		{
			name:  "character_set_connection",
			val:   "gbk",
			field: "$.charset",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				cs, _ := ctx.GetCharsetInfo()
				require.Equal(t, "gbk", cs)
				cs2, _ := vars.GetCharsetInfo()
				require.Equal(t, cs2, cs)
			},
		},
		{
			name:  "collation_connection",
			val:   "gbk_chinese_ci",
			field: "$.collation",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				_, coll := ctx.GetCharsetInfo()
				require.Equal(t, "gbk_chinese_ci", coll)
				_, coll2 := vars.GetCharsetInfo()
				require.Equal(t, coll2, coll)
			},
		},
		{
			name:  "default_collation_for_utf8mb4",
			val:   "utf8mb4_general_ci",
			field: "$.defaultCollationForUTF8MB4",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.Equal(t, "utf8mb4_general_ci", ctx.GetDefaultCollationForUTF8MB4())
				require.Equal(t, vars.DefaultCollationForUTF8MB4, ctx.GetDefaultCollationForUTF8MB4())
			},
		},
		{
			name:  strings.ToUpper("tidb_sysdate_is_now"), // test for settings an upper case variable
			val:   "1",
			field: "$.sysDateIsNow",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.True(t, ctx.GetSysdateIsNow())
				require.Equal(t, vars.SysdateIsNow, ctx.GetSysdateIsNow())
			},
		},
		{
			name:  "tidb_enable_noop_functions",
			val:   "warn",
			field: "$.noopFuncsMode",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.Equal(t, variable.WarnInt, ctx.GetNoopFuncsMode())
				require.Equal(t, vars.NoopFuncsMode, ctx.GetNoopFuncsMode())
			},
		},
		{
			name:  "block_encryption_mode",
			val:   "aes-256-cbc",
			field: "$.blockEncryptionMode",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.Equal(t, "aes-256-cbc", ctx.GetBlockEncryptionMode())
				blockMode, _ := vars.GetSystemVar(variable.BlockEncryptionMode)
				require.Equal(t, blockMode, ctx.GetBlockEncryptionMode())
			},
		},
		{
			name:  "group_concat_max_len",
			val:   "123456",
			field: "$.groupConcatMaxLen",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.Equal(t, uint64(123456), ctx.GetGroupConcatMaxLen())
				require.Equal(t, vars.GroupConcatMaxLen, ctx.GetGroupConcatMaxLen())
			},
		},
		{
			name:  "windowing_use_high_precision",
			val:   "0",
			field: "$.windowingUseHighPrecision",
			assert: func(ctx *StaticExprContext, vars *variable.SessionVars) {
				require.False(t, ctx.GetWindowingUseHighPrecision())
				require.Equal(t, vars.WindowingUseHighPrecision, ctx.GetWindowingUseHighPrecision())
			},
		},
	}

	// nonVarRelatedFields means the fields not related to any system variables.
	// To make sure that all the variables which affect the context state are covered in the above test list,
	// we need to test all inner fields except those in `nonVarRelatedFields` are changed after `LoadSystemVars`.
	nonVarRelatedFields := []string{
		"$.rng",
		"$.planCacheTracker",
		"$.columnIDAllocator",
		"$.connectionID",
	}

	// varsRelatedFields means the fields related to
	varsRelatedFields := make([]string, 0, len(vars))
	varsMap := make(map[string]string)
	sessionVars := variable.NewSessionVars(nil)
	for _, sysVar := range vars {
		varsMap[sysVar.name] = sysVar.val
		if sysVar.field != "" {
			varsRelatedFields = append(varsRelatedFields, sysVar.field)
		}
		require.NoError(t, sessionVars.SetSystemVar(sysVar.name, sysVar.val))
	}

	defaultCtx := NewStaticExprContext()
	ctx, err := defaultCtx.LoadSystemVars(varsMap)
	require.NoError(t, err)

	// Check all fields except these in `nonVarRelatedFields` are changed after `LoadSystemVars` to make sure
	// all system variables related fields are covered in the test list.
	deeptest.AssertRecursivelyNotEqual(
		t,
		defaultCtx.staticExprCtxState,
		ctx.staticExprCtxState,
		// ignore `evalCtx` because we'll test it standalone.
		deeptest.WithIgnorePath(append(nonVarRelatedFields, "$.evalCtx")),
	)

	// We need to compare the new context again with an empty one to make sure those values are set from sys vars,
	// not inherited from the empty go value.
	deeptest.AssertRecursivelyNotEqual(
		t,
		staticExprCtxState{},
		ctx.staticExprCtxState,
		// ignore `windowingUseHighPrecision` because we set it to `false` in test case.
		deeptest.WithIgnorePath(append(nonVarRelatedFields, "$.evalCtx", "$.windowingUseHighPrecision")),
	)

	// Check all system vars unrelated fields are not changed after `LoadSystemVars`.
	deeptest.AssertDeepClonedEqual(
		t,
		defaultCtx.staticExprCtxState,
		ctx.staticExprCtxState,
		deeptest.WithIgnorePath(append(varsRelatedFields, "$.evalCtx")),
		// LoadSystemVars only does shallow copy for `EvalContext` so we just need to compare the pointers.
		deeptest.WithPointerComparePath(nonVarRelatedFields),
	)

	for _, sysVar := range vars {
		sysVar.assert(ctx, sessionVars)
	}

	// additional tests for charset
	// setting charset should also affect collation
	ctx, err = defaultCtx.LoadSystemVars(map[string]string{
		"character_set_connection": "ascii",
	})
	require.NoError(t, err)
	cs, coll := ctx.GetCharsetInfo()
	require.Equal(t, "ascii", cs)
	require.Equal(t, "ascii_bin", coll)
	// setting collation should also affect charset
	ctx, err = defaultCtx.LoadSystemVars(map[string]string{
		"collation_connection": "latin1_bin",
	})
	require.NoError(t, err)
	cs, coll = ctx.GetCharsetInfo()
	require.Equal(t, "latin1", cs)
	require.Equal(t, "latin1_bin", coll)

	// additional test for EvalContext
	// LoadSystemVars should also affect EvalContext
	ctx, err = defaultCtx.LoadSystemVars(map[string]string{
		"div_precision_increment": "9",
		"time_zone":               "Asia/Tokyo",
	})
	require.NoError(t, err)
	require.Equal(t, 9, ctx.GetEvalCtx().GetDivPrecisionIncrement())
	require.Equal(t, "Asia/Tokyo", ctx.GetEvalCtx().Location().String())
}
