// Copyright 2022 PingCAP, Inc.
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
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestDoneTaskKeeper(t *testing.T) {
	n := newDoneTaskKeeper(kv.Key("a"))
	n.updateNextKey(0, kv.Key("b"))
	n.updateNextKey(1, kv.Key("c"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 0)

	n.updateNextKey(4, kv.Key("f"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 1)
	n.updateNextKey(3, kv.Key("e"))
	n.updateNextKey(5, kv.Key("g"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("c")))
	require.Len(t, n.doneTaskNextKey, 3)
	n.updateNextKey(2, kv.Key("d"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("g")))
	require.Len(t, n.doneTaskNextKey, 0)

	n.updateNextKey(6, kv.Key("h"))
	require.True(t, bytes.Equal(n.nextKey, kv.Key("h")))
}

func TestPickBackfillType(t *testing.T) {
	originMgr := ingest.LitBackCtxMgr
	originInit := ingest.LitInitialized
	defer func() {
		ingest.LitBackCtxMgr = originMgr
		ingest.LitInitialized = originInit
	}()
	mockMgr := ingest.NewMockBackendCtxMgr(
		func() sessionctx.Context {
			return nil
		})
	ingest.LitBackCtxMgr = mockMgr
	mockJob := &model.Job{
		ID: 1,
		ReorgMeta: &model.DDLReorgMeta{
			ReorgTp: model.ReorgTypeTxn,
		},
	}
	mockJob.ReorgMeta.IsFastReorg = true
	tp, err := pickBackfillType(mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxn)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = false
	tp, err = pickBackfillType(mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeTxnMerge)

	mockJob.ReorgMeta.ReorgTp = model.ReorgTypeNone
	ingest.LitInitialized = true
	tp, err = pickBackfillType(mockJob)
	require.NoError(t, err)
	require.Equal(t, tp, model.ReorgTypeLitMerge)
}

func assertStaticExprContextEqual(t *testing.T, sctx sessionctx.Context, exprCtx *contextstatic.StaticExprContext, warnHandler contextutil.WarnHandler) {
	exprCtxManualCheckFields := []struct {
		field string
		check func(*contextstatic.StaticExprContext)
	}{
		{
			field: "evalCtx",
			check: func(ctx *contextstatic.StaticExprContext) {
				require.NotZero(t, ctx.GetEvalCtx().CtxID())
				require.NotEqual(t, sctx.GetExprCtx().GetEvalCtx().CtxID(), ctx.GetEvalCtx().CtxID())
			},
		},
		{
			field: "blockEncryptionMode",
			check: func(context *contextstatic.StaticExprContext) {
				m := sctx.GetExprCtx().GetBlockEncryptionMode()
				if m == "" {
					// Empty string is not a valid encryption mode, so we expect the exprCtx.GetBlockEncryptionMode()
					// to return a default value "aes-128-ecb" at this time.
					// For some old codes, empty string can still work because it is not used in DDL.
					m = "aes-128-ecb"
				}
				require.Equal(t, m, context.GetBlockEncryptionMode())
			},
		},
		{
			field: "rng",
			check: func(ctx *contextstatic.StaticExprContext) {
				require.NotNil(t, ctx.Rng())
			},
		},
		{
			field: "planCacheTracker",
			check: func(ctx *contextstatic.StaticExprContext) {
				require.Equal(t, sctx.GetExprCtx().IsUseCache(), ctx.IsUseCache())
				ctx.SetSkipPlanCache("test reason")
				require.False(t, ctx.IsUseCache())
			},
		},
	}

	evalCtxManualCheckFields := []struct {
		field string
		check func(*contextstatic.StaticEvalContext)
	}{
		{
			field: "warnHandler",
			check: func(ctx *contextstatic.StaticEvalContext) {
				require.Same(t, warnHandler, ctx.GetWarnHandler())
			},
		},
		{
			field: "typeCtx.warnHandler",
			check: func(ctx *contextstatic.StaticEvalContext) {
				ec := ctx.ErrCtx()
				require.Equal(t, errctx.NewContextWithLevels(ec.LevelMap(), ctx), ec)
			},
		},
		{
			field: "typeCtx.loc",
			check: func(ctx *contextstatic.StaticEvalContext) {
				tc := ctx.TypeCtx()
				require.Same(t, tc.Location(), ctx.Location())
				require.Equal(t, sctx.GetSessionVars().Location().String(), tc.Location().String())
				require.Equal(t, sctx.GetSessionVars().StmtCtx.TimeZone().String(), tc.Location().String())
			},
		},
		{
			field: "errCtx.warnHandler",
			check: func(ctx *contextstatic.StaticEvalContext) {
				tc := ctx.TypeCtx()
				require.Equal(t, types.NewContext(tc.Flags(), tc.Location(), ctx), tc)
			},
		},
		{
			field: "currentTime",
			check: func(ctx *contextstatic.StaticEvalContext) {
				tm1, err := sctx.GetExprCtx().GetEvalCtx().CurrentTime()
				require.NoError(t, err)

				tm, err := ctx.CurrentTime()
				require.Equal(t, ctx.Location().String(), tm.Location().String())
				require.InDelta(t, tm1.Unix(), tm.Unix(), 2)
			},
		},
		{
			field: "requestVerificationFn",
			check: func(ctx *contextstatic.StaticEvalContext) {
				// RequestVerification should allow all privileges
				// that is the same with input session context (GetPrivilegeManager returns nil).
				require.Nil(t, privilege.GetPrivilegeManager(sctx))
				require.True(t, sctx.GetExprCtx().GetEvalCtx().RequestVerification("any", "any", "any", mysql.CreatePriv))
				require.True(t, ctx.RequestVerification("any", "any", "any", mysql.CreatePriv))
			},
		},
		{
			field: "requestDynamicVerificationFn",
			check: func(ctx *contextstatic.StaticEvalContext) {
				// RequestDynamicVerification should allow all privileges
				// that is the same with input session context (GetPrivilegeManager returns nil).
				require.Nil(t, privilege.GetPrivilegeManager(sctx))
				require.True(t, sctx.GetExprCtx().GetEvalCtx().RequestDynamicVerification("RESTRICTED_USER_ADMIN", true))
				require.True(t, ctx.RequestDynamicVerification("RESTRICTED_USER_ADMIN", true))
			},
		},
	}

	// check StaticExprContext except StaticEvalContext
	expected := sctx.GetExprCtx().(*mock.Context).IntoStatic()
	ignoreFields := make([]string, 0, len(exprCtxManualCheckFields))
	for _, f := range exprCtxManualCheckFields {
		f.check(exprCtx)
		ignoreFields = append(ignoreFields, "$.staticExprCtxState."+f.field)
	}
	deeptest.AssertDeepClonedEqual(t, expected, exprCtx, deeptest.WithIgnorePath(ignoreFields))

	// check StaticEvalContext
	ignoreFields = make([]string, 0, len(evalCtxManualCheckFields))
	ignoreFields = append(ignoreFields, "$.id")
	for _, f := range evalCtxManualCheckFields {
		f.check(exprCtx.GetStaticEvalCtx())
		ignoreFields = append(ignoreFields, "$.staticEvalCtxState."+f.field)
	}
	deeptest.AssertDeepClonedEqual(
		t,
		expected.GetStaticEvalCtx(),
		exprCtx.GetStaticEvalCtx(),
		deeptest.WithIgnorePath(ignoreFields),
	)
}

// TestReorgExprContext is used in refactor stage to make sure the newReorgExprCtx() is
// compatible with newReorgSessCtx(nil).GetExprCtx() to make it safe to replace `mock.Context` usage.
// After refactor, the TestReorgExprContext can be removed.
func TestReorgExprContext(t *testing.T) {
	// test default expr context
	store := &mockStorage{client: &mock.Client{}}
	sctx := newReorgSessCtx(store)
	defaultCtx := newReorgExprCtx()
	// should use an empty static warn handler by default
	evalCtx := defaultCtx.GetStaticEvalCtx()
	require.Equal(t, contextutil.NewStaticWarnHandler(0), evalCtx.GetWarnHandler())
	assertStaticExprContextEqual(t, sctx, defaultCtx, evalCtx.GetWarnHandler())
	defaultTypeCtx := evalCtx.TypeCtx()
	defaultErrCtx := evalCtx.ErrCtx()

	// test expr context from DDLReorgMeta
	for _, reorg := range []model.DDLReorgMeta{
		{
			SQLMode:           mysql.ModeStrictTransTables | mysql.ModeAllowInvalidDates,
			Location:          &model.TimeZoneLocation{Name: "Asia/Tokyo"},
			ReorgTp:           model.ReorgTypeLitMerge,
			ResourceGroupName: "rg1",
		},
		{
			SQLMode: mysql.ModeAllowInvalidDates,
			// should load location from system value when reorg.Location is nil
			Location:          nil,
			ReorgTp:           model.ReorgTypeTxnMerge,
			ResourceGroupName: "rg2",
		},
	} {
		sctx = newReorgSessCtx(store)
		require.NoError(t, initSessCtx(sctx, &reorg))
		ctx, err := newReorgExprCtxWithReorgMeta(&reorg, sctx.GetSessionVars().StmtCtx.WarnHandler)
		require.NoError(t, err)
		assertStaticExprContextEqual(t, sctx, ctx, ctx.GetStaticEvalCtx().GetWarnHandler())
		evalCtx := ctx.GetEvalCtx()
		tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
		// SQLMode should match DDLReorgMeta
		require.Equal(t, reorg.SQLMode, evalCtx.SQLMode())
		// Location should match DDLReorgMeta
		if reorg.Location != nil {
			require.Equal(t, reorg.Location.Name, evalCtx.Location().String())
		} else {
			loc := timeutil.SystemLocation()
			require.Same(t, loc, evalCtx.Location())
		}
		// Some fields should be different from the default context to make the test robust.
		require.NotEqual(t, defaultCtx.GetEvalCtx().SQLMode(), evalCtx.SQLMode())
		require.NotEqual(t, defaultTypeCtx.Flags(), tc.Flags())
		require.NotEqual(t, defaultErrCtx.LevelMap(), ec.LevelMap())
	}
}

type mockStorage struct {
	kv.Storage
	client kv.Client
}

func (s *mockStorage) GetClient() kv.Client {
	return s.client
}

func assertDistSQLCtxEqual(t *testing.T, expected *distsqlctx.DistSQLContext, actual *distsqlctx.DistSQLContext) {
	deeptest.AssertDeepClonedEqual(
		t, expected, actual,
		deeptest.WithPointerComparePath([]string{
			"$.WarnHandler",
			"$.Client",
		}),
		deeptest.WithIgnorePath([]string{
			"$.SessionMemTracker",
			"$.Location",
			"$.ErrCtx.warnHandler",
		}),
	)

	// manually check
	// SessionMemTracker
	require.Equal(t, expected.SessionMemTracker.Label(), actual.SessionMemTracker.Label())
	require.Equal(t, expected.SessionMemTracker.GetBytesLimit(), actual.SessionMemTracker.GetBytesLimit())
	require.Equal(t, expected.SessionMemTracker.BytesConsumed(), actual.SessionMemTracker.BytesConsumed())
	// Location
	require.Equal(t, expected.Location.String(), actual.Location.String())
	// ErrCtx
	require.Equal(t, errctx.NewContextWithLevels(expected.ErrCtx.LevelMap(), expected.WarnHandler), actual.ErrCtx)
}

// TestReorgExprContext is used in refactor stage to make sure the newDefaultReorgDistSQLCtx() is
// compatible with newReorgSessCtx(nil).GetDistSQLCtx() to make it safe to replace `mock.Context` usage.
// After refactor, the TestReorgExprContext can be removed.
func TestReorgDistSQLCtx(t *testing.T) {
	store := &mockStorage{client: &mock.Client{}}

	// test default dist sql context
	expected := newReorgSessCtx(store).GetDistSQLCtx()
	defaultCtx := newDefaultReorgDistSQLCtx(store.client, expected.WarnHandler)
	assertDistSQLCtxEqual(t, expected, defaultCtx)

	// test dist sql context from DDLReorgMeta
	for _, reorg := range []model.DDLReorgMeta{
		{
			SQLMode:           mysql.ModeStrictTransTables | mysql.ModeAllowInvalidDates,
			Location:          &model.TimeZoneLocation{Name: "Asia/Tokyo"},
			ReorgTp:           model.ReorgTypeLitMerge,
			ResourceGroupName: "rg1",
		},
		{
			SQLMode: mysql.ModeAllowInvalidDates,
			// should load location from system value when reorg.Location is nil
			Location:          nil,
			ReorgTp:           model.ReorgTypeTxnMerge,
			ResourceGroupName: "rg2",
		},
	} {
		sctx := newReorgSessCtx(store)
		require.NoError(t, initSessCtx(sctx, &reorg))
		expected = sctx.GetDistSQLCtx()
		ctx, err := newReorgDistSQLCtxWithReorgMeta(store.client, &reorg, expected.WarnHandler)
		require.NoError(t, err)
		assertDistSQLCtxEqual(t, expected, ctx)
		// Location should match DDLReorgMeta
		if reorg.Location != nil {
			require.Equal(t, reorg.Location.Name, ctx.Location.String())
		} else {
			loc := timeutil.SystemLocation()
			require.Same(t, loc, ctx.Location)
		}
		// ResourceGroupName should match DDLReorgMeta
		require.Equal(t, reorg.ResourceGroupName, ctx.ResourceGroupName)
		// Some fields should be different from the default context to make the test robust.
		require.NotEqual(t, defaultCtx.ErrCtx.LevelMap(), ctx.ErrCtx.LevelMap())
	}
}

func TestValidateAndFillRanges(t *testing.T) {
	mkRange := func(start, end string) kv.KeyRange {
		return kv.KeyRange{StartKey: []byte(start), EndKey: []byte(end)}
	}
	ranges := []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}
	err := validateAndFillRanges(ranges, []byte("a"), []byte("e"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}, ranges)

	// adjust first and last range.
	ranges = []kv.KeyRange{
		mkRange("a", "c"),
		mkRange("c", "e"),
		mkRange("e", "g"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "e"),
		mkRange("e", "f"),
	}, ranges)

	// first range startKey and last range endKey are empty.
	ranges = []kv.KeyRange{
		mkRange("", "c"),
		mkRange("c", "e"),
		mkRange("e", ""),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "e"),
		mkRange("e", "f"),
	}, ranges)
	ranges = []kv.KeyRange{
		mkRange("", "c"),
		mkRange("c", ""),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
	require.EqualValues(t, []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "f"),
	}, ranges)

	// invalid range.
	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", ""),
		mkRange("e", "f"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.Error(t, err)

	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("e", "f"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.Error(t, err)
}
