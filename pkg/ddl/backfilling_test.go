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
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
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
	ingest.LitDiskRoot = ingest.NewDiskRootImpl(t.TempDir())
	ingest.LitMemRoot = ingest.NewMemRootImpl(math.MaxInt64)
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
	require.Equal(t, tp, model.ReorgTypeIngest)
	ingest.LitInitialized = false
}

func assertStaticExprContextEqual(t *testing.T, sctx sessionctx.Context, exprCtx *exprstatic.ExprContext, warnHandler contextutil.WarnHandler) {
	exprCtxManualCheckFields := []struct {
		field string
		check func(*exprstatic.ExprContext)
	}{
		{
			field: "evalCtx",
			check: func(ctx *exprstatic.ExprContext) {
				require.NotZero(t, ctx.GetEvalCtx().CtxID())
				require.NotEqual(t, sctx.GetExprCtx().GetEvalCtx().CtxID(), ctx.GetEvalCtx().CtxID())
			},
		},
		{
			field: "blockEncryptionMode",
			check: func(context *exprstatic.ExprContext) {
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
			check: func(ctx *exprstatic.ExprContext) {
				require.NotNil(t, ctx.Rng())
			},
		},
		{
			field: "planCacheTracker",
			check: func(ctx *exprstatic.ExprContext) {
				require.Equal(t, sctx.GetExprCtx().IsUseCache(), ctx.IsUseCache())
				ctx.SetSkipPlanCache("test reason")
				require.False(t, ctx.IsUseCache())
			},
		},
	}

	evalCtxManualCheckFields := []struct {
		field string
		check func(*exprstatic.EvalContext)
	}{
		{
			field: "warnHandler",
			check: func(ctx *exprstatic.EvalContext) {
				require.Same(t, warnHandler, ctx.GetWarnHandler())
			},
		},
		{
			field: "typeCtx.warnHandler",
			check: func(ctx *exprstatic.EvalContext) {
				ec := ctx.ErrCtx()
				require.Equal(t, errctx.NewContextWithLevels(ec.LevelMap(), ctx), ec)
			},
		},
		{
			field: "typeCtx.loc",
			check: func(ctx *exprstatic.EvalContext) {
				tc := ctx.TypeCtx()
				require.Same(t, tc.Location(), ctx.Location())
				require.Equal(t, sctx.GetSessionVars().Location().String(), tc.Location().String())
				require.Equal(t, sctx.GetSessionVars().StmtCtx.TimeZone().String(), tc.Location().String())
			},
		},
		{
			field: "errCtx.warnHandler",
			check: func(ctx *exprstatic.EvalContext) {
				tc := ctx.TypeCtx()
				require.Equal(t, types.NewContext(tc.Flags(), tc.Location(), ctx), tc)
			},
		},
		{
			field: "currentTime",
			check: func(ctx *exprstatic.EvalContext) {
				tm1, err := sctx.GetExprCtx().GetEvalCtx().CurrentTime()
				require.NoError(t, err)

				tm, err := ctx.CurrentTime()
				require.Equal(t, ctx.Location().String(), tm.Location().String())
				require.InDelta(t, tm1.Unix(), tm.Unix(), 2)
				require.NoError(t, err)
			},
		},
	}

	// check ExprContext except EvalContext
	expected := sctx.GetExprCtx().(*mock.Context).IntoStatic()
	ignoreFields := make([]string, 0, len(exprCtxManualCheckFields))
	for _, f := range exprCtxManualCheckFields {
		f.check(exprCtx)
		ignoreFields = append(ignoreFields, "$.exprCtxState."+f.field)
	}
	deeptest.AssertDeepClonedEqual(t, expected, exprCtx, deeptest.WithIgnorePath(ignoreFields))

	// check EvalContext
	ignoreFields = make([]string, 0, len(evalCtxManualCheckFields))
	ignoreFields = append(ignoreFields, "$.id")
	for _, f := range evalCtxManualCheckFields {
		f.check(exprCtx.GetStaticEvalCtx())
		ignoreFields = append(ignoreFields, "$.evalCtxState."+f.field)
	}
	deeptest.AssertDeepClonedEqual(
		t,
		expected.GetStaticEvalCtx(),
		exprCtx.GetStaticEvalCtx(),
		deeptest.WithIgnorePath(ignoreFields),
	)
}

// newMockReorgSessCtx creates a mock session context for reorg test.
// In old implementations, DDL is using `mock.Context` to construct the contexts used in DDL reorg.
// After refactoring, we just need it to do test the new implementation is compatible with the old one.
func newMockReorgSessCtx(store kv.Storage) sessionctx.Context {
	c := mock.NewContext()
	c.Store = store
	c.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	tz := *time.UTC
	c.ResetSessionAndStmtTimeZone(&tz)
	return c
}

// TestReorgExprContext is used in refactor stage to make sure the newReorgExprCtx() is
// compatible with newMockReorgSessCtx(nil).GetExprCtx() to make it safe to replace `mock.Context` usage.
// After refactor, the TestReorgExprContext can be removed.
func TestReorgExprContext(t *testing.T) {
	// test default expr context
	store := &mockStorage{client: &mock.Client{}}
	sctx := newMockReorgSessCtx(store)
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
			ReorgTp:           model.ReorgTypeIngest,
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
		sctx = newMockReorgSessCtx(store)
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

func TestReorgTableMutateContext(t *testing.T) {
	originalRowFmt := vardef.GetDDLReorgRowFormat()
	defer vardef.SetDDLReorgRowFormat(originalRowFmt)

	exprCtx := exprstatic.NewExprContext()

	assertTblCtxMatchSessionCtx := func(ctx table.MutateContext, sctx sessionctx.Context) {
		sctxTblCtx := sctx.GetTableCtx()
		require.Equal(t, uint64(0), ctx.ConnectionID())
		require.Equal(t, sctxTblCtx.ConnectionID(), ctx.ConnectionID())

		require.False(t, ctx.InRestrictedSQL())
		require.Equal(t, sctxTblCtx.InRestrictedSQL(), ctx.InRestrictedSQL())

		require.Equal(t, variable.AssertionLevelOff, ctx.TxnAssertionLevel())
		require.Equal(t, sctxTblCtx.TxnAssertionLevel(), ctx.TxnAssertionLevel())

		require.Equal(t, vardef.GetDDLReorgRowFormat() != vardef.DefTiDBRowFormatV1, ctx.GetRowEncodingConfig().IsRowLevelChecksumEnabled)
		require.Equal(t, vardef.GetDDLReorgRowFormat() != vardef.DefTiDBRowFormatV1, ctx.GetRowEncodingConfig().RowEncoder.Enable)
		require.Equal(t, sctxTblCtx.GetRowEncodingConfig(), ctx.GetRowEncodingConfig())

		require.NotNil(t, ctx.GetMutateBuffers())
		require.Equal(t, sctxTblCtx.GetMutateBuffers(), ctx.GetMutateBuffers())

		require.Equal(t, vardef.DefTiDBShardAllocateStep, ctx.GetRowIDShardGenerator().GetShardStep())
		sctx.GetSessionVars().TxnCtx.StartTS = 123 // make sure GetRowIDShardGenerator() pass assert
		require.Equal(t, sctxTblCtx.GetRowIDShardGenerator().GetShardStep(), ctx.GetRowIDShardGenerator().GetShardStep())
		require.GreaterOrEqual(t, ctx.GetRowIDShardGenerator().GetCurrentShard(1), int64(0))

		alloc1, ok := sctxTblCtx.GetReservedRowIDAlloc()
		require.True(t, ok)
		alloc2, ok := ctx.GetReservedRowIDAlloc()
		require.True(t, ok)
		require.Equal(t, alloc1, alloc2)
		require.True(t, alloc2.Exhausted())

		statistics, ok := ctx.GetStatisticsSupport()
		require.False(t, ok)
		require.Nil(t, statistics)
		cached, ok := ctx.GetCachedTableSupport()
		require.False(t, ok)
		require.Nil(t, cached)
		temp, ok := ctx.GetTemporaryTableSupport()
		require.False(t, ok)
		require.Nil(t, temp)
		dml, ok := ctx.GetExchangePartitionDMLSupport()
		require.False(t, ok)
		require.Nil(t, dml)
	}

	// test when the row format is v1
	vardef.SetDDLReorgRowFormat(vardef.DefTiDBRowFormatV1)
	sctx := newMockReorgSessCtx(&mockStorage{client: &mock.Client{}})
	require.NoError(t, initSessCtx(sctx, &model.DDLReorgMeta{}))
	ctx := newReorgTableMutateContext(exprCtx)
	require.Same(t, exprCtx, ctx.GetExprCtx())
	assertTblCtxMatchSessionCtx(ctx, sctx)
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

func TestValidateAndFillRanges(t *testing.T) {
	mkRange := func(start, end string) kv.KeyRange {
		return kv.KeyRange{StartKey: []byte(start), EndKey: []byte(end)}
	}
	ranges := []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}
	err := validateAndFillRanges(ranges, []byte("b"), []byte("e"))
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

	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}
	err = validateAndFillRanges(ranges, []byte("a"), []byte("e"))
	require.Error(t, err)

	ranges = []kv.KeyRange{
		mkRange("b", "c"),
		mkRange("c", "d"),
		mkRange("d", "e"),
	}
	err = validateAndFillRanges(ranges, []byte("b"), []byte("f"))
	require.NoError(t, err)
}

func TestTuneTableScanWorkerBatchSize(t *testing.T) {
	reorgMeta := &model.DDLReorgMeta{}
	reorgMeta.Concurrency.Store(4)
	reorgMeta.BatchSize.Store(32)
	copCtx := &copr.CopContextSingleIndex{
		CopContextBase: &copr.CopContextBase{
			FieldTypes: []*types.FieldType{},
		},
	}
	wctx := workerpool.NewContext(context.Background())
	w := tableScanWorker{
		copCtx:        copCtx,
		ctx:           wctx,
		srcChkPool:    createChunkPool(copCtx, reorgMeta),
		hintBatchSize: 32,
		reorgMeta:     reorgMeta,
	}
	for range 10 {
		chk := w.getChunk()
		require.Equal(t, 32, chk.Capacity())
		w.srcChkPool.Put(chk)
	}
	reorgMeta.SetBatchSize(64)
	for range 10 {
		chk := w.getChunk()
		require.Equal(t, 64, chk.Capacity())
		w.srcChkPool.Put(chk)
	}
	wctx.Cancel()
}

func TestSplitRangesByKeys(t *testing.T) {
	k := func(ord int) kv.Key {
		return kv.Key([]byte{byte(ord)})
	}
	tests := []struct {
		name      string
		ranges    []kv.KeyRange
		splitKeys []kv.Key
		expected  []kv.KeyRange
	}{
		{
			name: "empty split keys",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
			splitKeys: []kv.Key{},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
		},
		{
			name: "single split key in middle",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(5)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(5)},
				{StartKey: k(5), EndKey: k(10)},
			},
		},
		{
			name: "multiple split keys in one range",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(20)},
			},
			splitKeys: []kv.Key{k(5), k(10), k(15)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(5)},
				{StartKey: k(5), EndKey: k(10)},
				{StartKey: k(10), EndKey: k(15)},
				{StartKey: k(15), EndKey: k(20)},
			},
		},
		{
			name: "split keys across multiple ranges",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
				{StartKey: k(10), EndKey: k(20)},
			},
			splitKeys: []kv.Key{k(5), k(15)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(5)},
				{StartKey: k(5), EndKey: k(10)},
				{StartKey: k(10), EndKey: k(15)},
				{StartKey: k(15), EndKey: k(20)},
			},
		},
		{
			name: "split key less than range start",
			ranges: []kv.KeyRange{
				{StartKey: k(5), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(3)},
			expected: []kv.KeyRange{
				{StartKey: k(5), EndKey: k(10)},
			},
		},
		{
			name: "split key greater than range end",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(15)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
		},
		{
			name: "split key equals range start",
			ranges: []kv.KeyRange{
				{StartKey: k(5), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(5)},
			expected: []kv.KeyRange{
				{StartKey: k(5), EndKey: k(10)},
			},
		},
		{
			name: "split key equals range end",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(10)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
		},
		{
			name: "split keys overlaps with range start and end",
			ranges: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(10)},
			},
			splitKeys: []kv.Key{k(0), k(5), k(10)},
			expected: []kv.KeyRange{
				{StartKey: k(0), EndKey: k(5)},
				{StartKey: k(5), EndKey: k(10)},
			},
		},
	}
	for _, tt := range tests {
		result := splitRangesByKeys(tt.ranges, tt.splitKeys)
		require.EqualValues(t, len(tt.expected), len(result), "keys mismatch", tt.name)
	}
}
