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

package kv

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestLitExprContext(t *testing.T) {
	cases := []struct {
		sqlMode       mysql.SQLMode
		sysVars       map[string]string
		timestamp     int64
		checkFlags    types.Flags
		checkErrLevel errctx.LevelMap
		check         func(types.Flags, errctx.LevelMap)
	}{
		{
			sqlMode:    mysql.ModeNone,
			timestamp:  1234567,
			checkFlags: types.DefaultStmtFlags | types.FlagTruncateAsWarning | types.FlagIgnoreZeroInDateErr,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelWarn
				m[errctx.ErrGroupBadNull] = errctx.LevelWarn
				m[errctx.ErrGroupDividedByZero] = errctx.LevelIgnore
				return m
			}(),
			sysVars: map[string]string{
				"max_allowed_packet":      "10240",
				"div_precision_increment": "5",
				"time_zone":               "Europe/Berlin",
				"default_week_format":     "2",
				"block_encryption_mode":   "aes-128-ofb",
				"group_concat_max_len":    "2048",
			},
		},
		{
			sqlMode: mysql.ModeStrictTransTables | mysql.ModeNoZeroDate | mysql.ModeNoZeroInDate |
				mysql.ModeErrorForDivisionByZero,
			checkFlags: types.DefaultStmtFlags,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelError
				m[errctx.ErrGroupBadNull] = errctx.LevelError
				m[errctx.ErrGroupDividedByZero] = errctx.LevelError
				return m
			}(),
		},
		{
			sqlMode:    mysql.ModeNoZeroDate | mysql.ModeNoZeroInDate | mysql.ModeErrorForDivisionByZero,
			checkFlags: types.DefaultStmtFlags | types.FlagTruncateAsWarning | types.FlagIgnoreZeroInDateErr,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelWarn
				m[errctx.ErrGroupBadNull] = errctx.LevelWarn
				m[errctx.ErrGroupDividedByZero] = errctx.LevelWarn
				return m
			}(),
		},
		{
			sqlMode:    mysql.ModeStrictTransTables | mysql.ModeNoZeroInDate,
			checkFlags: types.DefaultStmtFlags | types.FlagIgnoreZeroInDateErr,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelError
				m[errctx.ErrGroupBadNull] = errctx.LevelError
				m[errctx.ErrGroupDividedByZero] = errctx.LevelIgnore
				return m
			}(),
		},
		{
			sqlMode:    mysql.ModeStrictTransTables | mysql.ModeNoZeroDate,
			checkFlags: types.DefaultStmtFlags | types.FlagIgnoreZeroInDateErr,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelError
				m[errctx.ErrGroupBadNull] = errctx.LevelError
				m[errctx.ErrGroupDividedByZero] = errctx.LevelIgnore
				return m
			}(),
		},
		{
			sqlMode:    mysql.ModeStrictTransTables | mysql.ModeAllowInvalidDates,
			checkFlags: types.DefaultStmtFlags | types.FlagIgnoreZeroInDateErr | types.FlagIgnoreInvalidDateErr,
			checkErrLevel: func() errctx.LevelMap {
				m := stmtctx.DefaultStmtErrLevels
				m[errctx.ErrGroupTruncate] = errctx.LevelError
				m[errctx.ErrGroupBadNull] = errctx.LevelError
				m[errctx.ErrGroupDividedByZero] = errctx.LevelIgnore
				return m
			}(),
		},
	}

	// We need to compare the new introduced `*litExprContext` the same behavior with the old `session`.
	// After refactoring finished, we can remove the old session and this test.
	compareWithLegacySession := func(t *testing.T, ctx *litExprContext, opts *encode.SessionOptions) {
		if opts.SysVars == nil {
			opts.SysVars = make(map[string]string)
		}
		if _, ok := opts.SysVars["div_precision_increment"]; !ok {
			// It seems that `DefDivPrecisionIncrement` is not set as a default value in `newSession` and its
			// default value is 0.
			// We should set it manually to make test pass.
			// The legacy code has no bug for this default value because the `DefaultImportantVariables`
			// will be loaded every time to override this variable:
			// https://github.com/pingcap/tidb/blob/2e457b394f09165e23fa5121fcfd89c6e8a6e835/pkg/lightning/common/common.go#L33-L42
			opts.SysVars["div_precision_increment"] = strconv.Itoa(variable.DefDivPrecisionIncrement)
		}
		if _, ok := opts.SysVars["block_encryption_mode"]; !ok {
			// same reason with `DivPrecisionIncrement`, we need to set `block_encryption_mode` manually to pass test.
			opts.SysVars["block_encryption_mode"] = variable.DefBlockEncryptionMode
		}
		se := newSession(opts, log.L())
		seCtx := contextstatic.MakeExprContextStatic(se.exprCtx.SessionExprContext)
		deeptest.AssertDeepClonedEqual(t, seCtx, ctx.StaticExprContext, deeptest.WithIgnorePath([]string{
			"$.staticExprCtxState.evalCtx.id",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.typeCtx.loc",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.warnHandler",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.typeCtx.warnHandler",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.errCtx.warnHandler",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.currentTime",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.requestVerificationFn",
			"$.staticExprCtxState.evalCtx.staticEvalCtxState.requestDynamicVerificationFn",
			"$.staticExprCtxState.rng",
			"$.staticExprCtxState.planCacheTracker",
		}))
		currentTime, err := seCtx.GetEvalCtx().CurrentTime()
		require.NoError(t, err)
		seTime, err := seCtx.GetEvalCtx().CurrentTime()
		require.NoError(t, err)
		if opts.Timestamp == 0 {
			require.InDelta(t, seTime.Unix(), currentTime.Unix(), 2)
		} else {
			require.Equal(t, opts.Timestamp*1000000000, currentTime.UnixNano())
			require.Equal(t, seTime.UnixNano(), currentTime.UnixNano())
		}
		require.Equal(t, seCtx.GetEvalCtx().Location().String(), ctx.GetEvalCtx().Location().String())
	}

	for i, c := range cases {
		t.Run("case-"+strconv.Itoa(i), func(t *testing.T) {
			ctx, err := newLitExprContext(c.sqlMode, c.sysVars, c.timestamp)
			require.NoError(t, err)

			compareWithLegacySession(t, ctx, &encode.SessionOptions{
				SQLMode:   c.sqlMode,
				SysVars:   c.sysVars,
				Timestamp: c.timestamp,
			})

			evalCtx := ctx.GetEvalCtx()
			require.Equal(t, c.sqlMode, evalCtx.SQLMode())
			tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
			require.Same(t, evalCtx.Location(), tc.Location())
			require.Equal(t, c.checkFlags, tc.Flags())
			require.Equal(t, c.checkErrLevel, ec.LevelMap())

			// shares the same warning handler
			warns := []contextutil.SQLWarn{
				{Level: contextutil.WarnLevelWarning, Err: errors.New("mockErr1")},
				{Level: contextutil.WarnLevelWarning, Err: errors.New("mockErr2")},
				{Level: contextutil.WarnLevelWarning, Err: errors.New("mockErr3")},
			}
			require.Equal(t, 0, evalCtx.WarningCount())
			evalCtx.AppendWarning(warns[0].Err)
			tc.AppendWarning(warns[1].Err)
			ec.AppendWarning(warns[2].Err)
			require.Equal(t, warns, evalCtx.CopyWarnings(nil))

			// system vars
			timeZone := "SYSTEM"
			expectedMaxAllowedPacket := variable.DefMaxAllowedPacket
			expectedDivPrecisionInc := variable.DefDivPrecisionIncrement
			expectedDefaultWeekFormat := variable.DefDefaultWeekFormat
			expectedBlockEncryptionMode := variable.DefBlockEncryptionMode
			expectedGroupConcatMaxLen := variable.DefGroupConcatMaxLen
			for k, v := range c.sysVars {
				switch strings.ToLower(k) {
				case "time_zone":
					timeZone = v
				case "max_allowed_packet":
					expectedMaxAllowedPacket, err = strconv.ParseUint(v, 10, 64)
				case "div_precision_increment":
					expectedDivPrecisionInc, err = strconv.Atoi(v)
				case "default_week_format":
					expectedDefaultWeekFormat = v
				case "block_encryption_mode":
					expectedBlockEncryptionMode = v
				case "group_concat_max_len":
					expectedGroupConcatMaxLen, err = strconv.ParseUint(v, 10, 64)
				}
				require.NoError(t, err)
			}
			if strings.ToLower(timeZone) == "system" {
				require.Same(t, timeutil.SystemLocation(), evalCtx.Location())
			} else {
				require.Equal(t, timeZone, evalCtx.Location().String())
			}
			require.Equal(t, expectedMaxAllowedPacket, evalCtx.GetMaxAllowedPacket())
			require.Equal(t, expectedDivPrecisionInc, evalCtx.GetDivPrecisionIncrement())
			require.Equal(t, expectedDefaultWeekFormat, evalCtx.GetDefaultWeekFormatMode())
			require.Equal(t, expectedBlockEncryptionMode, ctx.GetBlockEncryptionMode())
			require.Equal(t, expectedGroupConcatMaxLen, ctx.GetGroupConcatMaxLen())

			now := time.Now()
			tm, err := evalCtx.CurrentTime()
			require.NoError(t, err)
			require.Same(t, evalCtx.Location(), tm.Location())
			if c.timestamp == 0 {
				// timestamp == 0 means use the current time.
				require.InDelta(t, now.Unix(), tm.Unix(), 2)
			} else {
				require.Equal(t, c.timestamp*1000000000, tm.UnixNano())
			}
			// CurrentTime returns the same value
			tm2, err := evalCtx.CurrentTime()
			require.NoError(t, err)
			require.Equal(t, tm.Nanosecond(), tm2.Nanosecond())
			require.Same(t, tm.Location(), tm2.Location())

			// currently we don't support optional properties
			require.Equal(t, exprctx.OptionalEvalPropKeySet(0), evalCtx.GetOptionalPropSet())
			// not build for plan cache
			require.False(t, ctx.IsUseCache())
			// rng not nil
			require.NotNil(t, ctx.Rng())
			// ConnectionID
			require.Equal(t, uint64(0), ctx.ConnectionID())
			// user vars
			userVars := evalCtx.GetUserVarsReader()
			_, ok := userVars.GetUserVarVal("a")
			require.False(t, ok)
			ctx.setUserVarVal("a", types.NewIntDatum(123))
			d, ok := userVars.GetUserVarVal("a")
			require.True(t, ok)
			require.Equal(t, types.NewIntDatum(123), d)
			ctx.unsetUserVar("a")
			_, ok = userVars.GetUserVarVal("a")
			require.False(t, ok)
		})
	}
}

func TestLitTableMutateContext(t *testing.T) {
	exprCtx, err := newLitExprContext(mysql.ModeNone, nil, 0)
	require.NoError(t, err)

	checkCommon := func(t *testing.T, tblCtx *litTableMutateContext) {
		require.Same(t, exprCtx, tblCtx.GetExprCtx())
		_, ok := tblCtx.AlternativeAllocators(&model.TableInfo{ID: 1})
		require.False(t, ok)
		require.Equal(t, uint64(0), tblCtx.ConnectionID())
		require.Equal(t, tblCtx.GetExprCtx().ConnectionID(), tblCtx.ConnectionID())
		require.False(t, tblCtx.InRestrictedSQL())
		require.NotNil(t, tblCtx.GetMutateBuffers())
		require.NotNil(t, tblCtx.GetMutateBuffers().GetWriteStmtBufs())
		alloc, ok := tblCtx.GetReservedRowIDAlloc()
		require.True(t, ok)
		require.NotNil(t, alloc)
		require.Equal(t, &stmtctx.ReservedRowIDAlloc{}, alloc)
		require.True(t, alloc.Exhausted())
		_, ok = tblCtx.GetBinlogSupport()
		require.False(t, ok)
		_, ok = tblCtx.GetCachedTableSupport()
		require.False(t, ok)
		_, ok = tblCtx.GetTemporaryTableSupport()
		require.False(t, ok)
		stats, ok := tblCtx.GetStatisticsSupport()
		require.True(t, ok)
		// test for `UpdatePhysicalTableDelta` and `GetColumnSize`
		stats.UpdatePhysicalTableDelta(123, 5, 2, variable.DeltaColsMap{1: 2, 3: 4})
		r := tblCtx.GetColumnSize(123)
		require.Equal(t, map[int64]int64{1: 2, 3: 4}, r)
		stats.UpdatePhysicalTableDelta(123, 8, 2, variable.DeltaColsMap{3: 5, 4: 3})
		r = tblCtx.GetColumnSize(123)
		require.Equal(t, map[int64]int64{1: 2, 3: 9, 4: 3}, r)
		// the result should be a cloned value
		r[1] = 100
		require.Equal(t, map[int64]int64{1: 2, 3: 9, 4: 3}, tblCtx.GetColumnSize(123))
		// test gets a non-existed table
		require.Empty(t, tblCtx.GetColumnSize(456))
	}

	// We need to compare the new introduced `*litTableMutateContext` the same behavior with the old `session`.
	// After refactoring finished, we can remove the old session and this test.
	compareWithLegacySession := func(ctx *litTableMutateContext, vars map[string]string) {
		se := newSession(&encode.SessionOptions{
			SQLMode: mysql.ModeNone,
			SysVars: vars,
		}, log.L())
		// make sure GetRowIDShardGenerator() internal assertion pass
		se.GetSessionVars().TxnCtx = &variable.TransactionContext{}
		se.GetSessionVars().TxnCtx.StartTS = 123
		seCtx := se.GetTableCtx()
		require.Equal(t, seCtx.ConnectionID(), ctx.ConnectionID())
		require.Equal(t, seCtx.InRestrictedSQL(), ctx.InRestrictedSQL())
		require.Equal(t, seCtx.TxnAssertionLevel(), ctx.TxnAssertionLevel())
		require.Equal(t, seCtx.GetMutateBuffers(), ctx.GetMutateBuffers())
		require.Equal(t, seCtx.EnableMutationChecker(), ctx.EnableMutationChecker())
		require.Equal(t, seCtx.GetRowEncodingConfig(), ctx.GetRowEncodingConfig())
		require.Equal(t, seCtx.GetRowIDShardGenerator().GetShardStep(), ctx.GetRowIDShardGenerator().GetShardStep())
		seAlloc, ok := seCtx.GetReservedRowIDAlloc()
		require.True(t, ok)
		alloc, ok := ctx.GetReservedRowIDAlloc()
		require.True(t, ok)
		require.Equal(t, seAlloc, alloc)
	}

	// test for default
	tblCtx, err := newLitTableMutateContext(exprCtx, nil)
	require.NoError(t, err)
	checkCommon(t, tblCtx)
	require.Equal(t, variable.AssertionLevelOff, tblCtx.TxnAssertionLevel())
	require.Equal(t, variable.DefTiDBEnableMutationChecker, tblCtx.EnableMutationChecker())
	require.False(t, tblCtx.EnableMutationChecker())
	require.Equal(t, tbctx.RowEncodingConfig{
		IsRowLevelChecksumEnabled: false,
		RowEncoder:                &rowcodec.Encoder{Enable: false},
	}, tblCtx.GetRowEncodingConfig())
	g := tblCtx.GetRowIDShardGenerator()
	require.NotNil(t, g)
	require.Equal(t, variable.DefTiDBShardAllocateStep, g.GetShardStep())
	compareWithLegacySession(tblCtx, nil)

	// test for load vars
	sysVars := map[string]string{
		"tidb_txn_assertion_level":     "STRICT",
		"tidb_enable_mutation_checker": "ON",
		"tidb_row_format_version":      "2",
		"tidb_shard_allocate_step":     "1234567",
	}
	tblCtx, err = newLitTableMutateContext(exprCtx, sysVars)
	require.NoError(t, err)
	checkCommon(t, tblCtx)
	require.Equal(t, variable.AssertionLevelStrict, tblCtx.TxnAssertionLevel())
	require.True(t, tblCtx.EnableMutationChecker())
	require.Equal(t, tbctx.RowEncodingConfig{
		IsRowLevelChecksumEnabled: false,
		RowEncoder:                &rowcodec.Encoder{Enable: true},
	}, tblCtx.GetRowEncodingConfig())
	g = tblCtx.GetRowIDShardGenerator()
	require.NotNil(t, g)
	require.NotEqual(t, variable.DefTiDBShardAllocateStep, g.GetShardStep())
	require.Equal(t, 1234567, g.GetShardStep())
	compareWithLegacySession(tblCtx, sysVars)

	// test for `RowEncodingConfig.IsRowLevelChecksumEnabled` which should be loaded from global variable.
	require.False(t, variable.EnableRowLevelChecksum.Load())
	defer variable.EnableRowLevelChecksum.Store(false)
	variable.EnableRowLevelChecksum.Store(true)
	sysVars = map[string]string{
		"tidb_row_format_version": "2",
	}
	tblCtx, err = newLitTableMutateContext(exprCtx, sysVars)
	require.NoError(t, err)
	require.Equal(t, tbctx.RowEncodingConfig{
		IsRowLevelChecksumEnabled: true,
		RowEncoder:                &rowcodec.Encoder{Enable: true},
	}, tblCtx.GetRowEncodingConfig())
	compareWithLegacySession(tblCtx, sysVars)
}