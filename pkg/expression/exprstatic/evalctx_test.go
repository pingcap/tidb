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

package exprstatic

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/stretchr/testify/require"
)

func TestNewStaticEvalCtx(t *testing.T) {
	// default context
	prevID := contextutil.GenContextID()
	ctx := NewEvalContext()
	require.Equal(t, prevID+1, ctx.CtxID())
	checkDefaultStaticEvalCtx(t, ctx)

	// with options
	prevID = ctx.CtxID()
	options, stateForTest := getEvalCtxOptionsForTest(t)
	ctx = NewEvalContext(options...)
	require.Equal(t, prevID+1, ctx.CtxID())
	checkOptionsStaticEvalCtx(t, ctx, stateForTest)
}

func checkDefaultStaticEvalCtx(t *testing.T, ctx *EvalContext) {
	mode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	require.Equal(t, mode, ctx.SQLMode())
	require.Same(t, time.UTC, ctx.Location())
	require.Equal(t, types.NewContext(types.StrictFlags, time.UTC, ctx), ctx.TypeCtx())
	require.Equal(t, errctx.NewContextWithLevels(errctx.LevelMap{}, ctx), ctx.ErrCtx())
	require.Equal(t, "", ctx.CurrentDB())
	require.Equal(t, variable.DefMaxAllowedPacket, ctx.GetMaxAllowedPacket())
	require.Equal(t, variable.DefDefaultWeekFormat, ctx.GetDefaultWeekFormatMode())
	require.Equal(t, variable.DefDivPrecisionIncrement, ctx.GetDivPrecisionIncrement())
	require.Empty(t, ctx.AllParamValues())
	require.Equal(t, variable.NewUserVars(), ctx.GetUserVarsReader())
	require.Nil(t, ctx.requestVerificationFn)
	require.Nil(t, ctx.requestDynamicVerificationFn)
	require.True(t, ctx.RequestVerification("test", "t1", "", mysql.CreatePriv))
	require.True(t, ctx.RequestDynamicVerification("RESTRICTED_USER_ADMIN", true))
	require.True(t, ctx.GetOptionalPropSet().IsEmpty())
	p, ok := ctx.GetOptionalPropProvider(exprctx.OptPropAdvisoryLock)
	require.Nil(t, p)
	require.False(t, ok)

	tm, err := ctx.CurrentTime()
	require.NoError(t, err)
	require.Same(t, time.UTC, tm.Location())
	require.InDelta(t, time.Now().Unix(), tm.Unix(), 5)

	warnHandler, ok := ctx.warnHandler.(*contextutil.StaticWarnHandler)
	require.True(t, ok)
	require.Equal(t, 0, warnHandler.WarningCount())
}

type evalCtxOptionsTestState struct {
	now           time.Time
	loc           *time.Location
	warnHandler   *contextutil.StaticWarnHandler
	userVars      *variable.UserVars
	ddlOwner      bool
	privCheckArgs []any
	privRet       bool
}

func getEvalCtxOptionsForTest(t *testing.T) ([]EvalCtxOption, *evalCtxOptionsTestState) {
	loc, err := time.LoadLocation("US/Eastern")
	require.NoError(t, err)
	s := &evalCtxOptionsTestState{
		now:         time.Now(),
		loc:         loc,
		warnHandler: contextutil.NewStaticWarnHandler(8),
		userVars:    variable.NewUserVars(),
	}

	provider1 := expropt.CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) {
		return &auth.UserIdentity{Username: "user1", Hostname: "host1"},
			[]*auth.RoleIdentity{{Username: "role1", Hostname: "host2"}}
	})

	provider2 := expropt.DDLOwnerInfoProvider(func() bool {
		return s.ddlOwner
	})

	return []EvalCtxOption{
		WithWarnHandler(s.warnHandler),
		WithSQLMode(mysql.ModeNoZeroDate | mysql.ModeStrictTransTables),
		WithTypeFlags(types.FlagAllowNegativeToUnsigned | types.FlagSkipASCIICheck),
		WithErrLevelMap(errctx.LevelMap{
			errctx.ErrGroupBadNull:       errctx.LevelError,
			errctx.ErrGroupDividedByZero: errctx.LevelWarn,
		}),
		WithLocation(loc),
		WithCurrentDB("db1"),
		WithCurrentTime(func() (time.Time, error) {
			return s.now, nil
		}),
		WithMaxAllowedPacket(12345),
		WithDefaultWeekFormatMode("3"),
		WithDivPrecisionIncrement(5),
		WithUserVarsReader(s.userVars),
		WithPrivCheck(func(db, table, column string, priv mysql.PrivilegeType) bool {
			require.Nil(t, s.privCheckArgs)
			s.privCheckArgs = []any{db, table, column, priv}
			return s.privRet
		}),
		WithDynamicPrivCheck(func(privName string, grantable bool) bool {
			require.Nil(t, s.privCheckArgs)
			s.privCheckArgs = []any{privName, grantable}
			return s.privRet
		}),
		WithOptionalProperty(provider1, provider2),
	}, s
}

func checkOptionsStaticEvalCtx(t *testing.T, ctx *EvalContext, s *evalCtxOptionsTestState) {
	require.Same(t, ctx.warnHandler, s.warnHandler)
	require.Equal(t, mysql.ModeNoZeroDate|mysql.ModeStrictTransTables, ctx.SQLMode())
	require.Equal(t,
		types.NewContext(types.FlagAllowNegativeToUnsigned|types.FlagSkipASCIICheck, s.loc, ctx),
		ctx.TypeCtx(),
	)
	require.Equal(t, errctx.NewContextWithLevels(errctx.LevelMap{
		errctx.ErrGroupBadNull:       errctx.LevelError,
		errctx.ErrGroupDividedByZero: errctx.LevelWarn,
	}, ctx), ctx.ErrCtx())
	require.Same(t, s.loc, ctx.Location())
	require.Equal(t, "db1", ctx.CurrentDB())
	current, err := ctx.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, current.UnixNano(), s.now.UnixNano())
	require.Same(t, s.loc, current.Location())
	require.Equal(t, uint64(12345), ctx.GetMaxAllowedPacket())
	require.Equal(t, "3", ctx.GetDefaultWeekFormatMode())
	require.Equal(t, 5, ctx.GetDivPrecisionIncrement())
	require.Same(t, s.userVars, ctx.GetUserVarsReader())

	s.privCheckArgs, s.privRet = nil, false
	require.False(t, ctx.RequestVerification("db", "table", "column", mysql.CreatePriv))
	require.Equal(t, []any{"db", "table", "column", mysql.CreatePriv}, s.privCheckArgs)
	s.privCheckArgs, s.privRet = nil, true
	require.True(t, ctx.RequestVerification("db2", "table2", "column2", mysql.UpdatePriv))
	require.Equal(t, []any{"db2", "table2", "column2", mysql.UpdatePriv}, s.privCheckArgs)
	s.privCheckArgs, s.privRet = nil, false
	require.False(t, ctx.RequestDynamicVerification("RESTRICTED_USER_ADMIN", true))
	require.Equal(t, []any{"RESTRICTED_USER_ADMIN", true}, s.privCheckArgs)
	s.privCheckArgs, s.privRet = nil, true
	require.True(t, ctx.RequestDynamicVerification("RESTRICTED_TABLES_ADMIN", false))
	require.Equal(t, []any{"RESTRICTED_TABLES_ADMIN", false}, s.privCheckArgs)

	var optSet exprctx.OptionalEvalPropKeySet
	optSet = optSet.Add(exprctx.OptPropCurrentUser).Add(exprctx.OptPropDDLOwnerInfo)
	require.Equal(t, optSet, ctx.GetOptionalPropSet())
	p, ok := ctx.GetOptionalPropProvider(exprctx.OptPropCurrentUser)
	require.True(t, ok)
	user, roles := p.(expropt.CurrentUserPropProvider)()
	require.Equal(t, &auth.UserIdentity{Username: "user1", Hostname: "host1"}, user)
	require.Equal(t, []*auth.RoleIdentity{{Username: "role1", Hostname: "host2"}}, roles)
	p, ok = ctx.GetOptionalPropProvider(exprctx.OptPropDDLOwnerInfo)
	s.ddlOwner = true
	require.True(t, ok)
	require.True(t, p.(expropt.DDLOwnerInfoProvider)())
	s.ddlOwner = false
	require.False(t, p.(expropt.DDLOwnerInfoProvider)())
	p, ok = ctx.GetOptionalPropProvider(exprctx.OptPropInfoSchema)
	require.False(t, ok)
	require.Nil(t, p)
}

func TestStaticEvalCtxCurrentTime(t *testing.T) {
	loc1, err := time.LoadLocation("US/Eastern")
	require.NoError(t, err)

	tm := time.UnixMicro(123456789).In(loc1)
	calls := 0
	getTime := func() (time.Time, error) {
		defer func() {
			calls++
		}()

		if calls < 2 {
			return time.Time{}, errors.NewNoStackError(fmt.Sprintf("err%d", calls))
		}

		if calls == 2 {
			return tm, nil
		}

		return time.Time{}, errors.NewNoStackError("should not reach here")
	}

	ctx := NewEvalContext(WithCurrentTime(getTime))

	// get time for the first two times should fail
	got, err := ctx.CurrentTime()
	require.EqualError(t, err, "err0")
	require.Equal(t, time.Time{}, got)

	got, err = ctx.CurrentTime()
	require.EqualError(t, err, "err1")
	require.Equal(t, time.Time{}, got)

	// the third time will success
	got, err = ctx.CurrentTime()
	require.Nil(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, time.UTC, got.Location())
	require.Equal(t, 3, calls)

	// next ctx should cache the time without calling inner function
	got, err = ctx.CurrentTime()
	require.Nil(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, time.UTC, got.Location())
	require.Equal(t, 3, calls)

	// CurrentTime should have the same location with `ctx.Location()`
	loc2, err := time.LoadLocation("Australia/Sydney")
	require.NoError(t, err)
	ctx = NewEvalContext(
		WithLocation(loc2),
		WithCurrentTime(func() (time.Time, error) {
			return tm, nil
		}),
	)
	got, err = ctx.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, loc2, got.Location())

	// Apply should copy the current time
	ctx2 := ctx.Apply()
	got, err = ctx2.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, loc2, got.Location())

	// Apply with location should change current time's location
	ctx2 = ctx.Apply(WithLocation(loc1))
	got, err = ctx2.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, loc1, got.Location())

	// Apply will not affect previous current time
	got, err = ctx.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, loc2, got.Location())

	// Apply with a different current time func
	ctx2 = ctx.Apply(WithCurrentTime(func() (time.Time, error) {
		return time.UnixMicro(987654321), nil
	}))
	got, err = ctx2.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, int64(987654321), got.UnixMicro())
	require.Same(t, loc2, got.Location())

	// Apply will not affect previous current time
	got, err = ctx.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), got.UnixNano())
	require.Same(t, loc2, got.Location())
}

func TestStaticEvalCtxWarnings(t *testing.T) {
	// default context should have a empty StaticWarningsHandler
	ctx := NewEvalContext()
	h, ok := ctx.warnHandler.(*contextutil.StaticWarnHandler)
	require.True(t, ok)
	require.Equal(t, 0, h.WarningCount())

	// WithWarnHandler should work
	ignoreHandler := contextutil.IgnoreWarn
	ctx = NewEvalContext(WithWarnHandler(ignoreHandler))
	require.True(t, ctx.warnHandler == ignoreHandler)

	// All contexts should use the same warning handler
	h = contextutil.NewStaticWarnHandler(8)
	ctx = NewEvalContext(WithWarnHandler(h))
	tc, ec := ctx.TypeCtx(), ctx.ErrCtx()
	h.AppendWarning(errors.NewNoStackError("warn0"))
	ctx.AppendWarning(errors.NewNoStackError("warn1"))
	tc.AppendWarning(errors.NewNoStackError("warn2"))
	ec.AppendWarning(errors.NewNoStackError("warn3"))
	require.Equal(t, 4, h.WarningCount())
	require.Equal(t, h.WarningCount(), ctx.WarningCount())

	// ctx.CopyWarnings
	warnings := ctx.CopyWarnings(nil)
	require.Equal(t, []contextutil.SQLWarn{
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn0")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn2")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn3")},
	}, warnings)
	require.Equal(t, 4, h.WarningCount())
	require.Equal(t, h.WarningCount(), ctx.WarningCount())

	// ctx.TruncateWarnings
	warnings = ctx.TruncateWarnings(2)
	require.Equal(t, []contextutil.SQLWarn{
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn2")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn3")},
	}, warnings)
	require.Equal(t, 2, h.WarningCount())
	require.Equal(t, h.WarningCount(), ctx.WarningCount())
	warnings = ctx.CopyWarnings(nil)
	require.Equal(t, []contextutil.SQLWarn{
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn0")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
	}, warnings)

	// Apply should use the old warning handler by default
	ctx2 := ctx.Apply()
	require.NotSame(t, ctx, ctx2)
	require.True(t, ctx.warnHandler == ctx2.warnHandler)
	require.True(t, ctx.warnHandler == h)

	// Apply with `WithWarnHandler`
	h2 := contextutil.NewStaticWarnHandler(16)
	ctx2 = ctx.Apply(WithWarnHandler(h2))
	require.True(t, ctx2.warnHandler == h2)
	require.True(t, ctx.warnHandler == h)

	// The type context and error context should use the new handler.
	ctx.TruncateWarnings(0)
	tc, ec = ctx.TypeCtx(), ctx.ErrCtx()
	tc2, ec2 := ctx2.TypeCtx(), ctx2.ErrCtx()
	tc2.AppendWarning(errors.NewNoStackError("warn4"))
	ec2.AppendWarning(errors.NewNoStackError("warn5"))
	tc.AppendWarning(errors.NewNoStackError("warn6"))
	ec.AppendWarning(errors.NewNoStackError("warn7"))
	require.Equal(t, []contextutil.SQLWarn{
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn4")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn5")},
	}, ctx2.CopyWarnings(nil))
	require.Equal(t, []contextutil.SQLWarn{
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn6")},
		{Level: contextutil.WarnLevelWarning, Err: errors.NewNoStackError("warn7")},
	}, ctx.CopyWarnings(nil))
}

func TestStaticEvalContextOptionalProps(t *testing.T) {
	ctx := NewEvalContext()
	require.True(t, ctx.GetOptionalPropSet().IsEmpty())

	ctx2 := ctx.Apply(WithOptionalProperty(
		expropt.CurrentUserPropProvider(func() (u *auth.UserIdentity, r []*auth.RoleIdentity) { return }),
	))
	var emptySet exprctx.OptionalEvalPropKeySet
	require.Equal(t, emptySet, ctx.GetOptionalPropSet())
	require.Equal(t, emptySet.Add(exprctx.OptPropCurrentUser), ctx2.GetOptionalPropSet())

	// Apply should override all optional properties
	ctx3 := ctx2.Apply(WithOptionalProperty(
		expropt.DDLOwnerInfoProvider(func() bool { return true }),
		expropt.InfoSchemaPropProvider(func(isDomain bool) infoschema.MetaOnlyInfoSchema { return nil }),
	))
	require.Equal(t,
		emptySet.Add(exprctx.OptPropDDLOwnerInfo).Add(exprctx.OptPropInfoSchema),
		ctx3.GetOptionalPropSet(),
	)
	require.Equal(t, emptySet, ctx.GetOptionalPropSet())
	require.Equal(t, emptySet.Add(exprctx.OptPropCurrentUser), ctx2.GetOptionalPropSet())
}

func TestUpdateStaticEvalContext(t *testing.T) {
	oldCtx := NewEvalContext()
	ctx := oldCtx.Apply()

	// Should return a different instance
	require.NotSame(t, oldCtx, ctx)

	// CtxID should be different
	require.Greater(t, ctx.CtxID(), oldCtx.CtxID())

	// inner state should not be the same address
	require.NotSame(t, &oldCtx.evalCtxState, &ctx.evalCtxState)

	// compare a state object by excluding some changed fields
	excludeChangedFields := func(s *evalCtxState) evalCtxState {
		state := *s
		state.typeCtx = types.DefaultStmtNoWarningContext
		state.errCtx = errctx.StrictNoWarningContext
		state.currentTime = nil
		return state
	}
	require.Equal(t, excludeChangedFields(&oldCtx.evalCtxState), excludeChangedFields(&ctx.evalCtxState))

	// check fields
	checkDefaultStaticEvalCtx(t, ctx)

	// apply options
	opts, optState := getEvalCtxOptionsForTest(t)
	ctx2 := oldCtx.Apply(opts...)
	require.Greater(t, ctx2.CtxID(), ctx.CtxID())
	checkOptionsStaticEvalCtx(t, ctx2, optState)

	// old ctx aren't affected
	checkDefaultStaticEvalCtx(t, oldCtx)

	// create with options
	opts, optState = getEvalCtxOptionsForTest(t)
	ctx3 := NewEvalContext(opts...)
	require.Greater(t, ctx3.CtxID(), ctx2.CtxID())
	checkOptionsStaticEvalCtx(t, ctx3, optState)
}

func TestParamList(t *testing.T) {
	paramList := variable.NewPlanCacheParamList()
	paramList.Append(types.NewDatum(1))
	paramList.Append(types.NewDatum(2))
	paramList.Append(types.NewDatum(3))
	ctx := NewEvalContext(
		WithParamList(paramList),
	)
	for i := 0; i < 3; i++ {
		val, err := ctx.GetParamValue(i)
		require.NoError(t, err)
		require.Equal(t, int64(i+1), val.GetInt64())
	}

	// after reset the paramList and append new one, the value is still persisted
	paramList.Reset()
	paramList.Append(types.NewDatum(4))
	for i := 0; i < 3; i++ {
		val, err := ctx.GetParamValue(i)
		require.NoError(t, err)
		require.Equal(t, int64(i+1), val.GetInt64())
	}
}

func TestMakeEvalContextStatic(t *testing.T) {
	// This test is to ensure that the `MakeEvalContextStatic` function works as expected.
	// It requires the developers to create a special `EvalContext`, whose every fields
	// are non-empty. Then, the `MakeEvalContextStatic` function is called to create a new
	// clone of it. Finally, the new clone is compared with the original one to ensure that
	// the fields are correctly copied.
	paramList := variable.NewPlanCacheParamList()
	paramList.Append(types.NewDatum(1))

	userVars := variable.NewUserVars()
	userVars.SetUserVarVal("a", types.NewStringDatum("v1"))
	userVars.SetUserVarVal("b", types.NewIntDatum(2))

	provider := expropt.DDLOwnerInfoProvider(func() bool {
		return true
	})

	obj := NewEvalContext(
		WithWarnHandler(contextutil.NewStaticWarnHandler(16)),
		WithSQLMode(mysql.ModeNoZeroDate|mysql.ModeStrictTransTables),
		WithTypeFlags(types.FlagAllowNegativeToUnsigned|types.FlagSkipASCIICheck),
		WithErrLevelMap(errctx.LevelMap{}),
		WithLocation(time.UTC),
		WithCurrentDB("db1"),
		WithCurrentTime(func() (time.Time, error) {
			return time.Now(), nil
		}),
		WithMaxAllowedPacket(12345),
		WithDefaultWeekFormatMode("3"),
		WithDivPrecisionIncrement(5),
		WithPrivCheck(func(db, table, column string, priv mysql.PrivilegeType) bool {
			return true
		}),
		WithDynamicPrivCheck(func(privName string, grantable bool) bool {
			return true
		}),
		WithParamList(paramList),
		WithUserVarsReader(userVars),
		WithOptionalProperty(provider),
		WithEnableRedactLog("test"),
	)
	obj.AppendWarning(errors.New("test warning"))

	ignorePath := []string{
		"$.evalCtxState.warnHandler.**",
		"$.evalCtxState.typeCtx.**",
		"$.evalCtxState.errCtx.**",
		"$.evalCtxState.currentTime.**",
		"$.evalCtxState.userVars.lock",
		"$.evalCtxState.props",
		"$.id",
	}
	deeptest.AssertRecursivelyNotEqual(t, obj, NewEvalContext(),
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.evalCtxState.requestVerificationFn",
			"$.evalCtxState.requestDynamicVerificationFn",
		}))

	staticObj := MakeEvalContextStatic(obj)

	deeptest.AssertDeepClonedEqual(t, obj, staticObj,
		deeptest.WithIgnorePath(ignorePath),
		deeptest.WithPointerComparePath([]string{
			"$.evalCtxState.warnHandler",
			"$.evalCtxState.paramList*.b",
			"$.evalCtxState.requestVerificationFn",
			"$.evalCtxState.requestDynamicVerificationFn",
		}),
	)

	require.Equal(t, obj.GetWarnHandler(), staticObj.GetWarnHandler())
	require.Equal(t, obj.typeCtx.Flags(), staticObj.typeCtx.Flags())
	require.Equal(t, obj.errCtx.LevelMap(), staticObj.errCtx.LevelMap())

	oldT, err := obj.CurrentTime()
	require.NoError(t, err)
	newT, err := staticObj.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, oldT.Unix(), newT.Unix())

	require.NotEqual(t, obj.GetOptionalPropSet(), staticObj.GetOptionalPropSet())
	// Now, it didn't copy any optional properties.
	require.Equal(t, exprctx.OptionalEvalPropKeySet(0), staticObj.GetOptionalPropSet())
}

func TestEvalCtxLoadSystemVars(t *testing.T) {
	vars := []struct {
		name   string
		val    string
		field  string
		assert func(ctx *EvalContext, vars *variable.SessionVars)
	}{
		{
			name:  "time_zone",
			val:   "Europe/Berlin",
			field: "$.typeCtx.loc",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, "Europe/Berlin", ctx.Location().String())
				require.Equal(t, vars.Location().String(), ctx.Location().String())
			},
		},
		{
			name:  "sql_mode",
			val:   "ALLOW_INVALID_DATES,ONLY_FULL_GROUP_BY",
			field: "$.sqlMode",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, mysql.ModeAllowInvalidDates|mysql.ModeOnlyFullGroupBy, ctx.SQLMode())
				require.Equal(t, vars.SQLMode, ctx.SQLMode())
			},
		},
		{
			name:  "timestamp",
			val:   "1234567890.123456",
			field: "$.currentTime",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				currentTime, err := ctx.CurrentTime()
				require.NoError(t, err)
				require.Equal(t, int64(1234567890123456), currentTime.UnixMicro())
				require.Equal(t, vars.Location().String(), currentTime.Location().String())
			},
		},
		{
			name:  strings.ToUpper("max_allowed_packet"), // test for settings an upper case variable
			val:   "524288",
			field: "$.maxAllowedPacket",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, uint64(524288), ctx.GetMaxAllowedPacket())
				require.Equal(t, vars.MaxAllowedPacket, ctx.GetMaxAllowedPacket())
			},
		},
		{
			name:  strings.ToUpper("tidb_redact_log"), // test for settings an upper case variable
			val:   "on",
			field: "$.enableRedactLog",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, "ON", ctx.GetTiDBRedactLog())
				require.Equal(t, vars.EnableRedactLog, ctx.GetTiDBRedactLog())
			},
		},
		{
			name:  "default_week_format",
			val:   "5",
			field: "$.defaultWeekFormatMode",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, "5", ctx.GetDefaultWeekFormatMode())
				mode, ok := vars.GetSystemVar(variable.DefaultWeekFormat)
				require.True(t, ok)
				require.Equal(t, mode, ctx.GetDefaultWeekFormatMode())
			},
		},
		{
			name:  "div_precision_increment",
			val:   "12",
			field: "$.divPrecisionIncrement",
			assert: func(ctx *EvalContext, vars *variable.SessionVars) {
				require.Equal(t, 12, ctx.GetDivPrecisionIncrement())
				require.Equal(t, vars.DivPrecisionIncrement, ctx.GetDivPrecisionIncrement())
			},
		},
	}

	// nonVarRelatedFields means the fields not related to any system variables.
	// To make sure that all the variables which affect the context state are covered in the above test list,
	// we need to test all inner fields except those in `nonVarRelatedFields` are changed after `LoadSystemVars`.
	nonVarRelatedFields := []string{
		"$.warnHandler",
		"$.typeCtx.flags",
		"$.typeCtx.warnHandler",
		"$.errCtx",
		"$.currentDB",
		"$.requestVerificationFn",
		"$.requestDynamicVerificationFn",
		"$.paramList",
		"$.userVars",
		"$.props",
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

	defaultEvalCtx := NewEvalContext()
	ctx, err := defaultEvalCtx.LoadSystemVars(varsMap)
	require.NoError(t, err)
	require.Greater(t, ctx.CtxID(), defaultEvalCtx.CtxID())

	// Check all fields except these in `nonVarRelatedFields` are changed after `LoadSystemVars` to make sure
	// all system variables related fields are covered in the test list.
	deeptest.AssertRecursivelyNotEqual(
		t,
		defaultEvalCtx.evalCtxState,
		ctx.evalCtxState,
		deeptest.WithIgnorePath(nonVarRelatedFields),
		deeptest.WithPointerComparePath([]string{"$.currentTime"}),
	)

	// We need to compare the new context again with an empty one to make sure those values are set from sys vars,
	// not inherited from the empty go value.
	deeptest.AssertRecursivelyNotEqual(
		t,
		evalCtxState{},
		ctx.evalCtxState,
		deeptest.WithIgnorePath(nonVarRelatedFields),
		deeptest.WithPointerComparePath([]string{"$.currentTime"}),
	)

	// Check all system vars unrelated fields are not changed after `LoadSystemVars`.
	deeptest.AssertDeepClonedEqual(
		t,
		defaultEvalCtx.evalCtxState,
		ctx.evalCtxState,
		deeptest.WithIgnorePath(append(
			varsRelatedFields,
			// Do not check warnHandler in `typeCtx` and `errCtx` because they should be changed to even if
			// they are not related to any system variable.
			"$.typeCtx.warnHandler",
			"$.errCtx.warnHandler",
		)),
		// LoadSystemVars only does shallow copy for `EvalContext` so we just need to compare the pointers.
		deeptest.WithPointerComparePath(nonVarRelatedFields),
	)

	for _, sysVar := range vars {
		sysVar.assert(ctx, sessionVars)
	}

	// additional check about @@timestamp
	// setting to `variable.DefTimestamp` should return the current timestamp
	ctx, err = defaultEvalCtx.LoadSystemVars(map[string]string{
		"timestamp": variable.DefTimestamp,
	})
	require.NoError(t, err)
	tm, err := ctx.CurrentTime()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Unix(), tm.Unix(), 5)
}
