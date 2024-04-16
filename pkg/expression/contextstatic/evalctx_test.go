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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultStaticEvalCtx(t *testing.T) {
	prevID := contextutil.GenContextID()
	ctx := NewStaticEvalContext()
	require.Equal(t, prevID+1, ctx.CtxID())
	checkDefaultStaticEvalCtx(t, ctx)
}

func checkDefaultStaticEvalCtx(t *testing.T, ctx *StaticEvalContext) {
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
	require.Nil(t, ctx.requestVerificationFn)
	require.Nil(t, ctx.requestDynamicVerificationFn)
	require.True(t, ctx.RequestVerification("test", "t1", "", mysql.CreatePriv))
	require.True(t, ctx.RequestDynamicVerification("RESTRICTED_USER_ADMIN", true))
	require.True(t, ctx.GetOptionalPropSet().IsEmpty())
	p, ok := ctx.GetOptionalPropProvider(context.OptPropAdvisoryLock)
	require.Nil(t, p)
	require.False(t, ok)

	tm, err := ctx.CurrentTime()
	require.NoError(t, err)
	require.Same(t, time.UTC, tm.Location())
	require.InDelta(t, time.Now().Unix(), tm.Unix(), 5)

	require.Equal(t, 0, ctx.WarningCount())
	warn1, warn2, warn3 :=
		errors.NewNoStackError("err1"), errors.NewNoStackError("err2"), errors.NewNoStackError("err3")
	ctx.AppendWarning(warn1)
	tc, ec := ctx.TypeCtx(), ctx.ErrCtx()
	tc.AppendWarning(warn2)
	ec.AppendWarning(warn3)
	require.Equal(t, 3, ctx.WarningCount())
	warns := ctx.TruncateWarnings(0)
	require.Equal(t, 0, ctx.WarningCount())
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: warn1},
		{Level: stmtctx.WarnLevelWarning, Err: warn2},
		{Level: stmtctx.WarnLevelWarning, Err: warn3},
	}, warns)
}

func TestStaticEvalCtxOptions(t *testing.T) {
	prevID := contextutil.GenContextID()
	options, stateForTest := getEvalCtxOptionsForTest(t)
	ctx := NewStaticEvalContext(options...)
	require.Equal(t, prevID+1, ctx.CtxID())
	checkOptionsStaticEvalCtx(t, ctx, stateForTest)
}

type evalCtxOptionsTestState struct {
	now           time.Time
	loc           *time.Location
	ddlOwner      bool
	privCheckArgs []any
	privRet       bool
}

func getEvalCtxOptionsForTest(t *testing.T) ([]StaticEvalCtxOption, *evalCtxOptionsTestState) {
	s := &evalCtxOptionsTestState{}

	loc, err := time.LoadLocation("US/Eastern")
	require.NoError(t, err)
	s.now = time.Now()
	s.loc = loc

	provider1 := contextopt.CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) {
		return &auth.UserIdentity{Username: "user1", Hostname: "host1"},
			[]*auth.RoleIdentity{{Username: "role1", Hostname: "host2"}}
	})

	provider2 := contextopt.DDLOwnerInfoProvider(func() bool {
		return s.ddlOwner
	})

	return []StaticEvalCtxOption{
		WithWarnings([]stmtctx.SQLWarn{
			{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
			{Level: stmtctx.WarnLevelError, Err: errors.NewNoStackError("warn2")},
		}),
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

func checkOptionsStaticEvalCtx(t *testing.T, ctx *StaticEvalContext, s *evalCtxOptionsTestState) {
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: stmtctx.WarnLevelError, Err: errors.NewNoStackError("warn2")},
	}, ctx.TruncateWarnings(0))
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

	var optSet context.OptionalEvalPropKeySet
	optSet = optSet.Add(context.OptPropCurrentUser).Add(context.OptPropDDLOwnerInfo)
	require.Equal(t, optSet, ctx.GetOptionalPropSet())
	p, ok := ctx.GetOptionalPropProvider(context.OptPropCurrentUser)
	require.True(t, ok)
	user, roles := p.(contextopt.CurrentUserPropProvider)()
	require.Equal(t, &auth.UserIdentity{Username: "user1", Hostname: "host1"}, user)
	require.Equal(t, []*auth.RoleIdentity{{Username: "role1", Hostname: "host2"}}, roles)
	p, ok = ctx.GetOptionalPropProvider(context.OptPropDDLOwnerInfo)
	s.ddlOwner = true
	require.True(t, ok)
	require.True(t, p.(contextopt.DDLOwnerInfoProvider)())
	s.ddlOwner = false
	require.False(t, p.(contextopt.DDLOwnerInfoProvider)())
	p, ok = ctx.GetOptionalPropProvider(context.OptPropInfoSchema)
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

	ctx := NewStaticEvalContext(WithCurrentTime(getTime))

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
	ctx = NewStaticEvalContext(
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
	// Nil warnings
	ctx := NewStaticEvalContext(WithWarnings(nil))
	require.Equal(t, 0, ctx.WarningCount())

	warns := make([]stmtctx.SQLWarn, 0, 8)
	warns = append(warns, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: stmtctx.WarnLevelError, Err: errors.NewNoStackError("warn2")},
		{Level: stmtctx.WarnLevelNote, Err: errors.NewNoStackError("warn3")},
	}...)

	// WithWarnings should copy the warnings
	ctx = NewStaticEvalContext(WithWarnings(warns))
	ctx.AppendWarning(errors.NewNoStackError("warn4"))
	require.Equal(t, 4, ctx.WarningCount())
	warns = warns[:4]
	require.Nil(t, warns[3].Err)

	// TypeCtx and ErrCtx should append warning to the context
	tc, ec := ctx.TypeCtx(), ctx.ErrCtx()
	tc.AppendWarning(errors.NewNoStackError("warn5"))
	ec.AppendWarning(errors.NewNoStackError("warn6"))
	require.Equal(t, 6, ctx.WarningCount())

	// Truncate warnings with out-of-range index
	require.Empty(t, ctx.TruncateWarnings(6))
	require.Empty(t, ctx.TruncateWarnings(7))
	require.Equal(t, 6, ctx.WarningCount())

	// Truncate warnings
	gotWarnings := ctx.TruncateWarnings(2)
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelNote, Err: errors.NewNoStackError("warn3")},
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn4")},
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn5")},
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn6")},
	}, gotWarnings)
	require.Equal(t, 2, ctx.WarningCount())

	gotWarnings = ctx.TruncateWarnings(0)
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: stmtctx.WarnLevelError, Err: errors.NewNoStackError("warn2")},
	}, gotWarnings)
	require.Equal(t, 0, ctx.WarningCount())

	// Truncate warnings should do copy
	ctx.AppendWarning(errors.NewNoStackError("warn7"))
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn1")},
		{Level: stmtctx.WarnLevelError, Err: errors.NewNoStackError("warn2")},
	}, gotWarnings)
	require.Equal(t, 1, ctx.WarningCount())
	gotWarnings[0] = stmtctx.SQLWarn{Level: stmtctx.WarnLevelNote, Err: errors.NewNoStackError("warn8")}
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn7")},
	}, ctx.TruncateWarnings(0))
	require.Equal(t, 0, ctx.WarningCount())

	// warnings should be cloned
	ctx.AppendWarning(errors.NewNoStackError("warn8"))
	ctx2 := ctx.Apply()
	ctx.AppendWarning(errors.NewNoStackError("warn9"))
	ctx2.AppendWarning(errors.NewNoStackError("warn10"))
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn8")},
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn9")},
	}, ctx.TruncateWarnings(0))
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn8")},
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn10")},
	}, ctx2.TruncateWarnings(0))

	// WithWarnings should override previous warnings
	ctx.AppendWarning(errors.NewNoStackError("warn11"))
	ctx2 = ctx.Apply(WithWarnings([]stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn12")},
	}))
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn11")},
	}, ctx.TruncateWarnings(0))
	require.Equal(t, []stmtctx.SQLWarn{
		{Level: stmtctx.WarnLevelWarning, Err: errors.NewNoStackError("warn12")},
	}, ctx2.TruncateWarnings(0))
}

func TestStaticEvalContextOptionalProps(t *testing.T) {
	ctx := NewStaticEvalContext()
	require.True(t, ctx.GetOptionalPropSet().IsEmpty())

	ctx2 := ctx.Apply(WithOptionalProperty(
		contextopt.CurrentUserPropProvider(func() (u *auth.UserIdentity, r []*auth.RoleIdentity) { return }),
	))
	var emptySet context.OptionalEvalPropKeySet
	require.Equal(t, emptySet, ctx.GetOptionalPropSet())
	require.Equal(t, emptySet.Add(context.OptPropCurrentUser), ctx2.GetOptionalPropSet())

	// Apply should override all optional properties
	ctx3 := ctx2.Apply(WithOptionalProperty(
		contextopt.DDLOwnerInfoProvider(func() bool { return true }),
		contextopt.InfoSchemaPropProvider(func(isDomain bool) infoschema.MetaOnlyInfoSchema { return nil }),
	))
	require.Equal(t,
		emptySet.Add(context.OptPropDDLOwnerInfo).Add(context.OptPropInfoSchema),
		ctx3.GetOptionalPropSet(),
	)
	require.Equal(t, emptySet, ctx.GetOptionalPropSet())
	require.Equal(t, emptySet.Add(context.OptPropCurrentUser), ctx2.GetOptionalPropSet())
}

func TestUpdateStaticEvalContext(t *testing.T) {
	oldCtx := NewStaticEvalContext()
	ctx := oldCtx.Apply()

	// Should return a different instance
	require.NotSame(t, oldCtx, ctx)

	// CtxID should be different
	require.NotEqual(t, oldCtx.CtxID(), ctx.CtxID())

	// inner state should not be the same address
	oldCtxState, ctxState := &oldCtx.staticEvalCtxState, &ctx.staticEvalCtxState
	require.NotSame(t, oldCtxState, ctxState)

	// compare a state object by excluding some changed fields
	excludeChangedFields := func(s *staticEvalCtxState) staticEvalCtxState {
		state := *s
		state.typeCtx = types.DefaultStmtNoWarningContext
		state.errCtx = errctx.StrictNoWarningContext
		state.warnings = nil
		state.currentTime = nil
		return state
	}
	require.Equal(t, excludeChangedFields(oldCtxState), excludeChangedFields(ctxState))

	// check fields
	checkDefaultStaticEvalCtx(t, ctx)

	// apply options
	opts, optState := getEvalCtxOptionsForTest(t)
	ctx = oldCtx.Apply(opts...)
	checkOptionsStaticEvalCtx(t, ctx, optState)

	// old ctx aren't affected
	checkDefaultStaticEvalCtx(t, oldCtx)
}
