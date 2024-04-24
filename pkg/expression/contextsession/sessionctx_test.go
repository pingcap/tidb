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

package contextsession_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/expression/contextsession"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSessionEvalContextBasic(t *testing.T) {
	ctx := mock.NewContext()
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	impl := contextsession.NewSessionEvalContext(ctx)
	require.True(t, impl.GetOptionalPropSet().IsFull())

	// should contain all the optional properties
	for i := 0; i < context.OptPropsCnt; i++ {
		provider, ok := impl.GetOptionalPropProvider(context.OptionalEvalPropKey(i))
		require.True(t, ok)
		require.NotNil(t, provider)
		require.Same(t, context.OptionalEvalPropKey(i).Desc(), provider.Desc())
	}

	ctx.ResetSessionAndStmtTimeZone(time.FixedZone("UTC+11", 11*3600))
	vars.SQLMode = mysql.ModeStrictTransTables | mysql.ModeNoZeroDate
	sc.SetTypeFlags(types.FlagIgnoreInvalidDateErr | types.FlagSkipUTF8Check)
	sc.SetErrLevels(errctx.LevelMap{errctx.ErrGroupDupKey: errctx.LevelWarn, errctx.ErrGroupBadNull: errctx.LevelIgnore})
	vars.CurrentDB = "db1"
	vars.MaxAllowedPacket = 123456

	// basic fields
	tc, ec := impl.TypeCtx(), sc.ErrCtx()
	require.Equal(t, tc, sc.TypeCtx())
	require.Equal(t, ec, impl.ErrCtx())
	require.Equal(t, vars.SQLMode, impl.SQLMode())
	require.Same(t, vars.Location(), impl.Location())
	require.Same(t, sc.TimeZone(), impl.Location())
	require.Same(t, tc.Location(), impl.Location())
	require.Equal(t, "db1", impl.CurrentDB())
	require.Equal(t, uint64(123456), impl.GetMaxAllowedPacket())
	require.Equal(t, "0", impl.GetDefaultWeekFormatMode())
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("default_week_format", "5"))
	require.Equal(t, "5", impl.GetDefaultWeekFormatMode())

	// handle warnings
	require.Equal(t, 0, impl.WarningCount())
	impl.AppendWarning(errors.New("err1"))
	require.Equal(t, 1, impl.WarningCount())
	tc.AppendWarning(errors.New("err2"))
	require.Equal(t, 2, impl.WarningCount())
	ec.AppendWarning(errors.New("err3"))
	require.Equal(t, 3, impl.WarningCount())

	for _, dst := range [][]contextutil.SQLWarn{
		nil,
		make([]contextutil.SQLWarn, 1),
		make([]contextutil.SQLWarn, 3),
		make([]contextutil.SQLWarn, 0, 3),
	} {
		warnings := impl.CopyWarnings(dst)
		require.Equal(t, 3, len(warnings))
		require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
		require.Equal(t, contextutil.WarnLevelWarning, warnings[1].Level)
		require.Equal(t, contextutil.WarnLevelWarning, warnings[2].Level)
		require.Equal(t, "err1", warnings[0].Err.Error())
		require.Equal(t, "err2", warnings[1].Err.Error())
		require.Equal(t, "err3", warnings[2].Err.Error())
	}

	warnings := impl.TruncateWarnings(1)
	require.Equal(t, 2, len(warnings))
	require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, contextutil.WarnLevelWarning, warnings[1].Level)
	require.Equal(t, "err2", warnings[0].Err.Error())
	require.Equal(t, "err3", warnings[1].Err.Error())

	warnings = impl.TruncateWarnings(0)
	require.Equal(t, 1, len(warnings))
	require.Equal(t, contextutil.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, "err1", warnings[0].Err.Error())
}

func TestSessionEvalContextCurrentTime(t *testing.T) {
	ctx := mock.NewContext()
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	impl := contextsession.NewSessionEvalContext(ctx)

	var now atomic.Pointer[time.Time]
	sc.SetStaleTSOProvider(func() (uint64, error) {
		v := time.UnixMilli(123456789)
		// should only be called once
		require.True(t, now.CompareAndSwap(nil, &v))
		return oracle.GoTimeToTS(v), nil
	})

	// now should return the stable TSO if set
	tm, err := impl.CurrentTime()
	require.NoError(t, err)
	v := now.Load()
	require.NotNil(t, v)
	require.Equal(t, v.UnixNano(), tm.UnixNano())

	// The second call should return the same value
	tm, err = impl.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, v.UnixNano(), tm.UnixNano())

	// now should return the system variable if "timestamp" is set
	sc.SetStaleTSOProvider(nil)
	sc.Reset()
	require.NoError(t, vars.SetSystemVar("timestamp", "7654321.875"))
	tm, err = impl.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, int64(7654321_875_000_000), tm.UnixNano())

	// The second call should return the same value
	tm, err = impl.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, int64(7654321_875_000_000), tm.UnixNano())

	// now should return the system current time if not stale TSO or "timestamp" is set
	require.NoError(t, vars.SetSystemVar("timestamp", "0"))
	sc.Reset()
	tm, err = impl.CurrentTime()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Unix(), tm.Unix(), 5)

	// The second call should return the same value
	tm2, err := impl.CurrentTime()
	require.NoError(t, err)
	require.Equal(t, tm.UnixNano(), tm2.UnixNano())
}

type mockPrivManager struct {
	tmock.Mock
	privilege.Manager
}

func (m *mockPrivManager) RequestVerification(
	activeRole []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType,
) bool {
	return m.Called(activeRole, db, table, column, priv).Bool(0)
}

func (m *mockPrivManager) RequestDynamicVerification(
	activeRoles []*auth.RoleIdentity, privName string, grantable bool,
) bool {
	return m.Called(activeRoles, privName, grantable).Bool(0)
}

func TestSessionEvalContextPrivilegeCheck(t *testing.T) {
	ctx := mock.NewContext()
	impl := contextsession.NewSessionEvalContext(ctx)
	activeRoles := []*auth.RoleIdentity{
		{Username: "role1", Hostname: "host1"},
		{Username: "role2", Hostname: "host2"},
	}
	ctx.GetSessionVars().ActiveRoles = activeRoles

	// no privilege manager should always return true for privilege check
	privilege.BindPrivilegeManager(ctx, nil)
	require.True(t, impl.RequestVerification("test", "tbl1", "col1", mysql.SuperPriv))
	require.True(t, impl.RequestDynamicVerification("RESTRICTED_TABLES_ADMIN", true))
	require.True(t, impl.RequestDynamicVerification("RESTRICTED_TABLES_ADMIN", false))

	// if privilege manager bound, it should return the privilege manager value
	mgr := &mockPrivManager{}
	privilege.BindPrivilegeManager(ctx, mgr)
	mgr.On("RequestVerification", activeRoles, "db1", "t1", "c1", mysql.CreatePriv).
		Return(true).Once()
	require.True(t, impl.RequestVerification("db1", "t1", "c1", mysql.CreatePriv))
	mgr.AssertExpectations(t)

	mgr.On("RequestVerification", activeRoles, "db2", "t2", "c2", mysql.SuperPriv).
		Return(false).Once()
	require.False(t, impl.RequestVerification("db2", "t2", "c2", mysql.SuperPriv))
	mgr.AssertExpectations(t)

	mgr.On("RequestDynamicVerification", activeRoles, "RESTRICTED_USER_ADMIN", false).
		Return(true).Once()
	require.True(t, impl.RequestDynamicVerification("RESTRICTED_USER_ADMIN", false))

	mgr.On("RequestDynamicVerification", activeRoles, "RESTRICTED_CONNECTION_ADMIN", true).
		Return(false).Once()
	require.False(t, impl.RequestDynamicVerification("RESTRICTED_CONNECTION_ADMIN", true))
}

func getProvider[T context.OptionalEvalPropProvider](
	t *testing.T,
	impl *contextsession.SessionEvalContext,
	key context.OptionalEvalPropKey,
) T {
	val, ok := impl.GetOptionalPropProvider(key)
	require.True(t, ok)
	p, ok := val.(T)
	require.True(t, ok)
	require.Equal(t, key, p.Desc().Key())
	return p
}

func TestSessionEvalContextOptProps(t *testing.T) {
	ctx := mock.NewContext()
	impl := contextsession.NewSessionEvalContext(ctx)

	// test for OptPropCurrentUser
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "user1", Hostname: "host1"}
	ctx.GetSessionVars().ActiveRoles = []*auth.RoleIdentity{
		{Username: "role1", Hostname: "host1"},
		{Username: "role2", Hostname: "host2"},
	}
	user, roles := getProvider[contextopt.CurrentUserPropProvider](t, impl, context.OptPropCurrentUser)()
	require.Equal(t, ctx.GetSessionVars().User, user)
	require.Equal(t, ctx.GetSessionVars().ActiveRoles, roles)

	// test for OptPropSessionVars
	sessVarsProvider := getProvider[*contextopt.SessionVarsPropProvider](t, impl, context.OptPropSessionVars)
	require.NotNil(t, sessVarsProvider)
	gotVars, err := contextopt.SessionVarsPropReader{}.GetSessionVars(impl)
	require.NoError(t, err)
	require.Same(t, ctx.GetSessionVars(), gotVars)

	// test for OptPropAdvisoryLock
	lockProvider := getProvider[*contextopt.AdvisoryLockPropProvider](t, impl, context.OptPropAdvisoryLock)
	gotCtx, ok := lockProvider.AdvisoryLockContext.(*mock.Context)
	require.True(t, ok)
	require.Same(t, ctx, gotCtx)

	// test for OptPropDDLOwnerInfo
	ddlInfoProvider := getProvider[contextopt.DDLOwnerInfoProvider](t, impl, context.OptPropDDLOwnerInfo)
	require.False(t, ddlInfoProvider())
	ctx.SetIsDDLOwner(true)
	require.True(t, ddlInfoProvider())
}

func TestSessionBuildContext(t *testing.T) {
	ctx := mock.NewContext()
	impl := contextsession.NewSessionExprContext(ctx)
	evalCtx, ok := impl.GetEvalCtx().(*contextsession.SessionEvalContext)
	require.True(t, ok)
	require.Same(t, evalCtx, impl.SessionEvalContext)
	require.True(t, evalCtx.GetOptionalPropSet().IsFull())
	require.Same(t, ctx, evalCtx.Sctx())

	// charset and collation
	vars := ctx.GetSessionVars()
	err := vars.SetSystemVar("character_set_connection", "gbk")
	require.NoError(t, err)
	err = vars.SetSystemVar("collation_connection", "gbk_chinese_ci")
	require.NoError(t, err)
	vars.DefaultCollationForUTF8MB4 = "utf8mb4_0900_ai_ci"

	charset, collate := impl.GetCharsetInfo()
	require.Equal(t, "gbk", charset)
	require.Equal(t, "gbk_chinese_ci", collate)
	require.Equal(t, "utf8mb4_0900_ai_ci", impl.GetDefaultCollationForUTF8MB4())

	// SysdateIsNow
	vars.SysdateIsNow = true
	require.True(t, impl.GetSysdateIsNow())

	// NoopFuncsMode
	vars.NoopFuncsMode = 2
	require.Equal(t, 2, impl.GetNoopFuncsMode())

	// Rng
	vars.Rng = mathutil.NewWithSeed(123)
	require.Same(t, vars.Rng, impl.Rng())

	// PlanCache
	vars.StmtCtx.EnablePlanCache()
	require.True(t, impl.IsUseCache())
	impl.SetSkipPlanCache("mockReason")
	require.False(t, impl.IsUseCache())

	// Alloc column id
	prevID := vars.PlanColumnID.Load()
	colID := impl.AllocPlanColumnID()
	require.Equal(t, colID, prevID+1)
	colID = impl.AllocPlanColumnID()
	require.Equal(t, colID, prevID+2)
	vars.AllocPlanColumnID()
	colID = impl.AllocPlanColumnID()
	require.Equal(t, colID, prevID+4)

	// InNullRejectCheck
	require.False(t, impl.IsInNullRejectCheck())

	// ConnID
	vars.ConnectionID = 123
	require.Equal(t, uint64(123), impl.ConnectionID())
}
