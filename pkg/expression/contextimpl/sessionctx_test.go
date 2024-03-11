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

package contextimpl_test

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextimpl"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestEvalContextImplWithSessionCtx(t *testing.T) {
	ctx := mock.NewContext()
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	impl := contextimpl.NewExprExtendedImpl(ctx)

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

	require.Equal(t, 0, impl.WarningCount())
	impl.AppendWarning(errors.New("err1"))
	require.Equal(t, 1, impl.WarningCount())
	tc.AppendWarning(errors.New("err2"))
	require.Equal(t, 2, impl.WarningCount())
	ec.AppendWarning(errors.New("err3"))
	require.Equal(t, 3, impl.WarningCount())

	warnings := impl.TruncateWarnings(1)
	require.Equal(t, 2, len(warnings))
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[1].Level)
	require.Equal(t, "err2", warnings[0].Err.Error())
	require.Equal(t, "err3", warnings[1].Err.Error())

	warnings = impl.TruncateWarnings(0)
	require.Equal(t, 1, len(warnings))
	require.Equal(t, stmtctx.WarnLevelWarning, warnings[0].Level)
	require.Equal(t, "err1", warnings[0].Err.Error())
}

func getProvider[T context.OptionalEvalPropProvider](
	t *testing.T,
	impl *contextimpl.ExprCtxExtendedImpl,
	key context.OptionalEvalPropKey,
) T {
	val, ok := impl.GetOptionalPropProvider(key)
	require.True(t, ok)
	p, ok := val.(T)
	require.True(t, ok)
	return p
}

func TestEvalContextImplWithSessionCtxForOptProps(t *testing.T) {
	ctx := mock.NewContext()
	impl := contextimpl.NewExprExtendedImpl(ctx)

	// test for OptPropCurrentUser
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "user1", Hostname: "host1"}
	ctx.GetSessionVars().ActiveRoles = []*auth.RoleIdentity{
		{Username: "role1", Hostname: "host1"},
		{Username: "role2", Hostname: "host2"},
	}
	user, roles := getProvider[contextopt.CurrentUserPropProvider](t, impl, context.OptPropCurrentUser)()
	require.Equal(t, ctx.GetSessionVars().User, user)
	require.Equal(t, ctx.GetSessionVars().ActiveRoles, roles)
}
