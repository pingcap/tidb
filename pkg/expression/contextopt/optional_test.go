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

package contextopt

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

type mockSessionVarsProvider struct {
	vars *variable.SessionVars
}

func (p mockSessionVarsProvider) GetSessionVars() *variable.SessionVars {
	return p.vars
}

func TestOptionalEvalPropProviders(t *testing.T) {
	var providers OptionalEvalPropProviders
	require.True(t, providers.PropKeySet().IsEmpty())

	var p context.OptionalEvalPropProvider
	var verify func(val context.OptionalEvalPropProvider)

	for i := 0; i < context.OptPropsCnt; i++ {
		key := context.OptionalEvalPropKey(i)
		switch key {
		case context.OptPropCurrentUser:
			user := &auth.UserIdentity{Username: "u1", Hostname: "h1"}
			roles := []*auth.RoleIdentity{{Username: "u2", Hostname: "h2"}, {Username: "u3", Hostname: "h3"}}
			p = CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) { return user, roles })
			verify = func(val context.OptionalEvalPropProvider) {
				user2, roles2 := val.(CurrentUserPropProvider)()
				require.Equal(t, user, user2)
				require.Equal(t, roles, roles2)
			}
		case context.OptPropSessionVars:
			vars := variable.NewSessionVars(nil)
			p = NewSessionVarsProvider(mockSessionVarsProvider{vars: vars})
			verify = func(val context.OptionalEvalPropProvider) {
				got := val.(*SessionVarsPropProvider).GetSessionVars()
				require.Same(t, vars, got)
			}
		case context.OptPropAdvisoryLock:
			type mockCtxType struct {
				AdvisoryLockContext
			}
			mockCtx := &mockCtxType{}
			p = NewAdvisoryLockPropProvider(mockCtx)
			verify = func(val context.OptionalEvalPropProvider) {
				got := val.(*AdvisoryLockPropProvider).AdvisoryLockContext
				require.Same(t, mockCtx, got)
			}
		case context.OptPropDDLOwnerInfo:
			isOwner := true
			p = DDLOwnerInfoProvider(func() bool { return isOwner })
			verify = func(val context.OptionalEvalPropProvider) {
				require.True(t, val.(DDLOwnerInfoProvider)())
				isOwner = false
				require.False(t, val.(DDLOwnerInfoProvider)())
			}
		default:
			require.Fail(t, "unexpected optional property key")
		}

		require.False(t, providers.PropKeySet().Contains(key))
		require.False(t, providers.Contains(key))
		providers.Add(p)
		require.True(t, providers.PropKeySet().Contains(key))
		require.True(t, providers.Contains(key))

		val, ok := providers.Get(key)
		require.True(t, ok)
		verify(val)
	}
}
