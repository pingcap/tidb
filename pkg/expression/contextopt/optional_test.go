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
	"github.com/stretchr/testify/require"
)

func TestOptionalEvalPropProviders(t *testing.T) {
	var providers OptionalEvalPropProviders
	require.True(t, providers.PropKeySet().IsEmpty())
	require.False(t, providers.Contains(context.OptPropCurrentUser))
	val, ok := providers.Get(context.OptPropCurrentUser)
	require.False(t, ok)
	require.Nil(t, val)

	var p context.OptionalEvalPropProvider

	user := &auth.UserIdentity{Username: "u1", Hostname: "h1"}
	roles := []*auth.RoleIdentity{{Username: "u2", Hostname: "h2"}, {Username: "u3", Hostname: "h3"}}
	p = CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) { return user, roles })
	providers.Add(p)
	require.True(t, providers.PropKeySet().Contains(context.OptPropCurrentUser))
	require.True(t, providers.Contains(context.OptPropCurrentUser))
	val, ok = providers.Get(context.OptPropCurrentUser)
	require.True(t, ok)
	user2, roles2 := val.(CurrentUserPropProvider)()
	require.Equal(t, user, user2)
	require.Equal(t, roles, roles2)
}
