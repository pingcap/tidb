// Copyright 2026 PingCAP, Inc.
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

package domain_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestStandbyInitialPrivilegeLoadFailureDoesNotFailBootstrap(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/mockStandbyModeForPrivilegeLoad", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/mockStandbyModeForPrivilegeLoad"))
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/mockLoadPrivilegeFailed", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/mockLoadPrivilegeFailed"))
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	require.NotNil(t, dom.PrivilegeHandle())
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1"}, nil, nil, nil))
}

func TestStandbyInitialSysVarCacheLoadFailureDoesNotFailBootstrap(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/mockStandbyModeForSysVarCacheLoad", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/mockStandbyModeForSysVarCacheLoad"))
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/mockLoadSysVarCacheFailed", "return(true)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/mockLoadSysVarCacheFailed"))
	})

	_, dom := testkit.CreateMockStoreAndDomain(t)
	require.NotNil(t, dom)
}
