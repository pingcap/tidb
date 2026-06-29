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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/extworkload"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

type gcv2AbortManager struct {
	extworkload.Manager
	role        config.ExternalWorkloadRole
	abortCalled bool
}

func (m *gcv2AbortManager) Close() error { return nil }
func (m *gcv2AbortManager) Role() config.ExternalWorkloadRole {
	return m.role
}
func (m *gcv2AbortManager) Meta() *keyspacepb.KeyspaceMeta { return nil }
func (m *gcv2AbortManager) AbortGCV2(context.Context) error {
	m.abortCalled = true
	return nil
}

func TestAbortGCV2(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Starter deploy mode is only available in nextgen kernel")
	}

	originDeployMode := deploymode.Get()
	originInTest := intest.InTest
	t.Cleanup(func() {
		extworkload.SetGlobalManager(nil)
		intest.InTest = originInTest
		require.NoError(t, deploymode.Set(originDeployMode))
	})
	require.NoError(t, deploymode.Set(deploymode.Starter))
	intest.InTest = true

	mgr := &gcv2AbortManager{role: config.RoleGCV2Worker}
	extworkload.SetGlobalManager(mgr)
	abortGCV2()
	require.True(t, mgr.abortCalled)

	mgr.abortCalled = false
	mgr.role = config.RoleMaster
	abortGCV2()
	require.False(t, mgr.abortCalled)
}
