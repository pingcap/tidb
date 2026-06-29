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

package extworkload

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type stubManager struct {
	Manager
	role config.ExternalWorkloadRole
	meta *keyspacepb.KeyspaceMeta
}

func (s *stubManager) Role() config.ExternalWorkloadRole { return s.role }
func (s *stubManager) Meta() *keyspacepb.KeyspaceMeta    { return s.meta }

type stubManagerStore struct {
	mgr Manager
}

func (s *stubManagerStore) ExternalWorkloadManager() Manager {
	return s.mgr
}

func TestRolePredicatesWhenDisabled(t *testing.T) {
	require.False(t, IsEnabled(nil))
	require.False(t, IsMaster(nil))
	require.False(t, IsGCV2Worker(nil))
	require.False(t, IsTTLTaskWorker(nil))
	require.False(t, IsAutoAnalyzeWorker(nil))
}

func TestRolePredicatesDedicated(t *testing.T) {
	cases := []struct {
		role config.ExternalWorkloadRole
		pred func(Manager) bool
	}{
		{config.RoleMaster, IsMaster},
		{config.RoleGCV2Worker, IsGCV2Worker},
		{config.RoleTTLTaskWorker, IsTTLTaskWorker},
		{config.RoleAutoAnalyzeWorker, IsAutoAnalyzeWorker},
	}
	for _, c := range cases {
		t.Run(string(c.role), func(t *testing.T) {
			mgr := &stubManager{role: c.role}
			for _, other := range cases {
				require.Equal(t, other.role == c.role, other.pred(mgr),
					"%s predicate result for role %s", other.role, c.role)
			}
		})
	}
}

func TestUseKeyspaceLevelGC(t *testing.T) {
	mgr := &stubManager{
		role: config.RoleMaster,
	}
	require.False(t, UseKeyspaceLevelGC(mgr))

	mgr.meta = &keyspacepb.KeyspaceMeta{
		Id:     1,
		Name:   "ks",
		Config: map[string]string{pd.KeyspaceConfigGCManagementType: pd.KeyspaceConfigGCManagementTypeKeyspaceLevel},
	}
	require.True(t, UseKeyspaceLevelGC(mgr))
}

func TestManagerFromStoreStarterGate(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("Starter deploy mode is only available in nextgen kernel")
	}

	origin := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(origin))
	})

	mgr := &stubManager{role: config.RoleMaster}
	store := &stubManagerStore{mgr: mgr}

	require.NoError(t, deploymode.Set(deploymode.Premium))
	require.Nil(t, GetManagerFromStore(store))

	require.NoError(t, deploymode.Set(deploymode.Starter))
	require.Same(t, mgr, GetManagerFromStore(store))
	store.mgr = nil
	require.Nil(t, GetManagerFromStore(store))
}
