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
	"github.com/stretchr/testify/require"
)

type stubManager struct {
	Manager
	role config.ExternalWorkloadRole
}

func (s *stubManager) Role() config.ExternalWorkloadRole { return s.role }
func (s *stubManager) Meta() *keyspacepb.KeyspaceMeta    { return nil }

func TestRolePredicatesWhenDisabled(t *testing.T) {
	restore := SetManagerForTest(nil)
	t.Cleanup(restore)

	require.False(t, IsEnabled())
	require.False(t, IsMaster())
	require.False(t, IsGCV2Worker())
	require.False(t, IsTTLTaskWorker())
	require.False(t, IsAutoAnalyzeWorker())
}

func TestRolePredicatesDedicated(t *testing.T) {
	cases := []struct {
		role config.ExternalWorkloadRole
		pred func() bool
	}{
		{config.RoleMaster, IsMaster},
		{config.RoleGCV2Worker, IsGCV2Worker},
		{config.RoleTTLTaskWorker, IsTTLTaskWorker},
		{config.RoleAutoAnalyzeWorker, IsAutoAnalyzeWorker},
	}
	for _, c := range cases {
		t.Run(string(c.role), func(t *testing.T) {
			restore := SetManagerForTest(&stubManager{role: c.role})
			t.Cleanup(restore)
			for _, other := range cases {
				require.Equal(t, other.role == c.role, other.pred(),
					"%s predicate result for role %s", other.role, c.role)
			}
		})
	}
}
