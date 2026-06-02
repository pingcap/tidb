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
	"context"
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

// stubManager is a minimal Manager for predicate tests. Only Role, Meta and
// GetBgTaskConfig are exercised; the rest panic if called so an accidental
// dependency surfaces immediately.
type stubManager struct {
	Manager
	role string
	bg   map[string]struct {
		count int
		auto  bool
		err   error
	}
}

func (s *stubManager) Role() string                   { return s.role }
func (s *stubManager) Meta() *keyspacepb.KeyspaceMeta { return nil }
func (s *stubManager) GetBgTaskConfig(_ context.Context, t string) (int, bool, error) {
	v, ok := s.bg[t]
	if !ok {
		return 0, false, nil
	}
	return v.count, v.auto, v.err
}

func TestRolePredicatesWhenDisabled(t *testing.T) {
	restore := SetManagerForTest(nil)
	t.Cleanup(restore)

	require.False(t, IsEnabled())
	require.False(t, IsMaster())
	require.False(t, IsGCWorker())
	require.False(t, IsTTLTaskWorker())
	require.False(t, IsAutoAnalyzeWorker())
	require.False(t, IsBgTaskEnabled(context.Background(), WorkerTypeDDL))
}

func TestRolePredicatesDedicated(t *testing.T) {
	cases := []struct {
		role string
		pred func() bool
	}{
		{config.RoleMaster, IsMaster},
		{config.RoleGCWorker, IsGCWorker},
		{config.RoleGCV2Worker, IsGCV2Worker},
		{config.RoleDDLWorker, IsDDLWorker},
		{config.RoleBatchWorker, IsBatchWorker},
		{config.RoleImportIntoWorker, IsImportIntoWorker},
		{config.RoleSharedWorker, IsSharedWorker},
		{config.RoleRemoteQueryWorker, IsRemoteQueryWorker},
		{config.RoleTTLTaskWorker, IsTTLTaskWorker},
		{config.RoleAutoAnalyzeWorker, IsAutoAnalyzeWorker},
	}
	for _, c := range cases {
		t.Run(c.role, func(t *testing.T) {
			restore := SetManagerForTest(&stubManager{role: c.role})
			t.Cleanup(restore)
			require.True(t, c.pred(), "%s predicate must be true for role %s", c.role, c.role)
		})
	}
}

// Shared worker is the special case: ttl / auto-analyze enablement is
// determined by the controller's bg-task config.
func TestSharedWorkerEnablement(t *testing.T) {
	mgr := &stubManager{
		role: config.RoleSharedWorker,
		bg: map[string]struct {
			count int
			auto  bool
			err   error
		}{
			WorkerTypeTTL:         {count: 1},
			WorkerTypeAutoAnalyze: {auto: true},
			WorkerTypeDDL:         {err: errors.New("rpc failed")},
		},
	}
	restore := SetManagerForTest(mgr)
	t.Cleanup(restore)

	require.True(t, IsTTLTaskWorker())
	require.True(t, IsAutoAnalyzeWorker())

	ctx := context.Background()
	require.True(t, IsBgTaskEnabled(ctx, WorkerTypeTTL))
	require.True(t, IsBgTaskEnabled(ctx, WorkerTypeAutoAnalyze))
	require.False(t, IsBgTaskEnabled(ctx, WorkerTypeDDL), "errors are treated as not-enabled")
	require.False(t, IsBgTaskEnabled(ctx, WorkerTypeBatch), "unknown types are not-enabled")
}
