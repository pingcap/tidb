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

//go:build nextgen

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/extworkload"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

type upgradeGCV2Manager struct {
	extworkload.Manager
	abortCount int
}

func (*upgradeGCV2Manager) Role() config.ExternalWorkloadRole {
	return config.RoleGCV2Worker
}

func (m *upgradeGCV2Manager) AbortGCV2(context.Context) error {
	m.abortCount++
	return nil
}

func TestUsePipelinedDMLDisabledInStarter(t *testing.T) {
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	s := &session{sessionVars: variable.NewSessionVars(nil)}
	s.sessionVars.BulkDMLEnabled = true
	s.sessionVars.StmtCtx.InInsertStmt = true

	require.False(t, s.usePipelinedDmlOrWarn(context.Background()))
	warnings := s.sessionVars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.EqualError(t, warnings[0].Err, "Pipelined DML is not supported in this deployment. Fallback to standard mode")
}

func TestUpgradeGCV2AbortUsesPostLockBootstrapVersion(t *testing.T) {
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	dom.Close()
	domap.Delete(store)

	mgr := &upgradeGCV2Manager{}
	extworkload.SetManagerForStore(store, mgr)
	runInBootstrapSession(store, currentBootstrapVersion-1)

	require.Zero(t, mgr.abortCount)
}
