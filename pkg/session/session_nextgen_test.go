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
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extworkload"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

type upgradeGCV2Manager struct {
	extworkload.Manager
	abortCount int
}

type bootstrapExternalWorkloadManager struct {
	role config.ExternalWorkloadRole
}

func (*upgradeGCV2Manager) Role() config.ExternalWorkloadRole {
	return config.RoleGCV2Worker
}

func (m *upgradeGCV2Manager) AbortGCV2(context.Context) error {
	m.abortCount++
	return nil
}

func (*bootstrapExternalWorkloadManager) Close() error { return nil }

func (m *bootstrapExternalWorkloadManager) Role() config.ExternalWorkloadRole {
	return m.role
}

func (*bootstrapExternalWorkloadManager) Meta() *keyspacepb.KeyspaceMeta { return nil }

func (*bootstrapExternalWorkloadManager) InitializeGCV2(context.Context, time.Duration) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) AbortGCV2(context.Context) error { return nil }

func (*bootstrapExternalWorkloadManager) RegisterGCV2(context.Context, uint64, time.Duration) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) RecycleGCV2(context.Context, uint64) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) UpdateGCLifeTime(context.Context, time.Duration) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) RegisterTTLTableInfo(context.Context, int64, bool) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) DeleteTTLTableInfo(context.Context, int64) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) RecycleTTLTask(context.Context, uint64) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) UpdateTTLJobEnable(context.Context, bool) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) RegisterAutoAnalyze(context.Context, uint64) error {
	return nil
}

func (*bootstrapExternalWorkloadManager) RecycleAutoAnalyze(context.Context, uint64) error {
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
	runInBootstrapSession(store, currentBootstrapVersion-1, domainCreateOptions{})

	require.Zero(t, mgr.abortCount)
}

func TestCreateSessionWithDomainOptionsAttachesExternalWorkloadManager(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	dom.Close()
	domap.Delete(store)

	mgr := &bootstrapExternalWorkloadManager{role: config.RoleMaster}
	se, err := createSessionWithDomainOptions(store, domainCreateOptions{extWorkloadMgr: mgr})
	require.NoError(t, err)
	newDom := domain.GetDomain(se)
	require.Same(t, mgr, newDom.ExternalWorkloadManager())

	se.Close()
	newDom.Close()
}

func TestBootstrapSessionWithExternalWorkloadManagerAttachesBootstrapDomain(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	mgr := &bootstrapExternalWorkloadManager{role: config.RoleMaster}
	sawBootstrapDomain := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/session/checkBootstrapExternalWorkloadManager", func(bootstrapDom *domain.Domain) {
		require.Equal(t, extworkload.Manager(mgr), bootstrapDom.ExternalWorkloadManager())
		sawBootstrapDomain = true
	})

	newDom, err := BootstrapSessionWithExternalWorkloadManager(store, mgr)
	require.NoError(t, err)
	require.True(t, sawBootstrapDomain)
	require.Equal(t, extworkload.Manager(mgr), newDom.ExternalWorkloadManager())

	newDom.Close()
	domap.Delete(store)
}
