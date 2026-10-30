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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestBootstrapSchemasDeploymentCheck(t *testing.T) {
	originalMode := deploymode.Get()
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	for _, mode := range deploymode.ModeList() {
		t.Run(mode.String(), func(t *testing.T) {
			require.NoError(t, deploymode.Set(mode))
			for _, tt := range []struct {
				name       string
				dbID       int64
				legacyDBID int64
				version    int64
				errMsg     string
			}{
				{name: "fresh"},
				{name: "interrupted nextgen bootstrap", dbID: metadef.SystemDatabaseID},
				{name: "bootstrapped nextgen", dbID: metadef.SystemDatabaseID, version: currentBootstrapVersion},
				{name: "interrupted classic bootstrap", dbID: 1, errMsg: "interrupted Classic bootstrap"},
				{name: "interrupted classic bootstrap at scan boundary", dbID: 10, errMsg: "interrupted Classic bootstrap"},
				{name: "bootstrapped classic", dbID: 1, version: currentBootstrapVersion},
				{name: "positive bootstrap version without mysql", version: currentBootstrapVersion},
				{name: "mixed schemas before bootstrap", dbID: metadef.SystemDatabaseID, legacyDBID: 1, errMsg: "both reserved-ID and legacy-ID mysql databases"},
				{name: "mixed schemas after bootstrap at scan boundary", dbID: metadef.SystemDatabaseID, legacyDBID: 10, version: currentBootstrapVersion, errMsg: "both reserved-ID and legacy-ID mysql databases"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					store, err := mockstore.NewMockStore()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, store.Close()) })
					ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
					require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
						m := meta.NewMutator(txn)
						// An unrelated early database must not be mistaken for mysql.
						if err := m.CreateSysDatabaseByID("other", 2); err != nil {
							return err
						}
						if tt.dbID != 0 {
							if err := m.CreateSysDatabaseByID(mysql.SystemDB, tt.dbID); err != nil {
								return err
							}
						}
						if tt.legacyDBID != 0 {
							if err := m.CreateSysDatabaseByID(mysql.SystemDB, tt.legacyDBID); err != nil {
								return err
							}
						}
						return m.FinishBootstrap(tt.version)
					}))

					errMsg := tt.errMsg
					if tt.dbID != metadef.SystemDatabaseID && tt.version > 0 && mode != deploymode.Starter {
						errMsg = `the cluster is already bootstrapped with a non-NextGen binary; please start with starter deployment mode (deploy-mode="starter")`
					}
					err = bootstrapSchemas(store)
					if errMsg == "" {
						require.NoError(t, err)
						if tt.dbID == metadef.SystemDatabaseID || tt.version == 0 {
							return
						}
					} else {
						require.ErrorContains(t, err, errMsg)
					}
					// Rejected startup and skipped Classic bootstrap must not change
					// reserved schemas or advance versions.
					require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
						m := meta.NewMutator(txn)
						db, err := m.GetDatabase(metadef.SystemDatabaseID)
						require.NoError(t, err)
						if tt.dbID == metadef.SystemDatabaseID {
							require.NotNil(t, db)
							require.Equal(t, mysql.SystemDB, db.Name.L)
							tables, err := m.ListTables(ctx, db.ID)
							require.NoError(t, err)
							require.Empty(t, tables)
						} else {
							require.Nil(t, db)
						}
						sysDB, err := m.GetDatabase(metadef.SysDatabaseID)
						require.NoError(t, err)
						require.Nil(t, sysDB)
						version, err := m.GetBootstrapVersion()
						require.NoError(t, err)
						require.Equal(t, tt.version, version)
						tableVersion, err := m.GetNextGenBootTableVersion()
						require.NoError(t, err)
						require.Zero(t, tableVersion)
						return nil
					}))
				})
			}
		})
	}
}

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
