// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/sessionctx"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestManagerInClassical(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("only test in classic kernel")
	}

	_, dom := testkit.CreateMockStoreAndDomain(t)
	_, err := dom.GetKSStore("aaa")
	require.ErrorContains(t, err, "cross keyspace session manager is not available in classic kernel or current keyspace")
}

func TestManager(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace is not supported in classic kernel")
	}
	integration.BeforeTestExternal(t)
	clientCount := 20
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: clientCount})
	defer cluster.Terminate(t)
	keyspaceIDs := map[string]uint32{
		keyspace.System: 1,
		"ks1":           2,
		"ks2":           3,
		"ks3":           4,
		"ksmdl":         5,
	}
	getETCDCli := func(ks string, ksID uint32) *clientv3.Client {
		for i := range clientCount {
			cli := cluster.Client(i)
			if cli == nil {
				continue
			}
			// we will close the client.
			cluster.TakeClient(i)
			codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: ksID, Name: ks})
			require.NoError(t, err)
			etcd.SetEtcdCliByNamespace(cli, keyspace.MakeKeyspaceEtcdNamespace(codec))
			return cli
		}
		require.Fail(t, "cannot find etcd client for keyspace %s", ks)
		return nil
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cliP **clientv3.Client, ks string) {
			id, ok := keyspaceIDs[ks]
			require.True(t, ok)
			*cliP = getETCDCli(ks, id)
		},
	)

	require.NoError(t, kvstore.Register(config.StoreTypeUniStore, mockstore.EmbedUnistoreDriver{}))
	sysKSStore, sysKSDom := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
	sysKSTK := testkit.NewTestKit(t, sysKSStore)
	sysKSTK.MustExec("use test")
	sysKSTK.MustExec("create table t(id int)")

	t.Run("same keyspace access", func(t *testing.T) {
		_, err := sysKSDom.GetKSSessPool(keyspace.System)
		require.ErrorContains(t, err, "cross keyspace session manager is not available in classic kernel or current keyspace")
	})

	t.Run("failed to get store in cross keyspace manager", func(t *testing.T) {
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
			func(fnP *func(string) (store kv.Storage, err error)) {
				*fnP = func(string) (store kv.Storage, err error) {
					return nil, errors.New("failed to get store")
				}
			},
		)
		_, err := sysKSDom.GetKSSessPool("ks1")
		require.ErrorContains(t, err, "failed to get store")
	})

	t.Run("cross keyspace session works, and only allowed to read/write system tables", func(t *testing.T) {
		// in uni-store, Store instances are completely isolated, even they have the
		// same keyspace name, so we store them here and mock the GetStore
		// TODO use a shared storage for all Store instances.
		storeMap := make(map[string]kv.Storage, 4)
		storeMap[keyspace.System] = sysKSStore
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
			func(fnP *func(string) (store kv.Storage, err error)) {
				*fnP = func(ks string) (store kv.Storage, err error) {
					return storeMap[ks], nil
				}
			},
		)
		// testkit.CreateMockStoreAndDomainForKS will close the store, we need to
		// avoid close twice.
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
			func(shouldCloseStore *bool) {
				*shouldCloseStore = false
			},
		)

		for _, ks := range []string{"ks1", "ks2", "ks3"} {
			userKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, ks)
			storeMap[ks] = userKSStore

			userTK := testkit.NewTestKit(t, userKSStore)
			userTK.MustExec("use test")
			userTK.MustExec("create table t(id int)")

			// must switch back to SYSTEM keyspace before creating a cross keyspace session
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = keyspace.System
			})
			// insert through a user keyspace session came from SYSTEM keyspace
			pool, err := sysKSDom.GetKSSessPool(ks)
			require.NoError(t, err)
			t.Cleanup(func() {
				// we have to close the cross keyspace session manager, as the
				// store will be closed by testkit.CreateMockStoreAndDomainForKS
				sysKSDom.CloseKSSessMgr(ks)
			})
			mgr := storage.NewTaskManager(pool)
			ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
			require.NoError(t, mgr.WithNewSession(func(se sessionctx.Context) error {
				_, err := importer.CreateJob(ctx, se.GetSQLExecutor(), "db", "tbl", 1, "", "", &importer.ImportParameters{}, 1)
				return err
			}))
			// verify through the user keyspace session from user keyspace
			userTK.MustQuery("select count(1) from mysql.tidb_import_jobs").Check(testkit.Rows("1"))

			// we cannot access user tables in cross keyspace session
			require.ErrorIs(t, mgr.WithNewSession(func(se sessionctx.Context) error {
				_, err2 := se.GetSQLExecutor().ExecuteInternal(ctx, "select * from test.t")
				return err2
			}), infoschema.ErrTableNotExists)

			// we cannot execute DDL in cross keyspace session
			require.ErrorContains(t, mgr.WithNewSession(func(se sessionctx.Context) error {
				_, err2 := se.GetSQLExecutor().ExecuteInternal(ctx, "create table test.t2(id int)")
				return err2
			}), "DDL is not supported in cross keyspace session")
		}
		// SYSTEM keyspace should not have any import jobs
		sysTK := testkit.NewTestKit(t, sysKSStore)
		sysTK.MustQuery("select count(1) from mysql.tidb_import_jobs").Check(testkit.Rows("0"))
	})

	t.Run("check cross keyspace session are recorded in coordinator and contains related tables", func(t *testing.T) {
		storeMap := make(map[string]kv.Storage, 2)
		storeMap[keyspace.System] = sysKSStore
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
			func(fnP *func(string) (store kv.Storage, err error)) {
				*fnP = func(ks string) (store kv.Storage, err error) {
					return storeMap[ks], nil
				}
			},
		)
		// testkit.CreateMockStoreAndDomainForKS will close the store, we need to
		// avoid close twice.
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
			func(shouldCloseStore *bool) {
				*shouldCloseStore = false
			},
		)
		userKS := "ksmdl"
		userKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, userKS)
		storeMap[userKS] = userKSStore

		// must switch back to SYSTEM keyspace before creating a cross keyspace session
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})

		pool, err := sysKSDom.GetKSSessPool(userKS)
		require.NoError(t, err)
		t.Cleanup(func() {
			// we have to close the cross keyspace session manager, as the
			// store will be closed by testkit.CreateMockStoreAndDomainForKS
			sysKSDom.CloseKSSessMgr(userKS)
		})

		crossKSMgr := sysKSDom.GetCrossKSMgr()
		sessMgr, ok := crossKSMgr.Get(userKS)
		require.True(t, ok)
		coordinator := sessMgr.Coordinator()
		require.Zero(t, coordinator.InternalSessionCount())

		mgr := storage.NewTaskManager(pool)
		ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
		require.NoError(t, mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
			exec := se.GetSQLExecutor()
			_, err2 := sqlexec.ExecSQL(ctx, exec, "select count(1) from mysql.tidb_import_jobs")
			require.NoError(t, err2)
			var tableIDCount int
			se.GetSessionVars().GetRelatedTableForMDL().Range(func(key, value any) bool {
				require.Equal(t, metadef.TiDBImportJobsTableID, key.(int64))
				tableIDCount++
				return true
			})
			require.EqualValues(t, 1, tableIDCount)
			require.True(t, coordinator.ContainsInternalSession(se))
			require.EqualValues(t, 1, coordinator.InternalSessionCount())
			return nil
		}))
		require.Zero(t, coordinator.InternalSessionCount())
	})
}
