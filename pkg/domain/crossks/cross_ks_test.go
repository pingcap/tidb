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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
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
	require.NoError(t, kvstore.Register(config.StoreTypeUniStore, mockstore.EmbedUnistoreDriver{}))
	sysKSStore, sysKSDom := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)

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

	t.Run("cross keyspace session pool works", func(t *testing.T) {
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

		for _, ks := range []string{"ks1", "ks2", "ks3"} {
			userKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, ks)
			storeMap[ks] = userKSStore

			// must switch back to SYSTEM keyspace before creating a cross keyspace session
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = keyspace.System
			})
			// insert through a user keyspace session came from SYSTEM keyspace
			pool, err := sysKSDom.GetKSSessPool(ks)
			require.NoError(t, err)
			mgr := storage.NewTaskManager(pool)
			ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
			require.NoError(t, mgr.WithNewSession(func(se sessionctx.Context) error {
				_, err := importer.CreateJob(ctx, se.GetSQLExecutor(), "db", "tbl", 1, "", &importer.ImportParameters{}, 1)
				return err
			}))
			// verify through the user keyspace session from user keyspace
			userTK := testkit.NewTestKit(t, userKSStore)
			userTK.MustQuery("select count(1) from mysql.tidb_import_jobs").Check(testkit.Rows("1"))
		}
		// SYSTEM keyspace should not have any import jobs
		sysTK := testkit.NewTestKit(t, sysKSStore)
		sysTK.MustQuery("select count(1) from mysql.tidb_import_jobs").Check(testkit.Rows("0"))
	})
}
