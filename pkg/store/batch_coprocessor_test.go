// Copyright 2020 PingCAP, Inc.
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

package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func createMockTiKVStoreOptions(tiflashNum int) []mockstore.MockTiKVStoreOption {
	return []mockstore.MockTiKVStoreOption{
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < tiflashNum {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	}
}

func TestStoreErr(t *testing.T) {
	store := testkit.CreateMockStore(t, createMockTiKVStoreOptions(1)...)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=OFF")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopCancelled", "1*return(true)"))

	err = tk.QueryToErr("select count(*) from t")
	require.Equal(t, context.Canceled, errors.Cause(err))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "1*return(\"tiflash0\")"))

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))
	err = tk.QueryToErr("select count(*) from t")
	require.Error(t, err)
}

func TestStoreSwitchPeer(t *testing.T) {
	store := testkit.CreateMockStore(t, createMockTiKVStoreOptions(2)...)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustExec("set @@session.tidb_allow_mpp=OFF")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"))

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/BatchCopRpcErrtiflash1", "return(\"tiflash1\")"))
	err = tk.QueryToErr("select count(*) from t")
	require.Error(t, err)
}
