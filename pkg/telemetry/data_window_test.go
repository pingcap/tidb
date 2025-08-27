// Copyright 2021 PingCAP, Inc.
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

package telemetry_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestBuiltinFunctionsUsage(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=0") // affect this UT

	// Clear builtin functions usage
	telemetry.GlobalBuiltinFunctionsUsage.Dump()
	usage := telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{}, usage)

	tk.MustExec("create table t (id int)")
	tk.MustQuery("select id + 1 - 2 from t")
	// Should manually invoke `Session.Close()` to report the usage information
	tk.Session().Close()
	usage = telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{"PlusInt": 1, "MinusInt": 1}, usage)
}

// withMockTiFlash sets the mockStore to have N TiFlash stores (naming as tiflash0, tiflash1, ...).
func withMockTiFlash(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < nodes {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
}

func TestTiflashUsage(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	tk.MustExec("alter table t set tiflash replica 1")

	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     1,
		Available: true,
	}

	telemetry.CurrentTiFlashPushDownCount.Swap(0)
	telemetry.CurrentTiflashTableScanWithFastScanCount.Swap(0)

	require.Equal(t, telemetry.CurrentTiflashTableScanCount.String(), "0")
	require.Equal(t, telemetry.CurrentTiflashTableScanWithFastScanCount.String(), "0")

	tk.MustExec("set session tidb_isolation_read_engines='tiflash';")
	tk.MustQuery(`select count(*) from t`)
	tk.MustExec(`set @@session.tiflash_fastscan=ON`)
	tk.MustExec(`set session tidb_isolation_read_engines="tiflash";`)
	tk.MustQuery(`select count(*) from test.t`)

	tk.Session().Close()
	require.Equal(t, telemetry.CurrentTiflashTableScanCount.String(), "2")
	require.Equal(t, telemetry.CurrentTiflashTableScanWithFastScanCount.String(), "1")
}
