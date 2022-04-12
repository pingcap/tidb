// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestIntegrationCopCache(t *testing.T) {
	originConfig := config.GetGlobalConfig()
	config.StoreGlobalConfig(config.NewConfig())
	defer config.StoreGlobalConfig(originConfig)

	cli := &regionProperityClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	var cluster testutils.Cluster
	store, dom, clean := testkit.CreateMockStoreAndDomain(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cluster = c
		}),
		mockstore.WithClientHijacker(hijackClient))
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key)")

	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tblInfo.Meta().ID
	tk.MustExec(`insert into t values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)`)
	tableStart := tablecodec.GenTableRecordPrefix(tid)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 6)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/cophandler/mockCopCacheInUnistore", `return(123)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/cophandler/mockCopCacheInUnistore"))
	}()

	rows := tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	require.Equal(t, "9", rows[0][2])
	require.Contains(t, rows[0][5], "cop_task: {num: 5")
	require.Contains(t, rows[0][5], "copr_cache_hit_ratio: 0.00")

	rows = tk.MustQuery("explain analyze select * from t").Rows()
	require.Equal(t, "12", rows[0][2])
	require.Contains(t, rows[0][5], "cop_task: {num: 6")
	hitRatioIdx := strings.Index(rows[0][5].(string), "copr_cache_hit_ratio:") + len("copr_cache_hit_ratio: ")
	require.GreaterOrEqual(t, hitRatioIdx, len("copr_cache_hit_ratio: "))
	hitRatio, err := strconv.ParseFloat(rows[0][5].(string)[hitRatioIdx:hitRatioIdx+4], 64)
	require.NoError(t, err)
	require.Greater(t, hitRatio, float64(0))

	// Test for cop cache disabled.
	cfg := config.NewConfig()
	cfg.TiKVClient.CoprCache.CapacityMB = 0
	config.StoreGlobalConfig(cfg)
	rows = tk.MustQuery("explain analyze select * from t where t.a < 10").Rows()
	require.Equal(t, "9", rows[0][2])
	require.Contains(t, rows[0][5], "copr_cache: disabled")
}
