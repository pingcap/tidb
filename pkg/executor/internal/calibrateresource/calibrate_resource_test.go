// Copyright 2023 PingCAP, Inc.
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

package calibrateresource_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

func TestCalibrateResource(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// first test resource_control flag
	tk.MustExec("SET GLOBAL tidb_enable_resource_control='OFF';")
	rs, err := tk.Exec("CALIBRATE RESOURCE")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(context.Background(), rs.NewChunk(nil))
	require.ErrorContains(t, err, "Resource control feature is disabled")

	tk.MustExec("SET GLOBAL tidb_enable_resource_control='ON';")

	do := domain.GetDomain(tk.Session())
	oldResourceCtl := do.ResourceGroupsController()
	defer func() {
		do.SetResourceGroupsController(oldResourceCtl)
	}()
	// changed in 7.5 (ref https://github.com/tikv/pd/pull/6538), but for test pass, use the old config
	oldCfg := rmclient.DefaultConfig()
	oldCfg.RequestUnit.ReadBaseCost = 0.25
	oldCfg.RequestUnit.ReadCostPerByte = 0.0000152587890625
	oldCfg.RequestUnit.WriteBaseCost = 1.0
	oldCfg.RequestUnit.WriteCostPerByte = 0.0009765625
	oldCfg.RequestUnit.CPUMsCost = 0.3333333333333333
	mockPrivider := &mockResourceGroupProvider{
		cfg: *oldCfg,
	}
	resourceCtl, err := rmclient.NewResourceGroupController(context.Background(), 1, mockPrivider, nil)
	require.NoError(t, err)
	do.SetResourceGroupsController(resourceCtl)

	// empty metrics error
	rs, err = tk.Exec("CALIBRATE RESOURCE")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(context.Background(), rs.NewChunk(nil))
	require.ErrorContains(t, err, "no server with type 'tikv' is found")

	// error sql
	_, err = tk.Exec("CALIBRATE RESOURCE WORKLOAD tpcc START_TIME '2020-02-12 10:35:00'")
	require.Error(t, err)

	// Mock for cluster info
	// information_schema.cluster_config
	instances := []string{
		"pd,127.0.0.1:32379,127.0.0.1:32380,mock-version,mock-githash,0",
		"tidb,127.0.0.1:34000,30080,mock-version,mock-githash,1001",
		"tikv,127.0.0.1:30160,30180,mock-version,mock-githash,0",
		"tikv,127.0.0.1:30161,30181,mock-version,mock-githash,0",
		"tikv,127.0.0.1:30162,30182,mock-version,mock-githash,0",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo", fpExpr))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo"))
	}()

	// Mock for metric table data.
	fpName := "github.com/pingcap/tidb/pkg/executor/mockMetricsTableData"
	require.NoError(t, failpoint.Enable(fpName, "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockMetricsDataFilter", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockMetricsDataFilter"))
	}()

	datetime := func(s string) types.Time {
		time, err := types.ParseTime(tk.Session().GetSessionVars().StmtCtx.TypeCtx(), s, mysql.TypeDatetime, types.MaxFsp)
		require.NoError(t, err)
		return time
	}
	now := time.Now()
	datetimeBeforeNow := func(dur time.Duration) types.Time {
		time := types.NewTime(types.FromGoTime(now.Add(-dur)), mysql.TypeDatetime, types.MaxFsp)
		return time
	}

	metricsData := `# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 49943
# HELP tikv_server_cpu_cores_quota Total CPU cores quota for TiKV server
# TYPE tikv_server_cpu_cores_quota gauge
tikv_server_cpu_cores_quota 8
# HELP tiflash_proxy_tikv_scheduler_write_flow The write flow passed through at scheduler level.
# TYPE tiflash_proxy_tikv_scheduler_write_flow gauge
tiflash_proxy_tikv_scheduler_write_flow 0
# HELP tiflash_proxy_tikv_server_cpu_cores_quota Total CPU cores quota for TiKV server
# TYPE tiflash_proxy_tikv_server_cpu_cores_quota gauge
tiflash_proxy_tikv_server_cpu_cores_quota 20
`
	// failpoint doesn't support string contains whitespaces and newline
	encodedData := base64.StdEncoding.EncodeToString([]byte(metricsData))
	fpExpr = `return("` + encodedData + `")`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockMetricsResponse", fpExpr))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockGOMAXPROCS", "return(40)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockGOMAXPROCS"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockMetricsResponse"))
	}()
	mockData := make(map[string][][]types.Datum)
	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE").Check(testkit.Rows("69768"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD TPCC").Check(testkit.Rows("69768"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_WRITE").Check(testkit.Rows("55823"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_ONLY").Check(testkit.Rows("34926"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_WRITE_ONLY").Check(testkit.Rows("109776"))

	// change total tidb cpu to less than tikv_cpu_quota
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/internal/calibrateresource/mockGOMAXPROCS", "return(8)"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE").Check(testkit.Rows("38760"))

	ru1 := [][]types.Datum{
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+40*time.Second), 2250.0),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+10*time.Second), 2200.0),
		types.MakeDatums(datetimeBeforeNow(10*time.Minute+10*time.Second), 2200.0),
		types.MakeDatums(datetimeBeforeNow(9*time.Minute+10*time.Second), 2100.0),
		types.MakeDatums(datetimeBeforeNow(8*time.Minute+10*time.Second), 2250.0),
		types.MakeDatums(datetimeBeforeNow(7*time.Minute+10*time.Second), 2300.0),
		types.MakeDatums(datetimeBeforeNow(6*time.Minute+10*time.Second), 2230.0),
		types.MakeDatums(datetimeBeforeNow(5*time.Minute+10*time.Second), 2210.0),
		types.MakeDatums(datetimeBeforeNow(4*time.Minute+10*time.Second), 2250.0),
		types.MakeDatums(datetimeBeforeNow(3*time.Minute+10*time.Second), 2330.0),
		types.MakeDatums(datetimeBeforeNow(2*time.Minute+10*time.Second), 2330.0),
		types.MakeDatums(datetimeBeforeNow(1*time.Minute+10*time.Second), 2300.0),
		types.MakeDatums(datetimeBeforeNow(10*time.Second), 2280.0),
	}
	mockData["resource_manager_resource_unit"] = ru1
	cpu1 := [][]types.Datum{
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+40*time.Second), "tidb-0", "tidb", 1.234),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+10*time.Second), "tidb-0", "tidb", 1.212),
		types.MakeDatums(datetimeBeforeNow(10*time.Minute+10*time.Second), "tidb-0", "tidb", 1.212),
		types.MakeDatums(datetimeBeforeNow(9*time.Minute+10*time.Second), "tidb-0", "tidb", 1.233),
		types.MakeDatums(datetimeBeforeNow(8*time.Minute+10*time.Second), "tidb-0", "tidb", 1.234),
		types.MakeDatums(datetimeBeforeNow(7*time.Minute+10*time.Second), "tidb-0", "tidb", 1.213),
		types.MakeDatums(datetimeBeforeNow(6*time.Minute+10*time.Second), "tidb-0", "tidb", 1.209),
		types.MakeDatums(datetimeBeforeNow(5*time.Minute+10*time.Second), "tidb-0", "tidb", 1.213),
		types.MakeDatums(datetimeBeforeNow(4*time.Minute+10*time.Second), "tidb-0", "tidb", 1.236),
		types.MakeDatums(datetimeBeforeNow(3*time.Minute+10*time.Second), "tidb-0", "tidb", 1.228),
		types.MakeDatums(datetimeBeforeNow(2*time.Minute+10*time.Second), "tidb-0", "tidb", 1.219),
		types.MakeDatums(datetimeBeforeNow(1*time.Minute+10*time.Second), "tidb-0", "tidb", 1.220),
		types.MakeDatums(datetimeBeforeNow(10*time.Second), "tidb-0", "tidb", 1.221),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+40*time.Second), "tikv-1", "tikv", 2.219),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+10*time.Second), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetimeBeforeNow(10*time.Minute+10*time.Second), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetimeBeforeNow(9*time.Minute+10*time.Second), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetimeBeforeNow(8*time.Minute+10*time.Second), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetimeBeforeNow(7*time.Minute+10*time.Second), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetimeBeforeNow(6*time.Minute+10*time.Second), "tikv-1", "tikv", 2.209),
		types.MakeDatums(datetimeBeforeNow(5*time.Minute+10*time.Second), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetimeBeforeNow(4*time.Minute+10*time.Second), "tikv-1", "tikv", 2.236),
		types.MakeDatums(datetimeBeforeNow(3*time.Minute+10*time.Second), "tikv-1", "tikv", 2.228),
		types.MakeDatums(datetimeBeforeNow(2*time.Minute+10*time.Second), "tikv-1", "tikv", 2.219),
		types.MakeDatums(datetimeBeforeNow(1*time.Minute+10*time.Second), "tikv-1", "tikv", 2.220),
		types.MakeDatums(datetimeBeforeNow(10*time.Second), "tikv-1", "tikv", 2.281),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+40*time.Second), "tikv-0", "tikv", 2.280),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+10*time.Second), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetimeBeforeNow(10*time.Minute+10*time.Second), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetimeBeforeNow(9*time.Minute+10*time.Second), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetimeBeforeNow(8*time.Minute+10*time.Second), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetimeBeforeNow(7*time.Minute+10*time.Second), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetimeBeforeNow(6*time.Minute+10*time.Second), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetimeBeforeNow(5*time.Minute+10*time.Second), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetimeBeforeNow(4*time.Minute+10*time.Second), "tikv-0", "tikv", 2.286),
		types.MakeDatums(datetimeBeforeNow(3*time.Minute+10*time.Second), "tikv-0", "tikv", 2.288),
		types.MakeDatums(datetimeBeforeNow(2*time.Minute+10*time.Second), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetimeBeforeNow(1*time.Minute+10*time.Second), "tikv-0", "tikv", 2.280),
		types.MakeDatums(datetimeBeforeNow(10*time.Second), "tikv-0", "tikv", 2.281),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+40*time.Second), "tikv-2", "tikv", 2.281),
		types.MakeDatums(datetimeBeforeNow(20*time.Minute+10*time.Second), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetimeBeforeNow(10*time.Minute+10*time.Second), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetimeBeforeNow(9*time.Minute+10*time.Second), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetimeBeforeNow(8*time.Minute+10*time.Second), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetimeBeforeNow(7*time.Minute+10*time.Second), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetimeBeforeNow(6*time.Minute+10*time.Second), "tikv-2", "tikv", 2.109),
		types.MakeDatums(datetimeBeforeNow(5*time.Minute+10*time.Second), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetimeBeforeNow(4*time.Minute+10*time.Second), "tikv-2", "tikv", 2.136),
		types.MakeDatums(datetimeBeforeNow(3*time.Minute+10*time.Second), "tikv-2", "tikv", 2.128),
		types.MakeDatums(datetimeBeforeNow(2*time.Minute+10*time.Second), "tikv-2", "tikv", 2.119),
		types.MakeDatums(datetimeBeforeNow(1*time.Minute+10*time.Second), "tikv-2", "tikv", 2.120),
		types.MakeDatums(datetimeBeforeNow(10*time.Second), "tikv-2", "tikv", 2.281),
	}
	mockData["process_cpu_usage"] = cpu1

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION '11m'").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 11 minute END_TIME now()").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME now() START_TIME now() - interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 11 minute DURATION interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 11 minute START_TIME now() - interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME now() DURATION interval 11 minute").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 21 minute END_TIME now() - interval 1 minute").Check(testkit.Rows("8141"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION '20m' START_TIME now() - interval 21 minute").Check(testkit.Rows("8141"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 20 minute START_TIME now() - interval 21 minute").Check(testkit.Rows("8141"))

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 21 minute END_TIME now() - interval 20 minute").Check(testkit.Rows("7978"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 4 minute").Check(testkit.Rows("8297"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 4 minute").Check(testkit.Rows("8297"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 4 minute END_TIME now()").Check(testkit.Rows("8297"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 8 minute").Check(testkit.Rows("8223"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 8 minute").Check(testkit.Rows("8223"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 8 minute END_TIME now() - interval 4 minute").Check(testkit.Rows("8147"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME now() - interval 8 minute DURATION interval 4 minute").Check(testkit.Rows("8147"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 4 minute START_TIME now() - interval 8 minute").Check(testkit.Rows("8147"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE DURATION interval 4 minute END_TIME now() - interval 4 minute").Check(testkit.Rows("8147"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME date_sub(now(), interval 4 minute ) DURATION '4m'").Check(testkit.Rows("8147"))

	// construct data for dynamic calibrate
	ru1 = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), 2280.0),
	}
	mockData["resource_manager_resource_unit"] = ru1

	cpu1 = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 1.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 1.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0", "tidb", 1.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 1.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tidb-0", "tidb", 1.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tidb-0", "tidb", 1.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tidb-0", "tidb", 1.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tidb-0", "tidb", 1.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tidb-0", "tidb", 1.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 1.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tidb-0", "tidb", 1.221),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-1", "tikv", 2.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-1", "tikv", 2.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-1", "tikv", 2.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-1", "tikv", 2.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-1", "tikv", 2.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-1", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-0", "tikv", 2.286),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-0", "tikv", 2.288),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 2.280),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-0", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-2", "tikv", 2.109),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-2", "tikv", 2.136),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-2", "tikv", 2.128),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-2", "tikv", 2.119),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-2", "tikv", 2.120),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-2", "tikv", 2.281),
	}
	mockData["process_cpu_usage"] = cpu1

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("8161"))

	cpu2 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 3.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 3.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0", "tidb", 3.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tidb-0", "tidb", 3.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tidb-0", "tidb", 3.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tidb-0", "tidb", 3.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tidb-0", "tidb", 3.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 3.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tidb-0", "tidb", 3.221),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-1", "tikv", 2.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-1", "tikv", 2.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-1", "tikv", 2.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-1", "tikv", 2.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-1", "tikv", 2.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-1", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-0", "tikv", 2.286),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-0", "tikv", 2.288),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 2.280),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-0", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-2", "tikv", 2.109),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-2", "tikv", 2.136),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-2", "tikv", 2.128),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-2", "tikv", 2.119),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-2", "tikv", 2.120),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-2", "tikv", 2.281),
	}
	mockData["process_cpu_usage"] = cpu2

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' START_TIME '2020-02-12 10:35:00'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' DURATION '5m' START_TIME '2020-02-12 10:35:00' ").Check(testkit.Rows("5616"))

	// Statistical time points do not correspond
	ruModify1 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:25:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:26:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:27:00"), 4.0),
		types.MakeDatums(datetime("2020-02-12 10:28:00"), 6.0),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), 3.0),
		types.MakeDatums(datetime("2020-02-12 10:30:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:31:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:33:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:34:00"), 8.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:46:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:48:00"), 8.0),
	}
	mockData["resource_manager_resource_unit"] = ruModify1
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'").Check(testkit.Rows("5616"))

	ruModify2 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:25:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:26:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:27:00"), 4.0),
		types.MakeDatums(datetime("2020-02-12 10:28:00"), 6.0),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:30:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:31:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:33:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:34:00"), 8.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), 29.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), 49.0),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), 47.0),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), 2250.0),
	}
	mockData["resource_manager_resource_unit"] = ruModify2
	cpu2Mofidy := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tidb-0", "tidb", 3.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 3.233),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tidb-0", "tidb", 3.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tidb-0", "tidb", 3.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tidb-0", "tidb", 3.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 3.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tidb-0", "tidb", 3.221),
		types.MakeDatums(datetime("2020-02-12 10:46:00"), "tidb-0", "tidb", 3.220),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), "tidb-0", "tidb", 3.236),
		types.MakeDatums(datetime("2020-02-12 10:48:00"), "tidb-0", "tidb", 3.220),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), "tidb-0", "tidb", 3.234),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-1", "tikv", 2.209),
		types.MakeDatums(datetime("2020-02-12 10:46:00"), "tikv-1", "tikv", 3.220),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), "tikv-1", "tikv", 2.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-1", "tikv", 2.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-1", "tikv", 2.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-1", "tikv", 2.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-1", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), "tikv-0", "tikv", 2.286),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-0", "tikv", 2.288),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 2.280),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-0", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-2", "tikv", 2.109),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), "tikv-2", "tikv", 2.136),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-2", "tikv", 2.128),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-2", "tikv", 2.119),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-2", "tikv", 2.120),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-2", "tikv", 2.281),
		types.MakeDatums(datetime("2020-02-12 10:48:00"), "tikv-2", "tikv", 3.220),
	}
	mockData["process_cpu_usage"] = cpu2Mofidy
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'").Check(testkit.Rows("5631"))

	ruModify3 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:25:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:26:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:27:00"), 4.0),
		types.MakeDatums(datetime("2020-02-12 10:28:00"), 6.0),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:30:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:31:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), 5.0),
		types.MakeDatums(datetime("2020-02-12 10:33:00"), 7.0),
		types.MakeDatums(datetime("2020-02-12 10:34:00"), 8.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), 29.0),
		types.MakeDatums(datetime("2020-02-12 10:36:20"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), 49.0),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), 47.0),
		types.MakeDatums(datetime("2020-02-12 10:42:20"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:47:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:49:00"), 2250.0),
	}
	mockData["resource_manager_resource_unit"] = ruModify3
	// because there are 20s difference in two time points, the result is changed.
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'").Check(testkit.Rows("5633"))

	ru2 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:25:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:26:00"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:27:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:28:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:30:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:31:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:33:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:34:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), 2280.0),
	}
	mockData["resource_manager_resource_unit"] = ru2
	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "The workload in selected time window is too low")

	ru3 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:25:00"), 2200.0),
		types.MakeDatums(datetime("2020-02-12 10:27:00"), 2100.0),
		types.MakeDatums(datetime("2020-02-12 10:28:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:30:00"), 2300.0),
		types.MakeDatums(datetime("2020-02-12 10:31:00"), 2230.0),
		types.MakeDatums(datetime("2020-02-12 10:33:00"), 2210.0),
		types.MakeDatums(datetime("2020-02-12 10:34:00"), 2250.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), 2330.0),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), 2280.0),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), 2280.0),
	}
	mockData["resource_manager_resource_unit"] = ru3
	cpu3 := [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:26:00"), "tidb-0", "tidb", 3.212),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tidb-0", "tidb", 3.233),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 3.209),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tidb-0", "tidb", 3.228),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 3.219),

		types.MakeDatums(datetime("2020-02-12 10:26:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:29:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:32:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 2.286),
	}
	mockData["process_cpu_usage"] = cpu3
	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:25:00' DURATION '20m'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "The workload in selected time window is too low")

	// flash back to init data.
	mockData["resource_manager_resource_unit"] = ru1
	mockData["process_cpu_usage"] = cpu2

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too long")

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '1m'").Check(testkit.Rows("5337"))

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:35:40'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too short")

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '25h'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too long")

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-13 10:46:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too long")

	mockData["process_cpu_usage"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 0.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 0.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0", "tidb", 0.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tidb-0", "tidb", 0.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tidb-0", "tidb", 0.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tidb-0", "tidb", 0.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tidb-0", "tidb", 0.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tidb-0", "tidb", 0.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 0.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tidb-0", "tidb", 0.221),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 0.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "tikv", 0.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 0.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-1", "tikv", 0.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-1", "tikv", 0.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-1", "tikv", 0.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-1", "tikv", 0.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-1", "tikv", 0.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-1", "tikv", 0.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 0.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 0.283),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-0", "tikv", 0.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "tikv", 0.289),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-0", "tikv", 0.283),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-0", "tikv", 0.286),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-0", "tikv", 0.288),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-0", "tikv", 0.289),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 0.280),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-0", "tikv", 0.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 0.133),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "tikv", 0.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 0.113),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-2", "tikv", 0.109),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-2", "tikv", 0.113),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-2", "tikv", 0.136),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-2", "tikv", 0.128),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-2", "tikv", 0.119),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-2", "tikv", 0.120),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-2", "tikv", 0.281),
	}

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-13 10:35:01'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "The workload in selected time window is too low, with which TiDB is unable to reach a capacity estimation")

	mockData["process_cpu_usage"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 3.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 3.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0", "tidb", 3.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 2.113),
	}
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("5492"))

	// tiflash
	mockData["resource_manager_resource_unit"] = [][]types.Datum{
		types.MakeDatums(datetime("2023-09-19 19:50:39.322000"), 465919.8102127319),
		types.MakeDatums(datetime("2023-09-19 19:51:39.322000"), 819764.9742611333),
		types.MakeDatums(datetime("2023-09-19 19:52:39.322000"), 520180.7089147462),
		types.MakeDatums(datetime("2023-09-19 19:53:39.322000"), 790496.4071700446),
		types.MakeDatums(datetime("2023-09-19 19:54:39.322000"), 545216.2174551424),
		types.MakeDatums(datetime("2023-09-19 19:55:39.322000"), 714332.5760632281),
		types.MakeDatums(datetime("2023-09-19 19:56:39.322000"), 577119.1037253677),
		types.MakeDatums(datetime("2023-09-19 19:57:39.322000"), 678005.0740038564),
		types.MakeDatums(datetime("2023-09-19 19:58:39.322000"), 592239.6784597588),
		types.MakeDatums(datetime("2023-09-19 19:59:39.322000"), 666552.6950822703),
		types.MakeDatums(datetime("2023-09-19 20:00:39.322000"), 689703.5663975218),
	}

	mockData["process_cpu_usage"] = [][]types.Datum{
		types.MakeDatums(datetime("2023-09-19 19:50:39.324000"), "127.0.0.1:10080", "tidb", 0.10511111111111152),
		types.MakeDatums(datetime("2023-09-19 19:51:39.324000"), "127.0.0.1:10080", "tidb", 0.1293333333333332),
		types.MakeDatums(datetime("2023-09-19 19:52:39.324000"), "127.0.0.1:10080", "tidb", 0.11088888888888908),
		types.MakeDatums(datetime("2023-09-19 19:53:39.324000"), "127.0.0.1:10080", "tidb", 0.12333333333333357),
		types.MakeDatums(datetime("2023-09-19 19:54:39.324000"), "127.0.0.1:10080", "tidb", 0.1160000000000006),
		types.MakeDatums(datetime("2023-09-19 19:55:39.324000"), "127.0.0.1:10080", "tidb", 0.11888888888888813),
		types.MakeDatums(datetime("2023-09-19 19:56:39.324000"), "127.0.0.1:10080", "tidb", 0.1106666666666658),
		types.MakeDatums(datetime("2023-09-19 19:57:39.324000"), "127.0.0.1:10080", "tidb", 0.11311111111111055),
		types.MakeDatums(datetime("2023-09-19 19:58:39.324000"), "127.0.0.1:10080", "tidb", 0.11222222222222247),
		types.MakeDatums(datetime("2023-09-19 19:59:39.324000"), "127.0.0.1:10080", "tidb", 0.11488888888888923),
		types.MakeDatums(datetime("2023-09-19 20:00:39.324000"), "127.0.0.1:10080", "tidb", 0.12733333333333371),
		types.MakeDatums(datetime("2023-09-19 19:50:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:51:39.325000"), "127.0.0.1:20180", "tikv", 0.02222222222222222),
		types.MakeDatums(datetime("2023-09-19 19:52:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:53:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:54:39.325000"), "127.0.0.1:20180", "tikv", 0.08888888888888888),
		types.MakeDatums(datetime("2023-09-19 19:55:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:56:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:57:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:58:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 19:59:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
		types.MakeDatums(datetime("2023-09-19 20:00:39.325000"), "127.0.0.1:20180", "tikv", 0.04444444444444444),
	}

	mockData["tidb_server_maxprocs"] = [][]types.Datum{
		types.MakeDatums(datetime("2023-09-19 19:50:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:51:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:52:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:53:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2022-09-19 19:54:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:55:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:56:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:57:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:58:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 19:59:39.329000"), "127.0.0.1:10080", 20.0),
		types.MakeDatums(datetime("2023-09-19 20:00:39.329000"), "127.0.0.1:10080", 20.0),
	}

	// change mock for cluster info, add tiflash
	instances = append(instances, "tiflash,127.0.0.1:3930,33940,mock-version,mock-githash,0")
	fpExpr = `return("` + strings.Join(instances, ";") + `")`
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockClusterInfo", fpExpr))

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2023-09-19 19:50:39' DURATION '10m'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "The workload in selected time window is too low")

	mockData["tiflash_process_cpu_usage"] = [][]types.Datum{
		types.MakeDatums(datetime("2023-09-19 19:50:39.327000"), "127.0.0.1:20292", "tiflash", 18.577777777777776),
		types.MakeDatums(datetime("2023-09-19 19:51:39.327000"), "127.0.0.1:20292", "tiflash", 17.666666666666668),
		types.MakeDatums(datetime("2023-09-19 19:52:39.327000"), "127.0.0.1:20292", "tiflash", 18.339038812074868),
		types.MakeDatums(datetime("2023-09-19 19:53:39.327000"), "127.0.0.1:20292", "tiflash", 17.82222222222222),
		types.MakeDatums(datetime("2023-09-19 19:54:39.327000"), "127.0.0.1:20292", "tiflash", 18.177777777777774),
		types.MakeDatums(datetime("2023-09-19 19:55:39.327000"), "127.0.0.1:20292", "tiflash", 17.911111111111108),
		types.MakeDatums(datetime("2023-09-19 19:56:39.327000"), "127.0.0.1:20292", "tiflash", 17.177777777777774),
		types.MakeDatums(datetime("2023-09-19 19:57:39.327000"), "127.0.0.1:20292", "tiflash", 16.17957550838982),
		types.MakeDatums(datetime("2023-09-19 19:58:39.327000"), "127.0.0.1:20292", "tiflash", 16.844444444444445),
		types.MakeDatums(datetime("2023-09-19 19:59:39.327000"), "127.0.0.1:20292", "tiflash", 17.71111111111111),
		types.MakeDatums(datetime("2023-09-19 20:00:39.327000"), "127.0.0.1:20292", "tiflash", 18.066666666666666),
	}

	mockData["tiflash_resource_manager_resource_unit"] = [][]types.Datum{
		types.MakeDatums(datetime("2023-09-19 19:50:39.318000"), 487049.3164728853),
		types.MakeDatums(datetime("2023-09-19 19:51:39.318000"), 821600.8181867122),
		types.MakeDatums(datetime("2023-09-19 19:52:39.318000"), 507566.26041673025),
		types.MakeDatums(datetime("2023-09-19 19:53:39.318000"), 771038.8122556474),
		types.MakeDatums(datetime("2023-09-19 19:54:39.318000"), 529128.4530634031),
		types.MakeDatums(datetime("2023-09-19 19:55:39.318000"), 777912.9275530444),
		types.MakeDatums(datetime("2023-09-19 19:56:39.318000"), 557595.6206041124),
		types.MakeDatums(datetime("2023-09-19 19:57:39.318000"), 688658.1706168016),
		types.MakeDatums(datetime("2023-09-19 19:58:39.318000"), 556400.2766714202),
		types.MakeDatums(datetime("2023-09-19 19:59:39.318000"), 712467.4348424983),
		types.MakeDatums(datetime("2023-09-19 20:00:39.318000"), 659167.0340155548),
	}

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2023-09-19 19:50:39' DURATION '10m'").Check(testkit.Rows("729439"))

	delete(mockData, "process_cpu_usage")
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("729439"))

	delete(mockData, "tiflash_process_cpu_usage")
	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "query metric error: pd unavailable")
}

type mockResourceGroupProvider struct {
	rmclient.ResourceGroupProvider
	cfg rmclient.Config
}

func (p *mockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...pd.OpOption) (*meta_storagepb.GetResponse, error) {
	if !bytes.Equal(pd.ControllerConfigPathPrefixBytes, key) {
		return nil, errors.New("unsupported configPath")
	}
	payload, _ := json.Marshal(&p.cfg)
	return &meta_storagepb.GetResponse{
		Count: 1,
		Kvs: []*meta_storagepb.KeyValue{
			{
				Key:   key,
				Value: payload,
			},
		},
	}, nil
}
