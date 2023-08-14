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
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
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

	mockPrivider := &mockResourceGroupProvider{
		cfg: rmclient.Config{
			RequestUnit: rmclient.RequestUnitConfig{
				ReadBaseCost:     0.25,
				ReadCostPerByte:  0.0000152587890625,
				WriteBaseCost:    1.0,
				WriteCostPerByte: 0.0009765625,
				CPUMsCost:        0.3333333333333333,
			},
		},
	}
	resourceCtl, err := rmclient.NewResourceGroupController(context.Background(), 1, mockPrivider, nil)
	require.NoError(t, err)
	do.SetResourceGroupsController(resourceCtl)

	// empty metrics error
	rs, err = tk.Exec("CALIBRATE RESOURCE")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(context.Background(), rs.NewChunk(nil))
	require.ErrorContains(t, err, "query metric error: pd unavailable")

	// error sql
	_, err = tk.Exec("CALIBRATE RESOURCE WORKLOAD tpcc START_TIME '2020-02-12 10:35:00'")
	require.Error(t, err)

	// Mock for metric table data.
	fpName := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	require.NoError(t, failpoint.Enable(fpName, "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/internal/calibrateresource/mockMetricsDataFilter", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/internal/calibrateresource/mockMetricsDataFilter"))
	}()

	datetime := func(s string) types.Time {
		time, err := types.ParseTime(tk.Session().GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp, nil)
		require.NoError(t, err)
		return time
	}
	now := time.Now()
	datetimeBeforeNow := func(dur time.Duration) types.Time {
		time := types.NewTime(types.FromGoTime(now.Add(-dur)), mysql.TypeDatetime, types.MaxFsp)
		return time
	}

	mockData := make(map[string][][]types.Datum)
	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})
	rs, err = tk.Exec("CALIBRATE RESOURCE")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	// because when mock metrics is empty, error is always `pd unavailable`, don't check detail.
	require.ErrorContains(t, err, "There is no CPU quota metrics, query metric error: pd unavailable")

	mockData["tikv_cpu_quota"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", 8.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", 8.0),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", 8.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", 8.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", 8.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", 8.0),
	}
	mockData["tidb_server_maxprocs"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", 40.0),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", 40.0),
	}
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE").Check(testkit.Rows("69768"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD TPCC").Check(testkit.Rows("69768"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_WRITE").Check(testkit.Rows("55823"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_ONLY").Check(testkit.Rows("34926"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_WRITE_ONLY").Check(testkit.Rows("109776"))

	// change total tidb cpu to less than tikv_cpu_quota
	mockData["tidb_server_maxprocs"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", 8.0),
	}
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

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'")
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

	delete(mockData, "process_cpu_usage")
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

func (p *mockResourceGroupProvider) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]pd.GlobalConfigItem, int64, error) {
	if configPath != "resource_group/controller" {
		return nil, 0, errors.New("unsupported configPath")
	}
	payload, _ := json.Marshal(&p.cfg)
	item := pd.GlobalConfigItem{
		PayLoad: payload,
	}
	return []pd.GlobalConfigItem{item}, 0, nil
}
