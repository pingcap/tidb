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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestCalibrateResource(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	var confItems [][]types.Datum
	var confErr error
	var confFunc executor.TestShowClusterConfigFunc = func() ([][]types.Datum, error) {
		return confItems, confErr
	}
	tk.Session().SetValue(executor.TestShowClusterConfigKey, confFunc)
	strs2Items := func(strs ...string) []types.Datum {
		items := make([]types.Datum, 0, len(strs))
		for _, s := range strs {
			items = append(items, types.NewStringDatum(s))
		}
		return items
	}

	// empty request-unit config error
	rs, err := tk.Exec("CALIBRATE RESOURCE")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(context.Background(), rs.NewChunk(nil))
	require.ErrorContains(t, err, "PD request-unit config not found")

	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2379", "controller.request-unit.read-base-cost", "0.25"))
	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2379", "controller.request-unit.read-cost-per-byte", "0.0000152587890625"))
	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2379", "controller.request-unit.read-cpu-ms-cost", "0.3333333333333333"))
	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2379", "controller.request-unit.write-base-cost", "1"))
	confItems = append(confItems, strs2Items("pd", "127.0.0.1:2379", "controller.request-unit.write-cost-per-byte", "0.0009765625"))

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
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()

	datetime := func(s string) types.Time {
		time, err := types.ParseTime(tk.Session().GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp, nil)
		require.NoError(t, err)
		return time
	}

	mockData := map[string][][]types.Datum{
		"tikv_cpu_quota": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", 8.0),
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", 8.0),
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", 8.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", 8.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", 8.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", 8.0),
		},
		"tidb_server_maxprocs": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", 40.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", 40.0),
		},
	}
	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE").Check(testkit.Rows("68569"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD TPCC").Check(testkit.Rows("68569"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_WRITE").Check(testkit.Rows("53026"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_READ_ONLY").Check(testkit.Rows("31463"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE WORKLOAD OLTP_WRITE_ONLY").Check(testkit.Rows("109776"))

	// change total tidb cpu to less than tikv_cpu_quota
	mockData["tidb_server_maxprocs"] = [][]types.Datum{
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", 8.0),
	}
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE").Check(testkit.Rows("38094"))

	// construct data for dynamic calibrate
	mockData["resource_manager_resource_unit"] = [][]types.Datum{
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

	mockData["process_cpu_usage"] = [][]types.Datum{
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

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("8161"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("8161"))

	mockData["process_cpu_usage"] = [][]types.Datum{
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

	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '10m'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' START_TIME '2020-02-12 10:35:00'").Check(testkit.Rows("5616"))
	tk.MustQueryWithContext(ctx, "CALIBRATE RESOURCE END_TIME '2020-02-12 10:45:00' DURATION '5m' START_TIME '2020-02-12 10:35:00' ").Check(testkit.Rows("5616"))

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too long")

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' DURATION '5m'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "the duration of calibration is too short")

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:43:00'")
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
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "tidb", 3.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "tidb", 3.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0", "tidb", 3.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tidb-0", "tidb", 3.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tidb-0", "tidb", 3.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tidb-0", "tidb", 3.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tidb-0", "tidb", 3.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tidb-0", "tidb", 0.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tidb-0", "tidb", 0.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tidb-0", "tidb", 0.221),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-1", "tikv", 2.212),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "tikv", 2.233),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "tikv", 2.234),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-1", "tikv", 2.209),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "tikv", 2.213),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-1", "tikv", 2.236),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-1", "tikv", 2.228),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-1", "tikv", 0.219),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-1", "tikv", 0.220),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-1", "tikv", 0.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "tikv", 2.282),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-0", "tikv", 2.284),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "tikv", 2.289),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-0", "tikv", 2.283),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-0", "tikv", 2.286),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-0", "tikv", 2.288),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-0", "tikv", 0.289),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-0", "tikv", 0.280),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-0", "tikv", 0.281),
		types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-2", "tikv", 2.112),
		types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-2", "tikv", 2.133),
		types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "tikv", 2.134),
		types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-2", "tikv", 2.109),
		types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-2", "tikv", 2.113),
		types.MakeDatums(datetime("2020-02-12 10:41:00"), "tikv-2", "tikv", 2.136),
		types.MakeDatums(datetime("2020-02-12 10:42:00"), "tikv-2", "tikv", 2.128),
		types.MakeDatums(datetime("2020-02-12 10:43:00"), "tikv-2", "tikv", 0.119),
		types.MakeDatums(datetime("2020-02-12 10:44:00"), "tikv-2", "tikv", 0.120),
		types.MakeDatums(datetime("2020-02-12 10:45:00"), "tikv-2", "tikv", 0.281),
	}

	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "The CPU utilizations of TiDB and TiKV are less than one tenth in some of the time")

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
	rs, err = tk.Exec("CALIBRATE RESOURCE START_TIME '2020-02-12 10:35:00' END_TIME '2020-02-12 10:45:00'")
	require.NoError(t, err)
	require.NotNil(t, rs)
	err = rs.Next(ctx, rs.NewChunk(nil))
	require.ErrorContains(t, err, "there are too few metrics points available")
}
