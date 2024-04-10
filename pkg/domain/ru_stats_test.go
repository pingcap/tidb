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

package domain_test

import (
	"context"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestWriteRUStatistics(t *testing.T) {
	tz, _ := time.LoadLocation("Asia/Shanghai")
	testWriteRUStatisticsTz(t, tz)

	// test with DST timezone.
	tz, _ = time.LoadLocation("Australia/Lord_Howe")
	testWriteRUStatisticsTz(t, tz)

	testWriteRUStatisticsTz(t, time.Local)
	testWriteRUStatisticsTz(t, time.UTC)
}

func testWriteRUStatisticsTz(t *testing.T, tz *time.Location) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newTestKit(t, store)

	testRUWriter := domain.NewRUStatsWriter(dom)
	testRMClient := &testRMClient{
		groups: []*rmpb.ResourceGroup{
			{
				Name: "default",
				RUStats: &rmpb.Consumption{
					RRU: 200.0,
					WRU: 150.0,
				},
			},
			{
				Name: "test",
				RUStats: &rmpb.Consumption{
					RRU: 100.0,
					WRU: 50.0,
				},
			},
		},
	}
	infoGroups := make(map[string]*model.ResourceGroupInfo, 2)
	infoGroups["default"] = &model.ResourceGroupInfo{
		ID:   1,
		Name: model.NewCIStr("default"),
	}
	infoGroups["test"] = &model.ResourceGroupInfo{
		ID:   2,
		Name: model.NewCIStr("test"),
	}
	testInfo := &testInfoschema{
		groups: infoGroups,
	}
	testInfoCache := infoschema.NewCache(nil, 1)
	testInfoCache.Insert(testInfo, uint64(time.Now().Unix()))
	testRUWriter.RMClient = testRMClient
	testRUWriter.InfoCache = testInfoCache

	tk.MustQuery("SELECT count(*) from mysql.request_unit_by_group").Check(testkit.Rows("0"))

	testRUWriter.StartTime = time.Date(2023, 12, 26, 0, 0, 1, 0, tz)
	require.NoError(t, testRUWriter.DoWriteRUStatistics(context.Background()))
	tk.MustQuery("SELECT resource_group, total_ru from mysql.request_unit_by_group").Check(testkit.Rows("default 350", "test 150"))

	// after 1 day, only 1 group has delta ru.
	testRMClient.groups[1].RUStats.RRU = 500
	testRUWriter.StartTime = time.Date(2023, 12, 27, 0, 0, 1, 0, tz)
	require.NoError(t, testRUWriter.DoWriteRUStatistics(context.Background()))
	tk.MustQuery("SELECT resource_group, total_ru from mysql.request_unit_by_group where end_time = '2023-12-27'").Check(testkit.Rows("test 400"))

	// test after 1 day with 0 delta ru, no data inserted.
	testRUWriter.StartTime = time.Date(2023, 12, 28, 0, 0, 1, 0, tz)
	require.NoError(t, testRUWriter.DoWriteRUStatistics(context.Background()))
	tk.MustQuery("SELECT count(*) from mysql.request_unit_by_group where end_time = '2023-12-28'").Check(testkit.Rows("0"))

	testRUWriter.StartTime = time.Date(2023, 12, 29, 0, 0, 0, 0, tz)
	testRMClient.groups[0].RUStats.WRU = 200
	require.NoError(t, testRUWriter.DoWriteRUStatistics(context.Background()))
	tk.MustQuery("SELECT resource_group, total_ru from mysql.request_unit_by_group where end_time = '2023-12-29'").Check(testkit.Rows("default 50"))

	// after less than 1 day, even if ru changes, no new rows inserted.
	// This is to test after restart, no unexpected data are inserted.
	testRMClient.groups[0].RUStats.RRU = 1000
	testRMClient.groups[1].RUStats.WRU = 2000
	testRUWriter.StartTime = time.Date(2023, 12, 29, 1, 0, 0, 0, tz)
	require.NoError(t, testRUWriter.DoWriteRUStatistics(context.Background()))
	tk.MustQuery("SELECT resource_group, total_ru from mysql.request_unit_by_group where end_time = '2023-12-29'").Check(testkit.Rows("default 50"))

	// after 61 days, old record should be GCed.
	testRUWriter.StartTime = time.Date(2023, 12, 26, 0, 0, 0, 0, tz).Add(92 * 24 * time.Hour)
	tk.MustQuery("SELECT count(*) from mysql.request_unit_by_group where end_time = '2023-12-26'").Check(testkit.Rows("2"))
	require.NoError(t, testRUWriter.GCOutdatedRecords(testRUWriter.StartTime))
	tk.MustQuery("SELECT count(*) from mysql.request_unit_by_group where end_time = '2023-12-26'").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT count(*) from mysql.request_unit_by_group where end_time = '2023-12-27'").Check(testkit.Rows("1"))
}

type testRMClient struct {
	pd.ResourceManagerClient
	groups []*rmpb.ResourceGroup
}

func (c *testRMClient) ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	return c.groups, nil
}

type testInfoschema struct {
	infoschema.InfoSchema
	groups map[string]*model.ResourceGroupInfo
}

func (is *testInfoschema) ResourceGroupByName(name model.CIStr) (*model.ResourceGroupInfo, bool) {
	g, ok := is.groups[name.L]
	return g, ok
}

func (is *testInfoschema) SchemaMetaVersion() int64 {
	return 1
}

func TestGetLastExpectedTime(t *testing.T) {
	tz, _ := time.LoadLocation("Asia/Shanghai")
	testGetLastExpectedTimeTz(t, tz)

	// test with DST affected timezone.
	tz, _ = time.LoadLocation("Australia/Lord_Howe")
	testGetLastExpectedTimeTz(t, tz)
	testGetLastExpectedTimeTz(t, time.Local)
}

func testGetLastExpectedTimeTz(t *testing.T, tz *time.Location) {
	// 2023-12-28 10:46:23.000
	now := time.Date(2023, 12, 28, 10, 46, 23, 0, tz)
	newTime := func(hour, minute int) time.Time {
		return time.Date(2023, 12, 28, hour, minute, 0, 0, tz)
	}

	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 5*time.Minute, tz), newTime(10, 45))
	require.Equal(t, domain.GetLastExpectedTimeTZ(time.Date(2023, 12, 28, 10, 45, 0, 0, tz), 5*time.Minute, tz), newTime(10, 45))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 10*time.Minute, tz), newTime(10, 40))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 30*time.Minute, tz), newTime(10, 30))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, time.Hour, tz), newTime(10, 0))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 3*time.Hour, tz), newTime(9, 0))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 4*time.Hour, tz), newTime(8, 0))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 12*time.Hour, tz), newTime(0, 0))
	require.Equal(t, domain.GetLastExpectedTimeTZ(now, 24*time.Hour, tz), newTime(0, 0))
	require.Equal(t, domain.GetLastExpectedTimeTZ(time.Date(2023, 12, 28, 0, 0, 0, 0, tz), 24*time.Hour, tz), newTime(0, 0))
}
