// Copyright 2019 PingCAP, Inc.
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

package helper_test

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"go.uber.org/zap"
)

func TestHotRegion(t *testing.T) {
	store, clean := createMockStore(t)
	defer clean()

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
	regionMetric, err := h.FetchHotRegion(pdapi.HotRead)
	require.NoError(t, err)

	expected := map[uint64]helper.RegionMetric{
		2: {
			FlowBytes:    100,
			MaxHotDegree: 1,
			Count:        0,
		},
		4: {
			FlowBytes:    200,
			MaxHotDegree: 2,
			Count:        0,
		},
	}
	require.Equal(t, expected, regionMetric)

	dbInfo := &model.DBInfo{
		Name: model.NewCIStr("test"),
	}
	require.NoError(t, err)

	res, err := h.FetchRegionTableIndex(regionMetric, []*model.DBInfo{dbInfo})
	require.NotEqual(t, res[0].RegionMetric, res[1].RegionMetric)
	require.NoError(t, err)
}

func TestGetRegionsTableInfo(t *testing.T) {
	store, clean := createMockStore(t)
	defer clean()

	h := helper.NewHelper(store)
	regionsInfo := getMockTiKVRegionsInfo()
	schemas := getMockRegionsTableInfoSchema()
	tableInfos := h.GetRegionsTableInfo(regionsInfo, schemas)
	require.Equal(t, getRegionsTableInfoAns(schemas), tableInfos)
}

func TestTiKVRegionsInfo(t *testing.T) {
	store, clean := createMockStore(t)
	defer clean()

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
	regionsInfo, err := h.GetRegionsInfo()
	require.NoError(t, err)
	require.Equal(t, getMockTiKVRegionsInfo(), regionsInfo)
}

func TestTiKVStoresStat(t *testing.T) {
	store, clean := createMockStore(t)
	defer clean()

	h := helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}

	stat, err := h.GetStoresStat()
	require.NoError(t, err)

	data, err := json.Marshal(stat)
	require.NoError(t, err)

	expected := `{"count":1,"stores":[{"store":{"id":1,"address":"127.0.0.1:20160","state":0,"state_name":"Up","version":"3.0.0-beta","labels":[{"key":"test","value":"test"}],"status_address":"","git_hash":"","start_timestamp":0},"status":{"capacity":"60 GiB","available":"100 GiB","leader_count":10,"leader_weight":999999.999999,"leader_score":999999.999999,"leader_size":1000,"region_count":200,"region_weight":999999.999999,"region_score":999999.999999,"region_size":1000,"start_ts":"2019-04-23T19:30:30+08:00","last_heartbeat_ts":"2019-04-23T19:31:30+08:00","uptime":"1h30m"}}]}`
	require.Equal(t, expected, string(data))
}

type mockStore struct {
	helper.Storage
	pdAddrs []string
}

func (s *mockStore) EtcdAddrs() ([]string, error) {
	return s.pdAddrs, nil
}

func (s *mockStore) StartGCWorker() error {
	panic("not implemented")
}

func (s *mockStore) TLSConfig() *tls.Config {
	panic("not implemented")
}

func (s *mockStore) Name() string {
	return "mock store"
}

func (s *mockStore) Describe() string {
	return ""
}

func createMockStore(t *testing.T) (store helper.Storage, clean func()) {
	s, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithMultiRegions(c, []byte("x"))
		}),
	)
	require.NoError(t, err)

	server := mockPDHTTPServer()

	store = &mockStore{
		s.(helper.Storage),
		[]string{"invalid_pd_address", server.URL[len("http://"):]},
	}

	clean = func() {
		server.Close()
		require.NoError(t, store.Close())
	}

	return
}

func mockPDHTTPServer() *httptest.Server {
	router := mux.NewRouter()
	router.HandleFunc(pdapi.HotRead, mockHotRegionResponse)
	router.HandleFunc(pdapi.Regions, mockTiKVRegionsInfoResponse)
	router.HandleFunc(pdapi.Stores, mockStoreStatResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	return httptest.NewServer(serverMux)
}

func mockHotRegionResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := helper.HotRegionsStat{
		RegionsStat: []helper.RegionStat{
			{
				FlowBytes: 100,
				RegionID:  2,
				HotDegree: 1,
			},
			{
				FlowBytes: 200,
				RegionID:  4,
				HotDegree: 2,
			},
		},
	}
	resp := helper.StoreHotRegionInfos{
		AsLeader: make(map[uint64]*helper.HotRegionsStat),
	}
	resp.AsLeader[0] = &regionsStat
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}

}

func getMockRegionsTableInfoSchema() []*model.DBInfo {
	return []*model.DBInfo{
		{
			Name: model.NewCIStr("test"),
			Tables: []*model.TableInfo{
				{
					ID:      41,
					Indices: []*model.IndexInfo{{ID: 1}},
				},
				{
					ID:      63,
					Indices: []*model.IndexInfo{{ID: 1}, {ID: 2}},
				},
				{
					ID:      66,
					Indices: []*model.IndexInfo{{ID: 1}, {ID: 2}, {ID: 3}},
				},
			},
		},
	}
}

func getRegionsTableInfoAns(dbs []*model.DBInfo) map[int64][]helper.TableInfo {
	ans := make(map[int64][]helper.TableInfo)
	db := dbs[0]
	ans[1] = []helper.TableInfo{}
	ans[2] = []helper.TableInfo{
		{db, db.Tables[0], true, db.Tables[0].Indices[0]},
		{db, db.Tables[0], false, nil},
	}
	ans[3] = []helper.TableInfo{
		{db, db.Tables[1], true, db.Tables[1].Indices[0]},
		{db, db.Tables[1], true, db.Tables[1].Indices[1]},
		{db, db.Tables[1], false, nil},
	}
	ans[4] = []helper.TableInfo{
		{db, db.Tables[2], false, nil},
	}
	ans[5] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil},
	}
	ans[6] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[0]},
	}
	ans[7] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[1]},
	}
	ans[8] = []helper.TableInfo{
		{db, db.Tables[2], true, db.Tables[2].Indices[1]},
		{db, db.Tables[2], true, db.Tables[2].Indices[2]},
		{db, db.Tables[2], false, nil},
	}
	return ans
}

func getMockTiKVRegionsInfo() *helper.RegionsInfo {
	regions := []helper.RegionInfo{
		{
			ID:       1,
			StartKey: "",
			EndKey:   "12341234",
			Epoch: helper.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: []helper.RegionPeer{
				{ID: 2, StoreID: 1},
				{ID: 15, StoreID: 51},
				{ID: 66, StoreID: 99, IsLearner: true},
				{ID: 123, StoreID: 111, IsLearner: true},
			},
			Leader: helper.RegionPeer{
				ID:      2,
				StoreID: 1,
			},
			DownPeers: []helper.RegionPeerStat{
				{
					helper.RegionPeer{ID: 66, StoreID: 99, IsLearner: true},
					120,
				},
			},
			PendingPeers: []helper.RegionPeer{
				{ID: 15, StoreID: 51},
			},
			WrittenBytes:    100,
			ReadBytes:       1000,
			ApproximateKeys: 200,
			ApproximateSize: 500,
		},
		// table: 41, record + index: 1
		{
			ID:       2,
			StartKey: "7480000000000000FF295F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF2B5F698000000000FF0000010000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 3, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 3, StoreID: 1},
		},
		// table: 63, record + index: 1, 2
		{
			ID:       3,
			StartKey: "7480000000000000FF3F5F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000010000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 4, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 4, StoreID: 1},
		},
		// table: 66, record
		{
			ID:       4,
			StartKey: "7480000000000000FF425F72C000000000FF0000000000000000FA",
			EndKey:   "",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 5, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 5, StoreID: 1},
		},
		// table: 66, record + index: 3
		{
			ID:       5,
			StartKey: "7480000000000000FF425F698000000000FF0000030000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 6, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 6, StoreID: 1},
		},
		// table: 66, index: 1
		{
			ID:       6,
			StartKey: "7480000000000000FF425F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000020000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 7, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 7, StoreID: 1},
		},
		// table: 66, index: 2
		{
			ID:       7,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000030000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 8, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 8, StoreID: 1},
		},
		// merge region 7, 5
		{
			ID:       8,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 9, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 9, StoreID: 1},
		},
	}
	return &helper.RegionsInfo{
		Count:   int64(len(regions)),
		Regions: regions,
	}
}

func mockTiKVRegionsInfoResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := getMockTiKVRegionsInfo()
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func mockStoreStatResponse(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	startTs, err := time.Parse(time.RFC3339, "2019-04-23T19:30:30+08:00")
	if err != nil {
		log.Panic("mock tikv store api response failed", zap.Error(err))
	}
	lastHeartbeatTs, err := time.Parse(time.RFC3339, "2019-04-23T19:31:30+08:00")
	if err != nil {
		log.Panic("mock tikv store api response failed", zap.Error(err))
	}
	storesStat := helper.StoresStat{
		Count: 1,
		Stores: []helper.StoreStat{
			{
				Store: helper.StoreBaseStat{
					ID:        1,
					Address:   "127.0.0.1:20160",
					State:     0,
					StateName: "Up",
					Version:   "3.0.0-beta",
					Labels: []helper.StoreLabel{
						{
							Key:   "test",
							Value: "test",
						},
					},
				},
				Status: helper.StoreDetailStat{
					Capacity:        "60 GiB",
					Available:       "100 GiB",
					LeaderCount:     10,
					LeaderWeight:    999999.999999,
					LeaderScore:     999999.999999,
					LeaderSize:      1000,
					RegionCount:     200,
					RegionWeight:    999999.999999,
					RegionScore:     999999.999999,
					RegionSize:      1000,
					StartTs:         startTs,
					LastHeartbeatTs: lastHeartbeatTs,
					Uptime:          "1h30m",
				},
			},
		},
	}
	data, err := json.MarshalIndent(storesStat, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func TestComputeTiFlashStatus(t *testing.T) {
	regionReplica := make(map[int64]int)
	// There are no region in this TiFlash store.
	resp1 := "0\n\n"
	// There are one region 1009 in this TiFlash store.
	resp2 := "2\n1009 1010 \n"
	br1 := bufio.NewReader(strings.NewReader(resp1))
	br2 := bufio.NewReader(strings.NewReader(resp2))
	err := helper.ComputeTiFlashStatus(br1, &regionReplica)
	require.NoError(t, err)
	err = helper.ComputeTiFlashStatus(br2, &regionReplica)
	require.NoError(t, err)
	require.Equal(t, len(regionReplica), 2)
	v, ok := regionReplica[1009]
	require.Equal(t, v, 1)
	require.Equal(t, ok, true)
	v, ok = regionReplica[1010]
	require.Equal(t, v, 1)
	require.Equal(t, ok, true)

	regionReplica2 := make(map[int64]int)
	var sb strings.Builder
	for i := 1000; i < 3000; i++ {
		sb.WriteString(fmt.Sprintf("%v ", i))
	}
	s := fmt.Sprintf("2000\n%v\n", sb.String())
	require.NoError(t, helper.ComputeTiFlashStatus(bufio.NewReader(strings.NewReader(s)), &regionReplica2))
	require.Equal(t, 2000, len(regionReplica2))

	regionReplica3 := make(map[int64]int)
	s2 := fmt.Sprintf("850\n1774 1846 1862 1766 1882 1942 1954 1970 1974 1986 1990 2018 2990 3838 3898 3942 3950 1002 3834 1966 3954 3958 2026 1150 1318 1322 3966 4118 3106 1006 1374 4142 1382 1390 1950 4166 1398 3982 2102 3110 1478 3974 2278 3114 1490 998 2282 2106 2378 2386 2394 2742 1514 1994 1362 2402 3130 1462 2406 1346 2490 2570 2578 2606 2494 2274 2610 2118 1182 1506 15398 2622 1486 3102 2270 3142 986 4174 3550 2526 2654 2630 3534 1474 3906 1174 2618 1554 2590 2846 3526 3094 3554 3814 3502 2518 1886 2842 4002 3498 1730 1186 1570 978 2034 1454 4006 2486 1582 2530 3542 2938 3986 3778 2410 1782 1726 1642 2994 3046 1558 3806 2150 1190 2602 3086 2014 2626 3474 3754 3506 2478 2970 3610 2542 1426 3546 1350 1314 3622 1386 2418 3462 1850 2086 1338 3670 2502 1830 1666 4038 3454 2798 1982 3674 1470 1550 4022 1194 3494 3562 4086 3874 3442 4026 3566 1518 2298 2522 1402 4082 3010 3294 3690 2114 1962 2778 1242 1998 3698 1762 2586 3682 2650 2046 2058 2022 1722 2510 2414 2850 3306 2010 3570 3434 1786 1270 1510 1914 2966 3914 1630 3626 3662 2830 1614 1414 3770 1226 3522 2126 3082 2474 2130 1262 2534 2470 2802 4078 3422 1274 2398 3650 2754 1878 2430 1422 1502 4050 3438 1890 1978 1934 3050 3018 2438 3990 2974 3846 3022 2350 2806 3514 3774 3994 3414 1418 2886 2110 1222 3866 1662 3282 3058 1874 3078 2870 3490 4046 1438 3930 3478 3026 1466 1258 3278 4182 994 3030 2998 1394 3538 3034 2834 3410 3894 1902 3350 3150 15754 2978 1458 3466 2934 2902 2910 2930 2498 1898 1866 3202 1702 3978 1098 4286 3810 3510 2434 2374 2338 3486 3694 2962 2562 3518 1410 2082 3166 2206 3330 4010 1142 2286 3206 1310 3070 3218 3042 2538 1658 1378 2818 3558 3370 2706 1834 1330 1430 2982 1238 2862 2238 4018 2198 2926 3530 1938 1650 2454 2866 3730 1498 1714 1870 1826 3274 4030 2366 3938 1494 2786 1114 2178 2354 3962 2906 2090 1930 4134 946 2346 1922 2450 1298 2506 3742 3450 1306 3394 2954 2714 3970 3926 2514 1166 3822 2922 1202 2362 1286 1918 2154 1482 1946 2950 2426 1406 2894 3654 2986 3006 2854 3702 2170 2958 1894 2458 4070 1450 3458 2918 3446 1910 2146 3998 15156 2758 1054 4242 3298 2258 1446 3238 2466 2390 3214 3002 1958 1030 4218 1802 2942 4110 922 2826 2674 2314 2646 2882 2746 1290 4186 3798 1214 3886 2422 1354 2226 3246 2762 3362 2318 1854 3850 2262 2814 1294 3738 3426 1838 3706 2890 1302 2142 2858 2482 990 4178 2878 3470 3758 1906 1706 1230 1434 3038 1046 4234 1366 3634 3946 2358 3826 2306 2218 1170 3354 2442 3090 3714 926 4114 3718 2310 2234 2094 2222 3318 3418 1342 3374 2210 1022 4210 4222 1034 2446 4034 1158 3230 3146 3314 1126 2250 2342 2038 1278 3098 2266 3854 2790 3858 3902 1118 2382 3818 2230 1254 1626 3154 1566 2326 1210 3842 2254 2070 3658 2334 3922 1370 4054 1646 3234 3782 2194 1794 3382 3226 1638 2766 1178 3158 3582 2810 2682 1082 4270 2898 2546 1154 4254 1066 3794 1218 1926 3258 1670 2370 3074 1546 3134 4066 2162 3750 1266 1134 2722 15009 3830 2242 4106 918 4094 2642 1042 4230 962 4150 2730 1542 2190 3386 1798 3290 2186 4074 2686 3250 3578 1246 3402 1814 3762 2174 3126 1538 1686 2182 1078 4266 2678 4278 1090 3726 2138 2078 3666 4190 4202 1014 1790 942 4130 15288 3378 1606 3194 1618 1018 4206 1758 3346 958 4146 2558 3310 2634 2294 3882 1822 2322 3910 1534 3122 2658 4258 1070 1058 4246 2006 3594 2002 3590 1634 3222 3138 1562 1590 3178 3190 1602 3062 3638 2050 4262 1074 2662 1062 4250 1578 2794 1206 2782 1522 938 4126 4138 950 914 4102 3326 1738 2666 3198 1610 3934 1530 3118 3918 2330 2734 1146 3210 1622 2770 3870 2554 954 1130 2718 1594 3182 3586 3342 1754 4238 1050 2638 1038 4226 1718 3602 1742 930 3390 1818 3406 4194 4058 1682 3270 1694 2062 3606 2042 3630 3642 2054 3242 1654 3366 1778 966 4154 2566 3678 3862 2066 2030 3618 3054 1102 4290 2702 1770 3358 1750 3338 2550 3174 1586 1574 3162 1198 2774 2698 1110 1734 3322 2710 1122 3186 1598 3890 2302 2290 3878 1806 2594 2582 982 4170 4274 1086 1026 4214 2614 3286 1698 4098 4062 1334 3710 2122 1710 3066 2726 1138 2750 1162 2738 1106 1094 4282 2694 3482 3614 2838 1250 2874 3686 2098 4122 934 2574 974 4162 2598 4198 1010 3786 2670 2246 2202 3790 3802 2214 3766 1858 3014 3170 1282 3646 2166 3722 2134 1746 3334 2074 3262 1674 2462 4014 4090 3302 1326 2914 1690 1678 3266 3254 4042 1842 3430 1526 2690 970 4158 1442 3746 2158 3734 1358 2946 3574 2822 1234 3598 3398 1810\n")
	require.NoError(t, helper.ComputeTiFlashStatus(bufio.NewReader(strings.NewReader(s2)), &regionReplica3))
	require.Equal(t, 2000, len(regionReplica3))
}

// TestTableRange tests the first part of GetPDRegionStats.
func TestTableRange(t *testing.T) {
	startKey := tablecodec.GenTableRecordPrefix(1)
	endKey := startKey.PrefixNext()
	// t+id+_r
	require.Equal(t, "7480000000000000015f72", startKey.String())
	// t+id+_s
	require.Equal(t, "7480000000000000015f73", endKey.String())

	startKey = tablecodec.EncodeTablePrefix(1)
	endKey = startKey.PrefixNext()
	// t+id
	require.Equal(t, "748000000000000001", startKey.String())
	// t+(id+1)
	require.Equal(t, "748000000000000002", endKey.String())
}
