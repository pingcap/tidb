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

package tikv_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	kv "github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

var (
	// Samples from importer backend for testing the Check***Version functions.
	// No need keep these versions in sync.
	requiredMinPDVersion   = *semver.New("2.1.0")
	requiredMinTiKVVersion = *semver.New("2.1.0")
	requiredMaxPDVersion   = *semver.New("6.0.0")
	requiredMaxTiKVVersion = *semver.New("6.0.0")
)

type mockGetStoresCli struct {
	pdhttp.Client
	storesInfo *pdhttp.StoresInfo
}

func (c mockGetStoresCli) GetStores(context.Context) (*pdhttp.StoresInfo, error) {
	return c.storesInfo, nil
}

func TestForAllStores(t *testing.T) {
	cli := mockGetStoresCli{}
	cli.storesInfo = &pdhttp.StoresInfo{
		Count: 3,
		Stores: []pdhttp.StoreInfo{
			{
				Store: pdhttp.MetaStore{
					ID:      1,
					Address: "127.0.0.1:20160",
					Version: "3.0.0-beta.1",
					State:   int64(metapb.StoreState_Up),
				},
			},
			{
				Store: pdhttp.MetaStore{
					ID:      5,
					Address: "127.0.0.1:20164",
					Version: "3.0.1",
					State:   int64(metapb.StoreState_Offline),
				},
			},
			{
				Store: pdhttp.MetaStore{
					ID:      4,
					Address: "127.0.0.1:20163",
					Version: "3.0.0",
					State:   int64(metapb.StoreState_Tombstone),
				},
			},
		},
	}

	ctx := context.Background()
	var (
		allStoresLock sync.Mutex
		allStores     []*pdhttp.MetaStore
	)
	err := kv.ForAllStores(ctx, cli, metapb.StoreState_Offline, func(c2 context.Context, store *pdhttp.MetaStore) error {
		allStoresLock.Lock()
		allStores = append(allStores, store)
		allStoresLock.Unlock()
		return nil
	})
	require.NoError(t, err)

	sort.Slice(allStores, func(i, j int) bool { return allStores[i].Address < allStores[j].Address })
	require.Equal(t, []*pdhttp.MetaStore{
		{
			ID:      1,
			Address: "127.0.0.1:20160",
			Version: "3.0.0-beta.1",
			State:   int64(metapb.StoreState_Up),
		},
		{
			ID:      5,
			Address: "127.0.0.1:20164",
			Version: "3.0.1",
			State:   int64(metapb.StoreState_Offline),
		},
	}, allStores)
}

func TestFetchModeFromMetrics(t *testing.T) {
	testCases := []struct {
		metrics string
		mode    import_sstpb.SwitchMode
		isErr   bool
	}{
		{
			metrics: `tikv_config_rocksdb{cf="default",name="hard_pending_compaction_bytes_limit"} 274877906944`,
			mode:    import_sstpb.SwitchMode_Normal,
		},
		{
			metrics: `tikv_config_rocksdb{cf="default",name="hard_pending_compaction_bytes_limit"} 0`,
			mode:    import_sstpb.SwitchMode_Import,
		},
		{
			metrics: ``,
			isErr:   true,
		},
	}

	for _, tc := range testCases {
		comment := fmt.Sprintf("test case '%s'", tc.metrics)
		mode, err := kv.FetchModeFromMetrics(tc.metrics)
		if tc.isErr {
			require.Error(t, err, comment)
		} else {
			require.NoError(t, err, comment)
			require.Equal(t, tc.mode, mode, comment)
		}
	}
}

type mockGetPDVersionCli struct {
	pdhttp.Client
	version string
}

func (c mockGetPDVersionCli) GetPDVersion(context.Context) (string, error) {
	return c.version, nil
}

func TestCheckPDVersion(t *testing.T) {
	ctx := context.Background()
	cli := mockGetPDVersionCli{}

	cli.version = "v4.0.0-rc.2-451-g760fb650"
	require.NoError(t, kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion))

	cli.version = "v4.0.0"
	require.NoError(t, kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion))

	cli.version = "v9999.0.0"
	err := kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	cli.version = "v6.0.0"
	err = kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	cli.version = "v6.0.0-beta"
	err = kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	cli.version = "v1.0.0"
	err = kv.CheckPDVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too old.*", err.Error())
}

func TestCheckTiKVVersion(t *testing.T) {
	ctx := context.Background()
	cli := mockGetStoresCli{}

	genStoresInfo := func(versions []string) *pdhttp.StoresInfo {
		stores := make([]pdhttp.StoreInfo, 0, len(versions))
		for i, v := range versions {
			stores = append(stores, pdhttp.StoreInfo{
				Store: pdhttp.MetaStore{
					Address: fmt.Sprintf("tikv%d.test:20160", i),
					Version: v,
				},
			})
		}
		return &pdhttp.StoresInfo{
			Count:  len(versions),
			Stores: stores,
		}
	}

	versions := []string{"4.1.0", "v4.1.0-alpha-9-ga27a7dd"}
	cli.storesInfo = genStoresInfo(versions)
	require.NoError(t, kv.CheckTiKVVersion(ctx, cli, requiredMinTiKVVersion, requiredMaxTiKVVersion))

	versions = []string{"9999.0.0", "4.0.0"}
	cli.storesInfo = genStoresInfo(versions)
	err := kv.CheckTiKVVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())

	versions = []string{"4.0.0", "1.0.0"}
	cli.storesInfo = genStoresInfo(versions)
	err = kv.CheckTiKVVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv1\.test:20160\) version too old.*`, err.Error())

	versions = []string{"6.0.0"}
	cli.storesInfo = genStoresInfo(versions)
	err = kv.CheckTiKVVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())

	versions = []string{"6.0.0-beta"}
	cli.storesInfo = genStoresInfo(versions)
	err = kv.CheckTiKVVersion(ctx, cli, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())
}
