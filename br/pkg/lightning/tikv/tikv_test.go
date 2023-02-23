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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	kv "github.com/pingcap/tidb/br/pkg/lightning/tikv"
	"github.com/stretchr/testify/require"
)

var (
	// Samples from importer backend for testing the Check***Version functions.
	// No need keep these versions in sync.
	requiredMinPDVersion   = *semver.New("2.1.0")
	requiredMinTiKVVersion = *semver.New("2.1.0")
	requiredMaxPDVersion   = *semver.New("6.0.0")
	requiredMaxTiKVVersion = *semver.New("6.0.0")
)

func TestForAllStores(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, err := w.Write([]byte(`
			{
				"count": 5,
				"stores": [
					{
						"store": {
							"id": 1,
							"address": "127.0.0.1:20160",
							"version": "3.0.0-beta.1",
							"state_name": "Up"
						},
						"status": {}
					},
					{
						"store": {
							"id": 2,
							"address": "127.0.0.1:20161",
							"version": "3.0.0-rc.1",
							"state_name": "Down"
						},
						"status": {}
					},
					{
						"store": {
							"id": 3,
							"address": "127.0.0.1:20162",
							"version": "3.0.0-rc.2",
							"state_name": "Disconnected"
						},
						"status": {}
					},
					{
						"store": {
							"id": 4,
							"address": "127.0.0.1:20163",
							"version": "3.0.0",
							"state_name": "Tombstone"
						},
						"status": {}
					},
					{
						"store": {
							"id": 5,
							"address": "127.0.0.1:20164",
							"version": "3.0.1",
							"state_name": "Offline"
						},
						"status": {}
					}
				]
			}
		`))
		require.NoError(t, err)
	}))
	defer server.Close()

	ctx := context.Background()
	var (
		allStoresLock sync.Mutex
		allStores     []*kv.Store
	)
	tls := common.NewTLSFromMockServer(server)
	err := kv.ForAllStores(ctx, tls, kv.StoreStateDown, func(c2 context.Context, store *kv.Store) error {
		allStoresLock.Lock()
		allStores = append(allStores, store)
		allStoresLock.Unlock()
		return nil
	})
	require.NoError(t, err)

	sort.Slice(allStores, func(i, j int) bool { return allStores[i].Address < allStores[j].Address })
	require.Equal(t, []*kv.Store{
		{
			Address: "127.0.0.1:20160",
			Version: "3.0.0-beta.1",
			State:   kv.StoreStateUp,
		},
		{
			Address: "127.0.0.1:20161",
			Version: "3.0.0-rc.1",
			State:   kv.StoreStateDown,
		},
		{
			Address: "127.0.0.1:20162",
			Version: "3.0.0-rc.2",
			State:   kv.StoreStateDisconnected,
		},
		{
			Address: "127.0.0.1:20164",
			Version: "3.0.1",
			State:   kv.StoreStateOffline,
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

func TestCheckPDVersion(t *testing.T) {
	var version string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/pd/api/v1/version", req.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(version))
		require.NoError(t, err)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	require.NoError(t, err)

	tls := common.NewTLSFromMockServer(mockServer)

	version = `{
    "version": "v4.0.0-rc.2-451-g760fb650"
}`
	require.NoError(t, kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion))

	version = `{
    "version": "v4.0.0"
}`
	require.NoError(t, kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion))

	version = `{
    "version": "v9999.0.0"
}`
	err = kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	version = `{
    "version": "v6.0.0"
}`
	err = kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	version = `{
    "version": "v6.0.0-beta"
}`
	err = kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too new.*", err.Error())

	version = `{
    "version": "v1.0.0"
}`
	err = kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, "PD version too old.*", err.Error())
}

func TestCheckTiKVVersion(t *testing.T) {
	var versions []string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, "/pd/api/v1/stores", req.URL.Path)
		w.WriteHeader(http.StatusOK)

		stores := make([]map[string]interface{}, 0, len(versions))
		for i, v := range versions {
			stores = append(stores, map[string]interface{}{
				"store": map[string]interface{}{
					"address": fmt.Sprintf("tikv%d.test:20160", i),
					"version": v,
				},
			})
		}
		err := json.NewEncoder(w).Encode(map[string]interface{}{
			"count":  len(versions),
			"stores": stores,
		})
		require.NoError(t, err)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	require.NoError(t, err)

	tls := common.NewTLSFromMockServer(mockServer)

	versions = []string{"4.1.0", "v4.1.0-alpha-9-ga27a7dd"}
	require.NoError(t, kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion))

	versions = []string{"9999.0.0", "4.0.0"}
	err = kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())

	versions = []string{"4.0.0", "1.0.0"}
	err = kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv1\.test:20160\) version too old.*`, err.Error())

	versions = []string{"6.0.0"}
	err = kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())

	versions = []string{"6.0.0-beta"}
	err = kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion)
	require.Error(t, err)
	require.Regexp(t, `TiKV \(at tikv0\.test:20160\) version too new.*`, err.Error())
}
