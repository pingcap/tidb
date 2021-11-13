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

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	kv "github.com/pingcap/tidb/br/pkg/lightning/tikv"
)

type tikvSuite struct{}

var _ = Suite(&tikvSuite{})

var (
	// Samples from importer backend for testing the Check***Version functions.
	// No need keep these versions in sync.
	requiredMinPDVersion   = *semver.New("2.1.0")
	requiredMinTiKVVersion = *semver.New("2.1.0")
	requiredMaxPDVersion   = *semver.New("6.0.0")
	requiredMaxTiKVVersion = *semver.New("6.0.0")
)

func (s *tikvSuite) TestForAllStores(c *C) {
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
		c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)

	sort.Slice(allStores, func(i, j int) bool { return allStores[i].Address < allStores[j].Address })

	c.Assert(allStores, DeepEquals, []*kv.Store{
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
	})
}

func (s *tikvSuite) TestFetchModeFromMetrics(c *C) {
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
		comment := Commentf("test case '%s'", tc.metrics)
		mode, err := kv.FetchModeFromMetrics(tc.metrics)
		if tc.isErr {
			c.Assert(err, NotNil, comment)
		} else {
			c.Assert(err, IsNil, comment)
			c.Assert(mode, Equals, tc.mode, comment)
		}
	}
}

func (s *tikvSuite) TestCheckPDVersion(c *C) {
	var version string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/pd/api/v1/version")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(version))
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	tls := common.NewTLSFromMockServer(mockServer)

	version = `{
    "version": "v4.0.0-rc.2-451-g760fb650"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), IsNil)

	version = `{
    "version": "v4.0.0"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), IsNil)

	version = `{
    "version": "v9999.0.0"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), ErrorMatches, "PD version too new.*")

	version = `{
    "version": "v6.0.0"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), ErrorMatches, "PD version too new.*")

	version = `{
    "version": "v6.0.0-beta"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), ErrorMatches, "PD version too new.*")

	version = `{
    "version": "v1.0.0"
}`
	c.Assert(kv.CheckPDVersion(ctx, tls, mockURL.Host, requiredMinPDVersion, requiredMaxPDVersion), ErrorMatches, "PD version too old.*")
}

func (s *tikvSuite) TestCheckTiKVVersion(c *C) {
	var versions []string
	ctx := context.Background()

	mockServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c.Assert(req.URL.Path, Equals, "/pd/api/v1/stores")
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
		c.Assert(err, IsNil)
	}))
	mockURL, err := url.Parse(mockServer.URL)
	c.Assert(err, IsNil)

	tls := common.NewTLSFromMockServer(mockServer)

	versions = []string{"4.1.0", "v4.1.0-alpha-9-ga27a7dd"}
	c.Assert(kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion), IsNil)

	versions = []string{"9999.0.0", "4.0.0"}
	c.Assert(kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion), ErrorMatches, `TiKV \(at tikv0\.test:20160\) version too new.*`)

	versions = []string{"4.0.0", "1.0.0"}
	c.Assert(kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion), ErrorMatches, `TiKV \(at tikv1\.test:20160\) version too old.*`)

	versions = []string{"6.0.0"}
	c.Assert(kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion), ErrorMatches, `TiKV \(at tikv0\.test:20160\) version too new.*`)

	versions = []string{"6.0.0-beta"}
	c.Assert(kv.CheckTiKVVersion(ctx, tls, mockURL.Host, requiredMinTiKVVersion, requiredMaxTiKVVersion), ErrorMatches, `TiKV \(at tikv0\.test:20160\) version too new.*`)
}
