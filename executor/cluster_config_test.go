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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"

	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/testkit"
)

type testClusterConfigSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testClusterConfigSuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testClusterConfigSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testClusterConfigSuite) TestTiDBClusterConfig(c *C) {
	// mock PD http server
	router := mux.NewRouter()

	type mockServer struct {
		address string
		server  *httptest.Server
	}
	const testServerCount = 3
	var testServers []*mockServer
	for i := 0; i < testServerCount; i++ {
		server := httptest.NewServer(router)
		address := strings.TrimPrefix(server.URL, "http://")
		testServers = append(testServers, &mockServer{
			address: address,
			server:  server,
		})
	}
	defer func() {
		for _, server := range testServers {
			server.server.Close()
		}
	}()

	// We check the counter to valid how many times request has been sent
	var requestCounter int32
	var mockConfig = func() (map[string]interface{}, error) {
		atomic.AddInt32(&requestCounter, 1)
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
		}
		return configuration, nil
	}

	// pd config
	router.Handle(pdapi.Config, fn.Wrap(mockConfig))
	// TiDB/TiKV config
	router.Handle("/config", fn.Wrap(mockConfig))

	// mock servers
	servers := []string{}
	for _, typ := range []string{"tidb", "tikv", "pd"} {
		for _, server := range testServers {
			servers = append(servers, strings.Join([]string{typ, server.address, server.address}, ","))
		}
	}

	fpName := "github.com/pingcap/tidb/executor/mockClusterConfigServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select type, `key`, value from information_schema.cluster_config").Check(testkit.Rows(
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tidb key1 value1",
		"tidb key2.nest1 n-value1",
		"tidb key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"tikv key1 value1",
		"tikv key2.nest1 n-value1",
		"tikv key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
		"pd key1 value1",
		"pd key2.nest1 n-value1",
		"pd key2.nest2 n-value2",
	))
	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
	c.Assert(requestCounter, Equals, int32(9))

	var cases = []struct {
		sql      string
		reqCount int32
	}{
		{
			sql:      "select * from information_schema.cluster_config where type='tikv'",
			reqCount: 3,
		},
		{
			sql:      "select * from information_schema.cluster_config where type='pd'",
			reqCount: 3,
		},
		{
			sql:      "select * from information_schema.cluster_config where type='tidb'",
			reqCount: 3,
		},
		{
			sql:      "select * from information_schema.cluster_config where 'tidb'=type",
			reqCount: 3,
		},
		{
			sql:      "select * from information_schema.cluster_config where type in ('tidb', 'tikv')",
			reqCount: 6,
		},
		{
			sql:      "select * from information_schema.cluster_config where type in ('tidb', 'tikv', 'pd')",
			reqCount: 9,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where address='%s'`,
				testServers[0].address),
			reqCount: 3,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type='tidb' and address='%s'`,
				testServers[0].address),
			reqCount: 1,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and address='%s'`,
				testServers[0].address),
			reqCount: 2,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and address in ('%s', '%s')`,
				testServers[0].address, testServers[0].address),
			reqCount: 2,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and address in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 4,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and type='pd' and address in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 0,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and address in ('%s', '%s') and address='%s'`,
				testServers[0].address, testServers[1].address, testServers[2].address),
			reqCount: 0,
		},
		{
			sql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('tidb', 'tikv') and address in ('%s', '%s') and address='%s'`,
				testServers[0].address, testServers[1].address, testServers[0].address),
			reqCount: 2,
		},
	}

	for _, ca := range cases {
		// reset the request counter
		requestCounter = 0
		tk.MustQuery(ca.sql)
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		c.Assert(requestCounter, Equals, ca.reqCount, Commentf("SQL: %s", ca.sql))
	}
}
