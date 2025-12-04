// Copyright 2023-2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package whitelist

import (
	"fmt"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWhiteListTable(t *testing.T) {
	Register4Test()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	tk := testkit.NewTestKit(t, store)
	// table is empty after init
	tk.MustQuery("select * from mysql.whitelist").Check(testkit.Rows())
	// name is less than 16
	tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(name) values('%s')", "1234567890123456"))
	tk.MustGetErrMsg(fmt.Sprintf("insert into mysql.whitelist(name) values('%s')", "12345678901234567"), "[types:1406]Data too long for column 'name' at row 1")
	// name is unique
	tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(name) values('%s')", "unique"))
	tk.MustGetErrMsg(fmt.Sprintf("insert into mysql.whitelist(name) values('%s')", "unique"), "[kv:1062]Duplicate entry 'unique' for key 'whitelist.name'")
	// list column is less than 65535
	var l65535, l65536 string
	for i := 0; i < 65535; i++ {
		l65535 += "1"
	}
	l65536 = l65535 + "1"
	tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list) values('%s')", l65535))
	tk.MustGetErrMsg(fmt.Sprintf("insert into mysql.whitelist(list) values('%s')", l65536), "[types:1406]Data too long for column 'list' at row 1")
	// besides id, other columns are allowed to null
	tk.MustExec("insert into mysql.whitelist values();")
}

func TestParseIPListFromTable(t *testing.T) {
	Register4Test()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-reload-enable", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-reload-enable"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	lists := [][]string{
		{"172.16.6.95/8", "172.0.0.0/8"},
		{"172.16.6.95/16", "172.16.0.0/16"},
		{"172.16.6.95/24", "172.16.6.0/24"},
		{"172.16.6.95/32", "172.16.6.95/32"},
		{"1.1.1.1/32,172.16.6.95/32", "172.16.6.95/32"},
		{"1.1.1.1/32, 172.16.6.95/32", "172.16.6.95/32"},
	}
	for _, list := range lists {
		tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list, action) values('%s', 'accept')", list[0]))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-parse-host",
			fmt.Sprintf(`return("%s")`, list[1])))
		require.True(t, conn.HandleWhiteList())
		tk.MustExec("delete from mysql.whitelist")
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-parse-host"))
}

func TestConnHandleWhiteList(t *testing.T) {
	Register4Test()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/mock-whitelist-config-enabled", `return`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-reload-enable", `return`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/mock-whitelist-config-enabled"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-reload-enable"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	cases := []struct {
		ipList  []string
		action  []string
		allowIP []string
		denyIP  []string
	}{
		// allow
		{
			ipList: []string{},
			action: []string{},
			allowIP: []string{
				"12.16.6.6",
			},
		},
		{
			ipList: []string{
				"172.16.6.95/8",
				"171.16.6.95/8",
			},
			action: []string{
				"accept",
				"reject",
			},
			allowIP: []string{
				"172.16.6.95",
				"172.1.6.95",
			},
			denyIP: []string{
				"171.2.2.2",
				"11.1.1.1",
			},
		},
		{
			ipList: []string{
				"172.16.6.95/16",
			},
			action: []string{
				"accept",
			},
			allowIP: []string{
				"172.16.1.6",
			},
			denyIP: []string{
				"172.2.2.2",
			},
		},
		{
			ipList: []string{
				"172.16.6.95/24",
			},
			action: []string{
				"accept",
			},
			allowIP: []string{
				"172.16.6.6",
			},
			denyIP: []string{
				"172.16.7.2",
			},
		},
		{
			ipList: []string{
				"172.16.6.95/32",
			},
			action: []string{
				"accept",
			},
			allowIP: []string{
				"172.16.6.95",
			},
			denyIP: []string{
				"172.16.6.96",
			},
		},
		{
			ipList: []string{
				"172.16.6.95/32",
				"172.16.6.95/24",
			},
			action: []string{
				"accept",
				"accept",
			},
			allowIP: []string{
				"172.16.6.6",
			},
			denyIP: []string{
				"172.16.5.96",
			},
		},
		// deny
		{
			ipList: []string{
				"171.16.6.95/8",
			},
			action: []string{
				"reject",
			},

			denyIP: []string{
				"171.16.6.95",
				"11.2.2.2",
			},
		},
	}
	for _, cs := range cases {
		for i := 0; i < len(cs.ipList); i++ {
			tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list, action) values('%s', '%s')", cs.ipList[i], cs.action[i]))
		}

		// allow ip
		for _, ip := range cs.allowIP {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/mock-whitelist-connection-host",
				fmt.Sprintf(`return("%s")`, ip)))
			conn := server.CreateMockConn(t, srv)
			require.True(t, conn.HandleWhiteList())
			conn.Close()
		}
		// deny ip
		for _, ip := range cs.denyIP {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/server/mock-whitelist-connection-host",
				fmt.Sprintf(`return("%s")`, ip)))
			conn := server.CreateMockConn(t, srv)
			require.False(t, conn.HandleWhiteList())
			conn.Close()
		}

		tk.MustExec("delete from mysql.whitelist")
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/server/mock-whitelist-connection-host"))
}
