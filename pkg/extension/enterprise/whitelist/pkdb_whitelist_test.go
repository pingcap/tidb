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
	"net"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

const (
	fpWhitelistReloadEnabled = "github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-reload-enable"
	fpWhitelistParseHost     = "github.com/pingcap/tidb/pkg/extension/enterprise/whitelist/mock-whitelist-parse-host"
	fpWhitelistConnHost      = "github.com/pingcap/tidb/pkg/server/mock-whitelist-connection-host"
)

func requireFpDisable(t *testing.T, failpath string) {
	t.Helper()
	err := failpoint.Disable(failpath)
	if err == nil {
		return
	}
	if errors.Cause(err) == failpoint.ErrNotExist {
		return
	}
	require.NoError(t, err)
}

func requireFpEnable(t *testing.T, failpath string, host ...string) {
	t.Helper()
	if len(host) == 0 {
		require.NoError(t, failpoint.Enable(failpath, "return"))
		return
	}
	require.NoError(t, failpoint.Enable(failpath, fmt.Sprintf("return(%q)", host[0])))
}

func requireConnAllowed(t *testing.T, srv *server.Server, host string, expected bool) {
	t.Helper()
	requireFpEnable(t, fpWhitelistConnHost, host)
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	if expected {
		require.True(t, conn.HandleWhiteList(), "host=%s", host)
	} else {
		require.False(t, conn.HandleWhiteList(), "host=%s", host)
	}
}

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
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	// The whitelist validator resolves ConnectionInfo.Host via net.LookupIP.
	// Use a stable IP that is covered by all test CIDRs to avoid depending on mock conn internals.
	conn.Context().GetSessionVars().ConnectionInfo.Host = "172.16.6.95"

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
		requireFpEnable(t, fpWhitelistParseHost, list[1])
		require.True(t, conn.HandleWhiteList())
		tk.MustExec("delete from mysql.whitelist")
	}
	requireFpDisable(t, fpWhitelistParseHost)
}

// Please make failpoint work
func TestConnHandleWhiteList(t *testing.T) {
	Register4Test()
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	tk.MustExec("set global pkdb_whitelist = on")

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
				"11.1.1.1",
			},
			denyIP: []string{
				"171.2.2.2",
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
			allowIP: []string{
				"11.2.2.2",
			},
			denyIP: []string{
				"171.16.6.95",
			},
		},
		// conflict
		{
			ipList: []string{
				"172.16.6.95/24",
				"172.16.6.95/24",
				"171.16.6.95/24",
			},
			action: []string{
				"accept",
				"reject",
				"accept",
			},
			allowIP: []string{
				"171.16.6.95",
			},
			denyIP: []string{
				"172.16.6.95",
			},
		},
		// conflict
		{
			ipList: []string{
				"172.16.6.95/24",
				"172.16.6.95/24",
			},
			action: []string{
				"accept",
				"reject",
			},
			allowIP: []string{
				"171.16.6.95",
			},
			denyIP: []string{
				"172.16.6.95",
			},
		},
	}
	for _, cs := range cases {
		for i := 0; i < len(cs.ipList); i++ {
			tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list, action) values('%s', '%s')", cs.ipList[i], cs.action[i]))
		}

		// allow ip
		for _, ip := range cs.allowIP {
			requireFpEnable(t, fpWhitelistConnHost, ip)
			conn := server.CreateMockConn(t, srv)
			require.True(t, conn.HandleWhiteList(), fmt.Sprintf("case: %#v\nip: %v\n", cs, ip))
			conn.Close()
		}
		// deny ip
		for _, ip := range cs.denyIP {
			requireFpEnable(t, fpWhitelistConnHost, ip)
			conn := server.CreateMockConn(t, srv)
			require.False(t, conn.HandleWhiteList(), fmt.Sprintf("case: %#v\nip: %v\n", cs, ip))
			conn.Close()
		}

		tk.MustExec("delete from mysql.whitelist")
	}
	requireFpDisable(t, fpWhitelistConnHost)
}

func TestConnHandleWhiteList_ActionNull(t *testing.T) {
	Register4Test()
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	tk.MustExec("set global pkdb_whitelist = on")
	tk.MustExec("delete from mysql.whitelist")
	// action=NULL (or invalid) is treated as default action "accept" in code.
	tk.MustExec("insert into mysql.whitelist(list) values('172.16.0.0/16')")
	requireConnAllowed(t, srv, "172.16.1.1", true)
	requireConnAllowed(t, srv, "172.17.1.1", false)
}

func TestConnHandleWhiteList_IPv6(t *testing.T) {
	Register4Test()
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	tk.MustExec("set global pkdb_whitelist = on")

	// whitelist only
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec("insert into mysql.whitelist(list, action) values('2001:db8::/32', 'accept')")
	requireConnAllowed(t, srv, "2001:db8::1", true)
	requireConnAllowed(t, srv, "2001:db9::1", false)

	// blacklist only
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec("insert into mysql.whitelist(list, action) values('2001:db8::/32', 'reject')")
	requireConnAllowed(t, srv, "2001:db8::1", false)
	requireConnAllowed(t, srv, "2001:db9::1", true)

	// mixed: blacklist has higher priority; also verify mixed-mode default allow.
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec("insert into mysql.whitelist(list, action) values('2001:db8::/32', 'accept')")
	tk.MustExec("insert into mysql.whitelist(list, action) values('2001:db8::1/128', 'reject')")
	requireConnAllowed(t, srv, "2001:db8::1", false)
	requireConnAllowed(t, srv, "2001:db9::1", true)
}

func TestConnHandleWhiteList_LocalhostMultiAddress(t *testing.T) {
	Register4Test()
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	ips, err := net.LookupIP("localhost")
	require.NoError(t, err)
	require.NotEmpty(t, ips)

	cidrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			cidrs = append(cidrs, ip4.String()+"/32")
			continue
		}
		cidrs = append(cidrs, ip.String()+"/128")
	}
	list := strings.Join(cidrs, ",")

	tk.MustExec("set global pkdb_whitelist = on")

	// whitelist: allow localhost (covers 127.0.0.1 and/or ::1 depending on resolver).
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list, action) values('%s', 'accept')", list))
	requireConnAllowed(t, srv, "localhost", true)

	// blacklist: deny localhost
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec(fmt.Sprintf("insert into mysql.whitelist(list, action) values('%s', 'reject')", list))
	requireConnAllowed(t, srv, "localhost", false)
}

func TestConnHandleWhiteList_PureIPNoMask(t *testing.T) {
	Register4Test()
	requireFpEnable(t, fpWhitelistReloadEnabled)
	defer requireFpDisable(t, fpWhitelistReloadEnabled)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()

	tk.MustExec("set global pkdb_whitelist = on")

	// Compatibility strategy (current behavior): pure IP without CIDR mask is invalid and ignored.
	// If the only rows are invalid, it is treated as "no rules" => allow all.
	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec("insert into mysql.whitelist(list, action) values('1.1.1.1', 'accept')")
	requireConnAllowed(t, srv, "2.2.2.2", true)

	tk.MustExec("delete from mysql.whitelist")
	tk.MustExec("insert into mysql.whitelist(list, action) values('1.1.1.1', 'reject')")
	requireConnAllowed(t, srv, "1.1.1.1", true)
}
