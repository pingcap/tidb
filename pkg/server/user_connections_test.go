// Copyright 2025 PingCAP, Inc.
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

package server

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestUserConnectionCount(t *testing.T) {
	cfg := util.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	store := testkit.CreateMockStore(t)
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER `zak`@`%`")

	newConn := func(host string) *clientConn {
		cc := &clientConn{
			connectionID: 1,
			alloc:        arena.NewAllocator(1024),
			chunkAlloc:   chunk.NewAllocator(),
			collation:    mysql.DefaultCollationID,
			peerHost:     "localhost",
			pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
			server:       srv,
			user:         "zak",
		}
		cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
		cc.ctx.GetSessionVars().User = &auth.UserIdentity{
			Username:     "zak",
			Hostname:     host,
			AuthUsername: "zak",
			AuthHostname: "%",
		}
		return cc
	}

	checkCount := func(expected int) {
		require.Equal(t, expected, newConn("127.0.0.1").getUserConnectionCount(&auth.UserIdentity{
			Username:     "zak",
			Hostname:     "127.0.0.1",
			AuthUsername: "zak",
			AuthHostname: "%",
		}))
	}

	acquireMany := func(hosts ...string) []*clientConn {
		conns := make([]*clientConn, 0, len(hosts))
		for _, host := range hosts {
			cc := newConn(host)
			require.NoError(t, cc.increaseUserConnectionsCount())
			conns = append(conns, cc)
		}
		return conns
	}
	closeAll := func(conns []*clientConn) {
		for _, cc := range conns {
			cc.decreaseUserConnectionCount()
		}
	}

	// test1: global MAX_USER_CONNECTIONS = 0 and max_user_connections = 0 in mysql.user for `zak`@`%`
	conns := acquireMany("127.0.0.1", "127.0.0.1", "127.0.0.1")
	checkCount(3)
	closeAll(conns)
	checkCount(0)

	// test2: global MAX_USER_CONNECTIONS = 0 and max_user_connections = 2 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 2")
	conns = acquireMany("127.0.0.1", "127.0.0.1")
	require.ErrorContains(t, newConn("127.0.0.1").increaseUserConnectionsCount(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	checkCount(2)
	closeAll(conns)
	checkCount(0)

	// test3: global MAX_USER_CONNECTIONS = 3 and max_user_connections = 0 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 0")
	tk.MustExec("set global max_user_connections = 3")
	conns = acquireMany("127.0.0.1", "127.0.0.1", "127.0.0.1")
	require.ErrorContains(t, newConn("127.0.0.1").increaseUserConnectionsCount(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	checkCount(3)
	closeAll(conns)
	checkCount(0)

	// test4: global MAX_USER_CONNECTIONS = 2 and max_user_connections = 3 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 3")
	tk.MustExec("set global max_user_connections = 2")
	conns = acquireMany("127.0.0.1", "127.0.0.1", "127.0.0.1")
	require.ErrorContains(t, newConn("127.0.0.1").increaseUserConnectionsCount(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	checkCount(3)
	closeAll(conns)
	checkCount(0)

	// test5: global MAX_USER_CONNECTIONS = 3 and max_user_connections = 2 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 2")
	tk.MustExec("set global max_user_connections = 3")
	conns = acquireMany("127.0.0.1", "127.0.0.1")
	require.ErrorContains(t, newConn("127.0.0.1").increaseUserConnectionsCount(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	checkCount(2)
	closeAll(conns)
	checkCount(0)

	// test6: zak@127.0.0.1 and zak@127.0.0.2 share the same zak@% limit.
	tk.MustExec("set global max_user_connections = 0")
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 3")
	conns = acquireMany("127.0.0.1", "127.0.0.1", "127.0.0.2")
	require.ErrorContains(t, newConn("127.0.0.2").increaseUserConnectionsCount(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	checkCount(3)
	closeAll(conns)
	checkCount(0)
}

func TestUserConnectionCountTracksOriginalIdentityAcrossChangeUser(t *testing.T) {
	cfg := util.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	store := testkit.CreateMockStore(t)
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER `zak`@`%` WITH MAX_USER_CONNECTIONS 1")
	tk.MustExec("CREATE USER `other`@`%`")

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "zak",
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
	cc.ctx.GetSessionVars().User = &auth.UserIdentity{
		Username:     "zak",
		Hostname:     "127.0.0.1",
		AuthUsername: "zak",
		AuthHostname: "%",
	}

	require.NoError(t, cc.increaseUserConnectionsCount())
	require.Equal(t, 1, cc.getUserConnectionCount(&auth.UserIdentity{Username: "zak", Hostname: "127.0.0.1", AuthUsername: "zak", AuthHostname: "%"}))

	cc.ctx.GetSessionVars().User = &auth.UserIdentity{
		Username:     "other",
		Hostname:     "127.0.0.1",
		AuthUsername: "other",
		AuthHostname: "%",
	}
	cc.decreaseUserConnectionCount()
	require.Equal(t, 0, cc.getUserConnectionCount(&auth.UserIdentity{Username: "zak", Hostname: "127.0.0.1", AuthUsername: "zak", AuthHostname: "%"}))
	require.Empty(t, cc.countedUser)
}
