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
	"github.com/pingcap/tidb/pkg/server/internal/packetio"
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

	// The global config is read during creating the store.
	store := testkit.CreateMockStore(t)
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER `zak`@`%`")

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          packetio.NewForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "zak",
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
	sess := cc.ctx.GetSessionVars()
	sess.User = &auth.UserIdentity{
		Username:     "zak",
		Hostname:     "127.0.0.1",
		AuthUsername: "zak",
		AuthHostname: "%",
	}

	var releases []func()
	acquireConnection := func() error {
		release, err := cc.increaseUserConnectionsCount()
		if err == nil {
			releases = append(releases, release)
		}
		return err
	}
	releaseConnection := func() {
		last := len(releases) - 1
		releases[last]()
		releases = releases[:last]
	}

	// test1: global MAX_USER_CONNECTIONS = 0 and max_user_conections = 0 in mysql.user for `zak`@`%`
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.Equal(t, cc.getUserConnectionCount(sess.User), 3)
	releaseConnection()
	releaseConnection()
	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	// test2: global MAX_USER_CONNECTIONS = 0 and max_user_conections = 2 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 2")
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.ErrorContains(t, acquireConnection(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	require.Equal(t, cc.getUserConnectionCount(sess.User), 2)
	releaseConnection()

	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	// test3: global MAX_USER_CONNECTIONS = 3 and max_user_conections = 0 in mysql.user for `zak`@`%`
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 0")
	tk.MustExec("set global max_user_connections = 3")
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.ErrorContains(t, acquireConnection(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	require.Equal(t, cc.getUserConnectionCount(sess.User), 3)
	releaseConnection()
	releaseConnection()
	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	// test4: global MAX_USER_CONNECTIONS = 2 and max_user_conections = 3 in mysql.user for `zak`@`%`
	// the max_user_connection attribute has a higher priority than global MAX_USER_CONNECTIONS.
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 3")
	tk.MustExec("set global max_user_connections = 2")
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.ErrorContains(t, acquireConnection(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	require.Equal(t, cc.getUserConnectionCount(sess.User), 3)
	releaseConnection()
	releaseConnection()
	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	// test5: global MAX_USER_CONNECTIONS = 3 and max_user_conections = 2 in mysql.user for `zak`@`%`
	// the max_user_connection attribute has a higher priority than global MAX_USER_CONNECTIONS.
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 2")
	tk.MustExec("set global max_user_connections = 3")
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	require.ErrorContains(t, acquireConnection(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	require.Equal(t, cc.getUserConnectionCount(sess.User), 2)
	releaseConnection()
	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	// test6: the user zak@127.0.0.1 and zak@127.0.0.2 have same limit(zak@127.0.0.1 + zap@127.0.0.2 <= 3).
	tk.MustExec("set global max_user_connections = 0")
	tk.MustExec("ALTER USER `zak`@`%` WITH MAX_USER_CONNECTIONS 3")
	require.NoError(t, acquireConnection())
	require.NoError(t, acquireConnection())
	sess2 := cc.ctx.GetSessionVars()
	sess2.User = &auth.UserIdentity{
		Username:     "zak",
		Hostname:     "127.0.0.2",
		AuthUsername: "zak",
		AuthHostname: "%",
	}
	require.NoError(t, acquireConnection())
	require.ErrorContains(t, acquireConnection(),
		"[server:1203]User zak@% has exceeded the 'max_user_connections' resource")
	require.Equal(t, cc.getUserConnectionCount(sess.User), 3)
	releaseConnection()
	releaseConnection()
	releaseConnection()
	require.Equal(t, cc.getUserConnectionCount(sess.User), 0)

	t.Run("release retains the original account after changing user", func(t *testing.T) {
		tk.MustExec("CREATE USER `other`@`%`")
		originalUser := sess.User
		releaseOriginal, err := cc.increaseUserConnectionsCount()
		require.NoError(t, err)

		sess.User = &auth.UserIdentity{
			Username:     "other",
			Hostname:     "127.0.0.1",
			AuthUsername: "other",
			AuthHostname: "%",
		}
		releaseOther, err := cc.increaseUserConnectionsCount()
		require.NoError(t, err)
		require.Equal(t, 1, cc.getUserConnectionCount(originalUser))
		require.Equal(t, 1, cc.getUserConnectionCount(sess.User))

		releaseOriginal()
		require.Equal(t, 0, cc.getUserConnectionCount(originalUser))
		require.Equal(t, 1, cc.getUserConnectionCount(sess.User))
		releaseOther()
		require.Equal(t, 0, cc.getUserConnectionCount(sess.User))
	})
}
