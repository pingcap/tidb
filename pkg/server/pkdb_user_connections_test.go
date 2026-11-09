// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestUser_connections_counter(t *testing.T) {
	cfg := util.NewTestConfig()
	cfg.Port = 0
	cfg.Status.StatusPort = 0

	// The global config is read during creating the store.
	store := testkit.CreateMockStore(t)
	drv := NewTiDBDriver(store)
	srv, err := NewServer(cfg, drv)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER auth_session_token")
	tk.MustExec("CREATE USER another_user")

	cc := &clientConn{
		connectionID: 1,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		collation:    mysql.DefaultCollationID,
		peerHost:     "localhost",
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		server:       srv,
		user:         "auth_session_token",
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
	sess := cc.ctx.GetSessionVars()
	sess.User = &auth.UserIdentity{
		Username:     "zak",
		Hostname:     "127.0.0.1",
		AuthUsername: "zak",
		AuthHostname: "127.0.0.1",
	}

	cc.incrementUserConnectionsCounter()
	cc.incrementUserConnectionsCounter()

	targetUser := sess.User.AuthUsername + sess.User.AuthHostname
	conns := cc.getUserConnectionsCounter(targetUser)
	require.Equal(t, conns, 2)

	cc.decrementUserConnectionsCounter()
	cc.decrementUserConnectionsCounter()
	conns = cc.getUserConnectionsCounter(targetUser)
	require.Equal(t, conns, 0)
}

func TestCheckConnectionCountWithReservedAdminConnections(t *testing.T) {
	cfg := util.NewTestConfig()
	cfg.Instance.MaxConnections = 2
	cfg.Instance.ReservedAdminConnections = 1
	server := &Server{cfg: cfg, clients: make(map[uint64]*clientConn)}
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))

	newConn := func(id uint64, user string) *clientConn {
		conn := &clientConn{connectionID: id, server: server, user: user}
		if user == "root" {
			conn.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
			conn.ctx.GetSessionVars().User = &auth.UserIdentity{AuthUsername: user}
		}
		return conn
	}

	for _, conn := range []*clientConn{newConn(1, "user"), newConn(2, "user")} {
		require.NoError(t, server.checkReservedAdminConnection(conn))
		server.clients[conn.connectionID] = conn
	}

	require.True(t, terror.ErrorEqual(server.checkReservedAdminConnection(newConn(3, "user")), servererr.ErrConCount))
	require.NoError(t, server.checkConnectionCount())
	admin := newConn(4, "root")
	server.cfg.Instance.ReservedAdminConnections = 0
	require.True(t, terror.ErrorEqual(server.checkConnectionCount(), servererr.ErrConCount))
	require.True(t, terror.ErrorEqual(server.checkReservedAdminConnection(admin), servererr.ErrConCount))
	server.cfg.Instance.ReservedAdminConnections = 1
	require.NoError(t, server.checkReservedAdminConnection(admin))
	server.clients[admin.connectionID] = admin
	require.True(t, terror.ErrorEqual(server.checkConnectionCount(), servererr.ErrConCount))
	require.True(t, terror.ErrorEqual(server.checkReservedAdminConnection(newConn(5, "root")), servererr.ErrConCount))
}

func TestCheckConnectionCountUnlimited(t *testing.T) {
	server := &Server{cfg: util.NewTestConfig(), clients: make(map[uint64]*clientConn)}
	server.cfg.Instance.MaxConnections = 0
	for i := uint64(1); i <= 3; i++ {
		conn := &clientConn{connectionID: i, server: server, user: "user"}
		require.NoError(t, server.checkReservedAdminConnection(conn))
		server.clients[conn.connectionID] = conn
	}
	require.NoError(t, server.checkConnectionCount())
}
