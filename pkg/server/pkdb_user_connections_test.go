// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

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
