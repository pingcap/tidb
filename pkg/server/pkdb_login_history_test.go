// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

import (
	"context"
	"testing"

	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestInsertLoginRecords(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn := CreateMockConn(t, sv)
	sctx := conn.Context().Session
	tk := testkit.NewTestKitWithSession(t, store, sctx)
	//se := session.NewSession(sctx, sctx, func(_ session.Session) {})

	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 0)

	c, ok := conn.(*mockConn)
	require.True(t, ok)

	// insert success record.
	user := c.ctx.GetSessionVars().User
	user.AuthUsername, user.AuthHostname = user.Username, user.Hostname
	err := insertLoginRecords(context.TODO(), c.clientConn, user, nil)
	require.Nil(t, err, "connection_id: %v", c.clientConn.connectionID)

	// insert success record with another connections.
	conn2 := CreateMockConn(t, sv)
	c2, ok := conn2.(*mockConn)
	require.True(t, ok)
	user = c2.ctx.GetSessionVars().User
	user.AuthUsername, user.AuthHostname = user.Username, user.Hostname
	err = insertLoginRecords(context.TODO(), c2.clientConn, user, nil)
	require.Nil(t, err, "connection_id: %v", c2.clientConn.connectionID)

	// insert a failure records.
	conn3 := CreateMockConn(t, sv)
	c3, ok := conn3.(*mockConn)
	require.True(t, ok)
	user = c2.ctx.GetSessionVars().User
	user.AuthUsername, user.AuthHostname = user.Username, user.Hostname
	result := servererr.ErrTooManyUserConnections.GenWithStackByArgs(user.Username)
	err = insertLoginRecords(context.TODO(), c3.clientConn, user, result)
	require.Nil(t, err, "connection_id: %v", c3.clientConn.connectionID)

	// select all of records from mysql.login_history.
	rows = tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 3)

	// select user records from information_schema.user_login_history.
	rows = tk.MustQuery("SELECT * FROM information_schema.user_login_history WHERE `result` = \"success\"").Rows()
	require.Len(t, rows, 2)

	rows = tk.MustQuery("SELECT * FROM information_schema.user_login_history WHERE `result` = \"fail\"").Rows()
	require.Len(t, rows, 1)
}

func TestInsertLoginHistoryTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := CreateMockServer(t, store)
	sv.SetDomain(dom)
	defer sv.Close()

	conn := CreateMockConn(t, sv)
	sctx := conn.Context().Session
	tk := testkit.NewTestKitWithSession(t, store, sctx)

	c, ok := conn.(*mockConn)
	require.True(t, ok)

	// the default value of variable.EnableLoginHistory is false.
	user := c.ctx.GetSessionVars().User
	user.AuthUsername, user.AuthHostname = user.Username, user.Hostname
	err := insertLoginHistoryTable(context.TODO(), c.clientConn, user, nil)
	require.Nil(t, err, "connection_id: %v", c.clientConn.connectionID)
	rows := tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 0)

	// variable.EnableLoginHistory = true
	variable.EnableLoginHistory.Store(true)
	err = insertLoginHistoryTable(context.TODO(), c.clientConn, user, nil)
	require.Nil(t, err, "connection_id: %v", c.clientConn.connectionID)
	rows = tk.MustQuery("SELECT * FROM mysql.login_history").Rows()
	require.Len(t, rows, 1)
}
