// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"bufio"
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

// MockConn is a mock connection.
type MockConn interface {
	// HandleQuery executes a statement
	HandleQuery(ctx context.Context, sql string) error
	// Context gets the TiDBContext
	Context() *TiDBContext
	// Dispatch executes command according to the command type
	Dispatch(ctx context.Context, data []byte) error
	// Close releases resources
	Close()
	// ID returns the connection ID.
	ID() uint64
	// GetOutbound replaces the internal outbound endpoint with a empty buffer, and return it
	GetOutput() *bytes.Buffer
}

type mockConn struct {
	*clientConn
	t *testing.T
}

// HandleQuery implements MockConn.HandleQuery
func (mc *mockConn) HandleQuery(ctx context.Context, sql string) error {
	return mc.handleQuery(ctx, sql)
}

// Context implements MockConn.Context
func (mc *mockConn) Context() *TiDBContext {
	return mc.getCtx()
}

// Dispatch implements MockConn.Dispatch
func (mc *mockConn) Dispatch(ctx context.Context, data []byte) error {
	return mc.dispatch(ctx, data)
}

// Close implements MockConn.Close
func (mc *mockConn) Close() {
	require.NoError(mc.t, mc.clientConn.Close())
}

// ID implements MockConn.ID
func (mc *mockConn) ID() uint64 {
	return mc.clientConn.connectionID
}

// GetOutput implements MockConn.GetOutbound
func (mc *mockConn) GetOutput() *bytes.Buffer {
	buf := bytes.NewBuffer([]byte{})
	mc.clientConn.pkt.SetBufWriter(bufio.NewWriter(buf))

	return buf
}

// CreateMockServer creates a mock server.
func CreateMockServer(t *testing.T, store kv.Storage) *Server {
	if !RunInGoTest {
		// If CreateMockServer is called in another package, RunInGoTest is not initialized.
		RunInGoTest = intest.InTest
	}
	tidbdrv := NewTiDBDriver(store)
	cfg := config.NewConfig()
	cfg.Socket = ""
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server.SetDomain(dom)
	return server
}

// CreateMockConn creates a mock connection together with a session.
func CreateMockConn(t *testing.T, server *Server) MockConn {
	extensions, err := extension.GetExtensions()
	require.NoError(t, err)

	connID := rand.Uint64()
	tc, err := server.driver.OpenCtx(connID, 0, uint8(tmysql.DefaultCollationID), "", nil, extensions.NewSessionExtensions())
	require.NoError(t, err)

	cc := &clientConn{
		connectionID: connID,
		server:       server,
		salt:         []byte{},
		collation:    tmysql.DefaultCollationID,
		alloc:        arena.NewAllocator(1024),
		chunkAlloc:   chunk.NewAllocator(),
		pkt:          internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		extensions:   tc.GetExtensions(),
	}
	cc.SetCtx(tc)
	cc.server.rwlock.Lock()
	server.clients[cc.connectionID] = cc
	cc.server.rwlock.Unlock()
	tc.Session.SetSessionManager(server)
	tc.Session.GetSessionVars().ConnectionInfo = cc.connectInfo()
	err = tc.Session.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil)
	require.NoError(t, err)
	return &mockConn{
		clientConn: cc,
		t:          t,
	}
}

// MockOSUserForAuthSocket mocks the OS user for AUTH_SOCKET plugin
func MockOSUserForAuthSocket(uname string) {
	mockOSUserForAuthSocketTest.Store(&uname)
}

// ClearOSUserForAuthSocket clears the mocked OS user for AUTH_SOCKET plugin
func ClearOSUserForAuthSocket() {
	mockOSUserForAuthSocketTest.Store(nil)
}
