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
	"flag"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/chunk"
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

// CreateMockServer creates a mock server.
func CreateMockServer(t *testing.T, store kv.Storage) *Server {
	if !RunInGoTest {
		// If CreateMockServer is called in another package, RunInGoTest is not initialized.
		RunInGoTest = flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil
	}
	tidbdrv := NewTiDBDriver(store)
	cfg := config.NewConfig()
	cfg.Socket = ""
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	cfg.Security.AutoTLS = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	return server
}

// CreateMockConn creates a mock connection together with a session.
func CreateMockConn(t *testing.T, store kv.Storage, server *Server) MockConn {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}

	cc := &clientConn{
		server:     server,
		salt:       []byte{},
		collation:  mysql.DefaultCollationID,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt: &packetIO{
			bufWriter: bufio.NewWriter(bytes.NewBuffer(nil)),
		},
	}
	cc.setCtx(tc)
	return &mockConn{
		clientConn: cc,
		t:          t,
	}
}
