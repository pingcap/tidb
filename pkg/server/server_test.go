// Copyright 2015 PingCAP, Inc.
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
	"crypto/tls"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metaservice"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/server/internal"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
)

func TestIssue46197(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	storage, err := extstore.NewExtStorage(ctx, "file://"+tempDir, "")
	require.NoError(t, err)
	extstore.SetGlobalExtStorageForTest(storage)
	defer func() {
		extstore.SetGlobalExtStorageForTest(nil)
		storage.Close()
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	defer server.Close()

	// Mock the content of the SQL file in PacketIO buffer.
	// First 4 bytes are the header, followed by the actual content.
	// This acts like we are sending "select * from t1;" from the client when tidb requests the "a.txt" file.
	var inBuffer bytes.Buffer
	_, err = inBuffer.Write([]byte{0x11, 0x00, 0x00, 0x01})
	require.NoError(t, err)
	_, err = inBuffer.Write([]byte("select * from t1;"))
	require.NoError(t, err)

	// clientConn setup
	brc := util.NewBufferedReadConn(&testutil.BytesConn{Buffer: inBuffer})
	pkt := internal.NewPacketIO(brc)
	pkt.SetBufWriter(bufio.NewWriter(bytes.NewBuffer(nil)))
	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        pkt,
		capability: mysql.ClientLocalFiles,
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})

	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")

	// 3 is mysql.ComQuery, followed by the SQL text.
	require.NoError(t, cc.dispatch(ctx, []byte("\u0003plan replayer dump explain 'a.txt'")))

	// clean up
	path := testdata.ConvertRowsToStrings(tk.MustQuery("select @@tidb_last_plan_replayer_token").Rows())
	require.NoError(t, storage.DeleteFile(ctx, filepath.Join(replayer.GetPlanReplayerDirName(), path[0])))
}

func TestGetConAttrs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)

	cc := &clientConn{
		server:     server,
		alloc:      arena.NewAllocator(1024),
		chunkAlloc: chunk.NewAllocator(),
		pkt:        internal.NewPacketIOForTest(bufio.NewWriter(bytes.NewBuffer(nil))),
		attrs: map[string]string{
			"_client_name": "tidb_test",
		},
		user:     "userA",
		peerHost: "foo.example.com",
	}
	cc.SetCtx(&TiDBContext{Session: tk.Session(), stmts: make(map[int]*TiDBStatement)})
	server.registerConn(cc)

	// Get attributes for all clients
	attrs := server.GetConAttrs(nil)
	require.Equal(t, attrs[1]["_client_name"], "tidb_test")

	// Get attributes for userA@foo.example.com, which should have results for connID 1.
	userA := &auth.UserIdentity{Username: "userA", Hostname: "foo.example.com"}
	attrs = server.GetConAttrs(userA)
	require.Equal(t, attrs[1]["_client_name"], "tidb_test")
	_, hasClientName := attrs[1]
	require.True(t, hasClientName)

	// Get attributes for userB@foo.example.com, which should NOT have results for connID 1.
	userB := &auth.UserIdentity{Username: "userB", Hostname: "foo.example.com"}
	attrs = server.GetConAttrs(userB)
	_, hasClientName = attrs[1]
	require.False(t, hasClientName)
}

func TestSeverHealth(t *testing.T) {
	t.Run("setup autoid service closes store on meta service error", func(t *testing.T) {
		originalOpenStore := openStoreForAutoIDService
		t.Cleanup(func() {
			openStoreForAutoIDService = originalOpenStore
		})

		mockStore := &autoIDServiceTestStore{}
		openStoreForAutoIDService = func(path string) (kv.Storage, error) {
			require.Equal(t, "tikv://127.0.0.1:2379", path)
			return mockStore, nil
		}

		s := &Server{
			cfg: &config.Config{
				Store: "tikv",
				Path:  "127.0.0.1:2379",
			},
		}

		s.setupAutoIDService(NewRPCServer(s.cfg, nil, s))

		require.True(t, mockStore.closed)
		require.Nil(t, s.autoIDService)
	})

	RunInGoTestChan = make(chan struct{})
	RunInGoTest = true
	store := testkit.CreateMockStore(t)
	tidbdrv := NewTiDBDriver(store)
	cfg := util.NewTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	require.False(t, server.health.Load(), "server should not be healthy")
	go func() {
		err = server.Run(nil)
		require.NoError(t, err)
	}()
	defer server.Close()
	for range RunInGoTestChan {
		// wait for server to be healthy
	}
	require.True(t, server.health.Load(), "server should be healthy")
}

type autoIDServiceTestStore struct {
	kv.Storage
	closed bool
}

func (s *autoIDServiceTestStore) Close() error {
	s.closed = true
	return nil
}

func (*autoIDServiceTestStore) MetaServiceInfo() (*metaservice.Info, error) {
	return nil, errors.New("meta service unavailable")
}

func (*autoIDServiceTestStore) GetPDAddrs() ([]string, error) {
	return nil, nil
}

func (*autoIDServiceTestStore) StartGCWorker() error {
	return nil
}

func (*autoIDServiceTestStore) TLSConfig() *tls.Config {
	return nil
}
