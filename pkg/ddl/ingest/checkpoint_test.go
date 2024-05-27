// Copyright 2023 PingCAP, Inc.
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

package ingest_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

func createDummyFile(t *testing.T, folder string) {
	f, err := os.Create(filepath.Join(folder, "test-file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

var (
	mockPTS    int64 = 12
	mockLTS    int64 = 34
	expectedTS       = oracle.ComposeTS(mockPTS, mockLTS)
)

type mockGetTSClient struct {
	pd.Client
}

func (m mockGetTSClient) GetTS(context.Context) (int64, int64, error) {
	return mockPTS, mockLTS, nil
}

func TestCheckpointManager(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("insert into mysql.tidb_ddl_reorg (job_id, ele_id, ele_type) values (1, 1, '_idx_');")
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	ctx := context.Background()
	sessPool := session.NewSessionPool(rs, store)
	flushCtrl := &dummyFlushCtrl{imported: false}
	tmpFolder := t.TempDir()
	createDummyFile(t, tmpFolder)
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1}, tmpFolder, mockGetTSClient{})
	require.NoError(t, err)
	defer mgr.Close()

	mgr.Register(1, []byte{'1', '9'})
	mgr.Register(2, []byte{'2', '9'})
	mgr.UpdateTotalKeys(1, 100, false)
	require.False(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	require.NoError(t, mgr.UpdateWrittenKeys(1, 100))
	require.False(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	mgr.UpdateTotalKeys(1, 100, true)
	require.NoError(t, mgr.UpdateWrittenKeys(1, 100))
	// The data is not imported to the storage yet.
	require.False(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	flushCtrl.imported = true // Mock the data is imported to the storage.
	require.NoError(t, mgr.UpdateWrittenKeys(2, 0))
	require.True(t, mgr.IsKeyProcessed([]byte{'1', '9'}))

	// Only when the last batch is completed, the job can be completed.
	mgr.UpdateTotalKeys(2, 50, false)
	mgr.UpdateTotalKeys(2, 50, true)
	require.NoError(t, mgr.UpdateWrittenKeys(2, 50))
	require.True(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	require.False(t, mgr.IsKeyProcessed([]byte{'2', '9'}))
	require.NoError(t, mgr.UpdateWrittenKeys(2, 50))
	require.True(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	require.True(t, mgr.IsKeyProcessed([]byte{'2', '9'}))

	// Only when the subsequent job is completed, the previous job can be completed.
	mgr.Register(3, []byte{'3', '9'})
	mgr.Register(4, []byte{'4', '9'})
	mgr.Register(5, []byte{'5', '9'})
	mgr.UpdateTotalKeys(3, 100, true)
	mgr.UpdateTotalKeys(4, 100, true)
	mgr.UpdateTotalKeys(5, 100, true)
	require.NoError(t, mgr.UpdateWrittenKeys(5, 100))
	require.NoError(t, mgr.UpdateWrittenKeys(4, 100))
	require.False(t, mgr.IsKeyProcessed([]byte{'3', '9'}))
	require.False(t, mgr.IsKeyProcessed([]byte{'4', '9'}))
}

func TestCheckpointManagerUpdateReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("insert into mysql.tidb_ddl_reorg (job_id, ele_id, ele_type) values (1, 1, '_idx_');")
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	ctx := context.Background()
	sessPool := session.NewSessionPool(rs, store)
	flushCtrl := &dummyFlushCtrl{imported: true}
	tmpFolder := t.TempDir()
	createDummyFile(t, tmpFolder)
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1}, tmpFolder, mockGetTSClient{})
	require.NoError(t, err)
	defer mgr.Close()

	mgr.Register(1, []byte{'1', '9'})
	mgr.UpdateTotalKeys(1, 100, true)
	require.NoError(t, mgr.UpdateWrittenKeys(1, 100))
	mgr.Flush() // Wait the global checkpoint to be updated to the reorg table.
	r, err := tk.Exec("select reorg_meta from mysql.tidb_ddl_reorg where job_id = 1 and ele_id = 1;")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	row := req.GetRow(0)
	require.Equal(t, 1, row.Len())
	reorgMetaRaw := row.GetBytes(0)
	reorgMeta := &ingest.JobReorgMeta{}
	require.NoError(t, json.Unmarshal(reorgMetaRaw, reorgMeta))
	require.Nil(t, r.Close())
	require.Equal(t, 100, reorgMeta.Checkpoint.GlobalKeyCount)
	require.Equal(t, 100, reorgMeta.Checkpoint.LocalKeyCount)
	require.EqualValues(t, []byte{'1', '9'}, reorgMeta.Checkpoint.GlobalSyncKey)
	require.EqualValues(t, []byte{'1', '9'}, reorgMeta.Checkpoint.LocalSyncKey)
	require.EqualValues(t, expectedTS, reorgMeta.Checkpoint.TS)
}

func TestCheckpointManagerResumeReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	reorgMeta := &ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			LocalSyncKey:   []byte{'2', '9'},
			LocalKeyCount:  100,
			GlobalSyncKey:  []byte{'1', '9'},
			GlobalKeyCount: 200,
			InstanceAddr:   ingest.InstanceAddr(),
			Version:        1,
			TS:             123456,
		},
	}
	reorgMetaRaw, err := json.Marshal(reorgMeta)
	require.NoError(t, err)
	tk.MustExec("insert into mysql.tidb_ddl_reorg (job_id, ele_id, ele_type, reorg_meta) values (1, 1, '_idx_', ?);", reorgMetaRaw)
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	ctx := context.Background()
	sessPool := session.NewSessionPool(rs, store)
	flushCtrl := &dummyFlushCtrl{imported: false}
	tmpFolder := t.TempDir()
	// checkpoint manager should not use local checkpoint if the folder is empty
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1}, tmpFolder, nil)
	require.NoError(t, err)
	defer mgr.Close()
	require.True(t, mgr.IsKeyProcessed([]byte{'1', '9'}))
	require.False(t, mgr.IsKeyProcessed([]byte{'2', '9'}))
	localCnt, globalNextKey := mgr.Status()
	require.Equal(t, 0, localCnt)
	require.EqualValues(t, []byte{'1', '9'}, globalNextKey)
	require.EqualValues(t, 123456, mgr.GetTS())

	createDummyFile(t, tmpFolder)
	mgr2, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1}, tmpFolder, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	require.True(t, mgr2.IsKeyProcessed([]byte{'1', '9'}))
	require.True(t, mgr2.IsKeyProcessed([]byte{'2', '9'}))
	localCnt, globalNextKey = mgr2.Status()
	require.Equal(t, 100, localCnt)
	require.EqualValues(t, []byte{'1', '9'}, globalNextKey)
	require.EqualValues(t, 123456, mgr.GetTS())
}

type dummyFlushCtrl struct {
	imported bool
}

func (d *dummyFlushCtrl) Flush(_ ingest.FlushMode) (bool, bool, int64, error) {
	return true, d.imported, 0, nil
}
