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
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1})
	require.NoError(t, err)
	defer mgr.Close()

	mgr.Register(1, []byte{'1', '9'})
	mgr.Register(2, []byte{'2', '9'})
	mgr.UpdateTotal(1, 100, false)
	require.False(t, mgr.IsComplete([]byte{'1', '9'}))
	require.NoError(t, mgr.UpdateCurrent(1, 100))
	require.False(t, mgr.IsComplete([]byte{'1', '9'}))
	mgr.UpdateTotal(1, 100, true)
	require.NoError(t, mgr.UpdateCurrent(1, 100))
	// The data is not imported to the storage yet.
	require.False(t, mgr.IsComplete([]byte{'1', '9'}))
	flushCtrl.imported = true // Mock the data is imported to the storage.
	require.NoError(t, mgr.UpdateCurrent(2, 0))
	require.True(t, mgr.IsComplete([]byte{'1', '9'}))

	// Only when the last batch is completed, the job can be completed.
	mgr.UpdateTotal(2, 50, false)
	mgr.UpdateTotal(2, 50, true)
	require.NoError(t, mgr.UpdateCurrent(2, 50))
	require.True(t, mgr.IsComplete([]byte{'1', '9'}))
	require.False(t, mgr.IsComplete([]byte{'2', '9'}))
	require.NoError(t, mgr.UpdateCurrent(2, 50))
	require.True(t, mgr.IsComplete([]byte{'1', '9'}))
	require.True(t, mgr.IsComplete([]byte{'2', '9'}))

	// Only when the subsequent job is completed, the previous job can be completed.
	mgr.Register(3, []byte{'3', '9'})
	mgr.Register(4, []byte{'4', '9'})
	mgr.Register(5, []byte{'5', '9'})
	mgr.UpdateTotal(3, 100, true)
	mgr.UpdateTotal(4, 100, true)
	mgr.UpdateTotal(5, 100, true)
	require.NoError(t, mgr.UpdateCurrent(5, 100))
	require.NoError(t, mgr.UpdateCurrent(4, 100))
	require.False(t, mgr.IsComplete([]byte{'3', '9'}))
	require.False(t, mgr.IsComplete([]byte{'4', '9'}))
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
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1})
	require.NoError(t, err)
	defer mgr.Close()

	mgr.Register(1, []byte{'1', '9'})
	mgr.UpdateTotal(1, 100, true)
	require.NoError(t, mgr.UpdateCurrent(1, 100))
	mgr.Sync() // Wait the global checkpoint to be updated to the reorg table.
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
}

func TestCheckpointManagerResumeReorg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	reorgMeta := &ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			LocalSyncKey:   []byte{'1', '9'},
			LocalKeyCount:  100,
			GlobalSyncKey:  []byte{'2', '9'},
			GlobalKeyCount: 200,
			InstanceAddr:   ingest.InitInstanceAddr(),
			Version:        1,
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
	mgr, err := ingest.NewCheckpointManager(ctx, flushCtrl, sessPool, 1, []int64{1})
	require.NoError(t, err)
	defer mgr.Close()
	require.True(t, mgr.IsComplete([]byte{'1', '9'}))
	require.True(t, mgr.IsComplete([]byte{'2', '9'}))
	localCnt, globalNextKey := mgr.Status()
	require.Equal(t, 100, localCnt)
	require.EqualValues(t, []byte{'2', '9'}, globalNextKey)
}

type dummyFlushCtrl struct {
	imported bool
}

func (d *dummyFlushCtrl) Flush(_ int64, _ ingest.FlushMode) (bool, bool, error) {
	return true, d.imported, nil
}
