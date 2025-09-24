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
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func createDummyFile(t *testing.T, folder string) {
	f, err := os.Create(filepath.Join(folder, "test-file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

type mockGetTSClient struct {
	pd.Client

	pts int64
	lts int64
}

func (m *mockGetTSClient) GetTS(context.Context) (int64, int64, error) {
	p, l := m.pts, m.lts
	m.pts++
	m.lts++
	return p, l, nil
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
			PhysicalID:     1,
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
	sessPool := session.NewSessionPool(rs)
	tmpFolder := t.TempDir()
	// checkpoint manager should not use local checkpoint if the folder is empty
	mgr, err := ingest.NewCheckpointManager(ctx, sessPool, 1, 1, tmpFolder, nil, kv.Key{})
	require.NoError(t, err)
	defer mgr.Close()
	localCnt, globalNextKey := mgr.TotalKeyCount(), mgr.NextStartKey()
	require.Equal(t, 0, localCnt)
	require.EqualValues(t, []byte{'1', '9'}, globalNextKey)
	require.EqualValues(t, 123456, mgr.GetImportTS())

	createDummyFile(t, tmpFolder)
	mgr2, err := ingest.NewCheckpointManager(ctx, sessPool, 1, 1, tmpFolder, nil, kv.Key{})
	require.NoError(t, err)
	defer mgr2.Close()
	localCnt, globalNextKey = mgr2.TotalKeyCount(), mgr2.NextStartKey()
	require.Equal(t, 100, localCnt)
	require.EqualValues(t, []byte{'2', '9'}, globalNextKey)
	require.EqualValues(t, 123456, mgr2.GetImportTS())
}
