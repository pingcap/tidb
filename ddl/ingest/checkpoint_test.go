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
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/testkit"
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
	flushCtrl := &dummyFlushCtrl{}
	mgr, err := ingest.NewCentralizedCheckpointManager(ctx, flushCtrl, sessPool, 1, 1, 10*time.Minute)
	require.NoError(t, err)
	mgr.Register(1, []byte{'1', '1'}, []byte{'1', '9'})
	mgr.UpdateTotal(1, 100, false)
	mgr.Sync()
	require.False(t, mgr.IsComplete(1, []byte{'1', '1'}, []byte{'1', '9'}))
	err = mgr.UpdateCurrent(1, 100)
	require.NoError(t, err)
	mgr.Sync()
	require.False(t, mgr.IsComplete(1, []byte{'1', '1'}, []byte{'1', '9'}))
	mgr.UpdateTotal(1, 100, true)
	err = mgr.UpdateCurrent(1, 100)
	require.NoError(t, err)
	mgr.Sync()
	require.True(t, mgr.IsComplete(1, []byte{'1', '1'}, []byte{'1', '9'}))
}

type dummyFlushCtrl struct {
	imported bool
}

func (d *dummyFlushCtrl) Flush(_ int64) (bool, error) {
	return d.imported, nil
}
