// Copyright 2024 PingCAP, Inc.
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

package systable_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestManager(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})
	// disable DDL to avoid it interfere the test
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()

	ctx := util.WithInternalSourceType(context.Background(), "test")
	mgr := systable.NewManager(session.NewSessionPool(pool))

	t.Run("GetJobByID", func(t *testing.T) {
		_, err := mgr.GetJobByID(ctx, 9999)
		require.ErrorIs(t, err, systable.ErrNotFound)
		tk.MustExec(`insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing)
					values(9999, 0, '1', '1', '{"id":9999}', 1, 0)`)
		job, err := mgr.GetJobByID(ctx, 9999)
		require.NoError(t, err)
		require.EqualValues(t, 9999, job.ID)
	})

	t.Run("GetMDLVer", func(t *testing.T) {
		_, err := mgr.GetMDLVer(ctx, 9999)
		require.ErrorIs(t, err, systable.ErrNotFound)
		tk.MustExec(`replace into mysql.tidb_mdl_info (job_id, version, table_ids)
						values(9999, 123, '1')`)
		ver, err := mgr.GetMDLVer(ctx, 9999)
		require.NoError(t, err)
		require.EqualValues(t, 123, ver)
	})

	t.Run("GetMinJobID", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")
		id, err := mgr.GetMinJobID(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 0, id)
		tk.MustExec(`insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing)
					values(123456, 0, '1', '1', '{"id":9998}', 1, 0)`)
		id, err = mgr.GetMinJobID(ctx, 0)
		require.NoError(t, err)
		require.EqualValues(t, 123456, id)
		id, err = mgr.GetMinJobID(ctx, 123456)
		require.NoError(t, err)
		require.EqualValues(t, 123456, id)
		id, err = mgr.GetMinJobID(ctx, 123457)
		require.NoError(t, err)
		require.EqualValues(t, 0, id)
	})

	t.Run("HasFlashbackClusterJob", func(t *testing.T) {
		tk.MustExec("delete from mysql.tidb_ddl_job")
		found, err := mgr.HasFlashbackClusterJob(ctx, 0)
		require.NoError(t, err)
		require.False(t, found)
		tk.MustExec(fmt.Sprintf(`insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing)
					values(123, 0, '1', '1', '{"id":9998}', %d, 0)`, model.ActionFlashbackCluster))
		found, err = mgr.HasFlashbackClusterJob(ctx, 0)
		require.NoError(t, err)
		require.True(t, found)
		found, err = mgr.HasFlashbackClusterJob(ctx, 123)
		require.NoError(t, err)
		require.True(t, found)
		found, err = mgr.HasFlashbackClusterJob(ctx, 124)
		require.NoError(t, err)
		require.False(t, found)
	})
}
