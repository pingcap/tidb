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

package ddl_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestGenGIDAndInsertJobsWithRetryOnErr(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	// disable DDL to avoid it interfere the test
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	dom.DDL().OwnerManager().CampaignCancel()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	ddlSe := sess.NewSession(tk.Session())
	jobs := []*ddl.JobWrapper{{
		Job: &model.Job{
			Type:       model.ActionCreateTable,
			SchemaName: "test",
			TableName:  "t1",
			Args:       []any{&model.TableInfo{}},
		},
	}}
	submitter := ddl.NewJobSubmitterForTest()
	// retry for 3 times
	currGID := getGlobalID(ctx, t, store)
	var counter int64
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockGenGIDRetryableError", `3*return(true)`)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/pkg/ddl/onGenGIDRetry", func() {
		m := submitter.DDLJobDoneChMap()
		require.Equal(t, 1, len(m.Keys()))
		_, ok := m.Load(currGID + counter*100 + 2)
		require.True(t, ok)
		counter++
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			_, err := m.GenGlobalIDs(100)
			require.NoError(t, err)
			return nil
		}))
	})
	require.Zero(t, len(submitter.DDLJobDoneChMap().Keys()))
	require.NoError(t, submitter.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobs))
	require.EqualValues(t, 3, counter)
	newGID := getGlobalID(ctx, t, store)
	require.Equal(t, currGID+300+2, newGID)
	m := submitter.DDLJobDoneChMap()
	require.Equal(t, 1, len(m.Keys()))
	_, ok := m.Load(newGID)
	require.True(t, ok)
}
