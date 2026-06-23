// Copyright 2026 PingCAP, Inc.
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
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func newJobSubmitterForInternalTest(t *testing.T) (*ddl.JobSubmitter, kv.Storage) {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}, 8, 8, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})

	sessPool := sess.NewSessionPool(pool)
	sysTblMgr := systable.NewManager(sessPool)
	return ddl.NewJobSubmitterWithDepsForTest(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL),
		store,
		nil,
		sessPool,
		sysTblMgr,
		systable.NewMinJobIDRefresher(sysTblMgr),
	), store
}

func TestJobSubmitterAddBatchDDLJobsRejectsBDRWithMixedCaseSchema(t *testing.T) {
	submitter, store := newJobSubmitterForInternalTest(t)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).SetBDRRole(string(ast.BDRRolePrimary))
	}))

	jobW := ddl.NewJobWrapperWithArgs(&model.Job{
		Version:    model.JobVersion2,
		Type:       model.ActionAlterTableMode,
		SchemaID:   100,
		TableID:    200,
		SchemaName: "MixedDB",
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: "mixeddb",
			Table:    "t",
		}},
	}, &model.AlterTableModeArgs{
		TableMode: model.TableModeImport,
		SchemaID:  100,
		TableID:   200,
	}, true)

	var err error
	require.NotPanics(t, func() {
		err = submitter.AddBatchDDLJobs2TableForTest([]*ddl.JobWrapper{jobW})
	})
	require.ErrorContains(t, err, "bdr role")
}
