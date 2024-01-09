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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDDLHistoryBasic(t *testing.T) {
	mkJob := func(id int64, schemaTableNames ...string) *model.Job {
		var schemaInfos []model.InvolvingSchemaInfo
		for _, schemaTableName := range schemaTableNames {
			ss := strings.Split(schemaTableName, ".")
			schemaInfos = append(schemaInfos, model.InvolvingSchemaInfo{
				Database: ss[0],
				Table:    ss[1],
			})
		}
		return &model.Job{
			ID:                  id,
			InvolvingSchemaInfo: schemaInfos,
		}
	}
	store := testkit.CreateMockStore(t)
	rs := pools.NewResourcePool(func() (pools.Resource, error) {
		newTk := testkit.NewTestKit(t, store)
		return newTk.Session(), nil
	}, 8, 8, 0)
	sessPool := session.NewSessionPool(rs, store)
	sessCtx, err := sessPool.Get()
	require.NoError(t, err)
	sess := sess.NewSession(sessCtx)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnLightning)
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return ddl.AddHistoryDDLJob(sess, t, mkJob(1, "test.t1"), false)
	})

	require.NoError(t, err)

	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return ddl.AddHistoryDDLJob(sess, t, mkJob(2, "test.t2"), false)
	})

	require.NoError(t, err)

	job, err := ddl.GetHistoryJobByID(sessCtx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), job.ID)

	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		jobs, err := ddl.GetLastNHistoryDDLJobs(m, 2)
		require.NoError(t, err)
		require.Equal(t, 2, len(jobs))
		return nil
	})

	require.NoError(t, err)

	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err := ddl.GetAllHistoryDDLJobs(m)
		require.NoError(t, err)
		return nil
	})

	require.NoError(t, err)

	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		jobs, err := ddl.ScanHistoryDDLJobs(m, 2, 2)
		require.NoError(t, err)
		require.Equal(t, 2, len(jobs))
		require.Equal(t, int64(2), jobs[0].ID)
		require.Equal(t, int64(1), jobs[1].ID)
		return nil
	})

	require.NoError(t, err)
}
