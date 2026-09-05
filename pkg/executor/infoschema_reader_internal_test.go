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

package executor

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestSetDataFromCheckConstraints(t *testing.T) {
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt := memtableRetriever{is: mockIs, extractor: &plannercore.InfoSchemaCheckConstraintsExtractor{}}
	sctx := defaultCtx()
	err := mt.setDataFromCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 4, len(mt.rows[0])) // 4 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
}

func TestSetDataFromTiDBCheckConstraints(t *testing.T) {
	mt := memtableRetriever{}
	sctx := defaultCtx()
	tblInfos := []*model.TableInfo{
		{
			ID:    1,
			Name:  ast.NewCIStr("t1"),
			State: model.StatePublic,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("t2"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t2_c1"),
					Table:      ast.NewCIStr("t2"),
					ExprString: "id<10",
					State:      model.StatePublic,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("t3"),
			Columns: []*model.ColumnInfo{
				{
					Name:      ast.NewCIStr("id"),
					FieldType: *types.NewFieldType(mysql.TypeLonglong),
					State:     model.StatePublic,
				},
			},
			Constraints: []*model.ConstraintInfo{
				{
					Name:       ast.NewCIStr("t3_c1"),
					Table:      ast.NewCIStr("t3"),
					ExprString: "id<10",
					State:      model.StateDeleteOnly,
				},
			},
			State: model.StatePublic,
		},
	}
	mockIs := infoschema.MockInfoSchema(tblInfos)
	mt.is = mockIs
	mt.extractor = &plannercore.InfoSchemaTiDBCheckConstraintsExtractor{}
	err := mt.setDataFromTiDBCheckConstraints(context.Background(), sctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(mt.rows))    // 1 row
	require.Equal(t, 6, len(mt.rows[0])) // 6 columns
	require.Equal(t, types.NewStringDatum("def"), mt.rows[0][0])
	require.Equal(t, types.NewStringDatum("test"), mt.rows[0][1])
	require.Equal(t, types.NewStringDatum("t2_c1"), mt.rows[0][2])
	require.Equal(t, types.NewStringDatum("(id<10)"), mt.rows[0][3])
	require.Equal(t, types.NewStringDatum("t2"), mt.rows[0][4])
	require.Equal(t, types.NewIntDatum(2), mt.rows[0][5])
}

func TestSetDataFromKeywords(t *testing.T) {
	mt := memtableRetriever{}
	err := mt.setDataFromKeywords()
	require.NoError(t, err)
	require.Equal(t, types.NewStringDatum("ADD"), mt.rows[0][0]) // Keyword: ADD
	require.Equal(t, types.NewIntDatum(1), mt.rows[0][1])        // Reserved: true(1)
}

// TestDataLockWaitsRetrieverBatchDigests is a regression test for two bugs in
// the DATA_LOCK_WAITS retriever's SQL-digest handling. The per-row digest slice
// is sized to the current batch, but the fill loop used to iterate every lock
// wait: with more than batchSize (1024) waiters it overran the batch-sized
// slice and panicked with "index out of range", and on later batches it also
// misaligned digests[rowIdx], attributing digests to the wrong rows.
//
// Each lock wait is given a distinct SQL digest so the test can assert the
// returned digest column matches the expected value for every row across all
// batches, which is what exercises the alignment fix (not just the row count).
func TestDataLockWaitsRetrieverBatchDigests(t *testing.T) {
	const numLockWaits = 1500 // > batchSize (1024) so the fill loop overran the batch-sized slice

	// Give each lock wait a distinct SQL digest, built and encoded with the same
	// production helpers the KV layer uses, so the test drives the real
	// encode/decode round trip and can assert per-row digest alignment across
	// batches (not just the row count).
	expectedDigests := make([]string, numLockWaits)
	lockWaits := make([]*deadlock.WaitForEntry, numLockWaits)
	for i := range lockWaits {
		digest := parser.NewDigest([]byte{byte(i), byte(i >> 8)})
		expectedDigests[i] = digest.String()
		lockWaits[i] = &deadlock.WaitForEntry{
			Txn:              uint64(i + 1),
			WaitForTxn:       uint64(i + 2),
			Key:              []byte{byte(i), byte(i >> 8)},
			ResourceGroupTag: kv.NewResourceGroupTagBuilder(nil).SetSQLDigest(digest).EncodeTagWithKey(nil),
		}
	}

	// Column order below fixes SQL_DIGEST at row index 1.
	const digestColIdx = 1
	r := &dataLockWaitsTableRetriever{
		table: &model.TableInfo{Name: ast.NewCIStr("DATA_LOCK_WAITS")},
		columns: []*model.ColumnInfo{
			{Name: ast.NewCIStr(infoschema.DataLockWaitsColumnKey)},
			{Name: ast.NewCIStr(infoschema.DataLockWaitsColumnSQLDigest)},
		},
		lockWaits:   lockWaits,
		initialized: true, // skip the privilege check and store fetch; drive the batch loop directly
	}
	r.batchRetrieverHelper.totalRows = len(lockWaits)
	r.batchRetrieverHelper.batchSize = 1024

	sctx := mock.NewContext()
	ctx := context.Background()

	// Rows are returned batch by batch, lockWaits first and in order, so the
	// accumulated rows line up 1:1 with lockWaits.
	var rows [][]types.Datum
	for !r.retrieved {
		batch, err := r.retrieve(ctx, sctx)
		require.NoError(t, err)
		rows = append(rows, batch...)
	}
	require.Len(t, rows, numLockWaits)
	for i, row := range rows {
		require.Equal(t, expectedDigests[i], row[digestColIdx].GetString(), "digest mismatch at row %d", i)
	}
}
