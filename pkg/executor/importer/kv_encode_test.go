// Copyright 2025 PingCAP, Inc.
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

package importer_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKVEncoderForDupResolve(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint primary key nonclustered) SHARD_ROW_ID_BITS = 6")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	doTestFn := func(t *testing.T, useIdentityAutoRowID bool, checkerFn func(handleVal int64)) {
		encodeCfg := &encode.EncodingConfig{
			Table:                table,
			UseIdentityAutoRowID: useIdentityAutoRowID,
		}
		controller := &importer.LoadDataController{
			ASTArgs: &importer.ASTArgs{},
			Plan:    &importer.Plan{},
			Table:   table,
		}
		encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
		require.NoError(t, err)
		for range 10 {
			pairs, err := encoder.Encode([]types.Datum{types.NewDatum(1)}, 1)
			require.NoError(t, err)
			require.Len(t, pairs.Pairs, 2)
			var metRecordKey bool
			for _, pair := range pairs.Pairs {
				if !tablecodec.IsRecordKey(pair.Key) {
					continue
				}
				metRecordKey = true
				handle, err := tablecodec.DecodeRowKey(pair.Key)
				require.NoError(t, err)
				checkerFn(handle.IntValue())
			}
			require.True(t, metRecordKey)
		}
	}

	t.Run("identity auto row id", func(t *testing.T) {
		doTestFn(t, true, func(handleVal int64) {
			require.EqualValues(t, 1, handleVal)
		})
	})

	t.Run("without identity auto row id", func(t *testing.T) {
		// we loop 10 times, at least one should have shard bit larger than 1
		var handleLargerThanOneCount int
		doTestFn(t, false, func(handleVal int64) {
			if handleVal > 1 {
				handleLargerThanOneCount++
			}
		})
		require.Greater(t, handleLargerThanOneCount, 1)
	})
}

func TestTableKVEncoderEmitsFullTextIndexKVs(t *testing.T) {
	colID := &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("id"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)}
	colBody := &model.ColumnInfo{ID: 2, Name: ast.NewCIStr("body"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeVarchar)}
	normalIdx := &model.IndexInfo{
		ID:      2,
		Name:    ast.NewCIStr("idx_body"),
		State:   model.StatePublic,
		Columns: []*model.IndexColumn{{Name: ast.NewCIStr("body"), Offset: 1, Length: types.UnspecifiedLength}},
	}
	fullTextIdx := &model.IndexInfo{
		ID:      3,
		Name:    ast.NewCIStr("idx_fulltext"),
		State:   model.StatePublic,
		Columns: []*model.IndexColumn{{Name: ast.NewCIStr("body"), Offset: 1, Length: types.UnspecifiedLength}},
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	}
	hybridIdx := &model.IndexInfo{
		ID:      4,
		Name:    ast.NewCIStr("idx_hybrid"),
		State:   model.StatePublic,
		Columns: []*model.IndexColumn{{Name: ast.NewCIStr("body"), Offset: 1, Length: types.UnspecifiedLength}},
		HybridInfo: &model.HybridIndexInfo{
			Sharding: &model.HybridShardingSpec{
				Columns: []*model.IndexColumn{{Name: ast.NewCIStr("body"), Offset: 1, Length: types.UnspecifiedLength}},
			},
		},
	}
	tblInfo := &model.TableInfo{
		ID:      11,
		Name:    ast.NewCIStr("t"),
		State:   model.StatePublic,
		Columns: []*model.ColumnInfo{colID, colBody},
		Indices: []*model.IndexInfo{normalIdx, fullTextIdx, hybridIdx},
	}
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc()), tblInfo)
	require.NoError(t, err)

	fieldMappings := make([]*importer.FieldMapping, 0, len(tbl.VisibleCols()))
	for _, col := range tbl.VisibleCols() {
		fieldMappings = append(fieldMappings, &importer.FieldMapping{Column: col})
	}
	collectPairInfo := func(t *testing.T, pairs *lkv.Pairs) (map[int64]struct{}, bool) {
		t.Helper()
		seenIndexIDs := make(map[int64]struct{})
		var seenRecord bool
		for _, pair := range pairs.Pairs {
			_, indexID, isRecordKey, err := tablecodec.DecodeKeyHead(pair.Key)
			require.NoError(t, err)
			if isRecordKey {
				seenRecord = true
				continue
			}
			seenIndexIDs[indexID] = struct{}{}
		}
		return seenIndexIDs, seenRecord
	}
	newController := func() *importer.LoadDataController {
		return &importer.LoadDataController{
			ASTArgs:       &importer.ASTArgs{},
			Plan:          &importer.Plan{},
			Table:         tbl,
			InsertColumns: tbl.VisibleCols(),
			FieldMappings: fieldMappings,
		}
	}

	encoder, err := importer.NewTableKVEncoder(
		&encode.EncodingConfig{Table: tbl},
		newController(),
	)
	require.NoError(t, err)

	pairs, err := encoder.Encode([]types.Datum{types.NewIntDatum(1), types.NewStringDatum("doc")}, 100)
	require.NoError(t, err)

	seenIndexIDs, seenRecord := collectPairInfo(t, pairs)
	require.True(t, seenRecord)
	require.Contains(t, seenIndexIDs, normalIdx.ID)
	require.Contains(t, seenIndexIDs, fullTextIdx.ID)
	require.NotContains(t, seenIndexIDs, hybridIdx.ID)

	dupResolveEncoder, err := importer.NewTableKVEncoderForDupResolve(
		&encode.EncodingConfig{Table: tbl},
		newController(),
	)
	require.NoError(t, err)

	dupResolvePairs, err := dupResolveEncoder.Encode([]types.Datum{types.NewIntDatum(2), types.NewStringDatum("doc")}, 200)
	require.NoError(t, err)

	seenIndexIDs, seenRecord = collectPairInfo(t, dupResolvePairs)
	require.True(t, seenRecord)
	require.Contains(t, seenIndexIDs, normalIdx.ID)
	require.NotContains(t, seenIndexIDs, fullTextIdx.ID)
	require.NotContains(t, seenIndexIDs, hybridIdx.ID)
}
