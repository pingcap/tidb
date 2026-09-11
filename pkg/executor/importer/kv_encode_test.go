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
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTableKVEncoderFullTextTiCIIndexKVs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tici(a bigint primary key clustered, b int, c text, index idx_b(b))")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	origTbl, err := do.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t_tici"))
	require.NoError(t, err)

	const (
		ticiIndexID   int64 = 1001
		hybridIndexID int64 = 1004
	)
	tblMeta := origTbl.Meta().Clone()
	tblMeta.Indices = append(tblMeta.Indices, &model.IndexInfo{
		ID:    ticiIndexID,
		Name:  pmodel.NewCIStr("idx_tici"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
		},
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	}, &model.IndexInfo{
		ID:    hybridIndexID,
		Name:  pmodel.NewCIStr("idx_hybrid"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
		},
		HybridInfo: &model.HybridIndexInfo{
			Sharding: &model.HybridShardingSpec{
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
				},
			},
		},
	})
	tbl := table.MockTableFromMeta(tblMeta)
	require.NotNil(t, tbl)

	normalIndexID := findIndexIDByName(t, tblMeta, "idx_b")
	row := []types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewStringDatum("doc")}

	cfg := &encode.EncodingConfig{
		Table:                tbl,
		UseIdentityAutoRowID: true,
	}
	dupResolveEncoder := newTableKVEncoderForTest(t, tbl, cfg, true)
	dupResolvePairs, err := dupResolveEncoder.Encode(row, 1)
	require.NoError(t, err)
	dupResolveIndexIDs := collectIndexIDs(t, dupResolvePairs)
	require.Equal(t, 1, countRecordKeys(dupResolvePairs))
	require.Equal(t, []int64{normalIndexID}, dupResolveIndexIDs)
	require.NotContains(t, dupResolveIndexIDs, ticiIndexID)
	require.NotContains(t, dupResolveIndexIDs, hybridIndexID)
	require.False(t, cfg.SkipTiCIIndexKVs)

	normalEncoder := newTableKVEncoderForTest(t, tbl, cfg, false)
	normalPairs, err := normalEncoder.Encode(row, 1)
	require.NoError(t, err)
	normalIndexIDs := collectIndexIDs(t, normalPairs)
	require.Equal(t, 1, countRecordKeys(normalPairs))
	require.ElementsMatch(t, []int64{normalIndexID, ticiIndexID}, normalIndexIDs)
	require.NotContains(t, normalIndexIDs, hybridIndexID)
}

func TestTableKVEncoderFullTextTiCIIndexKVsForPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_tici_partitioned(
		a bigint primary key clustered,
		b int,
		c text,
		index idx_b(b)
	) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than maxvalue
	)`)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	origTbl, err := do.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t_tici_partitioned"))
	require.NoError(t, err)

	const ticiIndexID int64 = 1002
	tblMeta := origTbl.Meta().Clone()
	tblMeta.Indices = append(tblMeta.Indices, &model.IndexInfo{
		ID:    ticiIndexID,
		Name:  pmodel.NewCIStr("idx_tici_partitioned"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
		},
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	})
	tbl := table.MockTableFromMeta(tblMeta)
	require.NotNil(t, tbl)
	require.NotNil(t, tblMeta.Partition)
	require.Len(t, tblMeta.Partition.Definitions, 2)

	row := []types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewStringDatum("doc")}
	normalEncoder := newTableKVEncoderForTest(t, tbl, nil, false)
	normalPairs, err := normalEncoder.Encode(row, 1)
	require.NoError(t, err)
	require.Equal(t, 1, countRecordKeys(normalPairs))

	expectedPartitionID := tblMeta.Partition.Definitions[0].ID
	indexTableIDs := collectIndexTableIDs(t, normalPairs)
	require.Equal(t, []int64{expectedPartitionID}, indexTableIDs[ticiIndexID])
}

func TestTableKVEncoderClearsKVsAfterTiCIIndexError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tici(a bigint primary key clustered, b int, c text, index idx_b(b))")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	origTbl, err := do.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t_tici"))
	require.NoError(t, err)

	const ticiIndexID int64 = 1003
	tblMeta := origTbl.Meta().Clone()
	ticiIdx := &model.IndexInfo{
		ID:    ticiIndexID,
		Name:  pmodel.NewCIStr("idx_tici"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("missing_column"), Offset: 99, Length: types.UnspecifiedLength},
		},
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	}
	tblMeta.Indices = append(tblMeta.Indices, ticiIdx)
	tbl := table.MockTableFromMeta(tblMeta)
	require.NotNil(t, tbl)

	normalIndexID := findIndexIDByName(t, tblMeta, "idx_b")
	encoder := newTableKVEncoderForTest(t, tbl, nil, false)
	_, err = encoder.Encode([]types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewStringDatum("bad")}, 1)
	require.Error(t, err)

	// Reuse the same encoder to make stale KVs from the failed encode observable.
	ticiIdx.Columns[0] = &model.IndexColumn{Name: pmodel.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength}
	pairs, err := encoder.Encode([]types.Datum{types.NewDatum(2), types.NewDatum(3), types.NewStringDatum("good")}, 2)
	require.NoError(t, err)
	require.Equal(t, 1, countRecordKeys(pairs))
	require.ElementsMatch(t, []int64{normalIndexID, ticiIndexID}, collectIndexIDs(t, pairs))
}

func newTableKVEncoderForTest(t *testing.T, tbl table.Table, cfg *encode.EncodingConfig, dupResolve bool) *importer.TableKVEncoder {
	t.Helper()
	fieldMappings := make([]*importer.FieldMapping, 0, len(tbl.VisibleCols()))
	for _, col := range tbl.VisibleCols() {
		fieldMappings = append(fieldMappings, &importer.FieldMapping{Column: col})
	}
	if cfg == nil {
		cfg = &encode.EncodingConfig{
			Table:                tbl,
			UseIdentityAutoRowID: true,
		}
	}
	if cfg.Logger.Logger == nil {
		cfg.Logger = log.L()
	}
	controller := &importer.LoadDataController{
		ASTArgs:       &importer.ASTArgs{},
		Plan:          &importer.Plan{},
		Table:         tbl,
		InsertColumns: tbl.VisibleCols(),
		FieldMappings: fieldMappings,
	}
	var (
		encoder *importer.TableKVEncoder
		err     error
	)
	if dupResolve {
		encoder, err = importer.NewTableKVEncoderForDupResolve(cfg, &importer.TableImporter{LoadDataController: controller})
	} else {
		encoder, err = importer.NewTableKVEncoder(cfg, &importer.TableImporter{LoadDataController: controller})
	}
	require.NoError(t, err)
	return encoder
}

func findIndexIDByName(t *testing.T, tblInfo *model.TableInfo, indexName string) int64 {
	t.Helper()
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == indexName {
			return idx.ID
		}
	}
	require.FailNow(t, "index not found", indexName)
	return 0
}

func collectIndexIDs(t *testing.T, kvPairs *kv.Pairs) []int64 {
	t.Helper()
	indexIDs := make([]int64, 0)
	for _, pair := range kvPairs.Pairs {
		if !tablecodec.IsIndexKey(pair.Key) {
			continue
		}
		indexID, err := tablecodec.DecodeIndexID(pair.Key)
		require.NoError(t, err)
		indexIDs = append(indexIDs, indexID)
	}
	return indexIDs
}

func collectIndexTableIDs(t *testing.T, kvPairs *kv.Pairs) map[int64][]int64 {
	t.Helper()
	tableIDsByIndexID := make(map[int64][]int64)
	for _, pair := range kvPairs.Pairs {
		if !tablecodec.IsIndexKey(pair.Key) {
			continue
		}
		tableID, indexID, isRecordKey, err := tablecodec.DecodeKeyHead(pair.Key)
		require.NoError(t, err)
		require.False(t, isRecordKey)
		tableIDsByIndexID[indexID] = append(tableIDsByIndexID[indexID], tableID)
	}
	return tableIDsByIndexID
}

func countRecordKeys(pairs *kv.Pairs) int {
	cnt := 0
	for _, pair := range pairs.Pairs {
		if tablecodec.IsRecordKey(pair.Key) {
			cnt++
		}
	}
	return cnt
}
