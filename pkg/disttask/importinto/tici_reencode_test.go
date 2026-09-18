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

package importinto_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConflictDataKVEncoderSkipsTiCIIndexKVs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_tici(a bigint primary key clustered, b int, c text, index idx_b(b))")
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	origTbl, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t_tici"))
	require.NoError(t, err)

	const ticiIndexID int64 = 1001
	tblMeta := origTbl.Meta().Clone()
	tblMeta.Indices = append(tblMeta.Indices, &metamodel.IndexInfo{
		ID:    ticiIndexID,
		Name:  model.NewCIStr("idx_tici"),
		State: metamodel.StatePublic,
		Columns: []*metamodel.IndexColumn{
			{Name: model.NewCIStr("c"), Offset: 2, Length: types.UnspecifiedLength},
		},
		FullTextInfo: &metamodel.FullTextIndexInfo{
			ParserType: metamodel.FullTextParserTypeStandardV1,
		},
	})
	tbl := table.MockTableFromMeta(tblMeta)
	controller, err := importer.NewLoadDataController(&importer.Plan{
		InImportInto: true,
		Format:       importer.DataFormatCSV,
	}, tbl, &importer.ASTArgs{})
	require.NoError(t, err)
	tableImporter, err := importer.NewTableImporterForTest(context.Background(), controller, "tici-reencode", store)
	require.NoError(t, err)
	t.Cleanup(tableImporter.Backend().CloseEngineMgr)
	encoder, err := tableImporter.GetKVEncoderForDupResolve()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, encoder.Close()) })

	pairs, err := encoder.Encode([]types.Datum{types.NewDatum(1), types.NewDatum(2), types.NewStringDatum("doc")}, 1)
	require.NoError(t, err)
	var normalIndexID int64
	for _, idx := range tblMeta.Indices {
		if idx.Name.L == "idx_b" {
			normalIndexID = idx.ID
			break
		}
	}
	require.NotZero(t, normalIndexID)

	var recordCount int
	var indexIDs []int64
	for _, pair := range pairs.Pairs {
		switch {
		case tablecodec.IsRecordKey(pair.Key):
			recordCount++
		case tablecodec.IsIndexKey(pair.Key):
			indexID, err := tablecodec.DecodeIndexID(pair.Key)
			require.NoError(t, err)
			indexIDs = append(indexIDs, indexID)
		}
	}
	require.Equal(t, 1, recordCount)
	require.Equal(t, []int64{normalIndexID}, indexIDs)
	require.NotContains(t, indexIDs, ticiIndexID)
}
