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

package importer

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	lkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func fillRowPreviousSlowPath(
	en *TableKVEncoder,
	record []types.Datum,
	row []types.Datum,
	hasValue []bool,
	rowID int64,
) error {
	record = record[:0]
	for i := range en.Columns {
		var theDatum *types.Datum
		doCast := true
		if hasValue[i] {
			theDatum = &row[i]
			doCast = false
		}
		value, err := en.ProcessColDatum(en.Columns[i], rowID, theDatum, doCast)
		if err != nil {
			return err
		}
		record = append(record, value)
	}
	return nil
}

func newTableKVEncoderForCreateTable(tb testing.TB, createSQL string) *TableKVEncoder {
	tb.Helper()
	p := parser.New()
	node, err := p.ParseOneStmt(createSQL, "", "")
	require.NoError(tb, err)

	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(tb, err)
	tblInfo.State = model.StatePublic
	tbl, err := tables.TableFromMeta(lkv.NewPanickingAllocators(tblInfo.SepAutoInc()), tblInfo)
	require.NoError(tb, err)

	encoder, err := newTableKVEncoderInner(
		&encode.EncodingConfig{Table: tbl},
		&LoadDataController{ASTArgs: &ASTArgs{}, Plan: &Plan{}, Table: tbl},
		[]*FieldMapping{{Column: tbl.VisibleCols()[0]}},
		[]*table.Column{tbl.VisibleCols()[0]},
	)
	require.NoError(tb, err)
	return encoder
}

func TestInitFillRowProcessorsFastPathSelection(t *testing.T) {
	t.Run("all normal columns", func(t *testing.T) {
		encoder := newTableKVEncoderForCreateTable(t, "create table t(a int not null, b int)")
		require.Equal(t, []int{0, 1}, encoder.normalColIdxs)
		require.Empty(t, encoder.specialColIdxs)
	})

	t.Run("auto increment exists", func(t *testing.T) {
		encoder := newTableKVEncoderForCreateTable(t, "create table t(id bigint auto_increment, v int, key(id))")
		require.Equal(t, []int{1}, encoder.normalColIdxs)
		require.ElementsMatch(t, []int{0}, encoder.specialColIdxs)
	})

	t.Run("generated exists", func(t *testing.T) {
		encoder := newTableKVEncoderForCreateTable(t, "create table t(a int, b int generated always as (a + 1) stored)")
		require.Equal(t, []int{0}, encoder.normalColIdxs)
		require.ElementsMatch(t, []int{1}, encoder.specialColIdxs)
	})
}

func TestFillRowNormalFastUsesDefaultForMissingColumn(t *testing.T) {
	encoder := newTableKVEncoderForCreateTable(t, "create table t(a int not null default 5, b int)")
	visibleCols := encoder.Columns
	require.Len(t, visibleCols, 2)

	bCol := visibleCols[1]
	encoder.fieldMappings = []*FieldMapping{{Column: bCol}}
	encoder.insertColumns = []*table.Column{bCol}

	record, err := encoder.parserData2TableData([]types.Datum{types.NewIntDatum(7)}, 11)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(record), 2)
	require.Equal(t, int64(5), record[0].GetInt64())
	require.Equal(t, int64(7), record[1].GetInt64())
}
