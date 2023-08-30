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

package cardinality

import (
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPseudoTable(t *testing.T) {
	ti := &model.TableInfo{}
	colInfo := &model.ColumnInfo{
		ID:        1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	ti.Columns = append(ti.Columns, colInfo)
	tbl := statistics.PseudoTable(ti)
	require.Len(t, tbl.Columns, 1)
	require.Greater(t, tbl.RealtimeCount, int64(0))
	sctx := mock.NewContext()
	count := columnLessRowCount(sctx, tbl, types.NewIntDatum(100), colInfo.ID)
	require.Equal(t, 3333, int(count))
	count, err := ColumnEqualRowCount(sctx, tbl, types.NewIntDatum(1000), colInfo.ID)
	require.NoError(t, err)
	require.Equal(t, 10, int(count))
	count, _ = columnBetweenRowCount(sctx, tbl, types.NewIntDatum(1000), types.NewIntDatum(5000), colInfo.ID)
	require.Equal(t, 250, int(count))
	ti.Columns = append(ti.Columns, &model.ColumnInfo{
		ID:        2,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		Hidden:    true,
		State:     model.StatePublic,
	})
	tbl = statistics.PseudoTable(ti)
	// We added a hidden column. The pseudo table still only have one column.
	require.Equal(t, len(tbl.Columns), 1)
}
