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

package copr

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNewCopContextSingleIndexIncludesSPFreshStoredColumn(t *testing.T) {
	cols := []*model.ColumnInfo{
		{
			ID:        1,
			Offset:    0,
			Name:      pmodel.NewCIStr("vec"),
			FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
			State:     model.StatePublic,
		},
		{
			ID:        2,
			Offset:    1,
			Name:      pmodel.NewCIStr("tenant_id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
			State:     model.StatePublic,
		},
	}
	idxInfo := &model.IndexInfo{
		ID:   11,
		Name: pmodel.NewCIStr("idx_vec"),
		Tp:   pmodel.IndexTypeSPFresh,
		Columns: []*model.IndexColumn{{
			Name:   cols[0].Name,
			Offset: cols[0].Offset,
		}},
		VectorInfo: &model.VectorIndexInfo{
			Kind:            model.VectorIndexKindSPFresh,
			StoredColumnIDs: []int64{cols[1].ID},
		},
	}
	tblInfo := &model.TableInfo{
		Name:    pmodel.NewCIStr("t"),
		Columns: cols,
		Indices: []*model.IndexInfo{idxInfo},
	}

	sctx := mock.NewContext()
	copCtx, err := NewCopContextSingleIndex(
		sctx.GetExprCtx(),
		sctx.GetSessionVars().StmtCtx.PushDownFlags(),
		tblInfo,
		idxInfo,
		"",
	)
	require.NoError(t, err)
	require.Len(t, copCtx.IndexColumnOutputOffsets(idxInfo.ID), 2) // vec + stored column
	base := copCtx.GetBase()
	names := make([]string, 0, len(base.ColumnInfos))
	for _, col := range base.ColumnInfos {
		names = append(names, col.Name.L)
	}
	require.Contains(t, names, "vec")
	require.Contains(t, names, "tenant_id")
}
