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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestAlterGeneratedColumnDependsOnAutoEmbeddingColumn(t *testing.T) {
	schema := &model.DBInfo{Name: pmodel.NewCIStr("test")}
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("txt"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *parsertypes.NewFieldType(mysql.TypeVarchar),
			},
			{
				ID:        2,
				Name:      pmodel.NewCIStr("v"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *parsertypes.NewFieldType(mysql.TypeTiDBVectorFloat32),
			},
			{
				ID:                  3,
				Name:                pmodel.NewCIStr("emb"),
				Offset:              2,
				State:               model.StatePublic,
				FieldType:           *parsertypes.NewFieldType(mysql.TypeTiDBVectorFloat32),
				GeneratedExprString: "embed_text('mock/model', `txt`)",
				GeneratedStored:     true,
				Dependences: map[string]struct{}{
					"txt": {},
				},
			},
			{
				ID:                  4,
				Name:                pmodel.NewCIStr("v_copy"),
				Offset:              3,
				State:               model.StatePublic,
				FieldType:           *parsertypes.NewFieldType(mysql.TypeTiDBVectorFloat32),
				GeneratedExprString: "`v`",
				GeneratedStored:     false,
				Dependences: map[string]struct{}{
					"v": {},
				},
			},
		},
	}
	tbl := tables.MockTableFromMeta(tblInfo)
	ctx := mock.NewContext()
	p := parser.New()

	stmt, err := p.ParseOneStmt("alter table t add column emb_copy vector as (emb)", "", "")
	require.NoError(t, err)
	addSpec := stmt.(*ast.AlterTableStmt).Specs[0]
	_, err = CreateNewColumn(ctx, schema, addSpec, tbl, addSpec.NewColumns[0])
	require.ErrorContains(t, err, "generated column on an auto-embedding column is not supported")

	stmt, err = p.ParseOneStmt("alter table t modify column v_copy vector as (emb)", "", "")
	require.NoError(t, err)
	modifySpec := stmt.(*ast.AlterTableStmt).Specs[0]
	oldCol := table.FindCol(tbl.Cols(), "v_copy")
	require.NotNil(t, oldCol)

	newCol := table.ToColumn(oldCol.ColumnInfo.Clone())
	newCol.Name = modifySpec.NewColumns[0].Name.Name
	newCol.FieldType = *modifySpec.NewColumns[0].Tp
	err = processModifyColumnOptions(ctx, newCol, modifySpec.NewColumns[0].Options)
	require.NoError(t, err)

	err = checkModifyGeneratedColumn(ctx, schema.Name, tbl, oldCol, newCol, modifySpec.NewColumns[0], modifySpec.Position)
	require.ErrorContains(t, err, "generated column on an auto-embedding column is not supported")
}
