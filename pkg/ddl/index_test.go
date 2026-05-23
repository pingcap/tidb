
package ddl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/internal/metabuild"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexColumnsValidation(t *testing.T) {
	colName := "c1"
	tableName := "t1"

	tests := []struct {
		name              string
		indexPartSpec     []*ast.IndexPartSpecification
		currentTableName  string
		expectedErr       error
	}{
		{
			name: "No qualifiers, valid",
			indexPartSpec: []*ast.IndexPartSpecification{
				{
					Column: &ast.ColumnName{
						Name: model.NewCIStr(colName),
					},
				},
			},
			currentTableName: tableName,
			expectedErr:      nil,
		},
		{
			name: "Schema qualifier, invalid",
			indexPartSpec: []*ast.IndexPartSpecification{
				{
					Column: &ast.ColumnName{
						Schema: model.NewCIStr("db1"),
						Name:   model.NewCIStr(colName),
					},
				},
			},
			currentTableName: tableName,
			expectedErr:      dbterror.ErrWrongColumnName,
		},
		{
			name: "Correct table qualifier, valid",
			indexPartSpec: []*ast.IndexPartSpecification{
				{
					Column: &ast.ColumnName{
						Table: model.NewCIStr(tableName),
						Name:  model.NewCIStr(colName),
					},
				},
			},
			currentTableName: tableName,
			expectedErr:      nil,
		},
		{
			name: "Incorrect table qualifier, invalid",
			indexPartSpec: []*ast.IndexPartSpecification{
				{
					Column: &ast.ColumnName{
						Table: model.NewCIStr("t2"),
						Name:  model.NewCIStr(colName),
					},
				},
			},
			currentTableName: tableName,
			expectedErr:      dbterror.ErrWrongColumnName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mocking necessary components
			ctx := &metabuild.Context{}
			columns := []*model.ColumnInfo{
				{
					Name: model.NewCIStr(colName),
				},
			}

			_, _, err := buildIndexColumns(ctx, columns, tt.indexPartSpec, model.ColumnarIndexTypeNA, tt.currentTableName)

			if tt.expectedErr != nil {
				require.NotNil(t, err)
				require.True(t, terror.ErrorEqual(err, tt.expectedErr), "expected error %v, got %v", tt.expectedErr, err)
				require.True(t, strings.Contains(err.Error(), "column '"+colName+"'"), "error message should contain column name")
				if tt.indexPartSpec[0].Column.Table.L != "" && !strings.EqualFold(tt.indexPartSpec[0].Column.Table.L, tt.currentTableName) {
					require.True(t, strings.Contains(err.Error(), fmt.Sprintf("specifies an invalid table prefix '%s'. Expected column from table '%s'", tt.indexPartSpec[0].Column.Table.O, tt.currentTableName)), "error message should contain table prefix info")
				} else if tt.indexPartSpec[0].Column.Schema.L != "" {
					require.True(t, strings.Contains(err.Error(), "specifies a database prefix, which is not allowed in index definition"), "error message should contain schema prefix info")
				}
			} else {
				require.Nil(t, err)
			}
		})
	}
}
