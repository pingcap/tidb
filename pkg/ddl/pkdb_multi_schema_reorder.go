// Copyright 2026 PingCAP, Inc.

package ddl

import (
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// pkdbReorderMultiSchemaSpecsForDropPrimaryKey applies a narrow multi-schema-only reorder:
// when the original primary key covers a column that is being dropped in the same statement,
// move the DROP PRIMARY KEY spec to the front so submitter-side prechecks see the intended order.
func pkdbReorderMultiSchemaSpecsForDropPrimaryKey(validSpecs []*ast.AlterTableSpec, tbInfo *model.TableInfo) []*ast.AlterTableSpec {
	dropPKIdx := pkdbFindDropPrimaryKeySpec(validSpecs)
	if dropPKIdx <= 0 {
		return validSpecs
	}

	pkCols := pkdbPrimaryKeyColumns(tbInfo)
	if len(pkCols) == 0 {
		return validSpecs
	}

	shouldMove := false
	for _, spec := range validSpecs {
		if spec.Tp != ast.AlterTableDropColumn || spec.OldColumnName == nil {
			continue
		}
		if _, ok := pkCols[spec.OldColumnName.Name.L]; ok {
			shouldMove = true
			break
		}
	}
	if !shouldMove {
		return validSpecs
	}

	reordered := make([]*ast.AlterTableSpec, 0, len(validSpecs))
	reordered = append(reordered, validSpecs[dropPKIdx])
	reordered = append(reordered, validSpecs[:dropPKIdx]...)
	reordered = append(reordered, validSpecs[dropPKIdx+1:]...)
	return reordered
}

func pkdbFindDropPrimaryKeySpec(validSpecs []*ast.AlterTableSpec) int {
	for i, spec := range validSpecs {
		if spec.Tp == ast.AlterTableDropPrimaryKey {
			return i
		}
		if spec.Tp == ast.AlterTableDropIndex && pmodel.NewCIStr(spec.Name).L == strings.ToLower(mysql.PrimaryKeyName) {
			return i
		}
	}
	return -1
}

func pkdbPrimaryKeyColumns(tbInfo *model.TableInfo) map[string]struct{} {
	pkCols := make(map[string]struct{})
	if tbInfo == nil || tbInfo.HasClusteredIndex() {
		return pkCols
	}

	if idx := tbInfo.GetPrimaryKey(); idx != nil && idx.Primary {
		for _, col := range idx.Columns {
			pkCols[col.Name.L] = struct{}{}
		}
	}
	return pkCols
}
