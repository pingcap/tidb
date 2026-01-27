package materializedview

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

const (
	MVLogColumnDMLType = "__mv_dml_type"
	MVLogColumnOldNew  = "__mv_old_new"

	MVLogDMLTypeInsert = "I"
	MVLogDMLTypeUpdate = "U"
	MVLogDMLTypeDelete = "D"

	MVLogOldNewOld = "O"
	MVLogOldNewNew = "N"
)

func FindLogTableInfoInSchema(is infoschema.InfoSchema, schema ast.CIStr, baseTableID int64) (*model.TableInfo, error) {
	tbls, err := is.SchemaTableInfos(context.Background(), schema)
	if err != nil {
		return nil, err
	}
	for _, tbl := range tbls {
		if tbl != nil && tbl.IsMaterializedViewLog() && tbl.MaterializedViewLogInfo.BaseTableID == baseTableID {
			return tbl, nil
		}
	}
	return nil, nil
}
