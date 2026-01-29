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

func FindLogTableInfo(is infoschema.InfoSchema, baseTableID int64) (*model.TableInfo, error) {
	for _, db := range is.AllSchemas() {
		tbl, err := FindLogTableInfoInSchema(is, db.Name, baseTableID)
		if err != nil {
			return nil, err
		}
		if tbl != nil {
			return tbl, nil
		}
	}
	return nil, nil
}

func HasDependentMaterializedView(is infoschema.InfoSchema, baseTableID int64) (bool, error) {
	for _, db := range is.AllSchemas() {
		tbls, err := is.SchemaTableInfos(context.Background(), db.Name)
		if err != nil {
			return false, err
		}
		for _, tbl := range tbls {
			if tbl != nil && tbl.IsMaterializedView() && tbl.MaterializedViewInfo != nil && tbl.MaterializedViewInfo.BaseTableID == baseTableID {
				return true, nil
			}
		}
	}
	return false, nil
}
