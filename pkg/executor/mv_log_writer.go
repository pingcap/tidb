package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/materializedview"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
)

func findMVLogTable(ctx context.Context, sctx sessionctx.Context, baseTbl table.Table) (table.Table, *model.MaterializedViewLogInfo, error) {
	if !sctx.GetSessionVars().EnableMaterializedViewDemo {
		return nil, nil, nil
	}

	baseInfo := baseTbl.Meta()
	if baseInfo == nil || baseInfo.IsMaterializedView() || baseInfo.IsMaterializedViewLog() {
		return nil, nil, nil
	}

	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	var logInfo *model.TableInfo
	if baseInfo.DBID != 0 {
		if dbInfo, ok := is.SchemaByID(baseInfo.DBID); ok {
			var err error
			logInfo, err = materializedview.FindLogTableInfoInSchema(is, dbInfo.Name, baseInfo.ID)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if logInfo == nil {
		for _, db := range is.AllSchemas() {
			var err error
			logInfo, err = materializedview.FindLogTableInfoInSchema(is, db.Name, baseInfo.ID)
			if err != nil {
				return nil, nil, err
			}
			if logInfo != nil {
				break
			}
		}
	}
	if logInfo == nil || logInfo.MaterializedViewLogInfo == nil {
		return nil, nil, nil
	}
	logTbl, ok := is.TableByID(ctx, logInfo.ID)
	if !ok {
		return nil, nil, errors.Errorf("mv log table %d not found in infoschema", logInfo.ID)
	}
	return logTbl, logInfo.MaterializedViewLogInfo, nil
}

func projectRowByColumnIDs(cols []*table.Column, row []types.Datum, columnIDs []int64) ([]types.Datum, error) {
	idToOffset := make(map[int64]int, len(cols))
	for i, col := range cols {
		idToOffset[col.ID] = i
	}
	out := make([]types.Datum, 0, len(columnIDs))
	for _, id := range columnIDs {
		off, ok := idToOffset[id]
		if !ok || off < 0 || off >= len(row) {
			return nil, errors.Errorf("column %d not found in row", id)
		}
		out = append(out, row[off])
	}
	return out, nil
}

func findColumnByID(cols []*table.Column, id int64) *table.Column {
	for _, col := range cols {
		if col.ID == id {
			return col
		}
	}
	return nil
}

func appendMVLogRow(ctx context.Context, sctx sessionctx.Context, logTbl table.Table, datums []types.Datum) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	pessimisticLazyCheck := getPessimisticLazyCheckMode(sctx.GetSessionVars())
	_, err = logTbl.AddRecord(sctx.GetTableCtx(), txn, datums, table.WithCtx(ctx), table.DupKeyCheckSkip, pessimisticLazyCheck)
	return err
}

func appendMVLogInsert(ctx context.Context, sctx sessionctx.Context, baseTbl table.Table, newData []types.Datum) error {
	logTbl, logInfo, err := findMVLogTable(ctx, sctx, baseTbl)
	if err != nil || logTbl == nil {
		return err
	}
	projected, err := projectRowByColumnIDs(baseTbl.Cols(), newData, logInfo.ColumnIDs)
	if err != nil {
		return err
	}
	projected = append(projected, types.NewStringDatum(materializedview.MVLogDMLTypeInsert), types.NewStringDatum(materializedview.MVLogOldNewNew))
	return appendMVLogRow(ctx, sctx, logTbl, projected)
}

func appendMVLogUpdate(ctx context.Context, sctx sessionctx.Context, baseTbl table.Table, oldData, newData []types.Datum) error {
	logTbl, logInfo, err := findMVLogTable(ctx, sctx, baseTbl)
	if err != nil || logTbl == nil {
		return err
	}
	oldProjected, err := projectRowByColumnIDs(baseTbl.Cols(), oldData, logInfo.ColumnIDs)
	if err != nil {
		return err
	}
	oldProjected = append(oldProjected, types.NewStringDatum(materializedview.MVLogDMLTypeUpdate), types.NewStringDatum(materializedview.MVLogOldNewOld))

	newProjected, err := projectRowByColumnIDs(baseTbl.Cols(), newData, logInfo.ColumnIDs)
	if err != nil {
		return err
	}
	newProjected = append(newProjected, types.NewStringDatum(materializedview.MVLogDMLTypeUpdate), types.NewStringDatum(materializedview.MVLogOldNewNew))

	if err := appendMVLogRow(ctx, sctx, logTbl, oldProjected); err != nil {
		return err
	}
	return appendMVLogRow(ctx, sctx, logTbl, newProjected)
}

func appendMVLogDelete(ctx context.Context, sctx sessionctx.Context, baseTbl table.Table, h kv.Handle) error {
	logTbl, logInfo, err := findMVLogTable(ctx, sctx, baseTbl)
	if err != nil || logTbl == nil {
		return err
	}

	cols := make([]*table.Column, 0, len(logInfo.ColumnIDs))
	for _, id := range logInfo.ColumnIDs {
		col := findColumnByID(baseTbl.Cols(), id)
		if col == nil {
			return errors.Errorf("column %d not found in base table", id)
		}
		cols = append(cols, col)
	}
	oldVals, err := tables.RowWithCols(baseTbl, sctx, h, cols)
	if err != nil {
		return err
	}
	oldVals = append(oldVals, types.NewStringDatum(materializedview.MVLogDMLTypeDelete), types.NewStringDatum(materializedview.MVLogOldNewOld))
	return appendMVLogRow(ctx, sctx, logTbl, oldVals)
}
