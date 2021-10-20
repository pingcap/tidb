package handle

import (
	"context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/trace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

type Handle struct {
	RecordCh chan []*trace.CETraceRecord
	Session  sessionctx.Context
}

func NewHandle(sctx sessionctx.Context) *Handle {
	h := &Handle{
		RecordCh: make(chan []*trace.CETraceRecord, 100),
		Session:  sctx,
	}
	return h
}

func (h *Handle) Run(rec *trace.CETraceRecord) {
	is := h.Session.GetInfoSchema().(infoschema.InfoSchema)
	tbl, ok := is.TableByID(rec.TableID)
	if !ok {
		logutil.BgLogger().Warn("[CE Trace] Failed to find table in infoschema",
			zap.Int64("table id", rec.TableID))
	}
	tblInfo := tbl.Meta()
	tableName := tblInfo.Name.O
	dbInfo, ok := is.SchemaByTable(tblInfo)
	if !ok {
		logutil.BgLogger().Warn("[CE Trace] Failed to find db in infoschema",
			zap.Int64("table id", rec.TableID),
			zap.String("table name", tableName))
	}
	dbName := dbInfo.Name.O
	ctx := context.Background()
	sql := "insert into mysql.optimizer_trace value (%?, %?, %?, %?, %?)"
	exec := h.Session.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(ctx, sql, rec.Type, dbName, tableName, rec.Expr, rec.RowCount)
	if err != nil {
		logutil.BgLogger().Warn("[CE Trace] Error from ParseWithParams", zap.Error(err))
	}
	_, _, err = exec.ExecRestrictedStmt(ctx, stmt)
	if err != nil {
		logutil.BgLogger().Warn("[CE Trace] Error from ExecRestrictedStmt", zap.Error(err))
	}
}

