package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ProcedureExec create \call \drop procedure exec plan.
type ProcedureExec struct {
	baseExecutor
	done          bool
	IsStrict      bool
	flag          int
	is            infoschema.InfoSchema
	Statement     ast.StmtNode
	oldSQLMode    string
	procedureInfo *plannercore.ProcedurebodyInfo
	Plan          *plannercore.ProcedurePlan
	warnings      []error
	outParam      map[string]string
	procedurePlan plannercore.ProcedureExec
}

// buildCreateProcedure Create stored procedure create executor.
func (b *executorBuilder) buildCreateProcedure(v *plannercore.CreateProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.ProcedureInfo,
		is:           b.is,
		done:         false,
		outParam:     make(map[string]string, 10),
	}
	b.err = errors.New("Currently only the Enterprise Edition supports stored procedures")
	return e
}

// buildCreateProcedure create stored procedure drop executor.
func (b *executorBuilder) buildDropProcedure(v *plannercore.DropProcedure) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor: base,
		Statement:    v.Procedure,
		is:           b.is,
		done:         false,
	}
	return e
}

// fetchShowCreateProcdure query stored procedure.
func (e *ShowExec) fetchShowCreateProcdure(ctx context.Context) error {
	if e.Procedure.Schema.O == "" {
		e.Procedure.Schema = model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)
	}
	_, ok := e.is.SchemaByName(e.Procedure.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	procedureInfo, err := getProcedureinfo(internalCtx, sqlExecutor, e.Procedure.Name.String(), e.Procedure.Schema.O)
	if err != nil {
		return err
	}
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	e.appendRow([]interface{}{procedureInfo.Name, procedureInfo.SQLMode, procedureInfo.Procedurebody, procedureInfo.CharacterSetClient,
		procedureInfo.CollationConnection, procedureInfo.ShemaCollation})
	return nil
}

// getProcedureinfo read stored procedure content.
func getProcedureinfo(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, name string, db string) (*plannercore.ProcedurebodyInfo, error) {
	sql := new(strings.Builder)
	//names = []string{"Procedure", "sql_mode", "Create Procedure", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select name, sql_mode ,definition_utf8,parameter_str,character_set_client, connection_collation,")
	sqlexec.MustFormatSQL(sql, "schema_collation from %n.%n where route_schema = %?  and name = %? and type = 'PROCEDURE' ", mysql.SystemDB, mysql.Routines, db, name)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return nil, err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 3)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, exeerrors.ErrSpDoesNotExist.GenWithStackByArgs("PROCEDURE", name)
	}
	if len(rows) != 1 {
		return nil, errors.New("Multiple stored procedures found in table " + mysql.Routines)
	}
	procedurebodyInfo := &plannercore.ProcedurebodyInfo{}
	procedurebodyInfo.Name = rows[0].GetString(0)
	procedurebodyInfo.Procedurebody = " CREATE PROCEDURE `" + rows[0].GetString(0) + "`(" + rows[0].GetString(3) + ") " + rows[0].GetString(2)
	procedurebodyInfo.SQLMode = rows[0].GetSet(1).String()
	procedurebodyInfo.CharacterSetClient = rows[0].GetString(4)
	procedurebodyInfo.CollationConnection = rows[0].GetString(5)
	procedurebodyInfo.ShemaCollation = rows[0].GetString(6)
	return procedurebodyInfo, nil
}

// fetchShowProcedureStatus implement SHOW PROCEDURE STATUS [LIKE 'pattern' | WHERE expr]
func (e *ShowExec) fetchShowProcedureStatus(ctx context.Context, showType string) error {
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sysSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer e.releaseSysSession(internalCtx, sysSession)
	sqlExecutor := sysSession.(sqlexec.SQLExecutor)
	err = e.getRowsProcedure(internalCtx, sqlExecutor, showType)
	if err != nil {
		return err
	}
	//names = []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment",
	//"character_set_client", "collation_connection", "Database Collation"}
	return nil
}

// getRowsProcedure read rows from table.
func (e *ShowExec) getRowsProcedure(ctx context.Context, sqlExecutor sqlexec.SQLExecutor, showType string) error {
	sql := new(strings.Builder)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)
	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	//names = []string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}
	sqlexec.MustFormatSQL(sql, "select route_schema, name, type, definer ,last_altered,created,security_type, comment,")
	sqlexec.MustFormatSQL(sql, "character_set_client, connection_collation,schema_collation from %n.%n where type = %?", mysql.SystemDB, mysql.Routines, showType)
	recordSet, err := sqlExecutor.ExecuteInternal(ctx, sql.String())
	if err != nil {
		return err
	}
	if recordSet != nil {
		defer recordSet.Close()
	}

	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, 1024)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if fieldFilter != "" && row.GetString(1) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(row.GetString(1)) {
			continue
		}
		e.appendRow([]interface{}{row.GetString(0), row.GetString(1), row.GetEnum(2).String(), row.GetString(3), row.GetTime(4), row.GetTime(5), row.GetEnum(6).String(),
			row.GetString(7), row.GetString(8), row.GetString(9), row.GetString(10)})
	}
	return nil
}

// buildCallProcedure generate the execution plan of the call.
func (b *executorBuilder) buildCallProcedure(v *plannercore.CallStmt) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &ProcedureExec{
		baseExecutor:  base,
		Statement:     v.Callstmt,
		flag:          0,
		is:            b.is,
		done:          false,
		oldSQLMode:    v.OldSQLMod,
		procedureInfo: v.ProcedureInfo,
		Plan:          v.Plan,
		outParam:      make(map[string]string, 10),
		procedurePlan: v.Plan.ProcedureExecPlan,
		IsStrict:      v.IsStrictMode,
	}
	b.err = errors.New("Currently only the Enterprise Edition supports stored procedures")
	return e
}
