// Copyright 2025 PingCAP, Inc.

package ddl

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

/// DDL Executor part.

type pkdbExecutorExtension interface {
	CreateTrigger(ctx sessionctx.Context, stmt *ast.CreateTriggerStmt) error
	DropTrigger(ctx sessionctx.Context, stmt *ast.DropTriggerStmt) error
	CreateProcedure(ctx sessionctx.Context, stmt *ast.CreateProcedureInfo) error
	DropProcedure(ctx sessionctx.Context, stmt *ast.DropProcedureStmt) error
	AlterProcedure(ctx sessionctx.Context, stmt *ast.AlterProcedureStmt) error
}

func (e *executor) CreateTrigger(ctx sessionctx.Context, stmt *ast.CreateTriggerStmt) error {
	ident := ast.Ident{Name: stmt.TableName.Name, Schema: stmt.TableName.Schema}
	// Use current txn infoschema to obtain temporary tables.
	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()

	trigSchema, ok := is.SchemaByName(stmt.TriggerName.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	if err := validateTriggerTargetTable(schema, t); err != nil {
		return err
	}
	if stmt.TriggerName.Schema.L != ident.Schema.L {
		return dbterror.ErrTrgInWrongSchema.FastGenByArgs()
	}

	triggerInfo, err := buildTriggerInfo(ctx, stmt)
	if err != nil {
		return err
	}

	if err := validateTriggerStmt(triggerInfo, stmt.TriggerBody); err != nil {
		return err
	}
	triggerStmtValidator := &triggerStmtValidator{triggerInfo: triggerInfo, tblInfo: t.Meta()}
	stmt.TriggerBody.Accept(triggerStmtValidator)
	if triggerStmtValidator.err != nil {
		return triggerStmtValidator.err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       trigSchema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     trigSchema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionCreateTrigger,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: trigSchema.Name.L,
			Table:    t.Meta().Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.CreateTriggerArgs{
		TriggerInfo: triggerInfo,
	}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		if stmt.IfNotExists && dbterror.ErrTrgAlreadyExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

func validateTriggerTargetTable(schema *model.DBInfo, tbl table.Table) error {
	tblInfo := tbl.Meta()
	if tidbutil.IsMemOrSysDB(schema.Name.L) {
		return errors.Trace(dbterror.ErrNoTriggersOnSystemSchema)
	} else if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrTrgOnViewOrTempTable.FastGenByArgs(tblInfo.Name.O)
	} else if tblInfo.IsView() || tblInfo.IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schema.Name.L, tblInfo.Name.L, "BASE TABLE")
	}
	return nil
}

type triggerStmtValidator struct {
	triggerInfo *model.TriggerInfo
	tblInfo     *model.TableInfo
	err         error
}

func (v *triggerStmtValidator) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (v *triggerStmtValidator) Leave(in ast.Node) (ast.Node, bool) {
	ti := v.triggerInfo
	colNameToCheck := ""
	switch node := in.(type) {
	case *ast.ColumnName:
		switch node.Table.L {
		case "old":
			if ti.Event == ast.TriggerEventInsert {
				v.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs(node.Table.O, ti.Event.String())
				return in, false
			}
			colNameToCheck = node.Name.L
		case "new":
			if ti.Event == ast.TriggerEventDelete {
				v.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs(node.Table.O, ti.Event.String())
				return in, false
			}
			colNameToCheck = node.Name.L
		default:
		}
	case *ast.VariableAssignment:
		// split name into specifier and column
		parts := strings.Split(node.Name, ".")
		if len(parts) == 2 {
			switch strings.ToLower(parts[0]) {
			case "old":
				switch ti.Event {
				case ast.TriggerEventInsert:
					v.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs(parts[0], ti.Event.String())
					return in, false
				case ast.TriggerEventUpdate, ast.TriggerEventDelete:
					v.err = dbterror.ErrTrgCantChangeRow.FastGenByArgs(parts[0], "")
					return in, false
				}
				colNameToCheck = strings.ToLower(strings.Trim(parts[1], "`"))
			case "new":
				if ti.Event == ast.TriggerEventDelete {
					v.err = dbterror.ErrTrgNoSuchRowInTrg.FastGenByArgs(parts[0], ti.Event.String())
					return in, false
				}
				if ti.Timing == ast.TriggerTimingAfter {
					v.err = dbterror.ErrTrgCantChangeRow.FastGenByArgs(parts[0], ti.Event.String())
					return in, false
				}
				colNameToCheck = strings.ToLower(strings.Trim(parts[1], "`"))
			default:
			}
		}
	}
	if len(colNameToCheck) > 0 {
		found := false
		for _, col := range v.tblInfo.Columns {
			if col.Name.L == colNameToCheck {
				found = true
				break
			}
		}
		if !found {
			v.err = dbterror.ErrBadField.FastGenByArgs(colNameToCheck, v.tblInfo.Name.L)
			return in, false
		}
	}
	return in, true
}

func validateTriggerStmt(ti *model.TriggerInfo, stmts ...ast.StmtNode) error {
	for _, stmt := range stmts {
		switch v := stmt.(type) {
		case *ast.SelectStmt:
			if v.SelectIntoOpt == nil {
				return exeerrors.ErrSpNoRetset.FastGenByArgs("TRIGGER")
			}
		case *ast.SetOprStmt:
			return exeerrors.ErrSpNoRetset.FastGenByArgs("TRIGGER")
		case *ast.ProcedureIfInfo:
			if err := validateTriggerStmt(ti, v.IfBody.ProcedureIfStmts...); err != nil {
				return err
			}
			return validateTriggerStmt(ti, v.IfBody.ProcedureElseStmt)
		case *ast.ProcedureBlock:
			return validateTriggerStmt(ti, v.ProcedureProcStmts...)
		case *ast.SimpleCaseStmt:
			for _, wh := range v.WhenCases {
				if err := validateTriggerStmt(ti, wh.ProcedureStmts...); err != nil {
					return err
				}
			}
			return validateTriggerStmt(ti, v.ElseCases...)
		case *ast.SearchCaseStmt:
			for _, wh := range v.WhenCases {
				if err := validateTriggerStmt(ti, wh.ProcedureStmts...); err != nil {
					return err
				}
			}
			return validateTriggerStmt(ti, v.ElseCases...)
		case *ast.ProcedureWhileStmt:
			return validateTriggerStmt(ti, v.Body...)
		case *ast.ProcedureRepeatStmt:
			return validateTriggerStmt(ti, v.Body...)
		case *ast.ProcedureLoopStmt:
			return validateTriggerStmt(ti, v.Body...)
		case *ast.ProcedureLabelBlock:
			if v.Block != nil {
				return validateTriggerStmt(ti, v.Block.ProcedureProcStmts...)
			}
			return nil
		case *ast.ProcedureReturnStmt:
			return dbterror.ErrSpBadreturn
		}
	}
	return nil
}

func (e *executor) DropTrigger(ctx sessionctx.Context, stmt *ast.DropTriggerStmt) error {
	// For DROP TRIGGER, we need to find the table that contains the trigger.
	// The trigger name can be qualified with schema: [schema.]trigger_name
	var schemaName, triggerName pmodel.CIStr
	if stmt.TriggerName.Schema.L != "" {
		schemaName = stmt.TriggerName.Schema
		triggerName = stmt.TriggerName.Name
	} else {
		// Use current schema if not specified
		//nolint:forbidigo
		schemaName = pmodel.NewCIStr(ctx.GetSessionVars().CurrentDB)
		triggerName = stmt.TriggerName.Name
	}

	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(schemaName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}

	tableName, tableID, ok := infoschema.TableByTriggerName(is, schemaName, triggerName)
	if !ok {
		err := dbterror.ErrTrgDoesNotExist.FastGenByArgs()
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tableID,
		SchemaName:     schema.Name.L,
		TableName:      tableName.L,
		Type:           model.ActionDropTrigger,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: schema.Name.L,
			Table:    tableName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.DropTriggerArgs{
		TriggerName: triggerName,
		IfExists:    stmt.IfExists,
	}
	if err := e.doDDLJob2(ctx, job, args); err != nil {
		if stmt.IfExists && dbterror.ErrTrgDoesNotExist.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

func buildTriggerInfo(ctx sessionctx.Context, stmt *ast.CreateTriggerStmt) (*model.TriggerInfo, error) {
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var createSQLBuilder strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(restoreFlag, &createSQLBuilder)); err != nil {
		return nil, err
	}
	var bodyBuilder strings.Builder
	if err := stmt.TriggerBody.Restore(format.NewRestoreCtx(restoreFlag, &bodyBuilder)); err != nil {
		return nil, err
	}

	//nolint:forbidigo
	sessCtx := ctx.GetSessionVars()
	chs, ok := sessCtx.GetSystemVar(variable.CharacterSetClient)
	if !ok {
		return nil, errors.New("unknown system var " + variable.CharacterSetClient)
	}
	collConn, ok := sessCtx.GetSystemVar(variable.CollationConnection)
	if !ok {
		return nil, errors.New("unknown system var " + variable.CollationConnection)
	}
	dbColl, ok := sessCtx.GetSystemVar(variable.CollationDatabase)
	if !ok {
		return nil, errors.New("unknown system var " + variable.CollationDatabase)
	}

	triggerInfo := &model.TriggerInfo{
		Name:                stmt.TriggerName.Name,
		Timing:              stmt.Timing,
		Event:               stmt.Event,
		Table:               stmt.TableName.Name,
		Order:               stmt.Order,
		CreateSQL:           createSQLBuilder.String(),
		Body:                bodyBuilder.String(),
		Definer:             stmt.Definer,
		State:               model.StateNone,
		SQLMode:             ctx.GetSessionVars().SQLMode,
		CharacterSetClient:  chs,
		CollationConnection: collConn,
		DatabaseCollation:   dbColl,
	}

	return triggerInfo, nil
}

func checkDatabaseTriggerConflict(_ context.Context, is infoschema.InfoSchema, schemaName pmodel.CIStr, triggerName pmodel.CIStr) error {
	_, _, ok := infoschema.TableByTriggerName(is, schemaName, triggerName)
	if ok {
		return dbterror.ErrTrgAlreadyExists.GenWithStackByArgs(triggerName.L)
	}
	return nil
}

/// DDL worker part.

func onCreateTrigger(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetCreateTriggerArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	triggerInfo := args.TriggerInfo
	triggerInfo.State = model.StateNone

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if findTriggerByName(tblInfo, triggerInfo.Name) != nil {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrTrgAlreadyExists.GenWithStackByArgs(triggerInfo.Name.L)
	}

	destOffset := -1
	if triggerInfo.Order.OrderType != ast.TriggerOrderNone {
		offset := -1
		for i, trg := range tblInfo.Triggers {
			if trg.Name.L == triggerInfo.Order.OtherTriggerName.L {
				offset = i
				break
			}
		}
		if offset == -1 {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrTrgDoesNotExist.GenWithStackByArgs(triggerInfo.Order.OtherTriggerName.L)
		}
		switch triggerInfo.Order.OrderType {
		case ast.TriggerOrderPrecedes:
			destOffset = offset
		case ast.TriggerOrderFollows:
			destOffset = offset + 1
		}
	}

	schema := pmodel.NewCIStr(job.SchemaName)
	if err := checkDatabaseTriggerConflict(jobCtx.stepCtx, jobCtx.infoCache.GetLatest(), schema, triggerInfo.Name); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	switch triggerInfo.State {
	case model.StateNone:
		triggerInfo.State = model.StatePublic
		triggerInfo.CreatedTimestamp = job.StartTS
		if destOffset == -1 {
			tblInfo.Triggers = append(tblInfo.Triggers, triggerInfo)
		} else {
			tblInfo.Triggers = append(tblInfo.Triggers[:destOffset],
				append([]*model.TriggerInfo{triggerInfo}, tblInfo.Triggers[destOffset:]...)...)
		}
		// Don't update table info update ts.
		err = jobCtx.metaMut.UpdateTable(job.SchemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, errors.Errorf("invalid trigger state %v", triggerInfo.State)
	}
}

func onDropTrigger(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetDropTriggerArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	trigger := findTriggerByName(tblInfo, args.TriggerName)
	if trigger == nil {
		if !args.IfExists {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrTrgDoesNotExist.GenWithStackByArgs(args.TriggerName.O)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	}

	removeTriggerByName(tblInfo, trigger.Name)
	// Don't update table info update ts.
	err = jobCtx.metaMut.UpdateTable(job.SchemaID, tblInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func findTriggerByName(tblInfo *model.TableInfo, triggerName pmodel.CIStr) *model.TriggerInfo {
	for _, trigger := range tblInfo.Triggers {
		if trigger.Name.L == triggerName.L {
			return trigger
		}
	}
	return nil
}

func removeTriggerByName(tblInfo *model.TableInfo, triggerName pmodel.CIStr) {
	newTriggers := make([]*model.TriggerInfo, 0, len(tblInfo.Triggers))
	for _, trigger := range tblInfo.Triggers {
		if trigger.Name.L != triggerName.L {
			newTriggers = append(newTriggers, trigger)
		}
	}
	tblInfo.Triggers = newTriggers
}

func checkTriggersMovedToWrongSchema(tblInfo *model.TableInfo, args *model.RenameTableArgs, job *model.Job) error {
	oldSchemaID := args.OldSchemaID
	newSchemaID := args.NewSchemaID
	if newSchemaID == 0 {
		newSchemaID = job.SchemaID
	}
	if len(tblInfo.Triggers) > 0 && newSchemaID != oldSchemaID {
		return errors.Trace(dbterror.ErrTrgInWrongSchema)
	}
	return nil
}
