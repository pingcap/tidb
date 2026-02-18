// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// checkContainDotColumn checks field contains the table name.
// for example :create table t (c1.c2 int default null).
func (p *preprocessor) checkContainDotColumn(stmt *ast.CreateTableStmt) {
	tName := stmt.Table.Name.String()
	sName := stmt.Table.Schema.String()

	for _, colDef := range stmt.Cols {
		// check schema and table names.
		if colDef.Name.Schema.O != sName && len(colDef.Name.Schema.O) != 0 {
			p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(colDef.Name.Schema.O)
			return
		}
		if colDef.Name.Table.O != tName && len(colDef.Name.Table.O) != 0 {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(colDef.Name.Table.O)
			return
		}
	}
}

func (p *preprocessor) stmtType() string {
	switch p.stmtTp {
	case TypeDelete:
		return "DELETE"
	case TypeUpdate:
		return "UPDATE"
	case TypeInsert:
		return "INSERT"
	case TypeDrop:
		return "DROP"
	case TypeCreate:
		return "CREATE"
	case TypeAlter:
		return "ALTER"
	case TypeRename:
		return "DROP, ALTER"
	case TypeRepair:
		return "SELECT, INSERT"
	case TypeShow:
		return "SHOW"
	case TypeImportInto:
		return "IMPORT INTO"
	default:
		return "SELECT" // matches Select and uncaught cases.
	}
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {
		if slices.Contains(p.preprocessWith.cteCanUsed, tn.Name.L) {
			p.preprocessWith.UpdateCTEConsumerCount(tn.Name.L)
			return
		}

		currentDB := p.sctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			p.err = errors.Trace(plannererrors.ErrNoDB)
			return
		}

		tn.Schema = ast.NewCIStr(currentDB)
	}

	if p.flag&inCreateOrDropTable > 0 {
		// The table may not exist in create table or drop table statement.
		if p.flag&inRepairTable > 0 {
			// Create stmt is in repair stmt, skip resolving the table to avoid error.
			return
		}
		// Create stmt is not in repair stmt, check the table not in repair list.
		if domainutil.RepairInfo.InRepairMode() {
			p.checkNotInRepair(tn)
		}
		return
	}
	// repairStmt: admin repair table A create table B ...
	// repairStmt's tableName is whether `inCreateOrDropTable` or `inRepairTable` flag.
	if p.flag&inRepairTable > 0 {
		p.handleRepairName(tn)
		return
	}

	if p.stmtTp == TypeSelect {
		if p.err = p.staleReadProcessor.OnSelectTable(tn); p.err != nil {
			return
		}
		if p.err = p.updateStateFromStaleReadProcessor(); p.err != nil {
			return
		}
	}

	table, err := p.tableByName(tn)
	if err != nil {
		p.err = err
		return
	}

	if !p.skipLockMDL() {
		table, err = tryLockMDLAndUpdateSchemaIfNecessary(p.ctx, p.sctx.GetPlanCtx(), ast.NewCIStr(tn.Schema.L), table, p.ensureInfoSchema())
		if err != nil {
			p.err = err
			return
		}
	}

	tableInfo := table.Meta()
	dbInfo, _ := infoschema.SchemaByTable(p.ensureInfoSchema(), tableInfo)
	// tableName should be checked as sequence object.
	if p.flag&inSequenceFunction > 0 {
		if !tableInfo.IsSequence() {
			p.err = infoschema.ErrWrongObject.GenWithStackByArgs(dbInfo.Name.O, tableInfo.Name.O, "SEQUENCE")
			return
		}
	}
	p.resolveCtx.AddTableName(&resolve.TableNameW{
		TableName: tn,
		DBInfo:    dbInfo,
		TableInfo: tableInfo,
	})
}

func (p *preprocessor) checkNotInRepair(tn *ast.TableName) {
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	if dbInfo == nil {
		return
	}
	if tableInfo != nil {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tn.Name.L, "this table is in repair")
	}
}

func (p *preprocessor) handleRepairName(tn *ast.TableName) {
	// Check the whether the repaired table is system table.
	if metadef.IsMemOrSysDB(tn.Schema.L) {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("memory or system database is not for repair")
		return
	}
	tableInfo, dbInfo := domainutil.RepairInfo.GetRepairedTableInfoByTableName(tn.Schema.L, tn.Name.L)
	// tableName here only has the schema rather than DBInfo.
	if dbInfo == nil {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("database " + tn.Schema.L + " is not in repair")
		return
	}
	if tableInfo == nil {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("table " + tn.Name.L + " is not in repair")
		return
	}
	p.sctx.SetValue(domainutil.RepairedTable, tableInfo)
	p.sctx.SetValue(domainutil.RepairedDatabase, dbInfo)
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	if node.DBName == "" {
		if node.Table != nil && node.Table.Schema.L != "" {
			node.DBName = node.Table.Schema.O
		} else {
			node.DBName = p.sctx.GetSessionVars().CurrentDB
		}
	} else if node.Table != nil && node.Table.Schema.L == "" {
		node.Table.Schema = ast.NewCIStr(node.DBName)
	}
	if node.User != nil && node.User.CurrentUser {
		// Fill the Username and Hostname with the current user.
		currentUser := p.sctx.GetSessionVars().User
		if currentUser != nil {
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveExecuteStmt(node *ast.ExecuteStmt) {
	prepared, err := GetPreparedStmt(node, p.sctx.GetSessionVars())
	if err != nil {
		p.err = err
		return
	}

	if p.err = p.staleReadProcessor.OnExecutePreparedStmt(prepared.SnapshotTSEvaluator); p.err == nil {
		if p.err = p.updateStateFromStaleReadProcessor(); p.err != nil {
			return
		}
	}
}

func (*preprocessor) resolveCreateTableStmt(node *ast.CreateTableStmt) {
	for _, val := range node.Constraints {
		if val.Refer != nil && val.Refer.Table.Schema.String() == "" {
			val.Refer.Table.Schema = node.Table.Schema
		}
	}
}

func (p *preprocessor) resolveAlterTableStmt(node *ast.AlterTableStmt) {
	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			p.flag |= inCreateOrDropTable
			break
		}
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Refer != nil {
			table := spec.Constraint.Refer.Table
			if table.Schema.L == "" && node.Table.Schema.L != "" {
				table.Schema = ast.NewCIStr(node.Table.Schema.L)
			}
			if spec.Constraint.Tp == ast.ConstraintForeignKey {
				// when foreign_key_checks is off, should ignore err when refer table is not exists.
				p.flag |= inCreateOrDropTable
			}
		}
	}
}

func (p *preprocessor) resolveCreateSequenceStmt(stmt *ast.CreateSequenceStmt) {
	sName := stmt.Name.Name.String()
	if util.IsInCorrectIdentifierName(sName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(sName)
		return
	}
}

func (p *preprocessor) checkFuncCastExpr(node *ast.FuncCastExpr) {
	if node.Tp.EvalType() == types.ETDecimal {
		if node.Tp.GetFlen() >= node.Tp.GetDecimal() && node.Tp.GetFlen() <= mysql.MaxDecimalWidth && node.Tp.GetDecimal() <= mysql.MaxDecimalScale {
			// valid
			return
		}

		var buf strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := node.Expr.Restore(restoreCtx); err != nil {
			p.err = err
			return
		}
		if node.Tp.GetFlen() < node.Tp.GetDecimal() {
			p.err = types.ErrMBiggerThanD.GenWithStackByArgs(buf.String())
			return
		}
		if node.Tp.GetFlen() > mysql.MaxDecimalWidth {
			p.err = types.ErrTooBigPrecision.GenWithStackByArgs(node.Tp.GetFlen(), buf.String(), mysql.MaxDecimalWidth)
			return
		}
		if node.Tp.GetDecimal() > mysql.MaxDecimalScale {
			p.err = types.ErrTooBigScale.GenWithStackByArgs(node.Tp.GetDecimal(), buf.String(), mysql.MaxDecimalScale)
			return
		}
	}
	if node.Tp.EvalType() == types.ETDatetime {
		if node.Tp.GetDecimal() > types.MaxFsp {
			p.err = types.ErrTooBigPrecision.GenWithStackByArgs(node.Tp.GetDecimal(), "CAST", types.MaxFsp)
			return
		}
	}
}

func (p *preprocessor) updateStateFromStaleReadProcessor() error {
	if p.initedLastSnapshotTS {
		return nil
	}

	if p.IsStaleness = p.staleReadProcessor.IsStaleness(); p.IsStaleness {
		p.LastSnapshotTS = p.staleReadProcessor.GetStalenessReadTS()
		p.SnapshotTSEvaluator = p.staleReadProcessor.GetStalenessTSEvaluatorForPrepare()
		p.InfoSchema = p.staleReadProcessor.GetStalenessInfoSchema()
		p.InfoSchema = &infoschema.SessionExtendedInfoSchema{InfoSchema: p.InfoSchema}
		// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
		// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
		// in stmtCtx
		if p.flag&initTxnContextProvider != 0 {
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			if !p.sctx.GetSessionVars().InTxn() {
				txnManager := sessiontxn.GetTxnManager(p.sctx)
				newTxnRequest := &sessiontxn.EnterNewTxnRequest{
					Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
					Provider: staleread.NewStalenessTxnContextProvider(p.sctx, p.LastSnapshotTS, p.InfoSchema),
				}
				if err := txnManager.EnterNewTxn(context.TODO(), newTxnRequest); err != nil {
					return err
				}
				if err := txnManager.OnStmtStart(context.TODO(), txnManager.GetCurrentStmt()); err != nil {
					return err
				}
				p.sctx.GetSessionVars().TxnCtx.StaleReadTs = p.LastSnapshotTS
			}
		}
	}
	p.initedLastSnapshotTS = true
	return nil
}

// ensureInfoSchema get the infoschema from the preprocessor.
// there some situations:
//   - the stmt specifies the schema version.
//   - session variable
//   - transaction context
func (p *preprocessor) ensureInfoSchema() infoschema.InfoSchema {
	if p.InfoSchema != nil {
		return p.InfoSchema
	}

	p.InfoSchema = sessiontxn.GetTxnManager(p.sctx).GetTxnInfoSchema()
	return p.InfoSchema
}

func (p *preprocessor) hasAutoConvertWarning(colDef *ast.ColumnDef) bool {
	sessVars := p.sctx.GetSessionVars()
	if !sessVars.SQLMode.HasStrictMode() && colDef.Tp.GetType() == mysql.TypeVarchar {
		colDef.Tp.SetType(mysql.TypeBlob)
		if colDef.Tp.GetCharset() == charset.CharsetBin {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colDef.Name.Name.O, "VARBINARY", "BLOB"))
		} else {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colDef.Name.Name.O, "VARCHAR", "TEXT"))
		}
		return true
	}
	return false
}

func tryLockMDLAndUpdateSchemaIfNecessary(ctx context.Context, sctx base.PlanContext, dbName ast.CIStr, tbl table.Table, is infoschema.InfoSchema) (retTbl table.Table, err error) {
	skipLock := false
	var lockedID int64

	defer func() {
		// if the table is not in public state, avoid running any queries on it. This verification is actually
		// not related to the MDL, but it's a good place to put it.
		//
		// This function may return a new table, so we need to check the return value in the `defer` block.
		if err == nil {
			if retTbl.Meta().State != model.StatePublic {
				err = infoschema.ErrTableNotExists.FastGenByArgs(dbName.L, retTbl.Meta().Name.L)
				retTbl = nil
			}
		}

		if err == nil {
			sctx.GetSessionVars().StmtCtx.RelatedTableIDs[retTbl.Meta().ID] = struct{}{}
		}

		if lockedID != 0 && err != nil {
			// because `err != nil`, the `retTbl` is `nil` and the `tbl` can also be `nil` here. We use the `lockedID` instead.
			sctx.GetSessionVars().GetRelatedTableForMDL().Delete(lockedID)
		}
	}()

	if !sctx.GetSessionVars().TxnCtx.EnableMDL {
		return tbl, nil
	}
	if is.SchemaMetaVersion() == 0 {
		return tbl, nil
	}
	if sctx.GetSessionVars().SnapshotInfoschema != nil {
		return tbl, nil
	}
	if sctx.GetSessionVars().TxnCtx.IsStaleness {
		return tbl, nil
	}
	if tbl.Meta().TempTableType == model.TempTableLocal {
		// Don't attach, don't lock.
		return tbl, nil
	} else if tbl.Meta().TempTableType == model.TempTableGlobal {
		skipLock = true
	}
	if IsAutoCommitTxn(sctx.GetSessionVars()) && sctx.GetSessionVars().StmtCtx.IsReadOnly {
		return tbl, nil
	}
	tableInfo := tbl.Meta()
	if _, ok := sctx.GetSessionVars().GetRelatedTableForMDL().Load(tableInfo.ID); !ok {
		if se, ok := is.(*infoschema.SessionExtendedInfoSchema); ok && skipLock && se.MdlTables != nil {
			if _, ok := se.MdlTables.TableByID(tableInfo.ID); ok {
				// Already attach.
				return tbl, nil
			}
		}

		// We need to write 0 to the map to block the txn.
		// If we don't write 0, consider the following case:
		// the background mdl check loop gets the mdl lock from this txn. But the domain infoSchema may be changed before writing the ver to the map.
		// In this case, this TiDB wrongly gets the mdl lock.
		if !skipLock {
			sctx.GetSessionVars().GetRelatedTableForMDL().Store(tableInfo.ID, int64(0))
			lockedID = tableInfo.ID
		}
		latestIS := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
		domainSchemaVer := latestIS.SchemaMetaVersion()
		tbl, err = latestIS.TableByName(ctx, dbName, tableInfo.Name)
		if err != nil {
			return nil, err
		}
		if !skipLock {
			sctx.GetSessionVars().GetRelatedTableForMDL().Store(tbl.Meta().ID, domainSchemaVer)
			lockedID = tbl.Meta().ID
		}
		// Check the table change, if adding new public index or modify a column, we need to handle them.
		if tbl.Meta().Revision != tableInfo.Revision && !sctx.GetSessionVars().IsPessimisticReadConsistency() {
			var copyTableInfo *model.TableInfo

			infoIndices := make(map[string]int64, len(tableInfo.Indices))
			for _, idx := range tableInfo.Indices {
				infoIndices[idx.Name.L] = idx.ID
			}

			for i, idx := range tbl.Meta().Indices {
				if idx.State != model.StatePublic {
					continue
				}
				id, found := infoIndices[idx.Name.L]
				if !found || id != idx.ID {
					if copyTableInfo == nil {
						copyTableInfo = tbl.Meta().Clone()
					}
					copyTableInfo.Indices[i].State = model.StateWriteReorganization
					dbInfo, _ := latestIS.SchemaByName(dbName)
					allocs := autoid.NewAllocatorsFromTblInfo(latestIS.GetAutoIDRequirement(), dbInfo.ID, copyTableInfo)
					tbl, err = table.TableFromMeta(allocs, copyTableInfo)
					if err != nil {
						return nil, err
					}
				}
			}
			// Check the column change.
			infoColumns := make(map[string]int64, len(tableInfo.Columns))
			for _, col := range tableInfo.Columns {
				infoColumns[col.Name.L] = col.ID
			}
			for _, col := range tbl.Meta().Columns {
				if col.State != model.StatePublic {
					continue
				}
				colid, found := infoColumns[col.Name.L]
				if found && colid != col.ID {
					logutil.BgLogger().Info("public column changed",
						zap.String("column", col.Name.L), zap.String("old_col", col.Name.L),
						zap.Int64("new id", col.ID), zap.Int64("old id", col.ID))
					return nil, domain.ErrInfoSchemaChanged.GenWithStack("public column %s has changed", col.Name)
				}
			}
		}

		se, ok := is.(*infoschema.SessionExtendedInfoSchema)
		if !ok {
			logutil.BgLogger().Error("InfoSchema is not SessionExtendedInfoSchema", zap.Stack("stack"))
			return nil, errors.New("InfoSchema is not SessionExtendedInfoSchema")
		}
		db, _ := infoschema.SchemaByTable(latestIS, tbl.Meta())
		err = se.UpdateTableInfo(db, tbl)
		if err != nil {
			return nil, err
		}
		curTxn, err := sctx.Txn(false)
		if err != nil {
			return nil, err
		}
		if curTxn.Valid() {
			curTxn.SetOption(kv.TableToColumnMaps, nil)
		}
		return tbl, nil
	}
	return tbl, nil
}

// skipLockMDL returns true if the preprocessor should skip the lock of MDL.
func (p *preprocessor) skipLockMDL() bool {
	// skip lock mdl for IMPORT INTO statement,
	// because it's a batch process and will do both DML and DDL.
	// skip lock mdl for ANALYZE statement.
	return p.flag&inImportInto > 0 || p.flag&inAnalyze > 0
}

// aliasChecker is used to check the alias of the table in delete statement.
//
//	for example: delete tt1 from t1 tt1,(select max(id) id from t2)tt2 where tt1.id<=tt2.id
//	  `delete tt1` will be transformed to `delete current_database.t1` by default.
//	   because `tt1` cannot be used as alias in delete statement.
//	   so we have to set `tt1` as alias by aliasChecker.
type aliasChecker struct{}

func (*aliasChecker) Enter(in ast.Node) (ast.Node, bool) {
	if deleteStmt, ok := in.(*ast.DeleteStmt); ok {
		// 1. check the tableRefs of deleteStmt to find the alias
		var aliases []*ast.CIStr
		if deleteStmt.TableRefs != nil && deleteStmt.TableRefs.TableRefs != nil {
			tableRefs := deleteStmt.TableRefs.TableRefs
			if val := getTableRefsAlias(tableRefs.Left); val != nil {
				aliases = append(aliases, val)
			}
			if val := getTableRefsAlias(tableRefs.Right); val != nil {
				aliases = append(aliases, val)
			}
		}
		// 2. check the Tables to tag the alias
		if deleteStmt.Tables != nil && deleteStmt.Tables.Tables != nil {
			for _, table := range deleteStmt.Tables.Tables {
				if table.Schema.String() != "" {
					continue
				}
				for _, alias := range aliases {
					if table.Name.L == alias.L {
						table.IsAlias = true
						break
					}
				}
			}
		}
		return in, true
	}
	return in, false
}

func getTableRefsAlias(tableRefs ast.ResultSetNode) *ast.CIStr {
	switch v := tableRefs.(type) {
	case *ast.Join:
		if v.Left != nil {
			return getTableRefsAlias(v.Left)
		}
	case *ast.TableSource:
		return &v.AsName
	}
	return nil
}

func (*aliasChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
