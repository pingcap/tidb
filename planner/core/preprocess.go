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
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/legacy"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/domainutil"
	utilparser "github.com/pingcap/tidb/util/parser"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	p.flag |= inPrepare
}

// InTxnRetry is a PreprocessOpt that indicates preprocess is executing under transaction retry.
func InTxnRetry(p *preprocessor) {
	p.flag |= inTxnRetry
}

// InitTxnContextProvider is a PreprocessOpt that indicates preprocess should init transaction's context
func InitTxnContextProvider(p *preprocessor) {
	p.flag |= initTxnContextProvider
}

// WithPreprocessorReturn returns a PreprocessOpt to initialize the PreprocessorReturn.
func WithPreprocessorReturn(ret *PreprocessorReturn) PreprocessOpt {
	return func(p *preprocessor) {
		p.PreprocessorReturn = ret
	}
}

// WithExecuteInfoSchemaUpdate return a PreprocessOpt to update the `Execute` infoSchema under some conditions.
func WithExecuteInfoSchemaUpdate(pe *PreprocessExecuteISUpdate) PreprocessOpt {
	return func(p *preprocessor) {
		p.PreprocessExecuteISUpdate = pe
	}
}

// TryAddExtraLimit trys to add an extra limit for SELECT or UNION statement when sql_select_limit is set.
func TryAddExtraLimit(ctx sessionctx.Context, node ast.StmtNode) ast.StmtNode {
	if ctx.GetSessionVars().SelectLimit == math.MaxUint64 || ctx.GetSessionVars().InRestrictedSQL {
		return node
	}
	if explain, ok := node.(*ast.ExplainStmt); ok {
		explain.Stmt = TryAddExtraLimit(ctx, explain.Stmt)
		return explain
	} else if sel, ok := node.(*ast.SelectStmt); ok {
		if sel.Limit != nil || sel.SelectIntoOpt != nil {
			return node
		}
		newSel := *sel
		newSel.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSel
	} else if setOprStmt, ok := node.(*ast.SetOprStmt); ok {
		if setOprStmt.Limit != nil {
			return node
		}
		newSetOpr := *setOprStmt
		newSetOpr.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newSetOpr
	}
	return node
}

// Preprocess resolves table names of the node, and checks some statements' validation.
// preprocessReturn used to extract the infoschema for the tableName and the timestamp from the asof clause.
func Preprocess(ctx sessionctx.Context, node ast.Node, preprocessOpt ...PreprocessOpt) error {
	v := preprocessor{
		ctx:                ctx,
		tableAliasInJoin:   make([]map[string]interface{}, 0),
		preprocessWith:     &preprocessWith{cteCanUsed: make([]string, 0), cteBeforeOffset: make([]int, 0)},
		staleReadProcessor: staleread.NewStaleReadProcessor(ctx),
	}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	// PreprocessorReturn must be non-nil before preprocessing
	if v.PreprocessorReturn == nil {
		v.PreprocessorReturn = &PreprocessorReturn{}
	}
	node.Accept(&v)
	// InfoSchema must be non-nil after preprocessing
	v.ensureInfoSchema()

	v.initTxnContextProviderIfNecessary(node)

	return errors.Trace(v.err)
}

type preprocessorFlag uint8

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropTable is set when visiting create/drop table statement.
	inCreateOrDropTable
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
	// inRepairTable is set when visiting a repair table statement.
	inRepairTable
	// inSequenceFunction is set when visiting a sequence function.
	// This flag indicates the tableName in these function should be checked as sequence object.
	inSequenceFunction
	// initTxnContextProvider is set when we should init txn context in preprocess
	initTxnContextProvider
)

// Make linter happy.
var _ = PreprocessorReturn{}.initedLastSnapshotTS

// PreprocessorReturn is used to retain information obtained in the preprocessor.
type PreprocessorReturn struct {
	initedLastSnapshotTS bool
	IsStaleness          bool
	SnapshotTSEvaluator  func(sessionctx.Context) (uint64, error)
	// LastSnapshotTS is the last evaluated snapshotTS if any
	// otherwise it defaults to zero
	LastSnapshotTS   uint64
	InfoSchema       infoschema.InfoSchema
	ReadReplicaScope string
}

// PreprocessExecuteISUpdate is used to update information schema for special Execute statement in the preprocessor.
type PreprocessExecuteISUpdate struct {
	ExecuteInfoSchemaUpdate func(node ast.Node, sctx sessionctx.Context) infoschema.InfoSchema
	Node                    ast.Node
}

// preprocessWith is used to record info from WITH statements like CTE name.
type preprocessWith struct {
	cteCanUsed      []string
	cteBeforeOffset []int
}

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	ctx    sessionctx.Context
	flag   preprocessorFlag
	stmtTp byte
	showTp ast.ShowStmtType

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]interface{}
	preprocessWith   *preprocessWith

	staleReadProcessor staleread.Processor

	// values that may be returned
	*PreprocessorReturn
	*PreprocessExecuteISUpdate
	err error
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.AdminStmt:
		p.checkAdminCheckTableGrammar(node)
	case *ast.DeleteStmt:
		p.stmtTp = TypeDelete
	case *ast.SelectStmt:
		p.stmtTp = TypeSelect
	case *ast.UpdateStmt:
		p.stmtTp = TypeUpdate
	case *ast.InsertStmt:
		p.stmtTp = TypeInsert
		// handle the insert table name imminently
		// insert into t with t ..., the insert can not see t here. We should hand it before the CTE statement
		p.handleTableName(node.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName))
	case *ast.ExecuteStmt:
		p.stmtTp = TypeExecute
		p.resolveExecuteStmt(node)
	case *ast.CreateTableStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.resolveCreateTableStmt(node)
		p.checkCreateTableGrammar(node)
	case *ast.CreateViewStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.checkCreateViewGrammar(node)
		p.checkCreateViewWithSelectGrammar(node)
	case *ast.DropTableStmt:
		p.flag |= inCreateOrDropTable
		p.stmtTp = TypeDrop
		p.checkDropTableGrammar(node)
	case *ast.RenameTableStmt:
		p.stmtTp = TypeRename
		p.flag |= inCreateOrDropTable
		p.checkRenameTableGrammar(node)
	case *ast.CreateIndexStmt:
		p.stmtTp = TypeCreate
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		p.stmtTp = TypeAlter
		p.resolveAlterTableStmt(node)
		p.checkAlterTableGrammar(node)
	case *ast.CreateDatabaseStmt:
		p.stmtTp = TypeCreate
		p.checkCreateDatabaseGrammar(node)
	case *ast.AlterDatabaseStmt:
		p.stmtTp = TypeAlter
		p.checkAlterDatabaseGrammar(node)
	case *ast.DropDatabaseStmt:
		p.stmtTp = TypeDrop
		p.checkDropDatabaseGrammar(node)
	case *ast.ShowStmt:
		p.stmtTp = TypeShow
		p.showTp = node.Tp
		p.resolveShowStmt(node)
	case *ast.SetOprSelectList:
		p.checkSetOprSelectList(node)
	case *ast.DeleteTableList:
		p.stmtTp = TypeDelete
		return in, true
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	case *ast.CreateBindingStmt:
		p.stmtTp = TypeCreate
		EraseLastSemicolon(node.OriginNode)
		EraseLastSemicolon(node.HintedNode)
		p.checkBindGrammar(node.OriginNode, node.HintedNode, p.ctx.GetSessionVars().CurrentDB)
		return in, true
	case *ast.DropBindingStmt:
		p.stmtTp = TypeDrop
		EraseLastSemicolon(node.OriginNode)
		if node.HintedNode != nil {
			EraseLastSemicolon(node.HintedNode)
			p.checkBindGrammar(node.OriginNode, node.HintedNode, p.ctx.GetSessionVars().CurrentDB)
		}
		return in, true
	case *ast.RecoverTableStmt, *ast.FlashBackTableStmt:
		// The specified table in recover table statement maybe already been dropped.
		// So skip check table name here, otherwise, recover table [table_name] syntax will return
		// table not exists error. But recover table statement is use to recover the dropped table. So skip children here.
		return in, true
	case *ast.RepairTableStmt:
		p.stmtTp = TypeRepair
		// The RepairTable should consist of the logic for creating tables and renaming tables.
		p.flag |= inRepairTable
		p.checkRepairTableGrammar(node)
	case *ast.CreateSequenceStmt:
		p.stmtTp = TypeCreate
		p.flag |= inCreateOrDropTable
		p.resolveCreateSequenceStmt(node)
	case *ast.DropSequenceStmt:
		p.stmtTp = TypeDrop
		p.flag |= inCreateOrDropTable
		p.checkDropSequenceGrammar(node)
	case *ast.FuncCastExpr:
		p.checkFuncCastExpr(node)
	case *ast.FuncCallExpr:
		if node.FnName.L == ast.NextVal || node.FnName.L == ast.LastVal || node.FnName.L == ast.SetVal {
			p.flag |= inSequenceFunction
		}

	case *ast.BRIEStmt:
		if node.Kind == ast.BRIEKindRestore {
			p.flag |= inCreateOrDropTable
		}
	case *ast.TableSource:
		isModeOracle := p.ctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
		if _, ok := node.Source.(*ast.SelectStmt); ok && !isModeOracle && len(node.AsName.L) == 0 {
			p.err = dbterror.ErrDerivedMustHaveAlias.GenWithStackByArgs()
		}
		if v, ok := node.Source.(*ast.TableName); ok && v.TableSample != nil {
			switch v.TableSample.SampleMethod {
			case ast.SampleMethodTypeTiDBRegion:
			default:
				p.err = expression.ErrInvalidTableSample.GenWithStackByArgs("Only supports REGIONS sampling method")
			}
		}
	case *ast.GroupByClause:
		p.checkGroupBy(node)
	case *ast.CommonTableExpression, *ast.SubqueryExpr:
		with := p.preprocessWith
		beforeOffset := len(with.cteCanUsed)
		with.cteBeforeOffset = append(with.cteBeforeOffset, beforeOffset)
		if cteNode, exist := node.(*ast.CommonTableExpression); exist && cteNode.IsRecursive {
			with.cteCanUsed = append(with.cteCanUsed, cteNode.Name.L)
		}
	case *ast.BeginStmt:
		// If the begin statement was like following:
		// start transaction read only as of timestamp ....
		// then we need set StmtCtx.IsStaleness as true in order to avoid take tso in PrepareTSFuture.
		if node.AsOf != nil {
			p.ctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		} else if p.ctx.GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
			// If the begin statement was like following:
			// set transaction read only as of timestamp ...
			// begin
			// then we need set StmtCtx.IsStaleness as true in order to avoid take tso in PrepareTSFuture.
			p.ctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		}
	default:
		p.flag &= ^parentIsJoin
	}
	return in, p.err != nil
}

// EraseLastSemicolon removes last semicolon of sql.
func EraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}

// EraseLastSemicolonInSQL removes last semicolon of the SQL.
func EraseLastSemicolonInSQL(sql string) string {
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		return sql[:len(sql)-1]
	}
	return sql
}

const (
	// TypeInvalid for unexpected types.
	TypeInvalid byte = iota
	// TypeSelect for SelectStmt.
	TypeSelect
	// TypeSetOpr for SetOprStmt.
	TypeSetOpr
	// TypeDelete for DeleteStmt.
	TypeDelete
	// TypeUpdate for UpdateStmt.
	TypeUpdate
	// TypeInsert for InsertStmt.
	TypeInsert
	// TypeDrop for DropStmt
	TypeDrop
	// TypeCreate for CreateStmt
	TypeCreate
	// TypeAlter for AlterStmt
	TypeAlter
	// TypeRename for RenameStmt
	TypeRename
	// TypeRepair for RepairStmt
	TypeRepair
	// TypeShow for ShowStmt
	TypeShow
	// TypeExecute for ExecuteStmt
	TypeExecute
)

func bindableStmtType(node ast.StmtNode) byte {
	switch node.(type) {
	case *ast.SelectStmt:
		return TypeSelect
	case *ast.SetOprStmt:
		return TypeSetOpr
	case *ast.DeleteStmt:
		return TypeDelete
	case *ast.UpdateStmt:
		return TypeUpdate
	case *ast.InsertStmt:
		return TypeInsert
	}
	return TypeInvalid
}

func (p *preprocessor) tableByName(tn *ast.TableName) (table.Table, error) {
	currentDB := p.ctx.GetSessionVars().CurrentDB
	if tn.Schema.String() != "" {
		currentDB = tn.Schema.L
	}
	if currentDB == "" {
		return nil, errors.Trace(ErrNoDB)
	}
	sName := model.NewCIStr(currentDB)
	is := p.ensureInfoSchema()

	// for 'SHOW CREATE VIEW/SEQUENCE ...' statement, ignore local temporary tables.
	if p.stmtTp == TypeShow && (p.showTp == ast.ShowCreateView || p.showTp == ast.ShowCreateSequence) {
		is = temptable.DetachLocalTemporaryTableInfoSchema(is)
	}

	tbl, err := is.TableByName(sName, tn.Name)
	if err != nil {
		// We should never leak that the table doesn't exist (i.e. attach ErrTableNotExists)
		// unless we know that the user has permissions to it, should it exist.
		// By checking here, this makes all SELECT/SHOW/INSERT/UPDATE/DELETE statements safe.
		currentUser, activeRoles := p.ctx.GetSessionVars().User, p.ctx.GetSessionVars().ActiveRoles
		if pm := privilege.GetPrivilegeManager(p.ctx); pm != nil {
			if !pm.RequestVerification(activeRoles, sName.L, tn.Name.O, "", mysql.AllPrivMask) {
				u := currentUser.Username
				h := currentUser.Hostname
				if currentUser.AuthHostname != "" {
					u = currentUser.AuthUsername
					h = currentUser.AuthHostname
				}
				return nil, ErrTableaccessDenied.GenWithStackByArgs(p.stmtType(), u, h, tn.Name.O)
			}
		}
		return nil, err
	}
	return tbl, err
}

func (p *preprocessor) checkBindGrammar(originNode, hintedNode ast.StmtNode, defaultDB string) {
	origTp := bindableStmtType(originNode)
	hintedTp := bindableStmtType(hintedNode)
	if origTp == TypeInvalid || hintedTp == TypeInvalid {
		p.err = errors.Errorf("create binding doesn't support this type of query")
		return
	}
	if origTp != hintedTp {
		p.err = errors.Errorf("hinted sql and original sql have different query types")
		return
	}
	if origTp == TypeInsert {
		origInsert, hintedInsert := originNode.(*ast.InsertStmt), hintedNode.(*ast.InsertStmt)
		if origInsert.Select == nil || hintedInsert.Select == nil {
			p.err = errors.Errorf("create binding only supports INSERT / REPLACE INTO SELECT")
			return
		}
	}

	// Check the bind operation is not on any temporary table.
	tblNames := extractTableList(originNode, nil, false)
	for _, tn := range tblNames {
		tbl, err := p.tableByName(tn)
		if err != nil {
			// If the operation is order is: drop table -> drop binding
			// The table doesn't  exist, it is not an error.
			if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				continue
			}
			p.err = err
			return
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			p.err = dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("create binding")
			return
		}
		tableInfo := tbl.Meta()
		dbInfo, _ := p.ensureInfoSchema().SchemaByTable(tableInfo)
		tn.TableInfo = tableInfo
		tn.DBInfo = dbInfo
	}

	originSQL := parser.Normalize(utilparser.RestoreWithDefaultDB(originNode, defaultDB, originNode.Text()))
	hintedSQL := parser.Normalize(utilparser.RestoreWithDefaultDB(hintedNode, defaultDB, hintedNode.Text()))
	if originSQL != hintedSQL {
		p.err = errors.Errorf("hinted sql and origin sql don't match when hinted sql erase the hint info, after erase hint info, originSQL:%s, hintedSQL:%s", originSQL, hintedSQL)
	}
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.CreateTableStmt:
		p.flag &= ^inCreateOrDropTable
		p.checkAutoIncrement(x)
		p.checkContainDotColumn(x)
	case *ast.CreateViewStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.DropTableStmt, *ast.AlterTableStmt, *ast.RenameTableStmt:
		p.flag &= ^inCreateOrDropTable
	case *driver.ParamMarkerExpr:
		if p.flag&inPrepare == 0 {
			p.err = parser.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			break
		}
		valid := false
		for i, length := 0, len(types.ExplainFormats); i < length; i++ {
			if strings.ToLower(x.Format) == types.ExplainFormats[i] {
				valid = true
				break
			}
		}
		if !valid {
			p.err = ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
		}
	case *ast.TableName:
		p.handleTableName(x)
	case *ast.Join:
		if len(p.tableAliasInJoin) > 0 {
			p.tableAliasInJoin = p.tableAliasInJoin[:len(p.tableAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			if len(x.Args) != 2 {
				p.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
				isValueExpr2 := false
				switch x.Args[1].(type) {
				case *driver.ValueExpr, *ast.UnaryOperationExpr:
					isValueExpr2 = true
				}

				if !isValueExpr1 || !isValueExpr2 {
					p.err = ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
				}
			}
			break
		}

		// no need sleep when retry transaction and avoid unexpect sleep caused by retry.
		if p.flag&inTxnRetry > 0 && x.FnName.L == ast.Sleep {
			if len(x.Args) == 1 {
				x.Args[0] = ast.NewValueExpr(0, "", "")
			}
		}

		if x.FnName.L == ast.NextVal || x.FnName.L == ast.LastVal || x.FnName.L == ast.SetVal {
			p.flag &= ^inSequenceFunction
		}
	case *ast.RepairTableStmt:
		p.flag &= ^inRepairTable
	case *ast.CreateSequenceStmt:
		p.flag &= ^inCreateOrDropTable
	case *ast.BRIEStmt:
		if x.Kind == ast.BRIEKindRestore {
			p.flag &= ^inCreateOrDropTable
		}
	case *ast.CommonTableExpression, *ast.SubqueryExpr:
		with := p.preprocessWith
		lenWithCteBeforeOffset := len(with.cteBeforeOffset)
		if lenWithCteBeforeOffset < 1 {
			p.err = ErrInternal.GenWithStack("len(cteBeforeOffset) is less than one.Maybe it was deleted in somewhere.Should match in Enter and Leave")
			break
		}
		beforeOffset := with.cteBeforeOffset[lenWithCteBeforeOffset-1]
		with.cteBeforeOffset = with.cteBeforeOffset[:lenWithCteBeforeOffset-1]
		with.cteCanUsed = with.cteCanUsed[:beforeOffset]
		if cteNode, exist := x.(*ast.CommonTableExpression); exist {
			with.cteCanUsed = append(with.cteCanUsed, cteNode.Name.L)
		}
	}

	return in, p.err == nil
}

func checkAutoIncrementOp(colDef *ast.ColumnDef, index int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[index].Tp == ast.ColumnOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == index+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionDefaultValue {
				if tmp, ok := op.Expr.(*driver.ValueExpr); ok {
					if !tmp.Datum.IsNull() {
						return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
					}
				}
			}
		}
	}
	if colDef.Options[index].Tp == ast.ColumnOptionDefaultValue && len(colDef.Options) != index+1 {
		if tmp, ok := colDef.Options[index].Expr.(*driver.ValueExpr); ok {
			if tmp.Datum.IsNull() {
				return hasAutoIncrement, nil
			}
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.ColumnOptionAutoIncrement {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	return hasAutoIncrement, nil
}

func isConstraintKeyTp(constraints []*ast.Constraint, colDef *ast.ColumnDef) bool {
	for _, c := range constraints {
		if c.Keys[0].Expr != nil {
			continue
		}
		// If the constraint as follows: primary key(c1, c2)
		// we only support c1 column can be auto_increment.
		if colDef.Name.Name.L != c.Keys[0].Column.Name.L {
			continue
		}
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			return true
		}
	}

	return false
}

func (p *preprocessor) checkAutoIncrement(stmt *ast.CreateTableStmt) {
	autoIncrementCols := make(map[*ast.ColumnDef]bool)

	for _, colDef := range stmt.Cols {
		var hasAutoIncrement bool
		var isKey bool
		for i, op := range colDef.Options {
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				p.err = err
				return
			}
			if ok {
				hasAutoIncrement = true
			}
			switch op.Tp {
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isKey = true
			}
		}
		if hasAutoIncrement {
			autoIncrementCols[colDef] = isKey
		}
	}

	if len(autoIncrementCols) < 1 {
		return
	}
	if len(autoIncrementCols) > 1 {
		p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		return
	}
	// Only have one auto_increment col.
	for col, isKey := range autoIncrementCols {
		if !isKey {
			isKey = isConstraintKeyTp(stmt.Constraints, col)
		}
		autoIncrementMustBeKey := true
		for _, opt := range stmt.Options {
			if opt.Tp == ast.TableOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
				autoIncrementMustBeKey = false
			}
		}
		if autoIncrementMustBeKey && !isKey {
			p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		}
		switch col.Tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24:
		default:
			p.err = errors.Errorf("Incorrect column specifier for column '%s'", col.Name.Name.O)
		}
	}

}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.mysql.com/doc/refman/5.7/en/union.html
//        https://mariadb.com/kb/en/intersect/
//        https://mariadb.com/kb/en/except/
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		switch s := sel.(type) {
		case *ast.SelectStmt:
			if s.SelectIntoOpt != nil {
				p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "INTO")
				return
			}
			if s.IsInBraces {
				continue
			}
			if s.Limit != nil {
				p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
				return
			}
			if s.OrderBy != nil {
				p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
				return
			}
		case *ast.SetOprSelectList:
			p.checkSetOprSelectList(s)
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if isIncorrectName(stmt.Name) && !stmt.AlterDefaultDatabase {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDropDatabaseGrammar(stmt *ast.DropDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = dbterror.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAdminCheckTableGrammar(stmt *ast.AdminStmt) {
	for _, table := range stmt.Tables {
		tableInfo, err := p.tableByName(table)
		if err != nil {
			p.err = err
			return
		}
		tempTableType := tableInfo.Meta().TempTableType
		if (stmt.Tp == ast.AdminCheckTable || stmt.Tp == ast.AdminChecksumTable || stmt.Tp == ast.AdminCheckIndex) && tempTableType != model.TempTableNone {
			if stmt.Tp == ast.AdminChecksumTable {
				p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("admin checksum table")
			} else if stmt.Tp == ast.AdminCheckTable {
				p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("admin check table")
			} else {
				p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("admin check index")
			}
			return
		}
	}
}

func (p *preprocessor) checkCreateTableGrammar(stmt *ast.CreateTableStmt) {
	if stmt.ReferTable != nil {
		schema := model.NewCIStr(p.ctx.GetSessionVars().CurrentDB)
		if stmt.ReferTable.Schema.String() != "" {
			schema = stmt.ReferTable.Schema
		}
		// get the infoschema from the context.
		tableInfo, err := p.ensureInfoSchema().TableByName(schema, stmt.ReferTable.Name)
		if err != nil {
			p.err = err
			return
		}
		tableMetaInfo := tableInfo.Meta()
		if tableMetaInfo.TempTableType != model.TempTableNone {
			p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("create table like")
			return
		}
		if stmt.TemporaryKeyword != ast.TemporaryNone {
			err := checkReferInfoForTemporaryTable(tableMetaInfo)
			if err != nil {
				p.err = err
				return
			}

		}
	}
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		for _, opt := range stmt.Options {
			switch opt.Tp {
			case ast.TableOptionShardRowID:
				p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
				return
			case ast.TableOptionPlacementPolicy:
				p.err = ErrOptOnTemporaryTable.GenWithStackByArgs("PLACEMENT")
				return
			}
		}
	}
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.Cols {
		if err := checkColumn(colDef); err != nil {
			// Try to convert to BLOB or TEXT, see issue #30328
			if !terror.ErrorEqual(err, types.ErrTooBigFieldLength) || !p.hasAutoConvertWarning(colDef) {
				p.err = err
				return
			}
		}
		isPrimary, err := checkColumnOptions(stmt.TemporaryKeyword != ast.TemporaryNone, colDef.Options)
		if err != nil {
			p.err = err
			return
		}
		countPrimaryKey += isPrimary
		if countPrimaryKey > 1 {
			p.err = infoschema.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
			if constraint.IsEmptyIndex {
				p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(constraint.Name)
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				p.err = infoschema.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checkIndexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if p.err = checkUnsupportedTableOptions(stmt.Options); p.err != nil {
		return
	}
	if stmt.Select != nil {
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.Cols) == 0 && stmt.ReferTable == nil {
		p.err = dbterror.ErrTableMustHaveColumns
		return
	}

	if stmt.Partition != nil {
		for _, def := range stmt.Partition.Definitions {
			pName := def.Name.String()
			if isIncorrectName(pName) {
				p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs(pName)
				return
			}
		}
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	vName := stmt.ViewName.Name.String()
	if isIncorrectName(vName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(vName)
		return
	}
	for _, col := range stmt.Cols {
		if isIncorrectName(col.String()) {
			p.err = dbterror.ErrWrongColumnName.GenWithStackByArgs(col)
			return
		}
	}
	if len(stmt.Definer.Username) > auth.UserNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Username, "user name", auth.UserNameMaxLength)
		return
	}
	if len(stmt.Definer.Hostname) > auth.HostNameMaxLength {
		p.err = dbterror.ErrWrongStringLength.GenWithStackByArgs(stmt.Definer.Hostname, "host name", auth.HostNameMaxLength)
		return
	}
}

func (p *preprocessor) checkCreateViewWithSelect(stmt ast.Node) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.SelectIntoOpt != nil {
			p.err = dbterror.ErrViewSelectClause.GenWithStackByArgs("INFO")
			return
		}
		if s.LockInfo != nil && s.LockInfo.LockType != ast.SelectLockNone {
			s.LockInfo.LockType = ast.SelectLockNone
			return
		}
	case *ast.SetOprSelectList:
		for _, sel := range s.Selects {
			p.checkCreateViewWithSelect(sel)
		}
	}
}

func (p *preprocessor) checkCreateViewWithSelectGrammar(stmt *ast.CreateViewStmt) {
	switch stmt := stmt.Select.(type) {
	case *ast.SelectStmt:
		p.checkCreateViewWithSelect(stmt)
	case *ast.SetOprStmt:
		for _, selectStmt := range stmt.SelectList.Selects {
			p.checkCreateViewWithSelect(selectStmt)
			if p.err != nil {
				return
			}
		}
	}
}

func (p *preprocessor) checkDropSequenceGrammar(stmt *ast.DropSequenceStmt) {
	p.checkDropTableNames(stmt.Sequences)
}

func (p *preprocessor) checkDropTableGrammar(stmt *ast.DropTableStmt) {
	p.checkDropTableNames(stmt.Tables)
	if stmt.TemporaryKeyword != ast.TemporaryNone {
		p.checkDropTemporaryTableGrammar(stmt)
	}
}

func (p *preprocessor) checkDropTemporaryTableGrammar(stmt *ast.DropTableStmt) {
	currentDB := model.NewCIStr(p.ctx.GetSessionVars().CurrentDB)
	for _, t := range stmt.Tables {
		if isIncorrectName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}

		schema := t.Schema
		if schema.L == "" {
			schema = currentDB
		}

		tbl, err := p.ensureInfoSchema().TableByName(schema, t.Name)
		if infoschema.ErrTableNotExists.Equal(err) {
			// Non-exist table will be checked in ddl executor
			continue
		}

		if err != nil {
			p.err = err
			return
		}

		tblInfo := tbl.Meta()
		if stmt.TemporaryKeyword == ast.TemporaryGlobal && tblInfo.TempTableType != model.TempTableGlobal {
			p.err = ErrDropTableOnTemporaryTable
			return
		}
	}
}

func (p *preprocessor) checkDropTableNames(tables []*ast.TableName) {
	for _, t := range tables {
		if isIncorrectName(t.Name.String()) {
			p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqTableAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.tableAliasInJoin = append(p.tableAliasInJoin, make(map[string]interface{}))
	}
	tableAliases := p.tableAliasInJoin[len(p.tableAliasInJoin)-1]
	isOracleMode := p.ctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
	if !isOracleMode {
		if err := isTableAliasDuplicate(stmt.Left, tableAliases); err != nil {
			p.err = err
			return
		}
		if err := isTableAliasDuplicate(stmt.Right, tableAliases); err != nil {
			p.err = err
			return
		}
	}
	p.flag |= parentIsJoin
}

func isTableAliasDuplicate(node ast.ResultSetNode, tableAliases map[string]interface{}) error {
	if ts, ok := node.(*ast.TableSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if tableNode, ok := ts.Source.(*ast.TableName); ok {
				if tableNode.Schema.L != "" {
					tabName = model.NewCIStr(fmt.Sprintf("%s.%s", tableNode.Schema.L, tableNode.Name.L))
				} else {
					tabName = tableNode.Name
				}
			}
		}
		_, exists := tableAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return ErrNonUniqTable.GenWithStackByArgs(tabName)
		}
		tableAliases[tabName.L] = nil
	}
	return nil
}

func checkColumnOptions(isTempTable bool, ops []*ast.ColumnOption) (int, error) {
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		switch op.Tp {
		case ast.ColumnOptionPrimaryKey:
			isPrimary = 1
		case ast.ColumnOptionGenerated:
			isGenerated = 1
			isStored = op.Stored
		case ast.ColumnOptionAutoRandom:
			if isTempTable {
				return isPrimary, ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
			}
		}
	}

	if isPrimary > 0 && isGenerated > 0 && !isStored {
		return isPrimary, ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	return isPrimary, nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	if stmt.IndexName == "" {
		p.err = dbterror.ErrWrongNameForIndex.GenWithStackByArgs(stmt.IndexName)
		return
	}
	p.err = checkIndexInfo(stmt.IndexName, stmt.IndexPartSpecifications)
}

func (p *preprocessor) checkGroupBy(stmt *ast.GroupByClause) {
	noopFuncsMode := p.ctx.GetSessionVars().NoopFuncsMode
	for _, item := range stmt.Items {
		if !item.NullOrder && noopFuncsMode != variable.OnInt {
			err := expression.ErrFunctionsNoopImpl.GenWithStackByArgs("GROUP BY expr ASC|DESC")
			if noopFuncsMode == variable.OffInt {
				p.err = err
				return
			}
			// NoopFuncsMode is Warn, append an error
			p.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
	}
}

func (p *preprocessor) checkRenameTableGrammar(stmt *ast.RenameTableStmt) {
	oldTable := stmt.TableToTables[0].OldTable.Name.String()
	newTable := stmt.TableToTables[0].NewTable.Name.String()

	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkRenameTable(oldTable, newTable string) {
	if isIncorrectName(oldTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(oldTable)
		return
	}

	if isIncorrectName(newTable) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(newTable)
		return
	}
}

func (p *preprocessor) checkRepairTableGrammar(stmt *ast.RepairTableStmt) {
	// Check create table stmt whether it's is in REPAIR MODE.
	if !domainutil.RepairInfo.InRepairMode() {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("TiDB is not in REPAIR MODE")
		return
	}
	if len(domainutil.RepairInfo.GetRepairTableList()) == 0 {
		p.err = dbterror.ErrRepairTableFail.GenWithStackByArgs("repair list is empty")
		return
	}

	// Check rename action as the rename statement does.
	oldTable := stmt.Table.Name.String()
	newTable := stmt.CreateStmt.Table.Name.String()
	p.checkRenameTable(oldTable, newTable)
}

func (p *preprocessor) checkAlterTableGrammar(stmt *ast.AlterTableStmt) {
	tName := stmt.Table.Name.String()
	if isIncorrectName(tName) {
		p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewTable != nil {
			ntName := spec.NewTable.Name.String()
			if isIncorrectName(ntName) {
				p.err = dbterror.ErrWrongTableName.GenWithStackByArgs(ntName)
				return
			}
		}
		for _, colDef := range spec.NewColumns {
			if p.err = checkColumn(colDef); p.err != nil {
				return
			}
		}
		if p.err = checkUnsupportedTableOptions(spec.Options); p.err != nil {
			return
		}
		switch spec.Tp {
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey, ast.ConstraintPrimaryKey:
				p.err = checkIndexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if p.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableAddStatistics, ast.AlterTableDropStatistics:
			statsName := spec.Statistics.StatsName
			if isIncorrectName(statsName) {
				msg := fmt.Sprintf("Incorrect statistics name: %s", statsName)
				p.err = ErrInternal.GenWithStack(msg)
				return
			}
		case ast.AlterTableAddPartitions:
			for _, def := range spec.PartDefinitions {
				pName := def.Name.String()
				if isIncorrectName(pName) {
					p.err = dbterror.ErrWrongPartitionName.GenWithStackByArgs(pName)
					return
				}
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateColumnName checks if index exists duplicated columns.
func checkDuplicateColumnName(IndexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(IndexPartSpecifications))
	for _, IndexColNameWithExpr := range IndexPartSpecifications {
		if IndexColNameWithExpr.Column != nil {
			name := IndexColNameWithExpr.Column.Name
			if _, ok := colNames[name.L]; ok {
				return infoschema.ErrColumnExists.GenWithStackByArgs(name)
			}
			colNames[name.L] = struct{}{}
		}
	}
	return nil
}

// checkIndexInfo checks index name, index column names and prefix lengths.
func checkIndexInfo(indexName string, IndexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, mysql.PrimaryKeyName) {
		return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(IndexPartSpecifications) > mysql.MaxKeyParts {
		return infoschema.ErrTooManyKeyParts.GenWithStackByArgs(mysql.MaxKeyParts)
	}
	for _, idxSpec := range IndexPartSpecifications {
		// -1 => unspecified/full, > 0 OK, 0 => error
		if idxSpec.Expr == nil && idxSpec.Length == 0 {
			return ErrKeyPart0.GenWithStackByArgs(idxSpec.Column.Name.O)
		}
	}
	return checkDuplicateColumnName(IndexPartSpecifications)
}

// checkUnsupportedTableOptions checks if there exists unsupported table options
func checkUnsupportedTableOptions(options []*ast.TableOption) error {
	var err error = nil
	for _, option := range options {
		switch option.Tp {
		case ast.TableOptionUnion:
			err = dbterror.ErrTableOptionUnionUnsupported
		case ast.TableOptionInsertMethod:
			err = dbterror.ErrTableOptionInsertMethodUnsupported
		case ast.TableOptionEngine:
			err = checkTableEngine(option.StrValue)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

var mysqlValidTableEngineNames = map[string]struct{}{
	"archive":    {},
	"blackhole":  {},
	"csv":        {},
	"example":    {},
	"federated":  {},
	"innodb":     {},
	"memory":     {},
	"merge":      {},
	"mgr_myisam": {},
	"myisam":     {},
	"ndb":        {},
	"heap":       {},
}

func checkTableEngine(engineName string) error {
	if _, have := mysqlValidTableEngineNames[strings.ToLower(engineName)]; !have {
		return dbterror.ErrUnknownEngine.GenWithStackByArgs(engineName)
	}
	return nil
}

func checkReferInfoForTemporaryTable(tableMetaInfo *model.TableInfo) error {
	if tableMetaInfo.AutoRandomBits != 0 {
		return ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random")
	}
	if tableMetaInfo.PreSplitRegions != 0 {
		return ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions")
	}
	if tableMetaInfo.Partition != nil {
		return ErrPartitionNoTemporary
	}
	if tableMetaInfo.ShardRowIDBits != 0 {
		return ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if tableMetaInfo.PlacementPolicyRef != nil {
		return ErrOptOnTemporaryTable.GenWithStackByArgs("placement")
	}

	return nil
}

// checkColumn checks if the column definition is valid.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func checkColumn(colDef *ast.ColumnDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if isIncorrectName(cName) {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.GetFlen() > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.GetType() {
	case mysql.TypeString:
		if tp.GetFlen() != types.UnspecifiedLength && tp.GetFlen() > mysql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, mysql.MaxFieldCharLength)
		}
	case mysql.TypeVarchar:
		if len(tp.GetCharset()) == 0 {
			// It's not easy to get the schema charset and table charset here.
			// The charset is determined by the order ColumnDefaultCharset --> TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the ddl.CreateTable.
			return nil
		}
		err := ddl.IsTooBigFieldLength(colDef.Tp.GetFlen(), colDef.Name.Name.O, tp.GetCharset())
		if err != nil {
			return err
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		// For FLOAT, the SQL standard permits an optional specification of the precision.
		// https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
		if tp.GetDecimal() == -1 {
			switch tp.GetType() {
			case mysql.TypeDouble:
				// For Double type flen and decimal check is moved to parser component
			default:
				if tp.GetFlen() > mysql.MaxDoublePrecisionLength {
					return types.ErrWrongFieldSpec.GenWithStackByArgs(colDef.Name.Name.O)
				}
			}
		} else {
			if tp.GetDecimal() > mysql.MaxFloatingTypeScale {
				return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), colDef.Name.Name.O, mysql.MaxFloatingTypeScale)
			}
			if tp.GetFlen() > mysql.MaxFloatingTypeWidth || tp.GetFlen() == 0 {
				return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxFloatingTypeWidth)
			}
			if tp.GetFlen() < tp.GetDecimal() {
				return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
			}
		}
	case mysql.TypeSet:
		if len(tp.GetElems()) > mysql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.GetElems() {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.GetType()), str)
			}
		}
	case mysql.TypeNewDecimal:
		if tp.GetDecimal() > mysql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.GetDecimal(), colDef.Name.Name.O, mysql.MaxDecimalScale)
		}

		if tp.GetFlen() > mysql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.GetFlen(), colDef.Name.Name.O, mysql.MaxDecimalWidth)
		}

		if tp.GetFlen() < tp.GetDecimal() {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colDef.Name.Name.O)
		}
		// If decimal and flen all equals 0, just set flen to default value.
		if tp.GetDecimal() == 0 && tp.GetFlen() == 0 {
			defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNewDecimal)
			tp.SetFlen(defaultFlen)
		}
	case mysql.TypeBit:
		if tp.GetFlen() <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.GetFlen() > mysql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, mysql.MaxBitDisplayWidth)
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether default value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in parser.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			return true
		}
	}
	return false
}

func isInvalidDefaultValue(colDef *ast.ColumnDef) bool {
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.ColumnOptionDefaultValue {
			if !(tp.GetType() == mysql.TypeTimestamp || tp.GetType() == mysql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

// isIncorrectName checks if the identifier is incorrect.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isIncorrectName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

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
	default:
		return "SELECT" // matches Select and uncaught cases.
	}
}

func (p *preprocessor) handleTableName(tn *ast.TableName) {
	if tn.Schema.L == "" {

		for _, cte := range p.preprocessWith.cteCanUsed {
			if cte == tn.Name.L {
				return
			}
		}

		currentDB := p.ctx.GetSessionVars().CurrentDB
		if currentDB == "" {
			p.err = errors.Trace(ErrNoDB)
			return
		}

		tn.Schema = model.NewCIStr(currentDB)
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

	tableInfo := table.Meta()
	dbInfo, _ := p.ensureInfoSchema().SchemaByTable(tableInfo)
	// tableName should be checked as sequence object.
	if p.flag&inSequenceFunction > 0 {
		if !tableInfo.IsSequence() {
			p.err = infoschema.ErrWrongObject.GenWithStackByArgs(dbInfo.Name.O, tableInfo.Name.O, "SEQUENCE")
			return
		}
	}
	tn.TableInfo = tableInfo
	tn.DBInfo = dbInfo
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
	if util.IsMemOrSysDB(tn.Schema.L) {
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
	p.ctx.SetValue(domainutil.RepairedTable, tableInfo)
	p.ctx.SetValue(domainutil.RepairedDatabase, dbInfo)
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	if node.DBName == "" {
		if node.Table != nil && node.Table.Schema.L != "" {
			node.DBName = node.Table.Schema.O
		} else {
			node.DBName = p.ctx.GetSessionVars().CurrentDB
		}
	} else if node.Table != nil && node.Table.Schema.L == "" {
		node.Table.Schema = model.NewCIStr(node.DBName)
	}
	if node.User != nil && node.User.CurrentUser {
		// Fill the Username and Hostname with the current user.
		currentUser := p.ctx.GetSessionVars().User
		if currentUser != nil {
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveExecuteStmt(node *ast.ExecuteStmt) {
	prepared, err := GetPreparedStmt(node, p.ctx.GetSessionVars())
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

func (p *preprocessor) resolveCreateTableStmt(node *ast.CreateTableStmt) {
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
				table.Schema = model.NewCIStr(node.Table.Schema.L)
			}
		}
	}
}

func (p *preprocessor) resolveCreateSequenceStmt(stmt *ast.CreateSequenceStmt) {
	sName := stmt.Name.Name.String()
	if isIncorrectName(sName) {
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
}

func (p *preprocessor) updateStateFromStaleReadProcessor() error {
	if p.initedLastSnapshotTS {
		return nil
	}

	if p.IsStaleness = p.staleReadProcessor.IsStaleness(); p.IsStaleness {
		p.LastSnapshotTS = p.staleReadProcessor.GetStalenessReadTS()
		p.SnapshotTSEvaluator = p.staleReadProcessor.GetStalenessTSEvaluatorForPrepare()
		p.InfoSchema = p.staleReadProcessor.GetStalenessInfoSchema()
		// If the select statement was like 'select * from t as of timestamp ...' or in a stale read transaction
		// or is affected by the tidb_read_staleness session variable, then the statement will be makred as isStaleness
		// in stmtCtx
		if p.flag&initTxnContextProvider != 0 {
			p.ctx.GetSessionVars().StmtCtx.IsStaleness = true
			if !p.ctx.GetSessionVars().InTxn() {
				txnManager := sessiontxn.GetTxnManager(p.ctx)
				newTxnRequest := &sessiontxn.EnterNewTxnRequest{
					Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
					Provider: staleread.NewStalenessTxnContextProvider(p.ctx, p.LastSnapshotTS, p.InfoSchema),
				}
				if err := txnManager.EnterNewTxn(context.TODO(), newTxnRequest); err != nil {
					return err
				}
				if err := txnManager.OnStmtStart(context.TODO()); err != nil {
					return err
				}
			}
		}
	}

	// It is a little hacking for the below codes. `ReadReplicaScope` is used both by stale read's closest read and local txn.
	// They are different features and the value for `ReadReplicaScope` will be conflicted in some scenes.
	// But because local txn is still an experimental feature, we should make stale read work first.
	if p.IsStaleness || p.ctx.GetSessionVars().GetReplicaRead().IsClosestRead() {
		// When stale read or closet read is set, we read the tidb's locality as the read replica scope
		p.ReadReplicaScope = config.GetTxnScopeFromConfig()
	} else {
		// Otherwise, use the scope from TxnCtx for local txn validation
		p.ReadReplicaScope = p.ctx.GetSessionVars().TxnCtx.TxnScope
	}

	p.initedLastSnapshotTS = true
	return nil
}

// ensureInfoSchema get the infoschema from the preprocessor.
// there some situations:
//    - the stmt specifies the schema version.
//    - session variable
//    - transaction context
func (p *preprocessor) ensureInfoSchema() infoschema.InfoSchema {
	if p.InfoSchema != nil {
		return p.InfoSchema
	}
	// `Execute` under some conditions need to see the latest information schema.
	if p.PreprocessExecuteISUpdate != nil {
		if newInfoSchema := p.ExecuteInfoSchemaUpdate(p.Node, p.ctx); newInfoSchema != nil {
			p.InfoSchema = newInfoSchema
			return p.InfoSchema
		}
	}
	p.InfoSchema = p.ctx.GetInfoSchema().(infoschema.InfoSchema)
	return p.InfoSchema
}

func (p *preprocessor) initTxnContextProviderIfNecessary(node ast.Node) {
	if p.err != nil || p.flag&initTxnContextProvider == 0 {
		return
	}

	if provider, ok := sessiontxn.GetTxnManager(p.ctx).GetContextProvider().(*legacy.SimpleTxnContextProvider); ok {
		// When the current provider is `legacy.SimpleTxnContextProvider` it should to keep the logic equals to the old implement.
		// After refactoring, the `legacy.SimpleTxnContextProvider` will be removed, and this code will be removed too.
		provider.InfoSchema = p.ensureInfoSchema()
	}
}

func (p *preprocessor) hasAutoConvertWarning(colDef *ast.ColumnDef) bool {
	sessVars := p.ctx.GetSessionVars()
	if !sessVars.SQLMode.HasStrictMode() && colDef.Tp.GetType() == mysql.TypeVarchar {
		colDef.Tp.SetType(mysql.TypeBlob)
		if colDef.Tp.GetCharset() == charset.CharsetBin {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.GenWithStackByArgs(colDef.Name.Name.O, "VARBINARY", "BLOB"))
		} else {
			sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.GenWithStackByArgs(colDef.Name.Name.O, "VARCHAR", "TEXT"))
		}
		return true
	}
	return false
}
