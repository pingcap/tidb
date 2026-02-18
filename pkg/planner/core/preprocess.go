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
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/tracing"
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
	} else if show, ok := node.(*ast.ShowStmt); ok {
		// Only when Limit is nil, for Show stmt Limit should always nil when be here,
		// and the show STMT's behavior should consist with MySQL does.
		if show.Limit != nil || !show.NeedLimitRSRow() {
			return node
		}
		newShow := *show
		newShow.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetSessionVars().SelectLimit, "", ""),
		}
		return &newShow
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
func Preprocess(ctx context.Context, sctx sessionctx.Context, node *resolve.NodeW, preprocessOpt ...PreprocessOpt) error {
	defer tracing.StartRegion(ctx, "planner.Preprocess").End()
	v := preprocessor{
		ctx:                ctx,
		sctx:               sctx,
		tableAliasInJoin:   make([]map[string]any, 0),
		preprocessWith:     &preprocessWith{cteCanUsed: make([]string, 0), cteBeforeOffset: make([]int, 0)},
		staleReadProcessor: staleread.NewStaleReadProcessor(ctx, sctx),
		varsMutable:        make(map[string]struct{}),
		varsReadonly:       make(map[string]struct{}),
		resolveCtx:         node.GetResolveContext(),
	}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	// PreprocessorReturn must be non-nil before preprocessing
	if v.PreprocessorReturn == nil {
		v.PreprocessorReturn = &PreprocessorReturn{}
	}
	node.Node.Accept(&v)
	// InfoSchema must be non-nil after preprocessing
	v.ensureInfoSchema()
	sctx.GetPlanCtx().SetReadonlyUserVarMap(v.varsReadonly)
	if len(v.varsReadonly) > 0 {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("read-only variables are used")
	}
	return errors.Trace(v.err)
}

type preprocessorFlag uint64

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropTable is set when visiting create/drop table/view/sequence,
	// rename table, alter table add foreign key, alter table in prepare stmt, and BR restore.
	// TODO need a better name to clarify it's meaning
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
	// inImportInto is set when visiting an import into statement.
	inImportInto
	// inAnalyze is set when visiting an analyze statement.
	inAnalyze
)

// PreprocessorReturn is used to retain information obtained in the preprocessor.
type PreprocessorReturn struct {
	initedLastSnapshotTS bool
	IsStaleness          bool
	SnapshotTSEvaluator  func(context.Context, sessionctx.Context) (uint64, error)
	// LastSnapshotTS is the last evaluated snapshotTS if any
	// otherwise it defaults to zero
	LastSnapshotTS uint64
	InfoSchema     infoschema.InfoSchema
}

// preprocessWith is used to record info from WITH statements like CTE name.
type preprocessWith struct {
	cteCanUsed      []string
	cteBeforeOffset []int
	// A stack is implemented using a two-dimensional array.
	// Each layer stores the cteList of the current query block.
	// For example:
	// Query: with cte1 as (with cte2 as (select * from t) select * from cte2) select * from cte1;
	// Query Block1: select * from t
	// cteStack: [[cte1],[cte2]] (when withClause is null, the cteStack will not be appended)
	// Query Block2: with cte2 as (xxx) select * from cte2
	// cteStack: [[cte1],[cte2]]
	// Query Block3: with cte1 as (xxx) select * from cte1;
	// cteStack: [[cte1]]
	// ** Only the cteStack of SelectStmt and SetOprStmt will be set. **
	cteStack [][]*ast.CommonTableExpression
}

func (pw *preprocessWith) UpdateCTEConsumerCount(tableName string) {
	// must search from the back to the front (from the inner layer to the outer layer)
	// For example:
	// Query: with cte1 as (with cte1 as (select * from t) select * from cte1) select * from cte1;
	// cteStack: [[cte1: outer, consumerCount=1], [cte1: inner, consumerCount=1]]
	for i := len(pw.cteStack) - 1; i >= 0; i-- {
		for _, cte := range pw.cteStack[i] {
			if cte.Name.L == tableName {
				cte.ConsumerCount++
				return
			}
		}
	}
}

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from parser.
type preprocessor struct {
	ctx    context.Context
	sctx   sessionctx.Context
	flag   preprocessorFlag
	stmtTp byte
	showTp ast.ShowStmtType

	// tableAliasInJoin is a stack that keeps the table alias names for joins.
	// len(tableAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	tableAliasInJoin []map[string]any
	preprocessWith   *preprocessWith

	staleReadProcessor staleread.Processor

	varsMutable  map[string]struct{}
	varsReadonly map[string]struct{}

	// values that may be returned
	*PreprocessorReturn
	err error

	resolveCtx *resolve.Context
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.AdminStmt:
		p.checkAdminCheckTableGrammar(node)
	case *ast.DeleteStmt:
		p.stmtTp = TypeDelete
	case *ast.SelectStmt:
		p.stmtTp = TypeSelect
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
		p.checkSelectNoopFuncs(node)
	case *ast.SetOprStmt:
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
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
		// Used in CREATE INDEX ...
		p.stmtTp = TypeCreate
		if p.flag&inPrepare != 0 {
			p.flag |= inCreateOrDropTable
		}
		p.checkCreateIndexGrammar(node)
	case *ast.AlterTableStmt:
		p.stmtTp = TypeAlter
		if p.flag&inPrepare != 0 {
			p.flag |= inCreateOrDropTable
		}
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
		if node.With != nil {
			p.preprocessWith.cteStack = append(p.preprocessWith.cteStack, node.With.CTEs)
		}
		p.checkSetOprSelectList(node)
	case *ast.DeleteTableList:
		p.stmtTp = TypeDelete
		return in, true
	case *ast.Join:
		p.checkNonUniqTableAlias(node)
	case *ast.CreateBindingStmt:
		p.stmtTp = TypeCreate
		if node.OriginNode != nil {
			// if node.PlanDigests is not empty, this binding will be created from history, the node.OriginNode and node.HintedNode should be nil
			EraseLastSemicolon(node.OriginNode)
			EraseLastSemicolon(node.HintedNode)
			p.checkBindGrammar(node.OriginNode, node.HintedNode, p.sctx.GetSessionVars().CurrentDB)
		}
		return in, true
	case *ast.DropBindingStmt:
		p.stmtTp = TypeDrop
		if node.OriginNode != nil {
			EraseLastSemicolon(node.OriginNode)
			if node.HintedNode != nil {
				EraseLastSemicolon(node.HintedNode)
				p.checkBindGrammar(node.OriginNode, node.HintedNode, p.sctx.GetSessionVars().CurrentDB)
			}
		}
		return in, true
	case *ast.RecoverTableStmt:
		// The specified table in recover table statement maybe already been dropped.
		// So skip check table name here, otherwise, recover table [table_name] syntax will return
		// table not exists error. But recover table statement is use to recover the dropped table. So skip children here.
		return in, true
	case *ast.FlashBackTableStmt:
		if len(node.NewName) > 0 {
			p.checkFlashbackTableGrammar(node)
		}
		return in, true
	case *ast.FlashBackDatabaseStmt:
		if len(node.NewName) > 0 {
			p.checkFlashbackDatabaseGrammar(node)
		}
		return in, true
	case *ast.RepairTableStmt:
		p.stmtTp = TypeRepair
		// The RepairTable should consist of the logic for creating tables and renaming tables.
		p.flag |= inRepairTable
		p.checkRepairTableGrammar(node)
	case *ast.ImportIntoStmt:
		p.stmtTp = TypeImportInto
		p.flag |= inImportInto
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
		// not support procedure right now.
		if node.Schema.L != "" {
			p.err = expression.ErrFunctionNotExists.GenWithStackByArgs("FUNCTION", node.Schema.L+"."+node.FnName.L)
		}
	case *ast.BRIEStmt:
		if node.Kind == ast.BRIEKindRestore {
			p.flag |= inCreateOrDropTable
		}
	case *ast.TableSource:
		isModeOracle := p.sctx.GetSessionVars().SQLMode&mysql.ModeOracle != 0
		_, isSelectStmt := node.Source.(*ast.SelectStmt)
		_, isSetOprStmt := node.Source.(*ast.SetOprStmt)
		if (isSelectStmt || isSetOprStmt) && !isModeOracle && len(node.AsName.L) == 0 {
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
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		} else if p.sctx.GetSessionVars().TxnReadTS.PeakTxnReadTS() > 0 {
			// If the begin statement was like following:
			// set transaction read only as of timestamp ...
			// begin
			// then we need set StmtCtx.IsStaleness as true in order to avoid take tso in PrepareTSFuture.
			p.sctx.GetSessionVars().StmtCtx.IsStaleness = true
			p.IsStaleness = true
		}
	case *ast.AnalyzeTableStmt:
		p.flag |= inAnalyze
	case *ast.VariableExpr:
		if node.Value != nil {
			p.varsMutable[node.Name] = struct{}{}
			delete(p.varsReadonly, node.Name)
		} else if p.stmtTp == TypeSelect {
			// Only check the variable in select statement.
			_, ok := p.varsMutable[node.Name]
			if !ok {
				p.varsReadonly[node.Name] = struct{}{}
			}
		}
	case *ast.Constraint:
		// Used in ALTER TABLE or CREATE TABLE
		p.checkConstraintGrammar(node)
	case *ast.ColumnName:
		if node.Name.L == model.ExtraCommitTSName.L &&
			(p.stmtTp == TypeSelect || p.stmtTp == TypeSetOpr || p.stmtTp == TypeUpdate || p.stmtTp == TypeDelete) {
			p.err = plannererrors.ErrInternal.GenWithStack("Usage of column name '%s' is not supported for now",
				model.ExtraCommitTSName.O)
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
	// TypeImportInto for ImportIntoStmt
	TypeImportInto
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
	currentDB := p.sctx.GetSessionVars().CurrentDB
	if tn.Schema.String() != "" {
		currentDB = tn.Schema.L
	}
	if currentDB == "" {
		return nil, errors.Trace(plannererrors.ErrNoDB)
	}
	sName := ast.NewCIStr(currentDB)
	is := p.ensureInfoSchema()

	// for 'SHOW CREATE VIEW/SEQUENCE ...' statement, ignore local temporary tables.
	if p.stmtTp == TypeShow && (p.showTp == ast.ShowCreateView || p.showTp == ast.ShowCreateSequence) {
		is = temptable.DetachLocalTemporaryTableInfoSchema(is)
	}

	tbl, err := is.TableByName(p.ctx, sName, tn.Name)
	if err != nil {
		// We should never leak that the table doesn't exist (i.e. attachplannererrors.ErrTableNotExists)
		// unless we know that the user has permissions to it, should it exist.
		// By checking here, this makes all SELECT/SHOW/INSERT/UPDATE/DELETE statements safe.
		currentUser, activeRoles := p.sctx.GetSessionVars().User, p.sctx.GetSessionVars().ActiveRoles
		if pm := privilege.GetPrivilegeManager(p.sctx); pm != nil {
			if !pm.RequestVerification(activeRoles, sName.L, tn.Name.O, "", mysql.AllPrivMask) {
				u := currentUser.Username
				h := currentUser.Hostname
				if currentUser.AuthHostname != "" {
					u = currentUser.AuthUsername
					h = currentUser.AuthHostname
				}
				return nil, plannererrors.ErrTableaccessDenied.GenWithStackByArgs(p.stmtType(), u, h, tn.Name.O)
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
	nodeW := resolve.NewNodeWWithCtx(originNode, p.resolveCtx)
	tblNames := ExtractTableList(nodeW, false)
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
		dbInfo, _ := infoschema.SchemaByTable(p.ensureInfoSchema(), tableInfo)
		p.resolveCtx.AddTableName(&resolve.TableNameW{
			TableName: tn,
			DBInfo:    dbInfo,
			TableInfo: tableInfo,
		})
	}
	aliasChecker := &aliasChecker{}
	originNode.Accept(aliasChecker)
	hintedNode.Accept(aliasChecker)
	originSQL, _ := bindinfo.NormalizeStmtForBinding(originNode, defaultDB, false)
	hintedSQL, _ := bindinfo.NormalizeStmtForBinding(hintedNode, defaultDB, false)
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
		if x.Format != "" && !valid {
			p.err = plannererrors.ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
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
					p.err = plannererrors.ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
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
			p.err = plannererrors.ErrInternal.GenWithStack("len(cteBeforeOffset) is less than one.Maybe it was deleted in somewhere.Should match in Enter and Leave")
			break
		}
		beforeOffset := with.cteBeforeOffset[lenWithCteBeforeOffset-1]
		with.cteBeforeOffset = with.cteBeforeOffset[:lenWithCteBeforeOffset-1]
		with.cteCanUsed = with.cteCanUsed[:beforeOffset]
		if cteNode, exist := x.(*ast.CommonTableExpression); exist {
			with.cteCanUsed = append(with.cteCanUsed, cteNode.Name.L)
		}
	case *ast.SelectStmt:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	case *ast.SetOprStmt:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	case *ast.SetOprSelectList:
		if x.With != nil {
			p.preprocessWith.cteStack = p.preprocessWith.cteStack[0 : len(p.preprocessWith.cteStack)-1]
		}
	}

	return in, p.err == nil
}

