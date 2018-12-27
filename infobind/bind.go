package infobind

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	log "github.com/sirupsen/logrus"
	"time"
)

var _ Manager = (*AstBind)(nil)

// User implements privilege.Manager interface.
// This is used to update or check ast.
type AstBind struct {
	parser         *parser.Parser
	lastUpdateTime time.Time
	is             infoschema.InfoSchema
	ctx            sessionctx.Context
	db             string
	*Handle
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetMatchedAst(sql ,db string) *BindData
	DeleteInvalidBind(key string)
	MatchHint(originalNode, hintedNode ast.Node) bool
	SetSchema(schema infoschema.InfoSchema)
	SetCtx(sctx sessionctx.Context)
	SetDB(db string)
}

const key keyType = 0

// BindManager binds Manager to context.
func BindBinderManager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetBindManager gets Checker from context.
func GetBindManager(ctx sessionctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
func (b *AstBind) GetMatchedAst(sql string, db string) *BindData {
	bindInfo := b.Handle.Get()
	if bindArray, ok := bindInfo.cache[sql]; ok {
		for i:=0; i < len(bindArray); i++ {
			if len(bindArray[i].DefaultDB) == 0 {
				return bindArray[i]
			}
			if bindArray[i].DefaultDB == db {
				return bindArray[i]
			}
		}
	}
	return nil
}

func (b *AstBind) DeleteInvalidBind(hash string) {
	bindInfo := b.Handle.Get()
	delete(bindInfo.cache, hash)
	b.Handle.bind.Store(bindInfo)
	//sql := fmt.Sprintf("delete from mysql.bind_info where update_time > \"%s\"", lastUpTime.Format("2006-01-02 15:04:05.000000"))
	//tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	//if err != nil {
	//	return errors.Trace(err), lastUpTime
	//}

}

func isPrimaryIndexHint(indexName model.CIStr) bool {
	return indexName.L == "primary"
}

func checkIndexName(paths []*model.IndexInfo, idxName model.CIStr, tblInfo *model.TableInfo) bool {

	for _, path := range paths {
		if path.Name.L == idxName.L {
			return true
		}
	}
	if isPrimaryIndexHint(idxName) && tblInfo.PKIsHandle {
		return true
	}
	return false
}

func checkHint(indexHints []*ast.IndexHint, tblInfo *model.TableInfo) bool {

	publicPaths := make([]*model.IndexInfo, 0, len(tblInfo.Indices)+1)
	for _, index := range tblInfo.Indices {
		if index.State == model.StatePublic {
			publicPaths = append(publicPaths, index)
		}
	}
	for _, hint := range indexHints {
		if hint.HintScope != ast.HintForScan {
			continue
		}
		for _, idxName := range hint.IndexNames {
			if checkIndexName(publicPaths, idxName, tblInfo) {
				return true
			}
		}

	}
	return false
}
func (b *AstBind) dataSourceBind(originalNode, hintedNode *ast.TableName) bool {

	if len(hintedNode.IndexHints) == 0 {
		return true
	}

	dbName := originalNode.Schema
	if dbName.L == "" {
		if b.db != b.ctx.GetSessionVars().CurrentDB {
			return false
		}
	}

	tbl, err := b.is.TableByName(dbName, originalNode.Name)
	if err != nil {
		return false
	}

	tableInfo := tbl.Meta()
	ok := checkHint(hintedNode.IndexHints, tableInfo)
	if !ok {
		log.Warning()
		return false
	}

	originalNode.IndexHints = append(originalNode.IndexHints, hintedNode.IndexHints...)

	return true
}

func (b *AstBind) SetSchema(schema infoschema.InfoSchema) {
	b.is = schema
}

func (b *AstBind) SetCtx(sctx sessionctx.Context) {
	if b.ctx == nil {
		b.ctx = sctx

	}
}
func (b *AstBind) SetDB(db string) {
	b.db = db
}

func (b *AstBind) joinBind(originalNode, hintedNode *ast.Join) bool {
	if originalNode.Right == nil {
		if hintedNode.Right != nil {
			return false
		}
		return b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	}

	ok := b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	if !ok {
		return false
	}

	ok = b.resultSetNodeBind(originalNode.Right, hintedNode.Right)
	return ok

}

func (b *AstBind) resultSetNodeBind(originalNode, hintedNode ast.ResultSetNode) bool {
	ok := false
	switch x := originalNode.(type) {
	case *ast.Join:
		if join, iok := hintedNode.(*ast.Join); iok {
			iok = b.joinBind(x, join)
			if iok {
				ok = true
			}
		}
	case *ast.TableSource:
		ts, iok := hintedNode.(*ast.TableSource)
		if !iok {
			break
		}
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			if value, iok := ts.Source.(*ast.SelectStmt); iok {
				return b.selectBind(v, value)
			}
			break
		case *ast.UnionStmt:
			ok = true
		case *ast.TableName:
			value, iok := ts.Source.(*ast.TableName)
			if iok {
				if iok := b.dataSourceBind(v, value); iok {
					ok = true
				}
			}
		}
	case *ast.SelectStmt:
		if sel, iok := hintedNode.(*ast.SelectStmt); iok {
			return b.selectBind(x, sel)
		}
	case *ast.UnionStmt:
		ok = true
	default:
	}
	return ok
}

func (b *AstBind) selectionBind(where ast.ExprNode, hindedWhere ast.ExprNode) bool {
	switch v := where.(type) {
	case *ast.SubqueryExpr:
		if v.Query != nil {
			if value, ok := hindedWhere.(*ast.SubqueryExpr); ok {
				return b.resultSetNodeBind(v.Query, value.Query)
			}
			return false
		}
	case *ast.ExistsSubqueryExpr:
		if v.Sel != nil {
			value, ok := hindedWhere.(*ast.ExistsSubqueryExpr)
			if !ok {
				return false
			}
			if value.Sel == nil {
				return false
			}

			return b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, value.Sel.(*ast.SubqueryExpr).Query)

		}
	case *ast.PatternInExpr:
		if v.Sel != nil {
			value, ok := hindedWhere.(*ast.PatternInExpr)
			if !ok {
				return false
			}
			if value.Sel == nil {
				return false
			}
			return b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, value.Sel.(*ast.SubqueryExpr).Query)
		}
	}
	return true

}

func (b *AstBind) selectBind(originalNode, hintedNode *ast.SelectStmt) bool {
	if originalNode.TableHints != nil {
		originalNode.TableHints = append(originalNode.TableHints, hintedNode.TableHints...)
	}
	if originalNode.From != nil {
		if hintedNode.From == nil {
			return false
		}
		ok := b.resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs)
		if !ok {
			return false
		}
	}
	if originalNode.Where != nil {
		if hintedNode.Where == nil {
			return false
		}
		return b.selectionBind(originalNode.Where, hintedNode.Where)
	}
	return true
}

func (b *AstBind) MatchHint(originalNode, hintedNode ast.Node) bool {

	switch x := originalNode.(type) {
	case *ast.SelectStmt:
		if value, ok := hintedNode.(*ast.SelectStmt); ok {
			log.Infof("sql %s match hint", originalNode.Text())
			return b.selectBind(x, value)
		}
	}
	log.Infof("sql %s mismatch hint", originalNode.Text())
	return false
}
