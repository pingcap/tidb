package infobind

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/zhaoxiaojie0415/parser"
	"github.com/zhaoxiaojie0415/parser/ast"
	"github.com/zhaoxiaojie0415/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	log "github.com/sirupsen/logrus"
)

var _ Manager = (*BindManager)(nil)

// User implements infobind.Manager interface.
// This is used to update or check Ast.
type BindManager struct {
	is             infoschema.InfoSchema
	currentDB      string
	*Handle
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetMatchedAst(sql, db string) *BindData
	MatchHint(originalNode ast.Node, is infoschema.InfoSchema, db string)
	GetAllBindData() []*BindData
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
func (b *BindManager) GetMatchedAst(sql string, db string) *BindData {
	bc := b.Handle.Get()
	if bindArray, ok := bc.Cache[sql]; ok {
		for _, v := range bindArray {
			if v.Status != 1 {
				continue
			}
			if len(v.Db) == 0 {
				return v
			}
			if v.Db == db {
				return v
			}
		}
	}
	return nil
}

func (b *BindManager) deleteBind(hash, db string) {
	bc := b.Handle.Get()
	if bindArray, ok := bc.Cache[hash]; ok {
		for _, v := range bindArray {
			if v.Db == db {
				v.Status = -1
				break
			}
		}
	}
	b.Handle.bind.Store(bc)
}

func (b *BindManager) GetAllBindData() []*BindData {
	bindDataArr := make([] *BindData,0)

	for _,bindData := range b.Get().Cache {
		bindDataArr = append(bindDataArr , bindData...)
	}

	return bindDataArr
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

func (b *BindManager) dataSourceBind(originalNode, hintedNode *ast.TableName) (bool, error) {

	if len(hintedNode.IndexHints) == 0 {
		return true, nil
	}

	dbName := originalNode.Schema
	if dbName.L == "" {
		dbName = model.NewCIStr(b.currentDB)
	}

	tbl, err := b.is.TableByName(dbName, originalNode.Name)
	if err != nil {
		errMsg := fmt.Sprintf("table %s or Db %s not exist",originalNode.Name.L, dbName.L)
		return false, errors.New(errMsg)
	}

	tableInfo := tbl.Meta()
	ok := checkHint(hintedNode.IndexHints, tableInfo)
	if !ok {
		errMsg := fmt.Sprintf("table %s missing hint", tableInfo.Name)
		return false, errors.New(errMsg)
	}

	originalNode.IndexHints = append(originalNode.IndexHints, hintedNode.IndexHints...)

	return true, nil
}

func (b *BindManager) joinBind(originalNode, hintedNode *ast.Join) (ok bool, err error) {
	if originalNode.Right == nil {
		if hintedNode.Right != nil {
			return
		}
		return b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	}

	ok , err = b.resultSetNodeBind(originalNode.Left, hintedNode.Left)
	if !ok {
		return
	}

	ok, err = b.resultSetNodeBind(originalNode.Right, hintedNode.Right)
	return

}

func (b *BindManager) resultSetNodeBind(originalNode, hintedNode ast.ResultSetNode) (ok bool, err error) {
	switch x := originalNode.(type) {
	case *ast.Join:
		if join, iok := hintedNode.(*ast.Join); iok {
			ok, err = b.joinBind(x, join)
		}
	case *ast.TableSource:
		ts, iok := hintedNode.(*ast.TableSource)
		if !iok {
			break
		}

		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			if value, iok := ts.Source.(*ast.SelectStmt); iok {
				ok, err = b.selectBind(v, value)	//todo 这个地方不ok没有做处理
			}
		case *ast.UnionStmt:	//todo 这个地方unionstmt不做处理了吗？
			ok = true
		case *ast.TableName:
			if value, iok := ts.Source.(*ast.TableName); iok {
				ok, err = b.dataSourceBind(v, value)
			}
		}
	case *ast.SelectStmt:
		if sel, iok := hintedNode.(*ast.SelectStmt); iok {
			ok, err = b.selectBind(x, sel)
		}
	case *ast.UnionStmt:
		ok = true
	default:
		ok = true
	}
	return
}

func (b *BindManager) selectionBind(where ast.ExprNode, hindedWhere ast.ExprNode) (ok bool, err error) {
	switch v := where.(type) {
	case *ast.SubqueryExpr:
		if v.Query != nil {
			if value, ok1 := hindedWhere.(*ast.SubqueryExpr); ok1 {
				ok ,err  = b.resultSetNodeBind(v.Query, value.Query)
			}
		}
	case *ast.ExistsSubqueryExpr:
		if v.Sel != nil {
			value, ok1 := hindedWhere.(*ast.ExistsSubqueryExpr)
			if ok1 && value.Sel != nil {
				ok, err = b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, value.Sel.(*ast.SubqueryExpr).Query)
			}
		}
	case *ast.PatternInExpr:
		if v.Sel != nil {
			value, ok1 := hindedWhere.(*ast.PatternInExpr)
			if ok1 && value.Sel != nil {
				ok, err = b.resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, value.Sel.(*ast.SubqueryExpr).Query)
			}
		}
	}
	return

}

func (b *BindManager) selectBind(originalNode, hintedNode *ast.SelectStmt) (ok bool,err error) {
	if originalNode.TableHints != nil {// todo 这个地方如果tableHints为nil了岂不是就加不进去hint了？
		originalNode.TableHints = append(originalNode.TableHints, hintedNode.TableHints...)
	}
	if originalNode.From != nil {
		if hintedNode.From == nil {
			return
		}
		ok, err = b.resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs)
		if !ok {
			return
		}
	}
	if originalNode.Where != nil {
		if hintedNode.Where == nil {
			return
		}
		ok, err = b.selectionBind(originalNode.Where, hintedNode.Where)
	}
	return
}

func (b *BindManager) MatchHint(originalNode ast.Node, is infoschema.InfoSchema, db string)  {
	var hintedNode ast.Node
	bc := b.Handle.Get()
	sql := originalNode.Text()
	hash := parser.Digest(sql)
	if bindArray, ok := bc.Cache[hash]; ok {
		for _, v := range bindArray {
			if v.Status != 1 {
				continue
			}
			if len(v.Db) == 0 || v.Db == db {
				hintedNode = v.Ast
			}
		}
	}
	if hintedNode == nil {
		log.Warnf("sql %s try match hint failed", sql)
		return
	}

	b.currentDB = db
	b.is = is

	switch x := originalNode.(type) {
	case *ast.SelectStmt:
		if value, ok := hintedNode.(*ast.SelectStmt); ok {
			success, err := b.selectBind(x, value)
			if err != nil{
				b.deleteBind(hash, db)
			}
			if success {
				log.Warnf("sql %s try match hint success", sql)
			} else {
				log.Warnf("sql %s try match hint failed, err: %v", sql, err)
			}
			return
		}
	}
	log.Warnf("sql %s try match hint failed", sql)
	return
}
