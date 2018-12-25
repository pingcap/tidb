package infobind

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"time"
)

var _ Manager = (*AstBind)(nil)

// User implements privilege.Manager interface.
// This is used to update or check ast.
type AstBind struct {
	Parser         *parser.Parser
	LastUpdateTime time.Time
	Is 				infoshema.InfoSchema
	*Handle
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetMatchedAst(key string) *BindData
	DeleteInvalidBind(key string)
	MatchHint(originalNode, hintedNode ast.Node) bool
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
func (b *AstBind) GetMatchedAst(key string) *BindData {
	bindInfo := b.Handle.Get()
	if bindData, ok := bindInfo.cache[key]; ok {
		return bindData
	}
	return nil
}

func (b *AstBind) DeleteInvalidBind(hash string) {
	bindInfo := b.Handle.Get()
	delete(bindInfo.cache, hash)
	b.Handle.bind.Store(bindInfo)
}

func (b *AstBind) dataSourceBind(originalNode, hintedNode *ast.TableName) bool {
	if len(hintedNode.IndexHints) > 0 {
		originalNode.IndexHints = append(originalNode.IndexHints, hintedNode.IndexHints...)
	}
	return true
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

func (b *AstBind) selectionBind(where ast.ExprNode, agMapper map[*ast.AggregateFuncExpr]int) bool {
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
		b.resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs)
	}
	if originalNode.Where != nil {
		return  b.selectionBind(originalNode.Where, nil)
	}
	return true
}

func (b *AstBind) MatchHint(originalNode, hintedNode ast.Node) bool {

	switch x := originalNode.(type) {
	case *ast.SelectStmt:
		if value, ok := hintedNode.(*ast.SelectStmt); ok {
			return b.selectBind(x, value)
		}
	}
	return false
}
