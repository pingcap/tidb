package infobind

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
)

var _ Manager = (*BindManager)(nil)

// User implements infobind.Manager interface.
// This is used to update or check Ast.
type BindManager struct {
	is                 infoschema.InfoSchema
	currentDB          string
	*Handle                    //global handle
	GlobalBindAccessor GlobalBindAccessor
	copy               bool
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	GetAllGlobalBindData() []*BindData
	AddGlobalBind(originSql string, bindSql string, defaultDb string) error
	RemoveGlobalBind(originSql string, defaultDb string) error
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

func (b *BindManager) AddGlobalBind(originSql string, bindSql string, defaultDb string) error {
	return b.GlobalBindAccessor.AddGlobalBind(originSql, bindSql, defaultDb)
}

func (b *BindManager) RemoveGlobalBind(originSql string, defaultDb string) error {
	return b.GlobalBindAccessor.DropGlobalBind(originSql, defaultDb)
}

func (b *BindManager) GetAllGlobalBindData() []*BindData {
	var bindDataArr []*BindData

	for _, tempBindDataArr := range b.Get().Cache {
		for _, bindData := range tempBindDataArr {
			bindDataArr = append(bindDataArr, bindData)
		}
	}

	return bindDataArr
}

type GlobalBindAccessor interface {
	DropGlobalBind(originSql string, defaultDb string) error
	AddGlobalBind(originSql string, bindSql string, defaultDb string) error
}
