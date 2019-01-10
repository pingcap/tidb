package infobind

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
)

var _ Manager = (*BindManager)(nil)

// BindManager use to manage both global bind info and session bind info
type BindManager struct {
	is            infoschema.InfoSchema
	currentDB     string
	SessionHandle *Handle //session handle
	*Handle               //global handle
	copy          bool
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
}

const key keyType = 0

//BindBinderManager binds Manager to context.
func BindBinderManager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}
