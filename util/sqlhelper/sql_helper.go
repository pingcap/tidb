package sqlhelper

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/rset"
)

// KeyType is a dummy type to avoid naming collision in session.
type KeyType struct{}

// String implements Stringer interface.
func (k *KeyType) String() string {
	return "sql_helper"
}

// SQLHelper is an interface provides executing restricted sql statement.
type SQLHelper interface {
	// ExecRestrictedSQL run sql statement in ctx with some restriction.
	ExecRestrictedSQL(ctx context.Context, sql string) (rset.Recordset, error)
}
