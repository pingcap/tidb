package sql

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/rset"
)

type MetaQueryType struct{}

func (m *MetaQueryType) String() string {
	return "meta-query"
}

type MetaQueryer interface {
	QueryMeta(ctx context.Context, sql string) (rset.Recordset, error)
}
