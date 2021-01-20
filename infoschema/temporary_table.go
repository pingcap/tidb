package infoschema

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/table"
)

// TemporaryTableSchema overloads InfoSchema to provide another InfoSchema.
type TemporarySchema struct {
	InfoSchema
	Temp map[string]table.Table
}

func (ts *TemporarySchema) TableByName(schema, table model.CIStr) (table.Table, error) {
	if tbl, ok := ts.Temp[table.L]; ok {
		return tbl, nil
	}
	return ts.InfoSchema.TableByName(schema, table)
}

func (ts *TemporarySchema) TableByID(id int64) (table.Table, bool) {
	for _, tbl := range ts.Temp {
		if tbl.Meta().ID == id {
			return tbl, true
		}
	}
	return ts.InfoSchema.TableByID(id)
}
