package testutil

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// GetTableByName gets table by name for test.
func GetTableByName(t *testing.T, tk *testkit.TestKit, db, table string) table.Table {
	dom := domain.GetDomain(tk.Session())
	// Make sure the table schema is the new schema.
	require.NoError(t, dom.Reload())
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	require.NoError(t, err)
	return tbl
}

// GetModifyColumn is used to get the changed column name after ALTER TABLE.
func GetModifyColumn(t *testing.T, tk *testkit.TestKit, db, tbl, colName string, allColumn bool) *table.Column {
	tt := GetTableByName(t, tk, db, tbl)
	colName = strings.ToLower(colName)
	var cols []*table.Column
	if allColumn {
		cols = tt.(*tables.TableCommon).Columns
	} else {
		cols = tt.Cols()
	}
	for _, col := range cols {
		if col.Name.L == colName {
			return col
		}
	}
	return nil
}
