package tables

import (
	"context"
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
)	

var _ table.Table = &temporaryTable{}

// temporaryTable implements the table.Table interface.
type temporaryTable struct {
	TableCommon
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *temporaryTable) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	// The modification is not on the TiKV.
	sessVars := sctx.GetSessionVars()
	txn := sessVars.TemporaryTable.Txn
	ret, err :=  t.addRecordWithTxn(sctx, txn, r, opts...)
	fmt.Println("in temporary table ... add record... size =", txn.Size(), txn.Len())
	return ret, err
}

func (t *temporaryTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, touched []bool) error {
	// The modification is not on the TiKV.
	sessVars := sctx.GetSessionVars()
	txn := sessVars.TemporaryTable.Txn
	return t.updateRecordWithTxn(ctx, txn, sctx, h, oldData, newData, touched)
}

func (t *temporaryTable) Type() table.Type {
	return table.TemporaryTable
}

// TemporaryTableFromMeta creates a Table instance from model.TableInfo.
func TemporaryTableFromMeta(allocs autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error) {
	return tableFromMeta(allocs, tblInfo, true)
}
