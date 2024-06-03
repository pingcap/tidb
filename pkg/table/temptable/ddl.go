// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package temptable

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
)

// TemporaryTableDDL is an interface providing ddl operations for temporary table
type TemporaryTableDDL interface {
	CreateLocalTemporaryTable(db *model.DBInfo, info *model.TableInfo) error
	DropLocalTemporaryTable(schema model.CIStr, tblName model.CIStr) error
	TruncateLocalTemporaryTable(schema model.CIStr, tblName model.CIStr) error
}

// temporaryTableDDL implements temptable.TemporaryTableDDL
type temporaryTableDDL struct {
	sctx sessionctx.Context
}

func (d *temporaryTableDDL) CreateLocalTemporaryTable(db *model.DBInfo, info *model.TableInfo) error {
	if _, err := ensureSessionData(d.sctx); err != nil {
		return err
	}
	info.DBID = db.ID
	tbl, err := newTemporaryTableFromTableInfo(d.sctx, info)
	if err != nil {
		return err
	}

	return ensureLocalTemporaryTables(d.sctx).AddTable(db, tbl)
}

func (d *temporaryTableDDL) DropLocalTemporaryTable(schema model.CIStr, tblName model.CIStr) error {
	tbl, err := checkLocalTemporaryExistsAndReturn(d.sctx, schema, tblName)
	if err != nil {
		return err
	}

	getLocalTemporaryTables(d.sctx).RemoveTable(schema, tblName)
	return d.clearTemporaryTableRecords(tbl.Meta().ID)
}

func (d *temporaryTableDDL) TruncateLocalTemporaryTable(schema model.CIStr, tblName model.CIStr) error {
	oldTbl, err := checkLocalTemporaryExistsAndReturn(d.sctx, schema, tblName)
	if err != nil {
		return err
	}

	oldTblInfo := oldTbl.Meta()
	newTbl, err := newTemporaryTableFromTableInfo(d.sctx, oldTblInfo.Clone())
	if err != nil {
		return err
	}

	localTempTables := getLocalTemporaryTables(d.sctx)
	db, _ := localTempTables.SchemaByID(oldTblInfo.DBID)
	localTempTables.RemoveTable(schema, tblName)
	if err = localTempTables.AddTable(db, newTbl); err != nil {
		return err
	}

	return d.clearTemporaryTableRecords(oldTblInfo.ID)
}

func (d *temporaryTableDDL) clearTemporaryTableRecords(tblID int64) error {
	sessionData := getSessionData(d.sctx)
	if sessionData == nil {
		return nil
	}

	tblPrefix := tablecodec.EncodeTablePrefix(tblID)
	endKey := tablecodec.EncodeTablePrefix(tblID + 1)
	iter, err := sessionData.Iter(tblPrefix, endKey)
	if err != nil {
		return err
	}

	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, tblPrefix) {
			break
		}

		err = sessionData.DeleteTableKey(tblID, key)
		if err != nil {
			return err
		}

		err = iter.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

func checkLocalTemporaryExistsAndReturn(sctx sessionctx.Context, schema model.CIStr, tblName model.CIStr) (table.Table, error) {
	ident := ast.Ident{Schema: schema, Name: tblName}
	localTemporaryTables := getLocalTemporaryTables(sctx)
	if localTemporaryTables == nil {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(ident.String())
	}

	tbl, exist := localTemporaryTables.TableByName(schema, tblName)
	if !exist {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(ident.String())
	}

	return tbl, nil
}

func getSessionData(sctx sessionctx.Context) variable.TemporaryTableData {
	return sctx.GetSessionVars().TemporaryTableData
}

func ensureSessionData(sctx sessionctx.Context) (variable.TemporaryTableData, error) {
	sessVars := sctx.GetSessionVars()
	if sessVars.TemporaryTableData == nil {
		// Create this txn just for getting a MemBuffer. It's a little tricky
		bufferTxn, err := sctx.GetStore().Begin(tikv.WithStartTS(0))
		if err != nil {
			return nil, err
		}

		sessVars.TemporaryTableData = variable.NewTemporaryTableData(bufferTxn.GetMemBuffer())
	}

	return sessVars.TemporaryTableData, nil
}

func newTemporaryTableFromTableInfo(sctx sessionctx.Context, tbInfo *model.TableInfo) (table.Table, error) {
	// Local temporary table uses a real table ID.
	// We could mock a table ID, but the mocked ID might be identical to an existing
	// real table, and then we'll get into trouble.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	err := kv.RunInNewTxn(ctx, sctx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		tblID, err := m.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		tbInfo.ID = tblID
		tbInfo.State = model.StatePublic
		return nil
	})
	if err != nil {
		return nil, err
	}

	// AutoID is allocated in mocked..
	alloc := autoid.NewAllocatorFromTempTblInfo(tbInfo)
	allocs := make([]autoid.Allocator, 0, 1)
	if alloc != nil {
		allocs = append(allocs, alloc)
	}
	return tables.TableFromMeta(autoid.NewAllocators(false, allocs...), tbInfo)
}

// GetTemporaryTableDDL gets the temptable.TemporaryTableDDL from session context
func GetTemporaryTableDDL(sctx sessionctx.Context) TemporaryTableDDL {
	return &temporaryTableDDL{
		sctx: sctx,
	}
}
