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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
)

type TemporaryTableManager struct {
	sctx sessionctx.Context
}

func GetTemporaryTableManager(sctx sessionctx.Context) *TemporaryTableManager {
	return &TemporaryTableManager{sctx: sctx}
}

func (m *TemporaryTableManager) AddLocalTemporaryTable(schema model.CIStr, tbl table.Table) error {
	if _, err := m.ensureSessionData(); err != nil {
		return err
	}

	return m.ensureLocalTemporaryTables().AddTable(schema, tbl)
}

func (m *TemporaryTableManager) RemoveLocalTemporaryTable(tableIdent ast.Ident) error {
	tbl, err := m.checkLocalTemporaryExistsAndReturn(tableIdent)
	if err != nil {
		return err
	}

	m.getLocalTemporaryTables().RemoveTable(tableIdent.Schema, tableIdent.Name)
	return m.clearTemporaryTableRecords(tbl.Meta().ID)
}

func (m *TemporaryTableManager) ReplaceAndTruncateLocalTemporaryTable(tableIdent ast.Ident, newTbl table.Table) error {
	oldTbl, err := m.checkLocalTemporaryExistsAndReturn(tableIdent)
	if err != nil {
		return err
	}

	newTblInfo := newTbl.Meta()
	if newTblInfo.Name.L != tableIdent.Name.L {
		return errors.New("name of newTbl and tblName must be equal")
	}

	oldTblID := oldTbl.Meta().ID
	if newTblInfo.ID == oldTblID {
		return errors.New("ID of newTbl and oldTbl cannot be equal")
	}

	localTempTables := m.getLocalTemporaryTables()
	localTempTables.RemoveTable(tableIdent.Schema, tableIdent.Name)
	if err = localTempTables.AddTable(tableIdent.Schema, newTbl); err != nil {
		return err
	}

	return m.clearTemporaryTableRecords(oldTblID)
}

func (m *TemporaryTableManager) checkLocalTemporaryExistsAndReturn(tableIdent ast.Ident) (table.Table, error) {
	localTemporaryTables := m.getLocalTemporaryTables()
	if localTemporaryTables == nil {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.String())
	}

	tbl, exist := localTemporaryTables.TableByName(tableIdent.Schema, tableIdent.Name)
	if !exist {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.String())
	}

	return tbl, nil
}

func (m *TemporaryTableManager) clearTemporaryTableRecords(tblID int64) error {
	sessionData := m.getSessionData()
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

func (m *TemporaryTableManager) getSessionData() variable.TemporaryTableData {
	return m.sctx.GetSessionVars().TemporaryTableData
}

func (m *TemporaryTableManager) ensureSessionData() (variable.TemporaryTableData, error) {
	sessVars := m.sctx.GetSessionVars()
	if sessVars.TemporaryTableData == nil {
		// Create this txn just for getting a MemBuffer. It's a little tricky
		bufferTxn, err := m.sctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
		if err != nil {
			return nil, err
		}

		sessVars.TemporaryTableData = variable.NewTemporaryTableData(bufferTxn.GetMemBuffer())
	}

	return sessVars.TemporaryTableData, nil
}

func (m *TemporaryTableManager) getLocalTemporaryTables() *infoschema.LocalTemporaryTables {
	tables := m.sctx.GetSessionVars().LocalTemporaryTables
	if tables == nil {
		return nil
	}

	return tables.(*infoschema.LocalTemporaryTables)
}

func (m *TemporaryTableManager) ensureLocalTemporaryTables() *infoschema.LocalTemporaryTables {
	sessVars := m.sctx.GetSessionVars()
	if sessVars.LocalTemporaryTables == nil {
		localTempTables := infoschema.NewLocalTemporaryTables()
		sessVars.LocalTemporaryTables = localTempTables
		return localTempTables
	}

	return sessVars.LocalTemporaryTables.(*infoschema.LocalTemporaryTables)
}
