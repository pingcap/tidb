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
// See the License for the specific language governing permissions and
// limitations under the License.

package temptable

import (
	"bytes"
	"context"

	"github.com/pingcap/tidb/util/tableutil"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	transaction "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	tikvkey "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

// SessionTemporaryTableManager is used to manage temporary tables
type SessionTemporaryTableManager struct {
	ctx         sessionctx.Context
	localTables *infoschema.LocalTemporaryTables
	memData     *tikv.MemDB

	cache struct {
		schemaMetaVersion    int64
		sortedGlobalTables   []table.Table
		sortedTableKeyRanges []*tikvkey.KeyRange
	}
}

// GetTemporaryTableManager get the TemporaryTableManager in session
func GetTemporaryTableManager(ctx sessionctx.Context) *SessionTemporaryTableManager {
	sessionVars := ctx.GetSessionVars()
	mgr := sessionVars.TemporaryTableManager
	if mgr == nil {
		mgr = &SessionTemporaryTableManager{ctx: ctx}
		sessionVars.TemporaryTableManager = mgr
	}

	return mgr.(*SessionTemporaryTableManager)
}

// UpdateSnapshotOptions is used for update snapshot options when schema changed
func (m *SessionTemporaryTableManager) UpdateSnapshotOptions(snap kv.Snapshot, is infoschema.InfoSchema) {
	if is.SchemaMetaVersion() != m.cache.schemaMetaVersion {
		m.cache.sortedGlobalTables = is.SortedGlobalTemporaryTables()
		m.rebuildKeyRanges()
		m.cache.schemaMetaVersion = is.SchemaMetaVersion()
	}

	snap.SetOption(kv.MemSnapshotSortedRanges, m.cache.sortedTableKeyRanges)
	snap.SetOption(kv.MemSnapshotDB, m.memData)
}

// AddLocalTemporaryTable add a local temporary table
func (m *SessionTemporaryTableManager) AddLocalTemporaryTable(schema model.CIStr, tb table.Table) error {
	m.ensureLocalTemporaryTables()
	if err := m.localTables.AddTable(schema, tb); err != nil {
		return err
	}

	m.rebuildKeyRanges()
	if m.memData == nil {
		m.memData = tikv.NewMemDB()
	}

	return nil
}

// RemoveLocalTemporaryTable remove a local temporary table. It also removes all data for this table.
func (m *SessionTemporaryTableManager) RemoveLocalTemporaryTable(schema, tn model.CIStr) error {
	if m.localTables == nil {
		return nil
	}

	tbl, exist := m.localTables.TableByName(schema, tn)
	if !exist {
		return nil
	}

	m.localTables.RemoveTable(schema, tn)
	m.rebuildKeyRanges()
	return m.ClearTemporaryTableRecords(tbl.Meta().ID)
}

// LocalTemporaryTableByID get the local temporary table by id
func (m *SessionTemporaryTableManager) LocalTemporaryTableByID(tblID int64) (table.Table, bool) {
	if m.localTables == nil {
		return nil, false
	}
	return m.localTables.TableByID(tblID)
}

// LocalTemporaryTableByName get the local temporary table by name
func (m *SessionTemporaryTableManager) LocalTemporaryTableByName(schema model.CIStr, name model.CIStr) (table.Table, bool) {
	if m.localTables == nil {
		return nil, false
	}
	return m.localTables.TableByName(schema, name)
}

// ClearTemporaryTableRecords clear a local table's data in session
func (m *SessionTemporaryTableManager) ClearTemporaryTableRecords(tblID int64) error {
	if m.memData == nil {
		return nil
	}

	memBuffer := transaction.NewMemBuffer(m.memData)
	tblPrefix := tablecodec.EncodeTablePrefix(tblID)
	endKey := tablecodec.EncodeTablePrefix(tblID + 1)

	iter, err := memBuffer.Iter(tblPrefix, endKey)
	if err != nil {
		return err
	}
	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, tblPrefix) {
			break
		}

		err = memBuffer.Delete(key)
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

// GetSessionData get the session data
func (m *SessionTemporaryTableManager) GetSessionData() kv.MemBuffer {
	if m.memData == nil {
		return nil
	}
	return transaction.NewMemBuffer(m.memData)
}

// WrapInformationSchema wrap the information schema with temporary table
func (m *SessionTemporaryTableManager) WrapInformationSchema(is infoschema.InfoSchema) infoschema.InfoSchema {
	// Already a wrapped one.
	if _, ok := is.(*infoschema.TemporaryTableAttachedInfoSchema); ok {
		return is
	}
	// No local temporary table.
	if m.localTables == nil {
		return is
	}

	return &infoschema.TemporaryTableAttachedInfoSchema{
		InfoSchema:           is,
		LocalTemporaryTables: m.localTables,
	}
}

// CommitTxnWithTemporaryData write dirty data to session memory and filter it out before committing the txn.
func (m *SessionTemporaryTableManager) CommitTxnWithTemporaryData(ctx context.Context, txn kv.Transaction) error {
	sessionData := m.GetSessionData()
	var stage kv.StagingHandle
	defer func() {
		// stage != kv.InvalidStagingHandle means error occurs, we need to cleanup sessionData
		if stage != kv.InvalidStagingHandle {
			sessionData.Cleanup(stage)
		}
	}()

	txnTempTables := m.ctx.GetSessionVars().TxnCtx.TemporaryTables
	for tblID, tbl := range txnTempTables {
		if !tbl.GetModified() {
			continue
		}

		if tbl.GetMeta().TempTableType != model.TempTableLocal {
			continue
		}
		if _, ok := m.LocalTemporaryTableByID(tblID); !ok {
			continue
		}

		if stage == kv.InvalidStagingHandle {
			stage = sessionData.Staging()
		}

		tblPrefix := tablecodec.EncodeTablePrefix(tblID)
		endKey := tablecodec.EncodeTablePrefix(tblID + 1)

		txnMemBuffer := txn.GetMemBuffer()
		iter, err := txnMemBuffer.Iter(tblPrefix, endKey)
		if err != nil {
			return err
		}

		for iter.Valid() {
			key := iter.Key()
			if !bytes.HasPrefix(key, tblPrefix) {
				break
			}

			value := iter.Value()
			if len(value) == 0 {
				err = sessionData.Delete(key)
			} else {
				err = sessionData.Set(key, iter.Value())
			}

			if err != nil {
				return err
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	txn.SetOption(kv.KVFilter, temporaryTableKVFilter(txnTempTables))
	err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if stage != kv.InvalidStagingHandle {
		sessionData.Release(stage)
		stage = kv.InvalidStagingHandle
	}

	return nil
}

func (m *SessionTemporaryTableManager) ensureLocalTemporaryTables() {
	if m.localTables == nil {
		m.localTables = infoschema.NewLocalTemporaryTables()
	}
}

func (m *SessionTemporaryTableManager) rebuildKeyRanges() {
	localCur := 0
	globalCur := 0
	var localTbls []table.Table
	if m.localTables != nil {
		localTbls = m.localTables.SortedTables()
	}
	globalTbls := m.cache.sortedGlobalTables

	ranges := make([]*tikvkey.KeyRange, 0, len(localTbls)+len(globalTbls))
	for localCur < len(localTbls) && globalCur < len(globalTbls) {
		tblID := localTbls[localCur].Meta().ID
		globalTblID := globalTbls[globalCur].Meta().ID
		if globalTblID < tblID {
			tblID = globalTblID
			globalCur++
		} else {
			localCur++
		}

		ranges = append(ranges, getTableKeyRange(tblID))
	}

	for localCur < len(localTbls) {
		ranges = append(ranges, getTableKeyRange(localTbls[localCur].Meta().ID))
		localCur++
	}

	for globalCur < len(globalTbls) {
		ranges = append(ranges, getTableKeyRange(globalTbls[globalCur].Meta().ID))
		globalCur++
	}

	m.cache.sortedTableKeyRanges = ranges
}

type temporaryTableKVFilter map[int64]tableutil.TempTable

func (m temporaryTableKVFilter) IsUnnecessaryKeyValue(key, value []byte, _ tikvkey.KeyFlags) bool {
	tid := tablecodec.DecodeTableID(key)
	if _, ok := m[tid]; ok {
		return true
	}

	// This is the default filter for all tables.
	return tablecodec.IsUntouchedIndexKValue(key, value)
}

func getTableKeyRange(tblID int64) *tikvkey.KeyRange {
	return &tikvkey.KeyRange{
		StartKey: tablecodec.EncodeTablePrefix(tblID),
		EndKey:   tablecodec.EncodeTablePrefix(tblID + 1),
	}
}
