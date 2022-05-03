// Copyright 2022-present PingCAP, Inc.
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

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"go.uber.org/zap"
)

// SchemasReplace specifies schemas information mapping old schemas to new schemas.

type OldID = int64
type NewID = int64

type TableReplace struct {
	OldTableInfo *model.TableInfo
	NewTableID   NewID
	NewName      string
	PartitionMap map[OldID]NewID
	IndexMap     map[OldID]NewID
}

type DBReplace struct {
	OldDBInfo *model.DBInfo
	NewDBID   NewID
	TableMap  map[OldID]*TableReplace
}

type SchemasReplace struct {
	DbMap           map[OldID]*DBReplace
	RewriteTS       uint64
	TableFilter     filter.Filter
	genGenGlobalID  func(ctx context.Context) (int64, error)
	genGenGlobalIDs func(ctx context.Context, n int) ([]int64, error)
}

// NewTableReplace creates a TableReplace struct.
func NewTableReplace(tableInfo *model.TableInfo, newID NewID, newTableName string) *TableReplace {
	return &TableReplace{
		OldTableInfo: tableInfo,
		NewTableID:   newID,
		NewName:      newTableName,
		PartitionMap: make(map[OldID]NewID),
		IndexMap:     make(map[OldID]NewID),
	}
}

// NewDBReplace creates a DBReplace struct.
func NewDBReplace(dbInfo *model.DBInfo, newID NewID) *DBReplace {
	return &DBReplace{
		OldDBInfo: dbInfo,
		NewDBID:   newID,
		TableMap:  make(map[OldID]*TableReplace),
	}
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbMap map[OldID]*DBReplace,
	restoreTS uint64,
	tableFilter filter.Filter,
	genID func(ctx context.Context) (int64, error),
	genIDs func(ctx context.Context, n int) ([]int64, error),
) *SchemasReplace {
	return &SchemasReplace{
		DbMap:           dbMap,
		RewriteTS:       restoreTS,
		TableFilter:     tableFilter,
		genGenGlobalID:  genID,
		genGenGlobalIDs: genIDs,
	}
}

func (sr *SchemasReplace) TranslateToSchema() *backup.Schemas {
	schemas := backup.NewBackupSchemas()

	for _, dr := range sr.DbMap {
		if dr.OldDBInfo == nil {
			continue
		}

		for _, tr := range dr.TableMap {
			if tr.OldTableInfo == nil {
				continue
			}

			schemas.AddSchema(dr.OldDBInfo, tr.OldTableInfo)
		}
	}
	return schemas
}

func (sr *SchemasReplace) rewriteKeyForDB(key []byte, cf string) ([]byte, bool, int64, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, dbID, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	rawMetaKey.UpdateField(meta.DBkey(dbReplace.NewDBID))
	if cf == "write" {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, dbID, nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte, _ int64) ([]byte, bool, error) {
	oldDBInfo := new(model.DBInfo)
	if err := json.Unmarshal(value, oldDBInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[oldDBInfo.ID]
	if !exist {
		// If the schema has existed, don't need generate a new ID.
		// Or we need a new ID to rewrite the dbID in kv entry.
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}

		dbReplace = NewDBReplace(oldDBInfo, newID)
		sr.DbMap[oldDBInfo.ID] = dbReplace
	} else {
		dbReplace.OldDBInfo = oldDBInfo
	}

	log.Debug("rewrite dbinfo", zap.String("dbName", dbReplace.OldDBInfo.Name.O),
		zap.Int64("old ID", oldDBInfo.ID), zap.Int64("new ID", dbReplace.NewDBID))

	newDBInfo := oldDBInfo.Clone()
	newDBInfo.ID = dbReplace.NewDBID
	newValue, err := json.Marshal(newDBInfo)
	if err != nil {
		return nil, false, err
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, 0, sr.rewriteDBInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newKey, needWrite, _, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) getDBIDFromTableKey(key []byte) (int64, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return meta.ParseDBKey(rawMetaKey.Key)
}

func (sr *SchemasReplace) rewriteKeyForTable(
	key []byte,
	cf string,
	parseField func([]byte) (tableID int64, err error),
	encodeField func(tableID int64) []byte,
) ([]byte, bool, int64, error) {
	var newID int64

	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Key)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	tableID, err := parseField(rawMetaKey.Field)
	if err != nil {
		log.Warn("parse table key failed", zap.ByteString("field", rawMetaKey.Field))
		return nil, false, 0, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		if newID, err = sr.genGenGlobalID(context.Background()); err != nil {
			return nil, false, dbID, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		if newID, err = sr.genGenGlobalID(context.Background()); err != nil {
			return nil, false, dbID, errors.Trace(err)
		}
		tableReplace = NewTableReplace(nil, newID, "")
		dbReplace.TableMap[tableID] = tableReplace
	}

	rawMetaKey.UpdateKey(meta.DBkey(dbReplace.NewDBID))
	rawMetaKey.UpdateField(encodeField(tableReplace.NewTableID))
	if cf == "write" {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, dbID, nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte, dbID int64) ([]byte, bool, error) {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	// update table ID
	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		dbReplace = NewDBReplace(nil, newID)
		sr.DbMap[dbID] = dbReplace
	}

	tableReplace, exist := dbReplace.TableMap[tableInfo.ID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		tableReplace = NewTableReplace(&tableInfo, newID, tableInfo.Name.O)
		dbReplace.TableMap[tableInfo.ID] = tableReplace
	} else {
		tableReplace.OldTableInfo = &tableInfo
	}

	log.Debug("rewrite tableInfo", zap.String("table-name", tableInfo.Name.String()),
		zap.Int64("old ID", tableInfo.ID), zap.Int64("new ID", tableReplace.NewTableID))

	newTableInfo := tableInfo.Clone()
	if newTableInfo.Partition != nil {
		newTableInfo.Partition = tableInfo.Partition.Clone()
	}
	newTableInfo.ID = tableReplace.NewTableID

	// update partition table ID
	partitions := newTableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist := tableReplace.PartitionMap[tbl.ID]
			if !exist {
				newID, err := sr.genGenGlobalID(context.Background())
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				tableReplace.PartitionMap[tbl.ID] = newID
			}

			log.Debug("update partition",
				zap.String("partition-name", tbl.Name.String()),
				zap.Int64("old-id", tbl.ID), zap.Int64("new-id", newID))
			partitions.Definitions[i].ID = newID
		}
	}

	// marshal to json
	newValue, err := json.Marshal(&newTableInfo)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	dbID, err := sr.getDBIDFromTableKey(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, dbID, sr.rewriteTableInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newKey, needWrite, dbID, err := sr.rewriteKeyForTable(e.Key, cf, meta.ParseTableKey, meta.TableKey)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKVEntryForAutoTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, _, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoTableIDKey,
		meta.AutoTableIDKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteKVEntryForSequenceKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, _, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseSequenceKey,
		meta.SequenceKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteKVEntryForAutoRandomTableIDKey(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, _, err := sr.rewriteKeyForTable(
		e.Key,
		cf,
		meta.ParseAutoRandomTableIDKey,
		meta.AutoRandomTableIDKey,
	)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: e.Value}, nil
}

func (sr *SchemasReplace) rewriteValue(
	value []byte,
	cf string,
	id int64,
	cbRewrite func([]byte, int64) ([]byte, bool, error),
) ([]byte, bool, error) {
	if cf == "default" {
		return cbRewrite(value, id)
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, false, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, true, nil
		}

		shortValue, needWrite, err := cbRewrite(rawWriteCFValue.GetShortValue(), id)
		if err != nil || !needWrite {
			return nil, needWrite, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		return rawWriteCFValue.EncodeTo(), true, nil
	} else {
		panic(fmt.Sprintf("not support cf:%s", cf))
	}
}

// RewriteKvEntry uses to rewrite tableID/dbID in entry.key and entry.value
func (sr *SchemasReplace) RewriteKvEntry(e *kv.Entry, cf string) (*kv.Entry, error) {
	// skip mDDLJob
	if !strings.HasPrefix(string(e.Key), "mDB") {
		return nil, nil
	}

	rawKey, err := ParseTxnMetaKeyFrom(e.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if meta.IsDBkey(rawKey.Field) {
		return sr.rewriteEntryForDB(e, cf)
	} else if meta.IsDBkey(rawKey.Key) {
		if meta.IsTableKey(rawKey.Field) {
			return sr.rewriteEntryForTable(e, cf)
		} else if meta.IsAutoTableIDKey(rawKey.Field) {
			return sr.rewriteKVEntryForAutoTableIDKey(e, cf)
		} else if meta.IsSequenceKey(rawKey.Field) {
			return sr.rewriteKVEntryForSequenceKey(e, cf)
		} else if meta.IsAutoRandomTableIDKey(rawKey.Field) {
			return sr.rewriteKVEntryForAutoRandomTableIDKey(e, cf)
		} else {
			return nil, nil
		}
	} else {
		return nil, nil
	}
}
