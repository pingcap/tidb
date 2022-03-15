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
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

// SchemasReplace specifies schemas information mapping old schemas to new schemas.

type OldID = int64
type NewID = int64

type DBReplace struct {
	DBID   NewID
	DBName string
}

type TableReplace struct {
	TableID   NewID
	TableName string
	IndexMap  map[OldID]NewID
}

type SchemasReplace struct {
	DbMap           map[OldID]DBReplace
	TableMap        map[OldID]TableReplace
	RewriteTS       uint64
	TableFilter     filter.Filter
	genGenGlobalID  func(ctx context.Context) (int64, error)
	genGenGlobalIDs func(ctx context.Context, n int) ([]int64, error)
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbMap map[OldID]DBReplace,
	tblMap map[OldID]TableReplace,
	restoreTS uint64,
	tableFilter filter.Filter,
	genID func(ctx context.Context) (int64, error),
	genIDs func(ctx context.Context, n int) ([]int64, error),
) *SchemasReplace {
	return &SchemasReplace{
		DbMap:           dbMap,
		TableMap:        tblMap,
		RewriteTS:       restoreTS,
		TableFilter:     tableFilter,
		genGenGlobalID:  genID,
		genGenGlobalIDs: genIDs,
	}
}

func (sr *SchemasReplace) updateDBReplace(OldID, NewID int64, dbName string) {
	dbReplace, exist := sr.DbMap[OldID]
	if !exist {
		sr.DbMap[OldID] = DBReplace{
			DBID:   NewID,
			DBName: dbName,
		}
	} else {
		dbReplace.DBID = NewID
		dbReplace.DBName = dbName
	}
}

func (sr *SchemasReplace) rewriteKeyForDB(key []byte, cf string) ([]byte, bool, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}

		dbReplace = DBReplace{DBID: newID}
		sr.DbMap[dbID] = dbReplace
	} else if len(dbReplace.DBName) > 0 {
		if !sr.TableFilter.MatchSchema(dbReplace.DBName) {
			return nil, false, nil
		}
	}

	rawMetaKey.UpdateField(meta.DBkey(dbReplace.DBID))
	if cf == "write" {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte) ([]byte, bool, error) {
	var dbInfo = model.DBInfo{}
	if err := json.Unmarshal(value, &dbInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace := sr.DbMap[dbInfo.ID]
	if dbReplace.DBName == "" {
		dbReplace.DBName = dbInfo.Name.O
	}

	log.Info("rewrite dbinfo", zap.String("dbName", dbReplace.DBName),
		zap.Int64("old ID", dbInfo.ID), zap.Int64("new ID", dbReplace.DBID))

	dbInfo.ID = dbReplace.DBID
	newValue, err := json.Marshal(&dbInfo)
	if err != nil {
		return nil, false, err
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteKVEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, sr.rewriteDBInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKeyForTable(key []byte, cf string) ([]byte, bool, error) {
	rawMetakey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetakey.Key)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	tableID, err := meta.ParseTableKey(rawMetakey.Field)
	if err != nil {
		log.Warn("parse table key failed", zap.ByteString("field", rawMetakey.Field))
		return nil, false, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		log.Warn("db not exists, skip this events", zap.Int64("DBID", dbID))
		return nil, false, errors.Annotatef(berrors.ErrKVUnknown, "database not exist, DBID:%v", dbID)
	}

	tableReplace, exist := sr.TableMap[tableID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, errors.Trace(err)
		}

		tableReplace = TableReplace{TableID: newID}
		sr.TableMap[tableID] = tableReplace
	} else if len(dbReplace.DBName) > 0 && len(tableReplace.TableName) > 0 {
		log.Info("match table", zap.String("dbName", dbReplace.DBName), zap.String("tableName", tableReplace.TableName))

		if !sr.TableFilter.MatchTable(dbReplace.DBName, tableReplace.TableName) {
			return nil, false, nil
		}
	}

	rawMetakey.UpdateKey(meta.DBkey(dbReplace.DBID))
	rawMetakey.UpdateField(meta.TableKey(tableReplace.TableID))
	if cf == "write" {
		rawMetakey.UpdateTS(sr.RewriteTS)
	}
	return rawMetakey.EncodeMetaKey(), true, nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte) ([]byte, bool, error) {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	// update table ID
	tableReplace, exist := sr.TableMap[tableInfo.ID]
	if !exist {
		return nil, false, nil
	}

	tableInfo.ID = tableReplace.TableID
	if tableReplace.TableName == "" {
		tableReplace.TableName = tableInfo.Name.O
	}

	log.Info("rewrite tableInfo", zap.String("table-name", tableInfo.Name.String()),
		zap.Int64("old ID", tableInfo.ID), zap.Int64("new ID", tableReplace.TableID))

	// update index ID
	// maybe not need to udpate
	// for _, index := range tableInfo.Indices {
	// 	newID, exist := tableReplace.IndexMap[index.ID]
	// 	if !exist {
	// 		continue
	// 	}

	// 	log.Info("update index id",
	// 		zap.String("table-name", tableInfo.Name.String()),
	// 		zap.Int64("old-index-id", index.ID), zap.Int64("new-index-id", newID))

	// 	index.ID = newID
	// }

	// update partition table ID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			tableReplace, exist := sr.TableMap[tbl.ID]
			if !exist {
				newID, err := sr.genGenGlobalID(context.Background())
				if err != nil {
					return nil, false, errors.Trace(err)
				}

				tableReplace = TableReplace{TableID: newID}
				sr.TableMap[tbl.ID] = tableReplace
			}

			log.Info("update partition",
				zap.String("table-name", tableInfo.Name.String()),
				zap.String("partition-name", tbl.Name.String()),
				zap.Int64("old-id", tbl.ID), zap.Int64("new-id", tableReplace.TableID))

			partitions.Definitions[i].ID = tableReplace.TableID
		}
	}

	// marshal to json
	newValue, err := json.Marshal(&tableInfo)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteKVEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, err := sr.rewriteKeyForTable(e.Key, cf)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, sr.rewriteTableInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteValue(
	value []byte,
	cf string,
	cbRewrite func([]byte) ([]byte, bool, error),
) ([]byte, bool, error) {
	if cf == "default" {
		return cbRewrite(value)
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, false, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, true, nil
		}

		shortValue, needWrite, err := cbRewrite(rawWriteCFValue.GetShortValue())
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
		return sr.rewriteKVEntryForDB(e, cf)
	} else if meta.IsDBkey(rawKey.Key) && meta.IsTableKey(rawKey.Field) {
		return sr.rewriteKVEntryForTable(e, cf)
	} else {
		return nil, nil
	}
}
