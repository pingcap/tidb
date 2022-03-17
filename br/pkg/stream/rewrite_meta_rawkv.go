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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

// SchemasReplace specifies schemas information mapping old schemas to new schemas.

type OldID = int64
type NewID = int64

type TableReplace struct {
	TableID      NewID
	OldName      string
	NewName      string
	PartitionMap map[OldID]NewID
	IndexMap     map[OldID]NewID
}

type DBReplace struct {
	DBID     NewID
	OldName  string
	NewName  string
	TableMap map[OldID]*TableReplace
}

type SchemasReplace struct {
	DbMap           map[OldID]*DBReplace
	RewriteTS       uint64
	TableFilter     filter.Filter
	genGenGlobalID  func(ctx context.Context) (int64, error)
	genGenGlobalIDs func(ctx context.Context, n int) ([]int64, error)
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

func (sr *SchemasReplace) updateDBReplace(OldID, NewID int64, dbName string) {
	dbReplace, exist := sr.DbMap[OldID]
	if !exist {
		sr.DbMap[OldID] = &DBReplace{
			DBID:    NewID,
			OldName: dbName,
		}
	} else {
		dbReplace.DBID = NewID
		dbReplace.OldName = dbName
	}
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
			return nil, false, 0, errors.Trace(err)
		}

		dbReplace = &DBReplace{
			DBID:     newID,
			TableMap: make(map[int64]*TableReplace),
		}
		sr.DbMap[dbID] = dbReplace
	}

	rawMetaKey.UpdateField(meta.DBkey(dbReplace.DBID))
	if cf == "write" {
		rawMetaKey.UpdateTS(sr.RewriteTS)
	}
	return rawMetaKey.EncodeMetaKey(), true, dbID, nil
}

func (sr *SchemasReplace) rewriteDBInfo(value []byte, _ int64) ([]byte, bool, error) {
	var dbInfo = model.DBInfo{}
	if err := json.Unmarshal(value, &dbInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	dbReplace := sr.DbMap[dbInfo.ID]
	if len(dbReplace.OldName) == 0 {
		dbReplace.OldName = dbInfo.Name.O
	}
	if len(dbReplace.NewName) == 0 {
		dbReplace.NewName = dbInfo.Name.O
	}

	log.Debug("rewrite dbinfo", zap.String("dbName", dbReplace.OldName),
		zap.Int64("old ID", dbInfo.ID), zap.Int64("new ID", dbReplace.DBID))

	dbInfo.ID = dbReplace.DBID
	newValue, err := json.Marshal(&dbInfo)
	if err != nil {
		return nil, false, err
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteKVEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, dbID, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, dbID, sr.rewriteDBInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKeyForTable(key []byte, cf string) ([]byte, bool, int64, error) {
	rawMetakey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetakey.Key)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	tableID, err := meta.ParseTableKey(rawMetakey.Field)
	if err != nil {
		log.Warn("parse table key failed", zap.ByteString("field", rawMetakey.Field))
		return nil, false, 0, errors.Trace(err)
	}

	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, 0, errors.Trace(err)
		}

		dbReplace = &DBReplace{
			DBID:     newID,
			TableMap: make(map[int64]*TableReplace),
		}
		sr.DbMap[dbID] = dbReplace
		log.Debug("db not exists, create new dbID", zap.Int64("oldID", dbID), zap.Int64("newID", newID))
	}

	tableReplace, exist := dbReplace.TableMap[tableID]
	if !exist {
		newID, err := sr.genGenGlobalID(context.Background())
		if err != nil {
			return nil, false, 0, errors.Trace(err)
		}

		tableReplace = &TableReplace{
			TableID:      newID,
			PartitionMap: make(map[OldID]NewID),
			IndexMap:     make(map[OldID]NewID),
		}
		dbReplace.TableMap[tableID] = tableReplace
	}

	rawMetakey.UpdateKey(meta.DBkey(dbReplace.DBID))
	rawMetakey.UpdateField(meta.TableKey(tableReplace.TableID))
	if cf == "write" {
		rawMetakey.UpdateTS(sr.RewriteTS)
	}
	return rawMetakey.EncodeMetaKey(), true, dbID, nil
}

func (sr *SchemasReplace) rewriteTableInfo(value []byte, dbID int64) ([]byte, bool, error) {
	var (
		tableInfo model.TableInfo
		newID     int64
		err       error
	)
	if err = json.Unmarshal(value, &tableInfo); err != nil {
		return nil, false, errors.Trace(err)
	}

	// update table ID
	dbReplace, exist := sr.DbMap[dbID]
	if !exist {
		return nil, false, nil
	}
	tableReplace, exist := dbReplace.TableMap[tableInfo.ID]
	if !exist {
		return nil, false, nil
	}

	if len(tableReplace.OldName) == 0 {
		tableReplace.OldName = tableInfo.Name.O
	}
	if len(tableReplace.NewName) == 0 {
		tableReplace.NewName = tableInfo.Name.O
	}

	log.Debug("rewrite tableInfo", zap.String("table-name", tableInfo.Name.String()),
		zap.Int64("old ID", tableInfo.ID), zap.Int64("new ID", tableReplace.TableID))
	tableInfo.ID = tableReplace.TableID

	// update partition table ID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			newID, exist = tableReplace.PartitionMap[tbl.ID]
			if !exist {
				if newID, err = sr.genGenGlobalID(context.Background()); err != nil {
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
	newValue, err := json.Marshal(&tableInfo)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return newValue, true, nil
}

func (sr *SchemasReplace) rewriteKVEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, needWrite, dbID, err := sr.rewriteKeyForTable(e.Key, cf)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	newValue, needWrite, err := sr.rewriteValue(e.Value, cf, dbID, sr.rewriteTableInfo)
	if err != nil || !needWrite {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
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
		return sr.rewriteKVEntryForDB(e, cf)
	} else if meta.IsDBkey(rawKey.Key) && meta.IsTableKey(rawKey.Field) {
		return sr.rewriteKVEntryForTable(e, cf)
	} else {
		return nil, nil
	}
}
