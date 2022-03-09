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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

// SchemasReplace specifies schemas information mapping old schemas to new schemas.

type OldID = int64
type NewID = int64

type TableReplace struct {
	TableID  NewID
	IndexMap map[OldID]NewID
}

type SchemasReplace struct {
	DbMap      map[OldID]NewID
	TableIDMap map[OldID]TableReplace
	RewriteTS  uint64
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	dbMap map[int64]int64,
	tblMap map[OldID]TableReplace,
	restoreTS uint64,
) *SchemasReplace {
	return &SchemasReplace{
		DbMap:      dbMap,
		TableIDMap: tblMap,
		RewriteTS:  restoreTS,
	}
}

func (sr *SchemasReplace) rewriteKeyForDB(key []byte, cf string) ([]byte, error) {
	rawMetaKey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newDbID, exist := sr.DbMap[dbID]
	if !exist {
		// It represents new created database if not found in map.
		return nil, nil
	}

	rawMetaKey.UpdateField(meta.DBkey(newDbID))
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteDB(value []byte) ([]byte, error) {
	dbInfo := model.DBInfo{}

	if err := json.Unmarshal(value, &dbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	newID, exist := sr.DbMap[dbInfo.ID]
	if !exist {
		return nil, nil
	}

	dbInfo.ID = newID
	newValue, err := json.Marshal(&dbInfo)
	if err != nil {
		return nil, err
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteKVEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil {
		return nil, errors.Trace(err)
	} else if newKey == nil {
		return nil, nil
	}

	newValue, err := sr.rewriteValue(e.Value, cf, sr.rewriteDB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKeyForTable(key []byte, cf string) ([]byte, error) {
	rawMetakey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetakey.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableID, err := meta.ParseTableKey(rawMetakey.Field)
	if err != nil {
		log.Info("parse table key failed", zap.ByteString("field", rawMetakey.Field))
		return nil, errors.Trace(err)
	}

	newDbID, exist := sr.DbMap[dbID]
	if !exist {
		// It represents new created database if not exist in map.
		return nil, nil
	}

	tableReplace, exist := sr.TableIDMap[tableID]
	if !exist {
		// It represents new created table if not exist in map.
		return nil, nil
	}

	rawMetakey.UpdateKey(meta.DBkey(newDbID))
	rawMetakey.UpdateField(meta.TableKey(tableReplace.TableID))
	if cf == "write" {
		rawMetakey.UpdateTS(sr.RewriteTS)
	}
	return rawMetakey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteTable(value []byte) ([]byte, error) {
	var tableInfo model.TableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// update table ID
	tableReplace, exist := sr.TableIDMap[tableInfo.ID]
	if !exist {
		return nil, nil
	}
	tableInfo.ID = tableReplace.TableID

	// update index ID
	for _, index := range tableInfo.Indices {
		newID, exist := tableReplace.IndexMap[index.ID]
		if !exist {
			continue
		}

		log.Info("update index id",
			zap.String("table-name", tableInfo.Name.String()),
			zap.Int64("old-index-id", index.ID), zap.Int64("new-index-id", newID))

		index.ID = newID
	}

	// update partition table ID
	partitions := tableInfo.GetPartitionInfo()
	if partitions != nil {
		for i, tbl := range partitions.Definitions {
			tableReplace, exist := sr.TableIDMap[tbl.ID]
			if !exist {
				continue
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
		return nil, errors.Trace(err)
	}
	return newValue, nil
}

func (sr *SchemasReplace) rewriteKVEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForTable(e.Key, cf)
	if err != nil {
		return nil, errors.Trace(err)
	} else if newKey == nil {
		return nil, nil
	}

	newValue, err := sr.rewriteValue(e.Value, cf, sr.rewriteTable)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteValue(
	value []byte,
	cf string,
	cbRewrite func([]byte) ([]byte, error),
) ([]byte, error) {
	if cf == "default" {
		return cbRewrite(value)
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, nil
		}

		shortValue, err := cbRewrite(rawWriteCFValue.GetShortValue())
		if err != nil {
			return nil, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		return rawWriteCFValue.EncodeTo(), nil
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
