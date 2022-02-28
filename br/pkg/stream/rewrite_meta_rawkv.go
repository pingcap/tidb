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

type SchemasInfo struct {
	DbInfo *model.DBInfo
	Tables map[string]*model.TableInfo // tableName -> *model.TableInfo
}

// SchemasReplace specifies schemas infomation mapping old schemas to new schemas.
type SchemasReplace struct {
	OldDBs     map[int64]*model.DBInfo
	OldTables  map[int64]*model.TableInfo
	NewSchemas map[string]*SchemasInfo
	RewriteTS  uint64
}

// NewSchemasReplace creates a SchemasReplace struct.
func NewSchemasReplace(
	oldDBs map[int64]*model.DBInfo,
	oldTables map[int64]*model.TableInfo,
	NewSchemas map[string]*SchemasInfo,
	restoreTS uint64,
) *SchemasReplace {
	return &SchemasReplace{
		OldDBs:     oldDBs,
		OldTables:  oldTables,
		NewSchemas: NewSchemas,
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

	dbName := sr.OldDBs[dbID].Name.String()
	newDbID := sr.NewSchemas[dbName].DbInfo.ID

	rawMetaKey.UpdateField(meta.DBkey(newDbID))
	return rawMetaKey.EncodeMetaKey(), nil
}

func (sr *SchemasReplace) rewriteValueForDB(value []byte, cf string) ([]byte, error) {
	var (
		newValue []byte
		dbInfo   model.DBInfo
		err      error
	)

	if cf == "default" {
		if err = json.Unmarshal(value, &dbInfo); err != nil {
			return nil, errors.Trace(err)
		}

		dbName := dbInfo.Name.String()
		newDbID := sr.NewSchemas[dbName].DbInfo.ID
		dbInfo.ID = newDbID
		if newValue, err = json.Marshal(&dbInfo); err != nil {
			return nil, err
		}
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, nil
		}

		shortValue := rawWriteCFValue.GetShortValue()
		if err := json.Unmarshal(shortValue, &dbInfo); err != nil {
			return nil, errors.Trace(err)
		}

		dbName := dbInfo.Name.String()
		newDbID := sr.NewSchemas[dbName].DbInfo.ID
		dbInfo.ID = newDbID
		if shortValue, err = json.Marshal(&dbInfo); err != nil {
			return nil, err
		}
		rawWriteCFValue.UpdateShortValue(shortValue)
		newValue = rawWriteCFValue.EncodeTo()
	} else {
		panic(fmt.Sprintf("not support cf:%s", cf))
	}

	return newValue, nil
}

func (sr *SchemasReplace) rewriteKVEntryForDB(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, err := sr.rewriteKeyForDB(e.Key, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, err := sr.rewriteValueForDB(e.Value, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func (sr *SchemasReplace) rewriteKeyForTable(key []byte, cf string) ([]byte, string, error) {
	rawMetakey, err := ParseTxnMetaKeyFrom(key)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetakey.Key)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	tableID, err := meta.ParseTableKey(rawMetakey.Field)
	if err != nil {
		log.Info("parse table key failed", zap.ByteString("field", rawMetakey.Field))
		return nil, "", errors.Trace(err)
	}

	dbName := sr.OldDBs[dbID].Name.String()
	tableName := sr.OldTables[tableID].Name.String()
	newDbID := sr.NewSchemas[dbName].DbInfo.ID
	newTableID := sr.NewSchemas[dbName].Tables[tableName].ID

	rawMetakey.UpdateKey(meta.DBkey(newDbID))
	rawMetakey.UpdateField(meta.TableKey(newTableID))
	if cf == "write" {
		rawMetakey.UpdateTS(sr.RewriteTS)
	}
	return rawMetakey.EncodeMetaKey(), dbName, nil
}

func (sr *SchemasReplace) rewriteValueForTable(value []byte, dbName string, cf string) ([]byte, error) {
	var (
		tableInfo model.TableInfo
		newValue  []byte
		err       error
	)

	if cf == "default" {
		if err = json.Unmarshal(value, &tableInfo); err != nil {
			return nil, errors.Trace(err)
		}

		tableName := tableInfo.Name.String()
		newTableID := sr.NewSchemas[dbName].Tables[tableName].ID
		log.Debug("rewrite value", zap.String("tableName", dbName+"."+tableName),
			zap.Int64("oldTableID", tableInfo.ID), zap.Int64("newTableID", newTableID))

		tableInfo.ID = newTableID
		if newValue, err = json.Marshal(&tableInfo); err != nil {
			return nil, errors.Trace(err)
		}
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err = rawWriteCFValue.ParseFrom(value); err != nil {
			return nil, errors.Trace(err)
		}

		if !rawWriteCFValue.HasShortValue() {
			return value, nil
		}

		shortValue := rawWriteCFValue.GetShortValue()
		if err = json.Unmarshal(shortValue, &tableInfo); err != nil {
			return nil, errors.Trace(err)
		}

		tableName := tableInfo.Name.String()
		newTableID := sr.NewSchemas[dbName].Tables[tableName].ID
		tableInfo.ID = newTableID
		if shortValue, err = json.Marshal(&tableInfo); err != nil {
			return nil, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		newValue = rawWriteCFValue.EncodeTo()
	} else {
		panic(fmt.Sprintf("not support cf:%s", cf))
	}

	return newValue, nil
}

func (sr *SchemasReplace) rewriteKVEntryForTable(e *kv.Entry, cf string) (*kv.Entry, error) {
	newKey, dbName, err := sr.rewriteKeyForTable(e.Key, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, err := sr.rewriteValueForTable(e.Value, dbName, cf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
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
