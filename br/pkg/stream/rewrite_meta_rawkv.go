// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package stream

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

type DbInfo struct {
	DbInfo *model.DBInfo
	Tables map[string]*model.TableInfo // tableName -> *model.TableInfo
}

type Schemas struct {
	OldDBs     map[int64]*model.DBInfo
	OldTables  map[int64]*model.TableInfo
	NewSchemas map[string]*DbInfo
}

func RewriteKeyForDB(key []byte, cf string, schemasInfo *Schemas) ([]byte, error) {
	rawMetaKey := new(RawMetaKey)
	if err := rawMetaKey.ParseRawMetaKey(key); err != nil {
		return nil, errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetaKey.Field)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbName := schemasInfo.OldDBs[dbID].Name.String()
	newDbID := schemasInfo.NewSchemas[dbName].DbInfo.ID

	rawMetaKey.UpdateField(meta.DBkey(newDbID))
	return rawMetaKey.EncodeMetaKey(), nil
}

func RewriteValueForDB(value []byte, cf string, schemasInfo *Schemas) ([]byte, error) {
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
		newDbID := schemasInfo.NewSchemas[dbName].DbInfo.ID
		dbInfo.ID = newDbID
		if newValue, err = json.Marshal(&dbInfo); err != nil {
			return nil, err
		}
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err := rawWriteCFValue.ParseWriteCFValue(value); err != nil {
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
		newDbID := schemasInfo.NewSchemas[dbName].DbInfo.ID
		dbInfo.ID = newDbID
		if shortValue, err = json.Marshal(&dbInfo); err != nil {
			return nil, err
		}
		rawWriteCFValue.UpdateShortValue(shortValue)
		newValue = rawWriteCFValue.EncodeWriteCFValue()
	} else {
		panic(fmt.Sprintf("not support cf:%s", cf))
	}

	return newValue, nil
}

func RewriteKVEntryForDB(e *kv.Entry, cf string, schemasInfo *Schemas) (*kv.Entry, error) {
	newKey, err := RewriteKeyForDB(e.Key, cf, schemasInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, err := RewriteValueForDB(e.Value, cf, schemasInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func RewriteKeyForTable(key []byte, cf string, schemasInfo *Schemas) ([]byte, string, error) {
	rawMetakey := new(RawMetaKey)
	if err := rawMetakey.ParseRawMetaKey(key); err != nil {
		return nil, "", errors.Trace(err)
	}

	dbID, err := meta.ParseDBKey(rawMetakey.Key)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	tableID, err := meta.ParseTableKey(rawMetakey.Field)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	dbName := schemasInfo.OldDBs[dbID].Name.String()
	tableName := schemasInfo.OldTables[tableID].Name.String()
	newDbID := schemasInfo.NewSchemas[dbName].DbInfo.ID
	newTableID := schemasInfo.NewSchemas[dbName].Tables[tableName].ID

	rawMetakey.UpdateKey(meta.DBkey(newDbID))
	rawMetakey.UpdateField(meta.TableKey(newTableID))
	return rawMetakey.EncodeMetaKey(), dbName, nil
}

func RewriteValueForTable(value []byte, dbName string, cf string, schemasInfo *Schemas) ([]byte, error) {
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
		newTableID := schemasInfo.NewSchemas[dbName].Tables[tableName].ID
		tableInfo.ID = newTableID
		if newValue, err = json.Marshal(&tableInfo); err != nil {
			return nil, errors.Trace(err)
		}
	} else if cf == "write" {
		rawWriteCFValue := new(RawWriteCFValue)
		if err = rawWriteCFValue.ParseWriteCFValue(value); err != nil {
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
		newTableID := schemasInfo.NewSchemas[dbName].Tables[tableName].ID
		tableInfo.ID = newTableID
		if shortValue, err = json.Marshal(&tableInfo); err != nil {
			return nil, errors.Trace(err)
		}

		rawWriteCFValue.UpdateShortValue(shortValue)
		newValue = rawWriteCFValue.EncodeWriteCFValue()
	} else {
		panic(fmt.Sprintf("not support cf:%s", cf))
	}

	return newValue, nil
}

func RewriteKVEntryForTable(e *kv.Entry, cf string, schemasInfo *Schemas) (*kv.Entry, error) {
	newKey, dbName, err := RewriteKeyForTable(e.Key, cf, schemasInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newValue, err := RewriteValueForTable(e.Value, dbName, cf, schemasInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kv.Entry{Key: newKey, Value: newValue}, nil
}

func RewriteKvEntry(e *kv.Entry, cf string, schemasInfo *Schemas) (*kv.Entry, error) {
	if bytes.HasPrefix(e.Key, []byte("mDBs")) {
		return RewriteKVEntryForDB(e, cf, schemasInfo)
	} else if bytes.HasPrefix(e.Key, []byte("mDB:")) {
		return RewriteKVEntryForTable(e, cf, schemasInfo)
	} else {
		return nil, nil
	}
}
