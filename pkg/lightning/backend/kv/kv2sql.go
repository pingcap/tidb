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

package kv

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
)

// TableKVDecoder is a KVDecoder that decodes the key-value pairs of a table.
type TableKVDecoder struct {
	tbl table.Table
	se  *Session
	// tableName is the unique table name in the form "`db`.`tbl`".
	tableName string
	genCols   []GeneratedCol
}

// Name implements KVDecoder.Name.
func (t *TableKVDecoder) Name() string {
	return t.tableName
}

// DecodeHandleFromRowKey implements KVDecoder.DecodeHandleFromRowKey.
func (*TableKVDecoder) DecodeHandleFromRowKey(key []byte) (kv.Handle, error) {
	return tablecodec.DecodeRowKey(key)
}

// DecodeHandleFromIndex implements KVDecoder.DecodeHandleFromIndex.
func (t *TableKVDecoder) DecodeHandleFromIndex(indexInfo *model.IndexInfo, key, value []byte) (kv.Handle, error) {
	cols := tables.BuildRowcodecColInfoForIndexColumns(indexInfo, t.tbl.Meta())
	return tablecodec.DecodeIndexHandle(key, value, len(cols))
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func (t *TableKVDecoder) DecodeRawRowData(h kv.Handle, value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	return tables.DecodeRawRowData(t.se, t.tbl.Meta(), h, t.tbl.Cols(), value)
}

// DecodeRawRowDataAsStr decodes raw row data into a string.
func (t *TableKVDecoder) DecodeRawRowDataAsStr(h kv.Handle, value []byte) (res string) {
	row, _, err := t.DecodeRawRowData(h, value)
	if err == nil {
		res, err = types.DatumsToString(row, true)
		if err == nil {
			return
		}
	}
	return fmt.Sprintf("/* ERROR: %s */", err)
}

// IterRawIndexKeys generates the raw index keys corresponding to the raw row,
// and then iterate them using `fn`. The input buffer will be reused.
func (t *TableKVDecoder) IterRawIndexKeys(h kv.Handle, rawRow []byte, fn func([]byte) error) error {
	row, _, err := t.DecodeRawRowData(h, rawRow)
	if err != nil {
		return err
	}
	if len(t.genCols) > 0 {
		for i, col := range t.tbl.Cols() {
			if col.IsGenerated() {
				row[i] = types.GetMinValue(&col.FieldType)
			}
		}
		if _, err := evalGeneratedColumns(t.se, row, t.tbl.Cols(), t.genCols); err != nil {
			return err
		}
	}

	indices := t.tbl.Indices()
	isCommonHandle := t.tbl.Meta().IsCommonHandle

	var buffer []types.Datum
	var indexBuffer []byte
	for _, index := range indices {
		// skip clustered PK
		if index.Meta().Primary && isCommonHandle {
			continue
		}

		indexValues, err := index.FetchValues(row, buffer)
		if err != nil {
			return err
		}
		sc := t.se.Vars.StmtCtx
		iter := index.GenIndexKVIter(sc.ErrCtx(), sc.TimeZone(), indexValues, h, nil)
		for iter.Valid() {
			indexKey, _, _, err := iter.Next(indexBuffer, nil)
			if err != nil {
				return err
			}
			if err := fn(indexKey); err != nil {
				return err
			}
			if len(indexKey) > len(indexBuffer) {
				indexBuffer = indexKey
			}
		}
	}

	return nil
}

// NewTableKVDecoder creates a new TableKVDecoder.
func NewTableKVDecoder(
	tbl table.Table,
	tableName string,
	options *encode.SessionOptions,
	logger log.Logger,
) (*TableKVDecoder, error) {
	se := NewSession(options, logger)
	cols := tbl.Cols()
	// Set CommonAddRecordCtx to session to reuse the slices and BufStore in AddRecord
	recordCtx := tables.NewCommonAddRecordCtx(len(cols))
	tables.SetAddRecordCtx(se, recordCtx)

	genCols, err := CollectGeneratedColumns(se, tbl.Meta(), cols)
	if err != nil {
		return nil, err
	}

	return &TableKVDecoder{
		tbl:       tbl,
		se:        se,
		tableName: tableName,
		genCols:   genCols,
	}, nil
}
