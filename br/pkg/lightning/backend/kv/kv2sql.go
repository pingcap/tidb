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

package kv

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

type TableKVDecoder struct {
	tbl table.Table
	se  *session
}

func (t *TableKVDecoder) DecodeHandleFromTable(key []byte) (kv.Handle, error) {
	return tablecodec.DecodeRowKey(key)
}

func (t *TableKVDecoder) EncodeHandleKey(h kv.Handle) kv.Key {
	return tablecodec.EncodeRowKeyWithHandle(t.tbl.Meta().ID, h)
}

func (t *TableKVDecoder) DecodeHandleFromIndex(indexInfo *model.IndexInfo, key []byte, value []byte) (kv.Handle, error) {
	cols := tables.BuildRowcodecColInfoForIndexColumns(indexInfo, t.tbl.Meta())
	return tablecodec.DecodeIndexHandle(key, value, len(cols))
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func (t *TableKVDecoder) DecodeRawRowData(h kv.Handle, value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	return tables.DecodeRawRowData(t.se, t.tbl.Meta(), h, t.tbl.Cols(), value)
}

func NewTableKVDecoder(tbl table.Table, options *SessionOptions) (*TableKVDecoder, error) {
	metric.KvEncoderCounter.WithLabelValues("open").Inc()
	se := newSession(options)
	cols := tbl.Cols()
	// Set CommonAddRecordCtx to session to reuse the slices and BufStore in AddRecord
	recordCtx := tables.NewCommonAddRecordCtx(len(cols))
	tables.SetAddRecordCtx(se, recordCtx)

	return &TableKVDecoder{
		tbl: tbl,
		se:  se,
	}, nil
}
