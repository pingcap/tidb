// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

type rowKey []types.Datum

type scanQueryGenerator struct {
	tbl       *ttlTable
	scanRange []rowKey
	expire    time.Time
}

func newScanQueryGenerator(tbl *ttlTable, expire time.Time, scanRange []rowKey) *scanQueryGenerator {
	return &scanQueryGenerator{
		tbl:       tbl,
		scanRange: scanRange,
		expire:    expire,
	}
}

func (g *scanQueryGenerator) NextQuery(limit int, continueFrom rowKey, exhausted bool) (string, error) {
	if exhausted {
		return "", nil
	}

	b := newSQLBuilder(g.tbl)
	if err := b.WriteSelect(); err != nil {
		return "", err
	}

	left := continueFrom
	if left == nil && len(g.scanRange) > 0 {
		left = g.scanRange[0]
	}

	if left != nil {
		if err := b.WriteCommonCondition(g.tbl.KeyColumns, ">", continueFrom); err != nil {
			return "", err
		}
	}

	if len(g.scanRange) > 1 {
		if err := b.WriteCommonCondition(g.tbl.KeyColumns, "<=", g.scanRange[1]); err != nil {
			return "", err
		}
	}

	if err := b.WriteExpireCondition(g.expire); err != nil {
		return "", err
	}

	if err := b.WriteOrderBy(g.tbl.KeyColumns, false); err != nil {
		return "", err
	}

	if err := b.WriteLimit(limit); err != nil {
		return "", err
	}

	return b.Build()
}

type ttlTable struct {
	*model.TableInfo
	Par           *model.PartitionDefinition
	Schema        model.CIStr
	KeyColumns    []*model.ColumnInfo
	KeyFieldTypes []*types.FieldType
	TimeColumn    *model.ColumnInfo
}

func isTTLTable(tbl *model.TableInfo) bool {
	for _, column := range tbl.Columns {
		if column.Name.L == "expire" {
			return true
		}
	}

	return false
}

func newTTLTable(schema model.CIStr, tbl *model.TableInfo, par *model.PartitionDefinition) (*ttlTable, error) {
	if !isTTLTable(tbl) {
		return nil, errors.Errorf("table '%s.%s' is not a TTL table", schema, tbl.Name)
	}

	ttlTbl := &ttlTable{
		TableInfo: tbl,
		Par:       par,
		Schema:    schema,
	}

	for _, column := range tbl.Columns {
		if column.Name.L == "expire" {
			ttlTbl.TimeColumn = column
			break
		}
	}

	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				ttlTbl.KeyColumns = []*model.ColumnInfo{tbl.Columns[i]}
				ttlTbl.KeyFieldTypes = []*types.FieldType{&tbl.Columns[i].FieldType}
			}
		}
	} else if tbl.IsCommonHandle {
		idxInfo := tables.FindPrimaryIndex(tbl)
		columns := make([]*model.ColumnInfo, len(idxInfo.Columns))
		fieldTypes := make([]*types.FieldType, len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			columns[i] = tbl.Columns[idxCol.Offset]
			fieldTypes[i] = &tbl.Columns[idxCol.Offset].FieldType
		}
		ttlTbl.KeyColumns = columns
		ttlTbl.KeyFieldTypes = fieldTypes
	} else {
		ttlTbl.KeyColumns = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
		ttlTbl.KeyFieldTypes = []*types.FieldType{&ttlTbl.KeyColumns[0].FieldType}
	}
	return ttlTbl, nil
}

func (t *ttlTable) GetPhysicalTableID() int64 {
	if t.Par != nil {
		return t.Par.ID
	}
	return t.ID
}

func (t *ttlTable) FormatDeleteQuery(keys []rowKey, expire time.Time) (string, error) {
	b := newSQLBuilder(t)

	if err := b.WriteDelete(); err != nil {
		return "", err
	}

	if err := b.WriteInCondition(t.KeyColumns, keys); err != nil {
		return "", err
	}

	if err := b.WriteExpireCondition(expire); err != nil {
		return "", err
	}

	return b.Build()
}
