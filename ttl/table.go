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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

func IsTTLTable(tbl *model.TableInfo) bool {
	for _, col := range tbl.Cols() {
		if col.Name.L == "expire" {
			return true
		}
	}
	return false
}

// PhysicalTable is used to provide some information for a physical table in TTL job
type PhysicalTable struct {
	ID     int64
	Schema model.CIStr
	*model.TableInfo
	// PartitionDef is the partition definition
	PartitionDef *model.PartitionDefinition
	// KeyColumns is the cluster index key columns for the table
	KeyColumns    []*model.ColumnInfo
	KeyFieldTypes []*types.FieldType
	// TimeColum is the time column used for TTL
	TimeColumn *model.ColumnInfo
}

func NewPhysicalTable(schema model.CIStr, tbl *model.TableInfo, par *model.PartitionDefinition) (*PhysicalTable, error) {
	t := &PhysicalTable{ID: tbl.ID, Schema: schema, PartitionDef: par}
	if par != nil {
		t.ID = par.ID
	}

	for _, col := range tbl.Cols() {
		if col.Name.L == "expire" {
			t.TimeColumn = col
		}
	}

	if t.TimeColumn == nil {
		return nil, errors.New("no time column")
	}

	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				t.KeyColumns = []*model.ColumnInfo{tbl.Columns[i]}
				t.KeyFieldTypes = []*types.FieldType{&tbl.Columns[i].FieldType}
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
		t.KeyColumns = columns
		t.KeyFieldTypes = fieldTypes
	} else {
		t.KeyColumns = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
		t.KeyFieldTypes = []*types.FieldType{&t.KeyColumns[0].FieldType}
	}
	return t, nil
}

// ValidateKey validates a key
func (t *PhysicalTable) ValidateKey(key []types.Datum) error {
	if len(t.KeyColumns) != len(key) {
		return errors.Errorf("invalid key length: %d, expected %d", len(key), len(t.KeyColumns))
	}
	return nil
}
