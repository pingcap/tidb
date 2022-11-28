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
	"github.com/pingcap/tidb/infoschema"
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
	t := &PhysicalTable{ID: tbl.ID, Schema: schema, TableInfo: tbl, PartitionDef: par}
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

type InfoSchemaTables struct {
	schemaVer int64
	tables    map[int64]*PhysicalTable
}

func NewInfoSchemaTables() *InfoSchemaTables {
	return &InfoSchemaTables{
		tables: make(map[int64]*PhysicalTable, 64),
	}
}

func (t *InfoSchemaTables) Foreach(fn func(t *PhysicalTable) bool) {
	for _, tbl := range t.tables {
		if !fn(tbl) {
			break
		}
	}
}

func (t *InfoSchemaTables) GetByID(id int64) (tbl *PhysicalTable, ok bool) {
	tbl, ok = t.tables[id]
	return
}

func (t *InfoSchemaTables) Update(is infoschema.InfoSchema) error {
	if is == nil {
		return errors.New("Cannot update tables with nil information schema")
	}

	if t.schemaVer == is.SchemaMetaVersion() {
		return nil
	}

	newTables := make(map[int64]*PhysicalTable, len(t.tables))
	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if !IsTTLTable(tblInfo) {
				continue
			}

			if tblInfo.Partition == nil {
				ttlTable, err := t.newTable(db.Name, tblInfo, nil)
				if err != nil {
					return err
				}
				newTables[ttlTable.ID] = ttlTable
				continue
			}

			for _, par := range tblInfo.Partition.Definitions {
				ttlTable, err := t.newTable(db.Name, tblInfo, &par)
				if err != nil {
					return err
				}
				newTables[ttlTable.ID] = ttlTable
				continue
			}
		}
	}

	t.schemaVer = is.SchemaMetaVersion()
	t.tables = newTables
	return nil
}

func (t *InfoSchemaTables) newTable(schema model.CIStr, tblInfo *model.TableInfo, par *model.PartitionDefinition) (*PhysicalTable, error) {
	id := tblInfo.ID
	if par != nil {
		id = par.ID
	}

	ttlTable, ok := t.tables[id]
	if ok && ttlTable.TableInfo == tblInfo {
		return ttlTable, nil
	}

	return NewPhysicalTable(schema, tblInfo, par)
}
