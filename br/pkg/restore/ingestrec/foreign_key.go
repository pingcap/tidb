// Copyright 2025 PingCAP, Inc.
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

package ingestrec

import (
	"context"
	"maps"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type ForeignKeyRecordKey struct {
	ChildSchemaNameO string
	ChildTableNameO  string
	FKNameO          string
}

type ForeignKeyRecord struct {
	ID                int64
	ChildSchemaNameO  string
	ChildTableNameO   string
	ParentSchemaNameO string
	ParentTableNameO  string
	FKNameO           string
	ChildCols         []ast.CIStr
	ParentCols        []ast.CIStr
	OnDelete          int
	OnUpdate          int
}

func newForeignKeyRecordKey(
	childSchemaNameO string,
	childTableNameO string,
	fk *model.FKInfo,
) (ForeignKeyRecordKey, *ForeignKeyRecord) {
	return ForeignKeyRecordKey{
			ChildSchemaNameO: childSchemaNameO,
			ChildTableNameO:  childTableNameO,
			FKNameO:          fk.Name.O,
		}, &ForeignKeyRecord{
			ID:                fk.ID,
			ChildSchemaNameO:  childSchemaNameO,
			ChildTableNameO:   childTableNameO,
			ParentSchemaNameO: fk.RefSchema.O,
			ParentTableNameO:  fk.RefTable.O,
			FKNameO:           fk.Name.O,
			ChildCols:         fk.Cols,
			ParentCols:        fk.RefCols,
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		}
}

type ForeignKeyRecordManager struct {
	fkRecordMap map[ForeignKeyRecordKey]*ForeignKeyRecord
}

func NewForeignKeyRecordManager() *ForeignKeyRecordManager {
	return &ForeignKeyRecordManager{
		fkRecordMap: make(map[ForeignKeyRecordKey]*ForeignKeyRecord),
	}
}

type TableForeignKeyRecordManager struct {
	fkRecordMap         map[ForeignKeyRecordKey]*ForeignKeyRecord
	referredFKRecordMap map[ForeignKeyRecordKey]*ForeignKeyRecord
}

func (m *ForeignKeyRecordManager) Merge(tm *TableForeignKeyRecordManager) {
	maps.Copy(m.fkRecordMap, tm.fkRecordMap)
	maps.Copy(m.fkRecordMap, tm.referredFKRecordMap)
}

func NewForeignKeyRecordManagerForTables(
	ctx context.Context,
	infoSchema infoschema.InfoSchema,
	dbName ast.CIStr,
	tableInfo *model.TableInfo,
) (*TableForeignKeyRecordManager, error) {
	tm := &TableForeignKeyRecordManager{
		fkRecordMap:         make(map[ForeignKeyRecordKey]*ForeignKeyRecord),
		referredFKRecordMap: make(map[ForeignKeyRecordKey]*ForeignKeyRecord),
	}
	tableFKs := tableInfo.ForeignKeys
	tableReferredFKs := infoSchema.GetTableReferredForeignKeys(dbName.L, tableInfo.Name.L)
	for _, tableFK := range tableFKs {
		if tableInfo.PKIsHandle && len(tableFK.Cols) == 1 {
			refColInfo := model.FindColumnInfo(tableInfo.Columns, tableFK.Cols[0].L)
			if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
				continue
			}
		}
		key, value := newForeignKeyRecordKey(dbName.O, tableInfo.Name.O, tableFK)
		tm.fkRecordMap[key] = value
	}
	for _, tableReferredFK := range tableReferredFKs {
		if tableInfo.PKIsHandle && len(tableReferredFK.Cols) == 1 {
			refColInfo := model.FindColumnInfo(tableInfo.Columns, tableReferredFK.Cols[0].L)
			if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
				continue
			}
		}
		childTableInfo, err := infoSchema.TableByName(ctx, tableReferredFK.ChildSchema, tableReferredFK.ChildTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, tableFK := range childTableInfo.Meta().ForeignKeys {
			if tableReferredFK.ChildFKName.O == tableFK.Name.O {
				key, value := newForeignKeyRecordKey(tableReferredFK.ChildSchema.O, tableReferredFK.ChildTable.O, tableFK)
				tm.referredFKRecordMap[key] = value
				break
			}
		}
	}
	return tm, nil
}

func (tm *TableForeignKeyRecordManager) RemoveForeignKeys(tableInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	for key, fkRecord := range tm.fkRecordMap {
		if model.IsIndexPrefixCovered(tableInfo, indexInfo, fkRecord.ChildCols...) {
			delete(tm.fkRecordMap, key)
		}
	}
	for key, fkRecord := range tm.referredFKRecordMap {
		if model.IsIndexPrefixCovered(tableInfo, indexInfo, fkRecord.ParentCols...) {
			delete(tm.referredFKRecordMap, key)
		}
	}
}
