// Copyright 2016 PingCAP, Inc.
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

package infoschema

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
)

// Builder builds a new InfoSchema.
type Builder struct {
	is     *infoSchema
	handle *Handle
}

// ApplyDiff applies SchemaDiff to the new InfoSchema.
func (b *Builder) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) error {
	b.is.schemaMetaVersion = diff.Version
	if diff.Type == model.ActionCreateSchema {
		return b.applyCreateSchema(m, diff)
	} else if diff.Type == model.ActionDropSchema {
		b.applyDropSchema(diff.SchemaID)
		return nil
	}
	roDBInfo, ok := b.is.schemas[diff.SchemaID]
	if !ok {
		return ErrDatabaseNotExists
	}
	var oldTableID, newTableID int64
	switch diff.Type {
	case model.ActionCreateTable:
		newTableID = diff.TableID
	case model.ActionDropTable:
		oldTableID = diff.TableID
	case model.ActionTruncateTable:
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
	default:
		oldTableID = diff.TableID
		newTableID = diff.TableID
	}
	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var alloc autoid.Allocator
	if oldTableID != 0 {
		alloc, _ = b.is.AllocByID(oldTableID)
		b.applyDropTable(roDBInfo.Name.L, oldTableID)
	}
	if newTableID != 0 {
		// All types except DropTable.
		err := b.applyCreateTable(m, roDBInfo, newTableID, alloc)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// The old DBInfo still holds a reference to old table info, we need to update it.
	b.updateDBInfo(roDBInfo, oldTableID, newTableID)
	return nil
}

// updateDBInfo clones a new DBInfo from old DBInfo, and update on the new one.
func (b *Builder) updateDBInfo(roDBInfo *model.DBInfo, oldTableID, newTableID int64) {
	newDbInfo := new(model.DBInfo)
	*newDbInfo = *roDBInfo
	newDbInfo.Tables = make([]*model.TableInfo, 0, len(roDBInfo.Tables))
	if newTableID != 0 {
		// All types except DropTable.
		newTblInfo := b.is.tables[newTableID].Meta()
		newDbInfo.Tables = append(newDbInfo.Tables, newTblInfo)
	}
	for _, tblInfo := range roDBInfo.Tables {
		if tblInfo.ID != oldTableID && tblInfo.ID != newTableID {
			newDbInfo.Tables = append(newDbInfo.Tables, tblInfo)
		}
	}
	b.is.schemas[newDbInfo.ID] = newDbInfo
}

func (b *Builder) applyCreateSchema(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// When we apply an old schema diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists
	}
	b.is.schemas[di.ID] = di
	b.is.schemaNameToID[di.Name.L] = di.ID
	return nil
}

func (b *Builder) applyDropSchema(schemaID int64) {
	di, ok := b.is.schemas[schemaID]
	if !ok {
		return
	}
	delete(b.is.schemas, di.ID)
	delete(b.is.schemaNameToID, di.Name.L)
	for _, tbl := range di.Tables {
		b.applyDropTable(di.Name.L, tbl.ID)
	}
}

func (b *Builder) applyCreateTable(m *meta.Meta, roDBInfo *model.DBInfo, tableID int64, alloc autoid.Allocator) error {
	tblInfo, err := m.GetTable(roDBInfo.ID, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return ErrTableNotExists
	}
	if alloc == nil {
		alloc = autoid.NewAllocator(b.handle.store, roDBInfo.ID)
	}
	tbl, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	b.is.tables[tblInfo.ID] = tbl
	tn := makeTableName(roDBInfo.Name.L, tblInfo.Name.L)
	b.is.tableNameToID[string(tn)] = tblInfo.ID
	return nil
}

func (b *Builder) applyDropTable(schemaName string, tableID int64) {
	tbl, ok := b.is.tables[tableID]
	if !ok {
		return
	}
	tblInfo := tbl.Meta()
	delete(b.is.tables, tblInfo.ID)
	tn := makeTableName(schemaName, tblInfo.Name.L)
	delete(b.is.tableNameToID, string(tn))
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema() *Builder {
	oldIS := b.handle.Get().(*infoSchema)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemaNames(oldIS)
	b.copyTableNames(oldIS)
	b.copySchemas(oldIS)
	b.copyTables(oldIS)
	return b
}

func (b *Builder) copySchemaNames(oldIS *infoSchema) {
	for k, v := range oldIS.schemaNameToID {
		b.is.schemaNameToID[k] = v
	}
}

func (b *Builder) copyTableNames(oldIS *infoSchema) {
	b.is.tableNameToID = make(map[string]int64, len(oldIS.tableNameToID))
	for k, v := range oldIS.tableNameToID {
		b.is.tableNameToID[k] = v
	}
}

func (b *Builder) copySchemas(oldIS *infoSchema) {
	for k, v := range oldIS.schemas {
		b.is.schemas[k] = v
	}
}

func (b *Builder) copyTables(oldIS *infoSchema) {
	b.is.tables = make(map[int64]table.Table, len(oldIS.tables))
	for k, v := range oldIS.tables {
		b.is.tables[k] = v
	}
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, schemaVersion int64) (*Builder, error) {
	err := b.initMemorySchemas()
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, di := range dbInfos {
		info.schemas[di.ID] = di
		info.schemaNameToID[di.Name.L] = di.ID
		for _, t := range di.Tables {
			alloc := autoid.NewAllocator(b.handle.store, di.ID)
			var tbl table.Table
			tbl, err = table.TableFromMeta(alloc, t)
			if err != nil {
				return nil, errors.Trace(err)
			}
			info.tables[t.ID] = tbl
			tname := makeTableName(di.Name.L, t.Name.L)
			info.tableNameToID[string(tname)] = t.ID
		}
	}
	return b, nil
}

func (b *Builder) initMemorySchemas() error {
	info := b.is
	info.schemaNameToID[infoSchemaDB.Name.L] = infoSchemaDB.ID
	info.schemas[infoSchemaDB.ID] = infoSchemaDB
	for _, t := range infoSchemaDB.Tables {
		tbl := b.handle.memSchema.nameToTable[t.Name.L]
		info.tables[t.ID] = tbl
		tname := makeTableName(infoSchemaDB.Name.L, t.Name.L)
		info.tableNameToID[string(tname)] = t.ID
	}

	perfHandle := b.handle.memSchema.perfHandle
	psDB := perfHandle.GetDBMeta()

	info.schemaNameToID[psDB.Name.L] = psDB.ID
	info.schemas[psDB.ID] = psDB
	for _, t := range psDB.Tables {
		tbl, ok := perfHandle.GetTable(t.Name.O)
		if !ok {
			return ErrTableNotExists.Gen("table `%s` is missing.", t.Name)
		}
		info.tables[t.ID] = tbl
		tname := makeTableName(psDB.Name.L, t.Name.L)
		info.tableNameToID[string(tname)] = t.ID
	}
	return nil
}

// Build sets new InfoSchema to the handle in the Builder.
func (b *Builder) Build() error {
	err := b.handle.refillMemoryTables(b.is.AllSchemas())
	if err != nil {
		return errors.Trace(err)
	}
	b.handle.value.Store(b.is)
	return nil
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(handle *Handle) *Builder {
	b := new(Builder)
	b.handle = handle
	b.is = &infoSchema{
		schemaNameToID: map[string]int64{},
		tableNameToID:  map[string]int64{},
		schemas:        map[int64]*model.DBInfo{},
		tables:         map[int64]table.Table{},
	}
	return b
}
