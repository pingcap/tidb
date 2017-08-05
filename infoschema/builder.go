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
	"fmt"
	"sort"

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
// Return the detal updated table IDs that are produced from SchemaDiff and an error.
func (b *Builder) ApplyDiff(m *meta.Meta, diff *model.SchemaDiff) ([]int64, error) {
	b.is.schemaMetaVersion = diff.Version
	if diff.Type == model.ActionCreateSchema {
		return nil, b.applyCreateSchema(m, diff)
	} else if diff.Type == model.ActionDropSchema {
		tblIDs := b.applyDropSchema(diff.SchemaID)
		return tblIDs, nil
	}

	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	var oldTableID, newTableID int64
	tblIDs := make([]int64, 0, 2)
	switch diff.Type {
	case model.ActionCreateTable:
		newTableID = diff.TableID
		tblIDs = append(tblIDs, newTableID)
	case model.ActionDropTable:
		oldTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID)
	case model.ActionTruncateTable:
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID, newTableID)
	default:
		oldTableID = diff.TableID
		newTableID = diff.TableID
		tblIDs = append(tblIDs, oldTableID)
	}
	b.copySchemaTables(roDBInfo.Name.L)
	b.copySortedTables(oldTableID, newTableID)

	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var alloc autoid.Allocator
	if tableIDIsValid(oldTableID) {
		if oldTableID == newTableID {
			alloc, _ = b.is.AllocByID(oldTableID)
		}
		if diff.Type == model.ActionRenameTable {
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				return nil, ErrDatabaseNotExists.GenByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			b.applyDropTable(oldRoDBInfo, oldTableID)
		} else {
			b.applyDropTable(roDBInfo, oldTableID)
		}
	}
	if tableIDIsValid(newTableID) {
		// All types except DropTable.
		err := b.applyCreateTable(m, roDBInfo, newTableID, alloc)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

// copySortedTables copies sortedTables for old table and new table for later modification.
func (b *Builder) copySortedTables(oldTableID, newTableID int64) {
	buckets := b.is.sortedTablesBuckets
	if tableIDIsValid(oldTableID) {
		bucketIdx := tableBucketIdx(oldTableID)
		oldSortedTables := buckets[bucketIdx]
		newSortedTables := make(sortedTables, len(oldSortedTables))
		copy(newSortedTables, oldSortedTables)
		buckets[bucketIdx] = newSortedTables
	}
	if tableIDIsValid(newTableID) && newTableID != oldTableID {
		oldSortedTables := buckets[tableBucketIdx(newTableID)]
		newSortedTables := make(sortedTables, len(oldSortedTables), len(oldSortedTables)+1)
		copy(newSortedTables, oldSortedTables)
		buckets[tableBucketIdx(newTableID)] = newSortedTables
	}
}

func (b *Builder) applyCreateSchema(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// When we apply an old schema diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists.GenByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	b.is.schemaMap[di.Name.L] = &schemaTables{dbInfo: di, tables: make(map[string]table.Table)}
	return nil
}

func (b *Builder) applyDropSchema(schemaID int64) []int64 {
	di, ok := b.is.SchemaByID(schemaID)
	if !ok {
		return nil
	}
	delete(b.is.schemaMap, di.Name.L)
	ids := make([]int64, 0, len(di.Tables))
	for _, tbl := range di.Tables {
		b.applyDropTable(di, tbl.ID)
		// TODO: If the table ID doesn't exist.
		ids = append(ids, tbl.ID)
	}
	return ids
}

func (b *Builder) applyCreateTable(m *meta.Meta, roDBInfo *model.DBInfo, tableID int64, alloc autoid.Allocator) error {
	tblInfo, err := m.GetTable(roDBInfo.ID, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return ErrTableNotExists.GenByArgs(
			fmt.Sprintf("(Schema ID %d)", roDBInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}
	if alloc == nil {
		schemaID := roDBInfo.ID
		if tblInfo.OldSchemaID != 0 {
			schemaID = tblInfo.OldSchemaID
		}
		alloc = autoid.NewAllocator(b.handle.store, schemaID)
	}
	tbl, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tableNames := b.is.schemaMap[roDBInfo.Name.L]
	tableNames.tables[tblInfo.Name.L] = tbl
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	sortedTbls = append(sortedTbls, tbl)
	sort.Sort(sortedTbls)
	b.is.sortedTablesBuckets[bucketIdx] = sortedTbls

	newTbl, ok := b.is.TableByID(tableID)
	if ok {
		roDBInfo.Tables = append(roDBInfo.Tables, newTbl.Meta())
	}
	return nil
}

func (b *Builder) applyDropTable(roDBInfo *model.DBInfo, tableID int64) {
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		return
	}
	if tableNames, ok := b.is.schemaMap[roDBInfo.Name.L]; ok {
		delete(tableNames.tables, sortedTbls[idx].Meta().Name.L)
	}
	// Remove the table in sorted table slice.
	b.is.sortedTablesBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// The old DBInfo still holds a reference to old table info, we need to remove it.
	for i, tblInfo := range roDBInfo.Tables {
		if tblInfo.ID == tableID {
			if i == len(roDBInfo.Tables)-1 {
				roDBInfo.Tables = roDBInfo.Tables[:i]
			} else {
				roDBInfo.Tables = append(roDBInfo.Tables[:i], roDBInfo.Tables[i+1:]...)
			}
			break
		}
	}
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema() *Builder {
	oldIS := b.handle.Get().(*infoSchema)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	copy(b.is.sortedTablesBuckets, oldIS.sortedTablesBuckets)
	return b
}

func (b *Builder) copySchemasMap(oldIS *infoSchema) {
	for k, v := range oldIS.schemaMap {
		b.is.schemaMap[k] = v
	}
}

// copySchemaTables creates a new schemaTables instance when a table in the database has changed.
// It also does modifications on the new one because old schemaTables must be read-only.
func (b *Builder) copySchemaTables(dbName string) {
	oldSchemaTables := b.is.schemaMap[dbName]
	newSchemaTables := &schemaTables{
		dbInfo: oldSchemaTables.dbInfo,
		tables: make(map[string]table.Table, len(oldSchemaTables.tables)),
	}
	for k, v := range oldSchemaTables.tables {
		newSchemaTables.tables[k] = v
	}
	b.is.schemaMap[dbName] = newSchemaTables
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, schemaVersion int64) (*Builder, error) {
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, di := range dbInfos {
		err := b.createSchemaTablesForDB(di)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	b.createSchemaTablesForPerfSchemaDB()
	b.createSchemaTablesForInfoSchemaDB()
	for _, v := range info.sortedTablesBuckets {
		sort.Sort(v)
	}
	return b, nil
}

func (b *Builder) createSchemaTablesForDB(di *model.DBInfo) error {
	schTbls := &schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(di.Tables)),
	}
	b.is.schemaMap[di.Name.L] = schTbls
	for _, t := range di.Tables {
		schemaID := di.ID
		if t.OldSchemaID != 0 {
			schemaID = t.OldSchemaID
		}
		alloc := autoid.NewAllocator(b.handle.store, schemaID)
		var tbl table.Table
		tbl, err := tables.TableFromMeta(alloc, t)
		if err != nil {
			return errors.Trace(err)
		}
		schTbls.tables[t.Name.L] = tbl
		sortedTbls := b.is.sortedTablesBuckets[tableBucketIdx(t.ID)]
		b.is.sortedTablesBuckets[tableBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}
	return nil
}

func (b *Builder) createSchemaTablesForPerfSchemaDB() {
	perfHandle := b.handle.perfHandle
	perfSchemaDB := perfHandle.GetDBMeta()
	perfSchemaTblNames := &schemaTables{
		dbInfo: perfSchemaDB,
		tables: make(map[string]table.Table, len(perfSchemaDB.Tables)),
	}
	b.is.schemaMap[perfSchemaDB.Name.L] = perfSchemaTblNames
	for _, t := range perfSchemaDB.Tables {
		tbl, ok := perfHandle.GetTable(t.Name.O)
		if !ok {
			continue
		}
		perfSchemaTblNames.tables[t.Name.L] = tbl
		bucketIdx := tableBucketIdx(t.ID)
		b.is.sortedTablesBuckets[bucketIdx] = append(b.is.sortedTablesBuckets[bucketIdx], tbl)
	}
}

func (b *Builder) createSchemaTablesForInfoSchemaDB() {
	infoSchemaSchemaTables := &schemaTables{
		dbInfo: infoSchemaDB,
		tables: make(map[string]table.Table, len(infoSchemaDB.Tables)),
	}
	b.is.schemaMap[infoSchemaDB.Name.L] = infoSchemaSchemaTables
	for _, t := range infoSchemaDB.Tables {
		tbl := createInfoSchemaTable(b.handle, t)
		infoSchemaSchemaTables.tables[t.Name.L] = tbl
		bucketIdx := tableBucketIdx(t.ID)
		b.is.sortedTablesBuckets[bucketIdx] = append(b.is.sortedTablesBuckets[bucketIdx], tbl)
	}
}

// Build sets new InfoSchema to the handle in the Builder.
func (b *Builder) Build() {
	b.handle.value.Store(b.is)
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(handle *Handle) *Builder {
	b := new(Builder)
	b.handle = handle
	b.is = &infoSchema{
		schemaMap:           map[string]*schemaTables{},
		sortedTablesBuckets: make([]sortedTables, bucketCount),
	}
	return b
}

func tableBucketIdx(tableID int64) int {
	return int(tableID % bucketCount)
}

func tableIDIsValid(tableID int64) bool {
	return tableID != 0
}
