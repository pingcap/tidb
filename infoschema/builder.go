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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
)

// Builder builds a new InfoSchema.
type Builder struct {
	is     *infoSchema
	handle *Handle
}

// ApplyDiff applies SchemaDiff to the new InfoSchema.
// Return the detail updated table IDs that are produced from SchemaDiff and an error.
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
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	var oldTableID, newTableID int64
	tblIDs := make([]int64, 0, 2)
	switch diff.Type {
	case model.ActionCreateTable, model.ActionRestoreTable:
		newTableID = diff.TableID
		tblIDs = append(tblIDs, newTableID)
	case model.ActionDropTable, model.ActionDropView:
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
	dbInfo := b.copySchemaTables(roDBInfo.Name.L)
	b.copySortedTables(oldTableID, newTableID)

	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var alloc autoid.Allocator
	if tableIDIsValid(oldTableID) {
		if oldTableID == newTableID && diff.Type != model.ActionRenameTable && diff.Type != model.ActionRebaseAutoID {
			alloc, _ = b.is.AllocByID(oldTableID)
		}
		if diff.Type == model.ActionRenameTable && diff.OldSchemaID != diff.SchemaID {
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				return nil, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			oldDBInfo := b.copySchemaTables(oldRoDBInfo.Name.L)
			b.applyDropTable(oldDBInfo, oldTableID)
		} else {
			b.applyDropTable(dbInfo, oldTableID)
		}
	}
	if tableIDIsValid(newTableID) {
		// All types except DropTableOrView.
		err := b.applyCreateTable(m, dbInfo, newTableID, alloc)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

// copySortedTables copies sortedTables for old table and new table for later modification.
func (b *Builder) copySortedTables(oldTableID, newTableID int64) {
	if tableIDIsValid(oldTableID) {
		b.copySortedTablesBucket(tableBucketIdx(oldTableID))
	}
	if tableIDIsValid(newTableID) && newTableID != oldTableID {
		b.copySortedTablesBucket(tableBucketIdx(newTableID))
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
		return ErrDatabaseNotExists.GenWithStackByArgs(
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

	// Copy the sortedTables that contain the table we are going to drop.
	bucketIdxMap := make(map[int]struct{})
	for _, tbl := range di.Tables {
		bucketIdxMap[tableBucketIdx(tbl.ID)] = struct{}{}
	}
	for bucketIdx := range bucketIdxMap {
		b.copySortedTablesBucket(bucketIdx)
	}

	ids := make([]int64, 0, len(di.Tables))
	di = di.Clone()
	for _, tbl := range di.Tables {
		b.applyDropTable(di, tbl.ID)
		// TODO: If the table ID doesn't exist.
		ids = append(ids, tbl.ID)
	}
	return ids
}

func (b *Builder) copySortedTablesBucket(bucketIdx int) {
	oldSortedTables := b.is.sortedTablesBuckets[bucketIdx]
	newSortedTables := make(sortedTables, len(oldSortedTables))
	copy(newSortedTables, oldSortedTables)
	b.is.sortedTablesBuckets[bucketIdx] = newSortedTables
}

func (b *Builder) applyCreateTable(m *meta.Meta, dbInfo *model.DBInfo, tableID int64, alloc autoid.Allocator) error {
	tblInfo, err := m.GetTable(dbInfo.ID, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	if alloc == nil {
		schemaID := dbInfo.ID
		alloc = autoid.NewAllocator(b.handle.store, tblInfo.GetDBID(schemaID), tblInfo.IsAutoIncColUnsigned())
	}
	tbl, err := tables.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tableNames := b.is.schemaMap[dbInfo.Name.L]
	tableNames.tables[tblInfo.Name.L] = tbl
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	sortedTbls = append(sortedTbls, tbl)
	sort.Sort(sortedTbls)
	b.is.sortedTablesBuckets[bucketIdx] = sortedTbls

	newTbl, ok := b.is.TableByID(tableID)
	if ok {
		dbInfo.Tables = append(dbInfo.Tables, newTbl.Meta())
	}
	return nil
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *model.TableInfo) {
	if !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 || tbInfo.Version >= model.TableInfoVersion2 {
		return
	}
	if tbInfo.Charset == charset.CharsetUTF8 {
		tbInfo.Charset = charset.CharsetUTF8MB4
		tbInfo.Collate = charset.CollationUTF8MB4
	}
	for _, col := range tbInfo.Columns {
		if col.Version < model.ColumnInfoVersion2 && col.Charset == charset.CharsetUTF8 {
			col.Charset = charset.CharsetUTF8MB4
			col.Collate = charset.CollationUTF8MB4
		}
	}
}

func (b *Builder) applyDropTable(dbInfo *model.DBInfo, tableID int64) {
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		return
	}
	if tableNames, ok := b.is.schemaMap[dbInfo.Name.L]; ok {
		delete(tableNames.tables, sortedTbls[idx].Meta().Name.L)
	}
	// Remove the table in sorted table slice.
	b.is.sortedTablesBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// The old DBInfo still holds a reference to old table info, we need to remove it.
	for i, tblInfo := range dbInfo.Tables {
		if tblInfo.ID == tableID {
			if i == len(dbInfo.Tables)-1 {
				dbInfo.Tables = dbInfo.Tables[:i]
			} else {
				dbInfo.Tables = append(dbInfo.Tables[:i], dbInfo.Tables[i+1:]...)
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
func (b *Builder) copySchemaTables(dbName string) *model.DBInfo {
	oldSchemaTables := b.is.schemaMap[dbName]
	newSchemaTables := &schemaTables{
		dbInfo: oldSchemaTables.dbInfo.Copy(),
		tables: make(map[string]table.Table, len(oldSchemaTables.tables)),
	}
	for k, v := range oldSchemaTables.tables {
		newSchemaTables.tables[k] = v
	}
	b.is.schemaMap[dbName] = newSchemaTables
	return newSchemaTables.dbInfo
}

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, schemaVersion int64) (*Builder, error) {
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, di := range dbInfos {
		err := b.createSchemaTablesForDB(di, tables.TableFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Initialize virtual tables.
	for _, driver := range drivers {
		err := b.createSchemaTablesForDB(driver.DBInfo, driver.TableFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// TODO: Update INFORMATION_SCHEMA schema to use virtual table.
	b.createSchemaTablesForInfoSchemaDB()
	for _, v := range info.sortedTablesBuckets {
		sort.Sort(v)
	}
	return b, nil
}

type tableFromMetaFunc func(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error)

func (b *Builder) createSchemaTablesForDB(di *model.DBInfo, tableFromMeta tableFromMetaFunc) error {
	schTbls := &schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(di.Tables)),
	}
	b.is.schemaMap[di.Name.L] = schTbls
	for _, t := range di.Tables {
		schemaID := di.ID
		alloc := autoid.NewAllocator(b.handle.store, t.GetDBID(schemaID), t.IsAutoIncColUnsigned())
		var tbl table.Table
		tbl, err := tableFromMeta(alloc, t)
		if err != nil {
			return errors.Trace(err)
		}
		schTbls.tables[t.Name.L] = tbl
		sortedTbls := b.is.sortedTablesBuckets[tableBucketIdx(t.ID)]
		b.is.sortedTablesBuckets[tableBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}
	return nil
}

type virtualTableDriver struct {
	*model.DBInfo
	TableFromMeta func(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error)
}

var drivers []*virtualTableDriver

// RegisterVirtualTable register virtual tables to the builder.
func RegisterVirtualTable(dbInfo *model.DBInfo, tableFromMeta tableFromMetaFunc) {
	drivers = append(drivers, &virtualTableDriver{dbInfo, tableFromMeta})
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
