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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/domainutil"
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
	switch diff.Type {
	case model.ActionCreateSchema:
		return nil, b.applyCreateSchema(m, diff)
	case model.ActionDropSchema:
		tblIDs := b.applyDropSchema(diff.SchemaID)
		return tblIDs, nil
	case model.ActionModifySchemaCharsetAndCollate:
		return nil, b.applyModifySchemaCharsetAndCollate(m, diff)
	case model.ActionAlterTableAlterPartition:
		// there is no need to udpate table schema
		return nil, b.applyPlacementUpdate(placement.GroupID(diff.TableID))
	}
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	var oldTableID, newTableID int64
	switch diff.Type {
	case model.ActionCreateTable, model.ActionCreateSequence, model.ActionRecoverTable:
		newTableID = diff.TableID
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		oldTableID = diff.TableID
	case model.ActionTruncateTable, model.ActionCreateView, model.ActionExchangeTablePartition:
		oldTableID = diff.OldTableID
		newTableID = diff.TableID
	default:
		oldTableID = diff.TableID
		newTableID = diff.TableID
	}
	// handle placement rule cache
	switch diff.Type {
	case model.ActionDropTable:
		b.applyPlacementDelete(placement.GroupID(oldTableID))
	case model.ActionTruncateTable:
		b.applyPlacementDelete(placement.GroupID(oldTableID))
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	case model.ActionRecoverTable:
		if err := b.applyPlacementUpdate(placement.GroupID(newTableID)); err != nil {
			return nil, errors.Trace(err)
		}
	}
	dbInfo := b.copySchemaTables(roDBInfo.Name.L)
	b.copySortedTables(oldTableID, newTableID)

	tblIDs := make([]int64, 0, 2)
	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var allocs autoid.Allocators
	if tableIDIsValid(oldTableID) {
		if oldTableID == newTableID && diff.Type != model.ActionRenameTable &&
			diff.Type != model.ActionExchangeTablePartition &&
			// For repairing table in TiDB cluster, given 2 normal node and 1 repair node.
			// For normal node's information schema, repaired table is existed.
			// For repair node's information schema, repaired table is filtered (couldn't find it in `is`).
			// So here skip to reserve the allocators when repairing table.
			diff.Type != model.ActionRepairTable &&
			// Alter sequence will change the sequence info in the allocator, so the old allocator is not valid any more.
			diff.Type != model.ActionAlterSequence {
			oldAllocs, _ := b.is.AllocByID(oldTableID)
			allocs = filterAllocators(diff, oldAllocs)
		}

		tmpIDs := tblIDs
		if (diff.Type == model.ActionRenameTable || diff.Type == model.ActionRenameTables) && diff.OldSchemaID != diff.SchemaID {
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				return nil, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			oldDBInfo := b.copySchemaTables(oldRoDBInfo.Name.L)
			tmpIDs = b.applyDropTable(oldDBInfo, oldTableID, tmpIDs)
		} else {
			tmpIDs = b.applyDropTable(dbInfo, oldTableID, tmpIDs)
		}

		if oldTableID != newTableID {
			// Update tblIDs only when oldTableID != newTableID because applyCreateTable() also updates tblIDs.
			tblIDs = tmpIDs
		}
	}
	if tableIDIsValid(newTableID) {
		// All types except DropTableOrView.
		var err error
		tblIDs, err = b.applyCreateTable(m, dbInfo, newTableID, allocs, diff.Type, tblIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if diff.AffectedOpts != nil {
		for _, opt := range diff.AffectedOpts {
			// Reduce the impact on DML when executing partition DDL. eg.
			// While session 1 performs the DML operation associated with partition 1,
			// the TRUNCATE operation of session 2 on partition 2 does not cause the operation of session 1 to fail.
			switch diff.Type {
			case model.ActionTruncateTablePartition:
				tblIDs = append(tblIDs, opt.OldTableID)
				b.applyPlacementDelete(placement.GroupID(opt.OldTableID))
				err := b.applyPlacementUpdate(placement.GroupID(opt.TableID))
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			case model.ActionDropTable, model.ActionDropTablePartition:
				b.applyPlacementDelete(placement.GroupID(opt.OldTableID))
				continue
			case model.ActionTruncateTable:
				b.applyPlacementDelete(placement.GroupID(opt.OldTableID))
				err := b.applyPlacementUpdate(placement.GroupID(opt.TableID))
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			case model.ActionRecoverTable:
				err := b.applyPlacementUpdate(placement.GroupID(opt.TableID))
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}
			var err error
			affectedDiff := &model.SchemaDiff{
				Version:     diff.Version,
				Type:        diff.Type,
				SchemaID:    opt.SchemaID,
				TableID:     opt.TableID,
				OldSchemaID: opt.OldSchemaID,
				OldTableID:  opt.OldTableID,
			}
			affectedIDs, err := b.ApplyDiff(m, affectedDiff)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tblIDs = append(tblIDs, affectedIDs...)
		}
	}
	return tblIDs, nil
}

func filterAllocators(diff *model.SchemaDiff, oldAllocs autoid.Allocators) autoid.Allocators {
	var newAllocs autoid.Allocators
	switch diff.Type {
	case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
		// Only drop auto-increment allocator.
		for _, alloc := range oldAllocs {
			if alloc.GetType() == autoid.RowIDAllocType || alloc.GetType() == autoid.AutoIncrementType {
				continue
			}
			newAllocs = append(newAllocs, alloc)
		}
	case model.ActionRebaseAutoRandomBase:
		// Only drop auto-random allocator.
		for _, alloc := range oldAllocs {
			if alloc.GetType() == autoid.AutoRandomType {
				continue
			}
			newAllocs = append(newAllocs, alloc)
		}
	default:
		// Keep all allocators.
		newAllocs = oldAllocs
	}
	return newAllocs
}

func appendAffectedIDs(affected []int64, tblInfo *model.TableInfo) []int64 {
	affected = append(affected, tblInfo.ID)
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			affected = append(affected, def.ID)
		}
	}
	return affected
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

func (b *Builder) applyModifySchemaCharsetAndCollate(m *meta.Meta, diff *model.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.copySchemaTables(di.Name.L)
	newDbInfo.Charset = di.Charset
	newDbInfo.Collate = di.Collate
	return nil
}

func (b *Builder) applyDropSchema(schemaID int64) []int64 {
	di, ok := b.is.SchemaByID(schemaID)
	if !ok {
		return nil
	}
	delete(b.is.schemaMap, di.Name.L)

	// Copy the sortedTables that contain the table we are going to drop.
	tableIDs := make([]int64, 0, len(di.Tables))
	bucketIdxMap := make(map[int]struct{}, len(di.Tables))
	for _, tbl := range di.Tables {
		bucketIdxMap[tableBucketIdx(tbl.ID)] = struct{}{}
		// TODO: If the table ID doesn't exist.
		tableIDs = appendAffectedIDs(tableIDs, tbl)
	}
	for bucketIdx := range bucketIdxMap {
		b.copySortedTablesBucket(bucketIdx)
	}

	di = di.Clone()
	for _, id := range tableIDs {
		b.applyDropTable(di, id, nil)
	}
	return tableIDs
}

func (b *Builder) copySortedTablesBucket(bucketIdx int) {
	oldSortedTables := b.is.sortedTablesBuckets[bucketIdx]
	newSortedTables := make(sortedTables, len(oldSortedTables))
	copy(newSortedTables, oldSortedTables)
	b.is.sortedTablesBuckets[bucketIdx] = newSortedTables
}

func (b *Builder) applyCreateTable(m *meta.Meta, dbInfo *model.DBInfo, tableID int64, allocs autoid.Allocators, tp model.ActionType, affected []int64) ([]int64, error) {
	tblInfo, err := m.GetTable(dbInfo.ID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schema diff, the table may has been dropped already, so we need to fall back to
		// full load.
		return nil, ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Table ID %d)", tableID),
		)
	}

	switch tp {
	case model.ActionDropTablePartition:
	case model.ActionTruncateTablePartition:
	default:
		pi := tblInfo.GetPartitionInfo()
		if pi != nil {
			for _, partition := range pi.Definitions {
				err = b.applyPlacementUpdate(placement.GroupID(partition.ID))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if tp != model.ActionTruncateTablePartition {
		affected = appendAffectedIDs(affected, tblInfo)
	}

	// Failpoint check whether tableInfo should be added to repairInfo.
	// Typically used in repair table test to load mock `bad` tableInfo into repairInfo.
	failpoint.Inject("repairFetchCreateTable", func(val failpoint.Value) {
		if val.(bool) {
			if domainutil.RepairInfo.InRepairMode() && tp != model.ActionRepairTable && domainutil.RepairInfo.CheckAndFetchRepairedTable(dbInfo, tblInfo) {
				failpoint.Return(nil, nil)
			}
		}
	})

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	if len(allocs) == 0 {
		allocs = autoid.NewAllocatorsFromTblInfo(b.handle.store, dbInfo.ID, tblInfo)
	} else {
		switch tp {
		case model.ActionRebaseAutoID, model.ActionModifyTableAutoIdCache:
			newAlloc := autoid.NewAllocator(b.handle.store, dbInfo.ID, tblInfo.IsAutoIncColUnsigned(), autoid.RowIDAllocType)
			allocs = append(allocs, newAlloc)
		case model.ActionRebaseAutoRandomBase:
			newAlloc := autoid.NewAllocator(b.handle.store, dbInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), autoid.AutoRandomType)
			allocs = append(allocs, newAlloc)
		}
	}
	tbl, err := tables.TableFromMeta(allocs, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
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
	return affected, nil
}

// ConvertCharsetCollateToLowerCaseIfNeed convert the charset / collation of table and its columns to lower case,
// if the table's version is prior to TableInfoVersion3.
func ConvertCharsetCollateToLowerCaseIfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion3 {
		return
	}
	tbInfo.Charset = strings.ToLower(tbInfo.Charset)
	tbInfo.Collate = strings.ToLower(tbInfo.Collate)
	for _, col := range tbInfo.Columns {
		col.Charset = strings.ToLower(col.Charset)
		col.Collate = strings.ToLower(col.Collate)
	}
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *model.TableInfo) {
	if tbInfo.Version >= model.TableInfoVersion2 || !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
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

func (b *Builder) applyDropTable(dbInfo *model.DBInfo, tableID int64, affected []int64) []int64 {
	bucketIdx := tableBucketIdx(tableID)
	sortedTbls := b.is.sortedTablesBuckets[bucketIdx]
	idx := sortedTbls.searchTable(tableID)
	if idx == -1 {
		return affected
	}
	if tableNames, ok := b.is.schemaMap[dbInfo.Name.L]; ok {
		tblInfo := sortedTbls[idx].Meta()
		delete(tableNames.tables, tblInfo.Name.L)
		affected = appendAffectedIDs(affected, tblInfo)
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
	return affected
}

func (b *Builder) applyPlacementDelete(id string) {
	delete(b.is.ruleBundleMap, id)
}

func (b *Builder) applyPlacementUpdate(id string) error {
	bundle, err := infosync.GetRuleBundle(nil, id)
	if err != nil {
		return err
	}

	if !bundle.IsEmpty() {
		b.is.ruleBundleMap[id] = bundle
	}
	return nil
}

// InitWithOldInfoSchema initializes an empty new InfoSchema by copies all the data from old InfoSchema.
func (b *Builder) InitWithOldInfoSchema() *Builder {
	oldIS := b.handle.Get().(*infoSchema)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	b.copyBundlesMap(oldIS)
	copy(b.is.sortedTablesBuckets, oldIS.sortedTablesBuckets)
	return b
}

func (b *Builder) copySchemasMap(oldIS *infoSchema) {
	for k, v := range oldIS.schemaMap {
		b.is.schemaMap[k] = v
	}
}

func (b *Builder) copyBundlesMap(oldIS *infoSchema) {
	for k, v := range oldIS.ruleBundleMap {
		b.is.ruleBundleMap[k] = v
	}
}

// copySchemaTables creates a new schemaTables instance when a table in the database has changed.
// It also does modifications on the new one because old schemaTables must be read-only.
// Note: please make sure the dbName is in lowercase.
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

// InitWithDBInfos initializes an empty new InfoSchema with a slice of DBInfo, all placement rules, and schema version.
func (b *Builder) InitWithDBInfos(dbInfos []*model.DBInfo, bundles []*placement.Bundle, schemaVersion int64) (*Builder, error) {
	info := b.is
	info.schemaMetaVersion = schemaVersion
	info.ruleBundleMap = make(map[string]*placement.Bundle, len(bundles))
	for _, bundle := range bundles {
		info.ruleBundleMap[bundle.ID] = bundle
	}

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

	// Sort all tables by `ID`
	for _, v := range info.sortedTablesBuckets {
		sort.Sort(v)
	}
	return b, nil
}

type tableFromMetaFunc func(alloc autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error)

func (b *Builder) createSchemaTablesForDB(di *model.DBInfo, tableFromMeta tableFromMetaFunc) error {
	schTbls := &schemaTables{
		dbInfo: di,
		tables: make(map[string]table.Table, len(di.Tables)),
	}
	b.is.schemaMap[di.Name.L] = schTbls

	for _, t := range di.Tables {
		allocs := autoid.NewAllocatorsFromTblInfo(b.handle.store, di.ID, t)
		var tbl table.Table
		tbl, err := tableFromMeta(allocs, t)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Build table `%s`.`%s` schema failed", di.Name.O, t.Name.O))
		}
		schTbls.tables[t.Name.L] = tbl
		sortedTbls := b.is.sortedTablesBuckets[tableBucketIdx(t.ID)]
		b.is.sortedTablesBuckets[tableBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}
	return nil
}

type virtualTableDriver struct {
	*model.DBInfo
	TableFromMeta tableFromMetaFunc
}

var drivers []*virtualTableDriver

// RegisterVirtualTable register virtual tables to the builder.
func RegisterVirtualTable(dbInfo *model.DBInfo, tableFromMeta tableFromMetaFunc) {
	drivers = append(drivers, &virtualTableDriver{dbInfo, tableFromMeta})
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
		ruleBundleMap:       map[string]*placement.Bundle{},
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
