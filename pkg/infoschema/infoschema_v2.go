// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// tableItem is the btree item sorted by name or by id.
type tableItem struct {
	dbName        ast.CIStr
	dbID          int64
	tableName     ast.CIStr
	tableID       int64
	schemaVersion int64
	tomb          bool
}

type schemaItem struct {
	schemaVersion int64
	dbInfo        *model.DBInfo
	tomb          bool
}

type schemaIDName struct {
	schemaVersion int64
	id            int64
	name          ast.CIStr
	tomb          bool
}

type referredForeignKeyItem struct {
	schemaVersion  int64
	dbName         string
	tableName      string
	referredFKInfo []*model.ReferredFKInfo
	tomb           bool
}

func (si *schemaItem) Name() string {
	return si.dbInfo.Name.L
}

// btreeSet updates the btree.
// Concurrent write is supported, but should be avoided as much as possible.
func btreeSet[T any](ptr *atomic.Pointer[btree.BTreeG[T]], item T) {
	succ := false
	for !succ {
		var t = ptr.Load()
		t2 := t.Clone()
		t2.ReplaceOrInsert(item)
		succ = ptr.CompareAndSwap(t, t2)
		if !succ {
			logutil.BgLogger().Info("infoschema v2 btree concurrently multiple writes detected, this should be rare")
		}
	}
}

// Data is the core data struct of infoschema V2.
type Data struct {
	// For the TableByName API, sorted by {dbName, tableName, schemaVersion} => tableID
	//
	// If the schema version +1 but a specific table does not change, the old record is
	// kept and no new {dbName, tableName, schemaVersion+1} => tableID record been added.
	//
	// It means as long as we can find an item in it, the item is available, even through the
	// schema version maybe smaller than required.
	byName atomic.Pointer[btree.BTreeG[*tableItem]]

	// For the TableByID API, sorted by {tableID, schemaVersion} => dbID
	// To reload model.TableInfo, we need both table ID and database ID for meta kv API.
	// It provides the tableID => databaseID mapping.
	// This mapping MUST be synced with byName.
	byID atomic.Pointer[btree.BTreeG[*tableItem]]

	// For the SchemaByName API, sorted by {dbName, schemaVersion} => model.DBInfo
	// Stores the full data in memory.
	schemaMap atomic.Pointer[btree.BTreeG[schemaItem]]

	// For the SchemaByID API, sorted by {id, schemaVersion}
	// Stores only id, name and schemaVersion in memory.
	schemaID2Name atomic.Pointer[btree.BTreeG[schemaIDName]]

	// referredForeignKeys records all table's ReferredFKInfo.
	referredForeignKeys atomic.Pointer[btree.BTreeG[*referredForeignKeyItem]]

	tableCache *Sieve[tableCacheKey, table.Table]

	// For information_schema/metrics_schema/performance_schema etc
	specials sync.Map

	// pid2tid is used by FindTableInfoByPartitionID, it stores {partitionID, schemaVersion} => table ID
	// Need full data in memory!
	pid2tid atomic.Pointer[btree.BTreeG[partitionItem]]

	// tableInfoResident stores {dbName, tableID, schemaVersion} => model.TableInfo
	// It is part of the model.TableInfo data kept in memory to accelerate the list tables API.
	// We observe the pattern that list table API always come with filter.
	// All model.TableInfo with special attributes are here, currently the special attributes including:
	//     TTLInfo, TiFlashReplica
	// PlacementPolicyRef, Partition might be added later, and also TableLock etc
	tableInfoResident atomic.Pointer[btree.BTreeG[tableInfoItem]]

	// the minimum ts of the recent used infoschema
	recentMinTS atomic.Uint64
}

type tableInfoItem struct {
	dbName        ast.CIStr
	tableID       int64
	schemaVersion int64
	tableInfo     *model.TableInfo
	tomb          bool
}

type partitionItem struct {
	partitionID   int64
	schemaVersion int64
	tableID       int64
	tomb          bool
}

type tableCacheKey struct {
	tableID       int64
	schemaVersion int64
}

const btreeDegree = 16

// NewData creates an infoschema V2 data struct.
func NewData() *Data {
	ret := &Data{
		tableCache: newSieve[tableCacheKey, table.Table](1024 * 1024 * size.MB),
	}
	ret.byID.Store(btree.NewG[*tableItem](btreeDegree, compareByID))
	ret.byName.Store(btree.NewG[*tableItem](btreeDegree, compareByName))
	ret.schemaMap.Store(btree.NewG[schemaItem](btreeDegree, compareSchemaItem))
	ret.schemaID2Name.Store(btree.NewG[schemaIDName](btreeDegree, compareSchemaByID))
	ret.pid2tid.Store(btree.NewG[partitionItem](btreeDegree, comparePartitionItem))
	ret.tableInfoResident.Store(btree.NewG[tableInfoItem](btreeDegree, compareTableInfoItem))
	ret.tableCache.SetStatusHook(newSieveStatusHookImpl())
	ret.referredForeignKeys.Store(btree.NewG[*referredForeignKeyItem](btreeDegree, compareReferredForeignKeyItem))
	return ret
}

// CacheCapacity is exported for testing.
func (isd *Data) CacheCapacity() uint64 {
	return isd.tableCache.Capacity()
}

// SetCacheCapacity sets the cache capacity size in bytes.
func (isd *Data) SetCacheCapacity(capacity uint64) {
	isd.tableCache.SetCapacityAndWaitEvict(capacity)
}

func (isd *Data) add(item tableItem, tbl table.Table) {
	btreeSet(&isd.byID, &item)
	btreeSet(&isd.byName, &item)
	isd.tableCache.Set(tableCacheKey{item.tableID, item.schemaVersion}, tbl)
	ti := tbl.Meta()
	if pi := ti.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			btreeSet(&isd.pid2tid, partitionItem{def.ID, item.schemaVersion, tbl.Meta().ID, false})
		}
	}
	if infoschemacontext.HasSpecialAttributes(ti) {
		btreeSet(&isd.tableInfoResident, tableInfoItem{
			dbName:        item.dbName,
			tableID:       item.tableID,
			schemaVersion: item.schemaVersion,
			tableInfo:     ti,
			tomb:          false})
	}
}

func (isd *Data) addSpecialDB(di *model.DBInfo, tables *schemaTables) {
	isd.specials.LoadOrStore(di.Name.L, tables)
}

func (isd *Data) addDB(schemaVersion int64, dbInfo *model.DBInfo) {
	dbInfo.Deprecated.Tables = nil
	btreeSet(&isd.schemaID2Name, schemaIDName{schemaVersion: schemaVersion, id: dbInfo.ID, name: dbInfo.Name})
	btreeSet(&isd.schemaMap, schemaItem{schemaVersion: schemaVersion, dbInfo: dbInfo})
}

func (isd *Data) remove(item tableItem) {
	item.tomb = true
	btreeSet(&isd.byID, &item)
	btreeSet(&isd.byName, &item)
	btreeSet(&isd.tableInfoResident, tableInfoItem{
		dbName:        item.dbName,
		tableID:       item.tableID,
		schemaVersion: item.schemaVersion,
		tableInfo:     nil,
		tomb:          true})
	isd.tableCache.Remove(tableCacheKey{item.tableID, item.schemaVersion})
}

func (isd *Data) deleteDB(dbInfo *model.DBInfo, schemaVersion int64) {
	item := schemaItem{schemaVersion: schemaVersion, dbInfo: dbInfo, tomb: true}
	btreeSet(&isd.schemaMap, item)
	btreeSet(&isd.schemaID2Name, schemaIDName{schemaVersion: schemaVersion, id: dbInfo.ID, name: dbInfo.Name, tomb: true})
}

type referredForeignKeysHelper struct {
	start           referredForeignKeyItem
	schemaVersion   int64
	referredFKInfos []*model.ReferredFKInfo
}

func (h *referredForeignKeysHelper) onItem(item *referredForeignKeyItem) bool {
	if item.dbName != h.start.dbName || item.tableName != h.start.tableName {
		return false
	}

	if item.schemaVersion <= h.schemaVersion {
		if !item.tomb { // If the item is a tomb record, all the foreign keys are deleted.
			h.referredFKInfos = item.referredFKInfo
		}
		return false
	}
	return true
}

func (isd *Data) getTableReferredForeignKeys(schema, table string, schemaMetaVersion int64) []*model.ReferredFKInfo {
	helper := referredForeignKeysHelper{
		start:           referredForeignKeyItem{dbName: schema, tableName: table, schemaVersion: math.MaxInt64},
		schemaVersion:   schemaMetaVersion,
		referredFKInfos: make([]*model.ReferredFKInfo, 0),
	}
	isd.referredForeignKeys.Load().DescendLessOrEqual(&helper.start, helper.onItem)
	return helper.referredFKInfos
}

// hasForeignKeyReference checks if a specific foreign key reference already exists
func (isd *Data) hasForeignKeyReference(refs []*model.ReferredFKInfo, schema, table, fkName ast.CIStr) bool {
	for _, ref := range refs {
		if ref.ChildSchema.L == schema.L &&
			ref.ChildTable.L == table.L &&
			ref.ChildFKName.L == fkName.L {
			return true
		}
	}
	return false
}

func (isd *Data) addReferredForeignKeys(schema ast.CIStr, tbInfo *model.TableInfo, schemaMetaVersion int64) {
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}

		// Get current foreign key references for the table
		refSchema, refTable := fk.RefSchema.L, fk.RefTable.L
		existingRefs := isd.getTableReferredForeignKeys(fk.RefSchema.L, fk.RefTable.L, schemaMetaVersion)

		// Skip if this specific foreign key reference already exists
		if isd.hasForeignKeyReference(existingRefs, schema, tbInfo.Name, fk.Name) {
			continue
		}

		// Create a new array with existing refs + new reference
		newRefs := make([]*model.ReferredFKInfo, 0, len(existingRefs)+1)
		newRefs = append(newRefs, existingRefs...)
		newRefs = append(newRefs, &model.ReferredFKInfo{
			Cols:        fk.RefCols,
			ChildSchema: schema,
			ChildTable:  tbInfo.Name,
			ChildFKName: fk.Name,
		})
		sort.Slice(newRefs, func(i, j int) bool {
			if newRefs[i].ChildSchema.L != newRefs[j].ChildSchema.L {
				return newRefs[i].ChildSchema.L < newRefs[j].ChildSchema.L
			}
			if newRefs[i].ChildTable.L != newRefs[j].ChildTable.L {
				return newRefs[i].ChildTable.L < newRefs[j].ChildTable.L
			}
			return newRefs[i].ChildFKName.L < newRefs[j].ChildFKName.L
		})
		btreeSet(&isd.referredForeignKeys, &referredForeignKeyItem{
			dbName:         refSchema,
			tableName:      refTable,
			schemaVersion:  schemaMetaVersion,
			referredFKInfo: newRefs,
		})
	}
}

func (isd *Data) deleteReferredForeignKeys(schema ast.CIStr, tbInfo *model.TableInfo, schemaMetaVersion int64) {
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}

		// Get current foreign key references for the table
		refSchema, refTable := fk.RefSchema.L, fk.RefTable.L
		existingRefs := isd.getTableReferredForeignKeys(refSchema, refTable, schemaMetaVersion)

		// Skip if this specific foreign key reference doesn't exist
		if !isd.hasForeignKeyReference(existingRefs, schema, tbInfo.Name, fk.Name) {
			continue
		}

		// Delete the reference
		if len(existingRefs) == 1 {
			// If this is the only reference, mark the whole item as deleted
			btreeSet(&isd.referredForeignKeys, &referredForeignKeyItem{
				dbName:         refSchema,
				tableName:      refTable,
				schemaVersion:  schemaMetaVersion,
				tomb:           true,
				referredFKInfo: nil,
			})
		} else {
			// If there are multiple references, create new array excluding this one
			// clone existingRefs to avoid modifying the original slice
			tmpRefs := append([]*model.ReferredFKInfo(nil), existingRefs...)
			newRefs := slices.DeleteFunc(tmpRefs, func(ref *model.ReferredFKInfo) bool {
				return ref.ChildSchema.L == schema.L &&
					ref.ChildTable.L == tbInfo.Name.L &&
					ref.ChildFKName.L == fk.Name.L
			})

			btreeSet(&isd.referredForeignKeys, &referredForeignKeyItem{
				dbName:         refSchema,
				tableName:      refTable,
				schemaVersion:  schemaMetaVersion,
				tomb:           false,
				referredFKInfo: newRefs,
			})
		}
	}
}

// gcCollectTableItem returns up to maxItems old tableItem versions for GC.
func gcCollectTableItem(bt *btree.BTreeG[*tableItem], cutVer int64, maxItems int) []*tableItem {
	var dels []*tableItem
	var prev *tableItem
	// Example:
	// gcOldVersion to v4
	//	db3 tbl1 v5
	//	db3 tbl1 v4
	//	db3 tbl1 v3    <- delete, because v3 < v4
	//	db2 tbl2 v1    <- keep, need to keep the latest version if all versions are less than v4
	//	db2 tbl2 v0    <- delete, because v0 < v4
	//	db1 tbl3 v4
	//	...
	// So the rule can be simplify to "remove all items whose (version < schemaVersion && previous item is same table)"
	bt.Descend(func(item *tableItem) bool {
		if item.schemaVersion < cutVer &&
			prev != nil &&
			prev.dbName.L == item.dbName.L &&
			prev.tableName.L == item.tableName.L {
			dels = append(dels, item)
			if len(dels) >= maxItems {
				return false
			}
		}
		prev = item
		return true
	})
	return dels
}

// gcCollectReferredForeignKeyItem returns up to maxItems old referredForeignKeyItem versions for GC.
func gcCollectReferredForeignKeyItem(bt *btree.BTreeG[*referredForeignKeyItem], cutVer int64, maxItems int) []*referredForeignKeyItem {
	var dels []*referredForeignKeyItem
	var prev *referredForeignKeyItem
	bt.Descend(func(item *referredForeignKeyItem) bool {
		if item.schemaVersion < cutVer &&
			prev != nil &&
			prev.dbName == item.dbName &&
			prev.tableName == item.tableName {
			dels = append(dels, item)
			if len(dels) >= maxItems {
				return false
			}
		}
		prev = item
		return true
	})
	return dels
}

// gcOldFKVersion performs GC of old referredForeignKeyItem entries up to maxItems.
func (isd *Data) gcOldFKVersion(schemaVersion int64) int {
	rfOld := isd.referredForeignKeys.Load()
	rfNew := rfOld.Clone()
	rfDels := gcCollectReferredForeignKeyItem(rfOld, schemaVersion, 1024)
	for _, fk := range rfDels {
		rfNew.Delete(fk)
	}
	if !isd.referredForeignKeys.CompareAndSwap(rfOld, rfNew) {
		logutil.BgLogger().Info("infoschema v2 GCOldVersion() referredForeignKeys gc conflict")
	}
	return len(rfDels)
}

// GCOldVersion compacts btree nodes by removing items older than schema version.
// exported for testing
func (isd *Data) GCOldVersion(schemaVersion int64) (int, int64) {
	if isd.byName.Load().Len() == 0 {
		return 0, 0
	}

	// collect and remove old tableItems
	dels := gcCollectTableItem(isd.byName.Load(), schemaVersion, 1024)
	byNameOld := isd.byName.Load()
	byNameNew := byNameOld.Clone()
	byIDOld := isd.byID.Load()
	byIDNew := byIDOld.Clone()
	for _, ti := range dels {
		byNameNew.Delete(ti)
		byIDNew.Delete(ti)
	}
	succ1 := isd.byID.CompareAndSwap(byIDOld, byIDNew)
	var succ2 bool
	if succ1 {
		succ2 = isd.byName.CompareAndSwap(byNameOld, byNameNew)
	}
	if !succ1 || !succ2 {
		logutil.BgLogger().Info("infoschema v2 GCOldVersion() writes conflict",
			zap.Bool("byID", succ1), zap.Bool("byName", succ2))
	}

	// collect and remove old referredForeignKeyItems
	_ = isd.gcOldFKVersion(schemaVersion)

	return len(dels), int64(isd.byName.Load().Len())
}

// resetBeforeFullLoad is called before a full recreate operation within builder.InitWithDBInfos().
// TODO: write a generics version to avoid repeated code.
func (isd *Data) resetBeforeFullLoad(schemaVersion int64) {
	resetTableInfoResidentBeforeFullLoad(&isd.tableInfoResident, schemaVersion)

	resetByIDBeforeFullLoad(&isd.byID, schemaVersion)
	resetByNameBeforeFullLoad(&isd.byName, schemaVersion)

	resetSchemaMapBeforeFullLoad(&isd.schemaMap, schemaVersion)
	resetSchemaID2NameBeforeFullLoad(&isd.schemaID2Name, schemaVersion)

	resetPID2TIDBeforeFullLoad(&isd.pid2tid, schemaVersion)
	resetFKBeforeFullLoad(&isd.referredForeignKeys, schemaVersion)
}

func resetByIDBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*tableItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]*tableItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item *tableItem) bool {
			if pivot.tableID == item.tableID {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, &tableItem{
				dbName:        item.dbName,
				dbID:          item.dbID,
				tableName:     item.tableName,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetByNameBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*tableItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]*tableItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item *tableItem) bool {
			if pivot.dbName == item.dbName && pivot.tableName == item.tableName {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, &tableItem{
				dbName:        item.dbName,
				dbID:          item.dbID,
				tableName:     item.tableName,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetTableInfoResidentBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[tableInfoItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]tableInfoItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item tableInfoItem) bool {
		if pivot.dbName == item.dbName && pivot.tableID == item.tableID {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, tableInfoItem{
			dbName:        item.dbName,
			tableID:       item.tableID,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetSchemaMapBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[schemaItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]schemaItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item schemaItem) bool {
		if pivot.Name() == item.Name() {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, schemaItem{
			dbInfo:        item.dbInfo,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetSchemaID2NameBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[schemaIDName]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]schemaIDName, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item schemaIDName) bool {
		if pivot.id == item.id {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, schemaIDName{
			id:            item.id,
			name:          item.name,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetPID2TIDBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[partitionItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]partitionItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item partitionItem) bool {
			if pivot.partitionID == item.partitionID {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, partitionItem{
				partitionID:   item.partitionID,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetFKBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*referredForeignKeyItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]*referredForeignKeyItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item *referredForeignKeyItem) bool {
		if pivot.dbName == item.dbName && pivot.tableName == item.tableName {
			return true
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, &referredForeignKeyItem{
			dbName:         item.dbName,
			tableName:      item.tableName,
			schemaVersion:  schemaVersion,
			tomb:           true,
			referredFKInfo: nil,
		})
	}
}

func compareByID(a, b *tableItem) bool {
	if a.tableID < b.tableID {
		return true
	}
	if a.tableID > b.tableID {
		return false
	}

	return a.schemaVersion < b.schemaVersion
}

func compareByName(a, b *tableItem) bool {
	if a.dbName.L < b.dbName.L {
		return true
	}
	if a.dbName.L > b.dbName.L {
		return false
	}

	if a.tableName.L < b.tableName.L {
		return true
	}
	if a.tableName.L > b.tableName.L {
		return false
	}

	return a.schemaVersion < b.schemaVersion
}

func compareTableInfoItem(a, b tableInfoItem) bool {
	if a.dbName.L < b.dbName.L {
		return true
	}
	if a.dbName.L > b.dbName.L {
		return false
	}

	if a.tableID < b.tableID {
		return true
	}
	if a.tableID > b.tableID {
		return false
	}
	return a.schemaVersion < b.schemaVersion
}

func comparePartitionItem(a, b partitionItem) bool {
	if a.partitionID < b.partitionID {
		return true
	}
	if a.partitionID > b.partitionID {
		return false
	}
	return a.schemaVersion < b.schemaVersion
}

func compareSchemaItem(a, b schemaItem) bool {
	if a.Name() < b.Name() {
		return true
	}
	if a.Name() > b.Name() {
		return false
	}
	return a.schemaVersion < b.schemaVersion
}

func compareSchemaByID(a, b schemaIDName) bool {
	if a.id < b.id {
		return true
	}
	if a.id > b.id {
		return false
	}
	return a.schemaVersion < b.schemaVersion
}

func compareReferredForeignKeyItem(a, b *referredForeignKeyItem) bool {
	if a.dbName != b.dbName {
		return a.dbName < b.dbName
	}
	if a.tableName != b.tableName {
		return a.tableName < b.tableName
	}
	return a.schemaVersion < b.schemaVersion
}

var _ InfoSchema = &infoschemaV2{}

type infoschemaV2 struct {
	*infoSchema // in fact, we only need the infoSchemaMisc inside it, but the builder rely on it.
	factory     func() (pools.Resource, error)
	ts          uint64
	*Data
}

// NewInfoSchemaV2 create infoschemaV2.
func NewInfoSchemaV2(r autoid.Requirement, factory func() (pools.Resource, error), infoData *Data) infoschemaV2 {
	return infoschemaV2{
		infoSchema: newInfoSchema(r),
		Data:       infoData,
		factory:    factory,
	}
}

func search(bt *btree.BTreeG[*tableItem], schemaVersion int64, end tableItem, matchFn func(a, b *tableItem) bool) (*tableItem, bool) {
	var ok bool
	var target *tableItem
	// Iterate through the btree, find the query item whose schema version is the largest one (latest).
	bt.DescendLessOrEqual(&end, func(item *tableItem) bool {
		if !matchFn(&end, item) {
			return false
		}
		if item.schemaVersion > schemaVersion {
			// We're seaching historical snapshot, and this record is newer than us, we can't use it.
			// Skip the record.
			return true
		}
		// schema version of the items should <= query's schema version.
		if !ok { // The first one found.
			ok = true
			target = item
		} else { // The latest one
			if item.schemaVersion > target.schemaVersion {
				target = item
			}
		}
		return true
	})
	if ok && target.tomb {
		// If the item is a tomb record, the table is dropped.
		ok = false
	}
	return target, ok
}

func (is *infoschemaV2) base() *infoSchema {
	return is.infoSchema
}

func (is *infoschemaV2) CloneAndUpdateTS(startTS uint64) *infoschemaV2 {
	tmp := *is
	tmp.ts = startTS
	return &tmp
}

func (is *infoschemaV2) searchTableItemByID(tableID int64) (*tableItem, bool) {
	eq := func(a, b *tableItem) bool { return a.tableID == b.tableID }
	return search(
		is.byID.Load(),
		is.infoSchema.schemaMetaVersion,
		tableItem{tableID: tableID, schemaVersion: math.MaxInt64},
		eq,
	)
}

// TableByID implements the InfoSchema interface.
// As opposed to TableByName, TableByID will not refill cache when schema cache
// miss and whether the available schema cache size is enough or not, unless the caller changes
// the behavior by passing a context use WithRefillOption.
func (is *infoschemaV2) TableByID(ctx context.Context, id int64) (val table.Table, ok bool) {
	if !tableIDIsValid(id) {
		return
	}

	is.keepAlive()
	itm, ok := is.searchTableItemByID(id)
	if !ok {
		return nil, false
	}

	if autoid.IsMemSchemaID(id) {
		if raw, exist := is.Data.specials.Load(itm.dbName.L); exist {
			schTbls := raw.(*schemaTables)
			val, ok = schTbls.tables[itm.tableName.L]
			return
		}
		return nil, false
	}

	forceRefill := false
	if opt := ctx.Value(refillOptionKey); opt != nil {
		forceRefill = opt.(bool)
	}

	// get cache with item key
	key := tableCacheKey{itm.tableID, itm.schemaVersion}
	tbl, found := is.tableCache.Get(key)
	if found && tbl != nil {
		return tbl, true
	}

	// Maybe the table is evicted? need to reload.
	ret, err := is.loadTableInfo(ctx, id, itm.dbID, is.ts, is.infoSchema.schemaMetaVersion)
	if err != nil || ret == nil {
		return nil, false
	}

	if forceRefill {
		is.tableCache.Set(key, ret)
	} else {
		is.refillIfNoEvict(key, ret)
	}
	return ret, true
}

// TableItemByID implements the InfoSchema interface.
// It only contains memory operations, no worries about accessing the storage.
func (is *infoschemaV2) TableItemByID(tableID int64) (TableItem, bool) {
	itm, ok := is.searchTableItemByID(tableID)
	if !ok {
		return TableItem{}, false
	}
	return TableItem{DBName: itm.dbName, TableName: itm.tableName, TableID: itm.tableID}, true
}

// TableItem is exported from tableItem.
type TableItem struct {
	DBName    ast.CIStr
	TableName ast.CIStr
	TableID   int64
}

// IterateAllTableItems is used for special performance optimization.
// Used by executor/infoschema_reader.go to handle reading from INFORMATION_SCHEMA.TABLES.
// If visit return false, stop the iterate process.
// NOTE: the output order is reversed by (dbName, tableName).
func (is *infoschemaV2) IterateAllTableItems(visit func(TableItem) bool) {
	maxv, ok := is.byName.Load().Max()
	if !ok {
		return
	}
	is.iterateAllTableItemsInternal(maxv, visit, nil)
}

// IterateAllTableItemsByDB is used for special performance optimization.
// If visit return false, stop the iterate process.
// NOTE: the output order is reversed by (dbName, tableName).
func (is *infoschemaV2) IterateAllTableItemsByDB(dbName ast.CIStr, visit func(TableItem) bool) {
	first := &tableItem{dbName: dbName, tableName: ast.NewCIStr(strings.Repeat(string([]byte{0xff}), 64)), schemaVersion: math.MaxInt64}
	is.iterateAllTableItemsInternal(first, visit, &dbName)
}

func (is *infoschemaV2) iterateAllTableItemsInternal(first *tableItem, visit func(TableItem) bool, targetDB *ast.CIStr) {
	var pivot *tableItem
	is.byName.Load().DescendLessOrEqual(first, func(item *tableItem) bool {
		if targetDB != nil && item.dbName.L != targetDB.L {
			return false
		}
		if item.schemaVersion > is.schemaMetaVersion {
			// skip MVCC version, those items are not visible to the queried schema version
			return true
		}
		if pivot != nil && pivot.dbName.L == item.dbName.L && pivot.tableName.L == item.tableName.L {
			// skip MVCC version, this db.table has been visited already
			return true
		}
		pivot = item
		if !item.tomb {
			return visit(TableItem{DBName: item.dbName, TableName: item.tableName, TableID: item.tableID})
		}
		return true
	})
}

// TableIsCached checks whether the table is cached.
func (is *infoschemaV2) TableIsCached(id int64) (ok bool) {
	return is.tableFromCache(id) != nil
}

// tableFromCache returns the table from cache if it exists, otherwise returns nil.
// It does not trigger a reload from storage.
func (is *infoschemaV2) tableFromCache(id int64) table.Table {
	if !tableIDIsValid(id) {
		return nil
	}

	itm, ok := is.searchTableItemByID(id)
	if !ok {
		return nil
	}

	if autoid.IsMemSchemaID(id) {
		if raw, exist := is.Data.specials.Load(itm.dbName.L); exist {
			schTbls := raw.(*schemaTables)
			if tbl, ok := schTbls.tables[itm.tableName.L]; ok {
				return tbl
			}
		}
		return nil
	}

	key := tableCacheKey{itm.tableID, itm.schemaVersion}
	tbl, found := is.tableCache.Get(key)
	if found && tbl != nil {
		return tbl
	}
	return nil
}

// IsSpecialDB tells whether the database is a special database.
func IsSpecialDB(dbName string) bool {
	return metadef.IsMemDB(dbName)
}

// EvictTable is exported for testing only.
func (is *infoschemaV2) EvictTable(schema, tbl ast.CIStr) {
	eq := func(a, b *tableItem) bool { return a.dbName == b.dbName && a.tableName == b.tableName }
	itm, ok := search(is.byName.Load(), is.infoSchema.schemaMetaVersion, tableItem{dbName: schema, tableName: tbl, schemaVersion: math.MaxInt64}, eq)
	if !ok {
		return
	}
	is.tableCache.Remove(tableCacheKey{itm.tableID, is.infoSchema.schemaMetaVersion})
	is.tableCache.Remove(tableCacheKey{itm.tableID, itm.schemaVersion})
}

type tableByNameHelper struct {
	end           tableItem
	schemaVersion int64
	found         bool
	res           *tableItem
}

func (h *tableByNameHelper) onItem(item *tableItem) bool {
	if item.dbName.L != h.end.dbName.L || item.tableName.L != h.end.tableName.L {
		h.found = false
		return false
	}
	if item.schemaVersion <= h.schemaVersion {
		if !item.tomb { // If the item is a tomb record, the database is dropped.
			h.found = true
			h.res = item
		}
		return false
	}
	return true
}

// TableByName implements the InfoSchema interface.
// When schema cache miss, it will fetch the TableInfo from TikV and refill cache.
func (is *infoschemaV2) TableByName(ctx context.Context, schema, tbl ast.CIStr) (t table.Table, err error) {
	if IsSpecialDB(schema.L) {
		if raw, ok := is.specials.Load(schema.L); ok {
			tbNames := raw.(*schemaTables)
			if t, ok = tbNames.tables[tbl.L]; ok {
				return
			}
		}
		return nil, ErrTableNotExists.FastGenByArgs(schema, tbl)
	}

	is.keepAlive()
	start := time.Now()
	var h tableByNameHelper
	h.end = tableItem{dbName: schema, tableName: tbl, schemaVersion: math.MaxInt64}
	h.schemaVersion = is.infoSchema.schemaMetaVersion
	is.byName.Load().DescendLessOrEqual(&h.end, h.onItem)

	if !h.found {
		return nil, ErrTableNotExists.FastGenByArgs(schema, tbl)
	}
	itm := h.res

	// Get from the cache with old key
	oldKey := tableCacheKey{itm.tableID, itm.schemaVersion}
	res, found := is.tableCache.Get(oldKey)
	if found && res != nil {
		metrics.TableByNameHitDuration.Observe(float64(time.Since(start)))
		return res, nil
	}

	// Maybe the table is evicted? need to reload.
	ret, err := is.loadTableInfo(ctx, itm.tableID, itm.dbID, is.ts, is.infoSchema.schemaMetaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	forceRefill := true
	if opt := ctx.Value(refillOptionKey); opt != nil {
		forceRefill = opt.(bool)
	}
	if forceRefill {
		is.tableCache.Set(oldKey, ret)
	} else {
		is.refillIfNoEvict(oldKey, ret)
	}

	metrics.TableByNameMissDuration.Observe(float64(time.Since(start)))
	return ret, nil
}

// TableInfoByName implements InfoSchema.TableInfoByName
func (is *infoschemaV2) TableInfoByName(schema, table ast.CIStr) (*model.TableInfo, error) {
	tbl, err := is.TableByName(context.Background(), schema, table)
	return getTableInfo(tbl), err
}

// TableInfoByID implements InfoSchema.TableInfoByID
func (is *infoschemaV2) TableInfoByID(id int64) (*model.TableInfo, bool) {
	tbl, ok := is.TableByID(context.Background(), id)
	return getTableInfo(tbl), ok
}

// keepAlive prevents the "GC life time is shorter than transaction duration" error on infoschema v2.
// It works by collecting the min TS of the during infoschem v2 API calls, and
// reports the min TS to info.InfoSyncer.
func (is *infoschemaV2) keepAlive() {
	for {
		v := is.Data.recentMinTS.Load()
		if v <= is.ts {
			break
		}
		succ := is.Data.recentMinTS.CompareAndSwap(v, is.ts)
		if succ {
			break
		}
	}
}

// SchemaTableInfos implements MetaOnlyInfoSchema.
func (is *infoschemaV2) SchemaTableInfos(ctx context.Context, schema ast.CIStr) ([]*model.TableInfo, error) {
	if IsSpecialDB(schema.L) {
		raw, ok := is.Data.specials.Load(schema.L)
		if ok {
			schTbls := raw.(*schemaTables)
			tables := make([]table.Table, 0, len(schTbls.tables))
			for _, tbl := range schTbls.tables {
				tables = append(tables, tbl)
			}
			return getTableInfoList(tables), nil
		}
		return nil, nil // something wrong?
	}

	is.keepAlive()

	// Fast path: all tables are in cache. Optimize for the few tables' scenario.
	// We check cache and collect tables in a single pass to avoid redundant lookups.
	if is.tableCache.Under70PercentUsage() {
		db, ok := is.SchemaByName(schema)
		if ok && db != nil {
			tables := make([]*model.TableInfo, 0)
			allInCache := true
			is.IterateAllTableItemsByDB(db.Name, func(t TableItem) bool {
				tbl := is.tableFromCache(t.TableID)
				if tbl == nil {
					allInCache = false
					return false
				}
				tables = append(tables, tbl.Meta())
				return true
			})
			if allInCache {
				// Sort by ID to keep the order consistent with fetching from storage.
				sort.Slice(tables, func(i, j int) bool {
					return tables[i].ID < tables[j].ID
				})
				return tables, nil
			}
		}
	}

retry:
	dbInfo, ok := is.SchemaByName(schema)
	if !ok {
		return nil, nil
	}
	snapshot := is.r.Store().GetSnapshot(kv.NewVersion(is.ts))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewReader(snapshot)
	tblInfos, err := m.ListTables(ctx, dbInfo.ID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return nil, nil
		}
		// Flashback statement could cause such kind of error.
		// In theory that error should be handled in the lower layer, like client-go.
		// But it's not done, so we retry here.
		if strings.Contains(err.Error(), "in flashback progress") {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			goto retry
		}
		return nil, errors.Trace(err)
	}
	return tblInfos, nil
}

// SchemaSimpleTableInfos implements MetaOnlyInfoSchema.
func (is *infoschemaV2) SchemaSimpleTableInfos(ctx context.Context, schema ast.CIStr) ([]*model.TableNameInfo, error) {
	if IsSpecialDB(schema.L) {
		raw, ok := is.Data.specials.Load(schema.L)
		if ok {
			schTbls := raw.(*schemaTables)
			ret := make([]*model.TableNameInfo, 0, len(schTbls.tables))
			for _, tbl := range schTbls.tables {
				ret = append(ret, &model.TableNameInfo{
					ID:   tbl.Meta().ID,
					Name: tbl.Meta().Name,
				})
			}
			return ret, nil
		}
		return nil, nil // something wrong?
	}

	// Ascend is much more difficult than Descend.
	// So the data is taken out first and then dedup in Descend order.
	var tableItems []*tableItem
	is.byName.Load().AscendGreaterOrEqual(&tableItem{dbName: schema}, func(item *tableItem) bool {
		if item.dbName.L != schema.L {
			return false
		}
		if is.infoSchema.schemaMetaVersion >= item.schemaVersion {
			tableItems = append(tableItems, item)
		}
		return true
	})
	if len(tableItems) == 0 {
		return nil, nil
	}
	tblInfos := make([]*model.TableNameInfo, 0, len(tableItems))
	var curr *tableItem
	for i := len(tableItems) - 1; i >= 0; i-- {
		item := tableItems[i]
		if curr == nil || curr.tableName != tableItems[i].tableName {
			curr = item
			if !item.tomb {
				tblInfos = append(tblInfos, &model.TableNameInfo{
					ID:   item.tableID,
					Name: item.tableName,
				})
			}
		}
	}
	return tblInfos, nil
}

// FindTableInfoByPartitionID implements InfoSchema.FindTableInfoByPartitionID
func (is *infoschemaV2) FindTableInfoByPartitionID(
	partitionID int64,
) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition) {
	tbl, db, partDef := is.FindTableByPartitionID(partitionID)
	return getTableInfo(tbl), db, partDef
}

func (is *infoschemaV2) SchemaByName(schema ast.CIStr) (val *model.DBInfo, ok bool) {
	if IsSpecialDB(schema.L) {
		raw, ok := is.Data.specials.Load(schema.L)
		if !ok {
			return nil, false
		}
		schTbls, ok := raw.(*schemaTables)
		return schTbls.dbInfo, ok
	}

	var dbInfo model.DBInfo
	dbInfo.Name = schema
	is.Data.schemaMap.Load().DescendLessOrEqual(schemaItem{
		dbInfo:        &dbInfo,
		schemaVersion: math.MaxInt64,
	}, func(item schemaItem) bool {
		if item.Name() != schema.L {
			ok = false
			return false
		}
		if item.schemaVersion <= is.infoSchema.schemaMetaVersion {
			if !item.tomb { // If the item is a tomb record, the database is dropped.
				ok = true
				val = item.dbInfo
			}
			return false
		}
		return true
	})
	return
}

func (is *infoschemaV2) allSchemas(visit func(*model.DBInfo)) {
	var last *model.DBInfo
	is.Data.schemaMap.Load().Descend(func(item schemaItem) bool {
		if item.schemaVersion > is.infoSchema.schemaMetaVersion {
			// Skip the versions that we are not looking for.
			return true
		}

		// Dedup the same db record of different versions.
		if last != nil && last.Name == item.dbInfo.Name {
			return true
		}
		last = item.dbInfo

		if !item.tomb {
			visit(item.dbInfo)
		}
		return true
	})
	is.Data.specials.Range(func(key, value any) bool {
		sc := value.(*schemaTables)
		visit(sc.dbInfo)
		return true
	})
}

func (is *infoschemaV2) AllSchemas() (schemas []*model.DBInfo) {
	is.allSchemas(func(di *model.DBInfo) {
		schemas = append(schemas, di)
	})
	return
}

func (is *infoschemaV2) AllSchemaNames() []ast.CIStr {
	rs := make([]ast.CIStr, 0, is.Data.schemaMap.Load().Len())
	is.allSchemas(func(di *model.DBInfo) {
		rs = append(rs, di.Name)
	})
	return rs
}

func (is *infoschemaV2) SchemaExists(schema ast.CIStr) bool {
	_, ok := is.SchemaByName(schema)
	return ok
}

func (is *infoschemaV2) searchPartitionItemByPartitionID(partitionID int64) (pi partitionItem, ok bool) {
	is.pid2tid.Load().DescendLessOrEqual(partitionItem{partitionID: partitionID, schemaVersion: math.MaxInt64},
		func(item partitionItem) bool {
			if item.partitionID != partitionID {
				return false
			}
			if item.schemaVersion > is.infoSchema.schemaMetaVersion {
				// Skip the record.
				return true
			}
			if item.schemaVersion <= is.infoSchema.schemaMetaVersion {
				pi = item
				ok = !item.tomb
				return false
			}
			return true
		},
	)
	return pi, ok
}

// TableItemByPartitionID implements InfoSchema.TableItemByPartitionID.
// It returns the lightweight meta info, no worries about access the storage.
func (is *infoschemaV2) TableItemByPartitionID(partitionID int64) (TableItem, bool) {
	pi, ok := is.searchPartitionItemByPartitionID(partitionID)
	if !ok {
		return TableItem{}, false
	}
	return is.TableItemByID(pi.tableID)
}

// TableIDByPartitionID implements InfoSchema.TableIDByPartitionID.
func (is *infoschemaV2) TableIDByPartitionID(partitionID int64) (tableID int64, ok bool) {
	pi, ok := is.searchPartitionItemByPartitionID(partitionID)
	if !ok {
		return
	}
	return pi.tableID, true
}

func (is *infoschemaV2) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	pi, ok := is.searchPartitionItemByPartitionID(partitionID)
	if !ok {
		return nil, nil, nil
	}

	tbl, ok := is.TableByID(context.Background(), pi.tableID)
	if !ok {
		// something wrong?
		return nil, nil, nil
	}

	dbID := tbl.Meta().DBID
	dbInfo, ok := is.SchemaByID(dbID)
	if !ok {
		// something wrong?
		return nil, nil, nil
	}

	partInfo := tbl.Meta().GetPartitionInfo()
	var def *model.PartitionDefinition
	for i := range partInfo.Definitions {
		pdef := &partInfo.Definitions[i]
		if pdef.ID == partitionID {
			def = pdef
			break
		}
	}

	return tbl, dbInfo, def
}

func (is *infoschemaV2) TableExists(schema, table ast.CIStr) bool {
	_, err := is.TableByName(context.Background(), schema, table)
	return err == nil
}

func (is *infoschemaV2) SchemaByID(id int64) (*model.DBInfo, bool) {
	if autoid.IsMemSchemaID(id) {
		var st *schemaTables
		is.Data.specials.Range(func(key, value any) bool {
			tmp := value.(*schemaTables)
			if tmp.dbInfo.ID == id {
				st = tmp
				return false
			}
			return true
		})
		if st == nil {
			return nil, false
		}
		return st.dbInfo, true
	}
	var ok bool
	var name ast.CIStr
	is.Data.schemaID2Name.Load().DescendLessOrEqual(schemaIDName{
		id:            id,
		schemaVersion: math.MaxInt64,
	}, func(item schemaIDName) bool {
		if item.id != id {
			ok = false
			return false
		}
		if item.schemaVersion <= is.infoSchema.schemaMetaVersion {
			if !item.tomb { // If the item is a tomb record, the database is dropped.
				ok = true
				name = item.name
			}
			return false
		}
		return true
	})
	if !ok {
		return nil, false
	}
	return is.SchemaByName(name)
}

// GetTableReferredForeignKeys implements InfoSchema.GetTableReferredForeignKeys
func (is *infoschemaV2) GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo {
	is.keepAlive()
	return is.Data.getTableReferredForeignKeys(schema, table, is.infoSchema.schemaMetaVersion)
}

func (is *infoschemaV2) loadTableInfo(ctx context.Context, tblID, dbID int64, ts uint64, schemaVersion int64) (table.Table, error) {
	defer tracing.StartRegion(ctx, "infoschema.loadTableInfo").End()
	failpoint.Inject("mockLoadTableInfoError", func(_ failpoint.Value) {
		failpoint.Return(nil, errors.New("mockLoadTableInfoError"))
	})
	// Try to avoid repeated concurrency loading.
	res, err, _ := loadTableSF.Do(fmt.Sprintf("%d-%d-%d", dbID, tblID, schemaVersion), func() (any, error) {
	retry:
		snapshot := is.r.Store().GetSnapshot(kv.NewVersion(ts))
		// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
		// the meta region leader is slow.
		snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
		m := meta.NewReader(snapshot)

		tblInfo, err := m.GetTable(dbID, tblID)
		if err != nil {
			// Flashback statement could cause such kind of error.
			// In theory that error should be handled in the lower layer, like client-go.
			// But it's not done, so we retry here.
			if strings.Contains(err.Error(), "in flashback progress") {
				time.Sleep(200 * time.Millisecond)
				goto retry
			}

			return nil, errors.Trace(err)
		}

		// table removed.
		if tblInfo == nil {
			return nil, errors.Trace(ErrTableNotExists.FastGenByArgs(
				fmt.Sprintf("(Schema ID %d)", dbID),
				fmt.Sprintf("(Table ID %d)", tblID),
			))
		}

		ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
		ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)
		allocs := autoid.NewAllocatorsFromTblInfo(is.r, dbID, tblInfo)
		ret, err := tableFromMeta(allocs, is.factory, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return ret, err
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	if res == nil {
		return nil, errors.Trace(ErrTableNotExists.FastGenByArgs(
			fmt.Sprintf("(Schema ID %d)", dbID),
			fmt.Sprintf("(Table ID %d)", tblID),
		))
	}
	return res.(table.Table), nil
}

var loadTableSF = &singleflight.Group{}

// IsV2 tells whether an InfoSchema is v2 or not.
func IsV2(is InfoSchema) (bool, *infoschemaV2) {
	ret, ok := is.(*infoschemaV2)
	return ok, ret
}

func applyTableUpdate(b *Builder, m meta.Reader, diff *model.SchemaDiff) ([]int64, error) {
	if b.enableV2 {
		return b.applyTableUpdateV2(m, diff)
	}
	return b.applyTableUpdate(m, diff)
}

func applyCreateSchema(b *Builder, m meta.Reader, diff *model.SchemaDiff) error {
	return b.applyCreateSchema(m, diff)
}

func applyDropSchema(b *Builder, diff *model.SchemaDiff) []int64 {
	if b.enableV2 {
		return b.applyDropSchemaV2(diff)
	}
	return b.applyDropSchema(diff)
}

func applyRecoverSchema(b *Builder, m meta.Reader, diff *model.SchemaDiff) ([]int64, error) {
	if diff.ReadTableFromMeta {
		// recover tables under the database and set them to diff.AffectedOpts
		s := b.store.GetSnapshot(kv.MaxVersion)
		recoverMeta := meta.NewReader(s)
		tables, err := recoverMeta.ListSimpleTables(diff.SchemaID)
		if err != nil {
			return nil, err
		}
		diff.AffectedOpts = make([]*model.AffectedOption, 0, len(tables))
		for _, t := range tables {
			diff.AffectedOpts = append(diff.AffectedOpts, &model.AffectedOption{
				SchemaID:    diff.SchemaID,
				OldSchemaID: diff.SchemaID,
				TableID:     t.ID,
				OldTableID:  t.ID,
			})
		}
	}

	if b.enableV2 {
		return b.applyRecoverSchemaV2(m, diff)
	}
	return b.applyRecoverSchema(m, diff)
}

func (b *Builder) applyRecoverSchemaV2(m meta.Reader, diff *model.SchemaDiff) ([]int64, error) {
	if di, ok := b.infoschemaV2.SchemaByID(diff.SchemaID); ok {
		return nil, ErrDatabaseExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", di.ID),
		)
	}
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b.infoschemaV2.addDB(diff.Version, di)
	return applyCreateTables(b, m, diff)
}

func applyModifySchemaCharsetAndCollate(b *Builder, m meta.Reader, diff *model.SchemaDiff) error {
	if b.enableV2 {
		return b.applyModifySchemaCharsetAndCollateV2(m, diff)
	}
	return b.applyModifySchemaCharsetAndCollate(m, diff)
}

func applyModifySchemaDefaultPlacement(b *Builder, m meta.Reader, diff *model.SchemaDiff) error {
	if b.enableV2 {
		return b.applyModifySchemaDefaultPlacementV2(m, diff)
	}
	return b.applyModifySchemaDefaultPlacement(m, diff)
}

func applyDropTable(b *Builder, diff *model.SchemaDiff, dbInfo *model.DBInfo, tableID int64, affected []int64) []int64 {
	if b.enableV2 {
		return b.applyDropTableV2(diff, dbInfo, tableID, affected)
	}
	return b.applyDropTable(diff, dbInfo, tableID, affected)
}

func applyCreateTables(b *Builder, m meta.Reader, diff *model.SchemaDiff) ([]int64, error) {
	return b.applyCreateTables(m, diff)
}

func updateInfoSchemaBundles(b *Builder) {
	if b.enableV2 {
		b.updateInfoSchemaBundlesV2(&b.infoschemaV2)
	} else {
		b.updateInfoSchemaBundles(b.infoSchema)
	}
}

func oldSchemaInfo(b *Builder, diff *model.SchemaDiff) (*model.DBInfo, bool) {
	if b.enableV2 {
		return b.infoschemaV2.SchemaByID(diff.OldSchemaID)
	}

	oldRoDBInfo, ok := b.infoSchema.SchemaByID(diff.OldSchemaID)
	if ok {
		oldRoDBInfo = b.getSchemaAndCopyIfNecessary(oldRoDBInfo.Name.L)
	}
	return oldRoDBInfo, ok
}

// allocByID returns the Allocators of a table.
func allocByID(b *Builder, id int64) (autoid.Allocators, bool) {
	var is InfoSchema
	if b.enableV2 {
		is = &b.infoschemaV2
	} else {
		is = b.infoSchema
	}
	tbl, ok := is.TableByID(context.Background(), id)
	if !ok {
		return autoid.Allocators{}, false
	}
	return tbl.Allocators(nil), true
}

// TODO: more UT to check the correctness.
func (b *Builder) applyTableUpdateV2(m meta.Reader, diff *model.SchemaDiff) ([]int64, error) {
	oldDBInfo, ok := b.infoschemaV2.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}

	oldTableID, newTableID, err := b.getTableIDs(m, diff)
	if err != nil {
		return nil, err
	}
	b.updateBundleForTableUpdate(diff, newTableID, oldTableID)

	tblIDs, allocs, err := dropTableForUpdate(b, newTableID, oldTableID, oldDBInfo, diff)
	if err != nil {
		return nil, err
	}

	if tableIDIsValid(newTableID) {
		// All types except DropTableOrView.
		var err error
		tblIDs, err = applyCreateTable(b, m, oldDBInfo, newTableID, allocs, diff.Type, tblIDs, diff.Version)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tblIDs, nil
}

func (b *Builder) applyDropSchemaV2(diff *model.SchemaDiff) []int64 {
	di, ok := b.infoschemaV2.SchemaByID(diff.SchemaID)
	if !ok {
		return nil
	}

	tableIDs := make([]int64, 0, len(di.Deprecated.Tables))
	tables, err := b.infoschemaV2.SchemaTableInfos(context.Background(), di.Name)
	terror.Log(err)
	for _, tbl := range tables {
		tableIDs = appendAffectedIDs(tableIDs, tbl)
	}

	for _, id := range tableIDs {
		b.deleteBundle(b.infoSchema, id)
		b.applyDropTableV2(diff, di, id, nil)
	}
	b.infoData.deleteDB(di, diff.Version)
	return tableIDs
}

func (b *Builder) applyDropTableV2(diff *model.SchemaDiff, dbInfo *model.DBInfo, tableID int64, affected []int64) []int64 {
	// Remove the table in temporaryTables
	if b.infoSchemaMisc.temporaryTableIDs != nil {
		delete(b.infoSchemaMisc.temporaryTableIDs, tableID)
	}

	table, ok := b.infoschemaV2.TableByID(context.Background(), tableID)
	if !ok {
		return nil
	}
	tblInfo := table.Meta()

	// The old DBInfo still holds a reference to old table info, we need to remove it.
	b.infoSchema.deleteReferredForeignKeys(dbInfo.Name, tblInfo)
	b.infoschemaV2.Data.deleteReferredForeignKeys(dbInfo.Name, tblInfo, diff.Version)

	if pi := table.Meta().GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			btreeSet(&b.infoData.pid2tid, partitionItem{def.ID, diff.Version, table.Meta().ID, true})
		}
	}

	b.infoData.remove(tableItem{
		dbName:        dbInfo.Name,
		dbID:          dbInfo.ID,
		tableName:     tblInfo.Name,
		tableID:       tblInfo.ID,
		schemaVersion: diff.Version,
	})
	affected = appendAffectedIDs(affected, tblInfo)

	return affected
}

func (b *Builder) applyModifySchemaCharsetAndCollateV2(m meta.Reader, diff *model.SchemaDiff) error {
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
	oldDBInfo, _ := b.infoschemaV2.SchemaByID(diff.SchemaID)
	newDBInfo := oldDBInfo.Clone()
	newDBInfo.Charset = di.Charset
	newDBInfo.Collate = di.Collate
	b.infoschemaV2.deleteDB(di, diff.Version)
	b.infoschemaV2.addDB(diff.Version, newDBInfo)
	return nil
}

func (b *Builder) applyModifySchemaDefaultPlacementV2(m meta.Reader, diff *model.SchemaDiff) error {
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
	oldDBInfo, _ := b.infoschemaV2.SchemaByID(diff.SchemaID)
	newDBInfo := oldDBInfo.Clone()
	newDBInfo.PlacementPolicyRef = di.PlacementPolicyRef
	b.infoschemaV2.deleteDB(di, diff.Version)
	b.infoschemaV2.addDB(diff.Version, newDBInfo)
	return nil
}

func (b *bundleInfoBuilder) updateInfoSchemaBundlesV2(is *infoschemaV2) {
	if b.deltaUpdate {
		b.completeUpdateTablesV2(is)
		for tblID := range b.updateTables {
			b.updateTableBundles(is, tblID)
		}
		return
	}

	// do full update bundles
	is.ruleBundleMap = make(map[int64]*placement.Bundle)
	tmp := is.ListTablesWithSpecialAttribute(infoschemacontext.PlacementPolicyAttribute)
	for _, v := range tmp {
		for _, tbl := range v.TableInfos {
			b.updateTableBundles(is, tbl.ID)
		}
	}
}

func (b *bundleInfoBuilder) completeUpdateTablesV2(is *infoschemaV2) {
	if len(b.updatePolicies) == 0 {
		return
	}

	dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.AllSpecialAttribute)
	for _, db := range dbs {
		for _, tbl := range db.TableInfos {
			tblInfo := tbl
			if tblInfo.PlacementPolicyRef != nil {
				if _, ok := b.updatePolicies[tblInfo.PlacementPolicyRef.ID]; ok {
					b.markTableBundleShouldUpdate(tblInfo.ID)
				}
			}
		}
	}
}

func (is *infoschemaV2) ListTablesWithSpecialAttribute(filter infoschemacontext.SpecialAttributeFilter) []infoschemacontext.TableInfoResult {
	ret := make([]infoschemacontext.TableInfoResult, 0, 10)
	var currDB string
	var lastTableID int64
	var res infoschemacontext.TableInfoResult
	is.Data.tableInfoResident.Load().Descend(func(item tableInfoItem) bool {
		if item.schemaVersion > is.infoSchema.schemaMetaVersion {
			// Skip the versions that we are not looking for.
			return true
		}
		// Dedup the same record of different versions.
		if lastTableID != 0 && lastTableID == item.tableID {
			return true
		}
		lastTableID = item.tableID

		if item.tomb {
			return true
		}

		if !filter(item.tableInfo) {
			return true
		}

		if currDB == "" {
			currDB = item.dbName.L
			res = infoschemacontext.TableInfoResult{DBName: item.dbName}
			res.TableInfos = append(res.TableInfos, item.tableInfo)
		} else if currDB == item.dbName.L {
			res.TableInfos = append(res.TableInfos, item.tableInfo)
		} else {
			ret = append(ret, res)
			res = infoschemacontext.TableInfoResult{DBName: item.dbName}
			res.TableInfos = append(res.TableInfos, item.tableInfo)
		}
		return true
	})
	if len(res.TableInfos) > 0 {
		ret = append(ret, res)
	}
	return ret
}

type refillOption struct{}

var refillOptionKey refillOption

// WithRefillOption controls the infoschema v2 cache refill operation.
// By default, TableByID does not refill schema cache if the current size is greater
// than 70% of the capacity, and TableByName does.
// The behavior can be changed by providing the context.Context.
func WithRefillOption(ctx context.Context, evict bool) context.Context {
	return context.WithValue(ctx, refillOptionKey, evict)
}

// refillIfNoEvict refills the table cache only if the current size is less
// than 70% of the capacity. We want to cache as many tables as possible, but
// we also want to avoid evicting useful cached tables by some list operations.
func (is *infoschemaV2) refillIfNoEvict(key tableCacheKey, value table.Table) {
	if is.tableCache.Under70PercentUsage() {
		is.tableCache.Set(key, value)
	}
}
