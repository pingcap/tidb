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
	"fmt"
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/scalalang2/golang-fifo"
	"github.com/scalalang2/golang-fifo/sieve"
	"github.com/tidwall/btree"
	"golang.org/x/sync/singleflight"
)

// tableItem is the btree item sorted by name or by id.
type tableItem struct {
	dbName        string
	dbID          int64
	tableName     string
	tableID       int64
	schemaVersion int64
}

type schemaItem struct {
	schemaVersion int64
	dbInfo        *model.DBInfo
}

func (si *schemaItem) Name() string {
	return si.dbInfo.Name.L
}

// versionAndTimestamp is the tuple of schema version and timestamp.
type versionAndTimestamp struct {
	schemaVersion int64
	timestamp     uint64
}

// Data is the core data struct of infoschema V2.
type Data struct {
	// For the TableByName API, sorted by {dbName, tableName, tableID} => schemaVersion
	//
	// If the schema version +1 but a specific table does not change, the old record is
	// kept and no new {dbName, tableName, tableID} => schemaVersion+1 record been added.
	//
	// It means as long as we can find an item in it, the item is available, even through the
	// schema version maybe smaller than required.
	//
	// *IMPORTANT RESTRICTION*: Do we have the full data in memory? NO!
	byName *btree.BTreeG[tableItem]

	// For the TableByID API, sorted by {tableID}
	// To reload model.TableInfo, we need both table ID and database ID for meta kv API.
	// It provides the tableID => databaseID mapping.
	//
	// *IMPORTANT RESTRICTION*: Do we have the full data in memory? NO!
	// But this mapping should be synced with byName.
	byID *btree.BTreeG[tableItem]

	tableCache fifo.Cache[tableCacheKey, table.Table]

	// For the SchemaByName API, sorted by {dbName, schemaVersion} => model.DBInfo
	// Stores the full data in memory.
	schemaMap *btree.BTreeG[schemaItem]

	// sorted by both SchemaVersion and timestamp in descending order, assume they have same order
	mu struct {
		sync.RWMutex
		versionTimestamps []versionAndTimestamp
	}

	// For information_schema/metrics_schema/performance_schema etc
	specials map[string]*schemaTables
}

func (isd *Data) getVersionByTS(ts uint64) (int64, bool) {
	isd.mu.RLock()
	defer isd.mu.RUnlock()

	return isd.getVersionByTSNoLock(ts)
}

func (isd *Data) getVersionByTSNoLock(ts uint64) (int64, bool) {
	// search one by one instead of binary search, because the timestamp of a schema could be 0
	// this is ok because the size of h.tableCache is small (currently set to 16)
	// moreover, the most likely hit element in the array is the first one in steady mode
	// thus it may have better performance than binary search
	for i, vt := range isd.mu.versionTimestamps {
		if vt.timestamp == 0 || ts < vt.timestamp {
			// is.timestamp == 0 means the schema ts is unknown, so we can't use it, then just skip it.
			// ts < is.timestamp means the schema is newer than ts, so we can't use it too, just skip it to find the older one.
			continue
		}
		// ts >= is.timestamp must be true after the above condition.
		if i == 0 {
			// the first element is the latest schema, so we can return it directly.
			return vt.schemaVersion, true
		}
		if isd.mu.versionTimestamps[i-1].schemaVersion == vt.schemaVersion+1 && isd.mu.versionTimestamps[i-1].timestamp > ts {
			// This first condition is to make sure the schema version is continuous. If last(cache[i-1]) schema-version is 10,
			// but current(cache[i]) schema-version is not 9, then current schema is not suitable for ts.
			// The second condition is to make sure the cache[i-1].timestamp > ts >= cache[i].timestamp, then the current schema is suitable for ts.
			return vt.schemaVersion, true
		}
		// current schema is not suitable for ts, then break the loop to avoid the unnecessary search.
		break
	}

	return 0, false
}

type tableCacheKey struct {
	tableID       int64
	schemaVersion int64
}

// NewData creates an infoschema V2 data struct.
func NewData() *Data {
	ret := &Data{
		byID:      btree.NewBTreeG[tableItem](compareByID),
		byName:    btree.NewBTreeG[tableItem](compareByName),
		schemaMap: btree.NewBTreeG[schemaItem](compareSchemaItem),
		// TODO: limit by size instead of by table count.
		tableCache: sieve.New[tableCacheKey, table.Table](1000),
		specials:   make(map[string]*schemaTables),
	}
	return ret
}

func (isd *Data) add(item tableItem, tbl table.Table) {
	isd.byID.Set(item)
	isd.byName.Set(item)
	isd.tableCache.Set(tableCacheKey{item.tableID, item.schemaVersion}, tbl)
}

func (isd *Data) addSpecialDB(di *model.DBInfo, tables *schemaTables) {
	isd.specials[di.Name.L] = tables
}

func (isd *Data) addDB(schemaVersion int64, dbInfo *model.DBInfo) {
	isd.schemaMap.Set(schemaItem{schemaVersion: schemaVersion, dbInfo: dbInfo})
}

func compareByID(a, b tableItem) bool {
	if a.tableID < b.tableID {
		return true
	}
	if a.tableID > b.tableID {
		return false
	}

	return a.dbID < b.dbID
}

func compareByName(a, b tableItem) bool {
	if a.dbName < b.dbName {
		return true
	}
	if a.dbName > b.dbName {
		return false
	}

	if a.tableName < b.tableName {
		return true
	}
	if a.tableName > b.tableName {
		return false
	}

	return a.tableID < b.tableID
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

var _ InfoSchema = &infoschemaV2{}

type infoschemaV2 struct {
	*infoSchema   // in fact, we only need the infoSchemaMisc inside it, but the builder rely on it.
	r             autoid.Requirement
	ts            uint64
	schemaVersion int64
	*Data
}

func search(bt *btree.BTreeG[tableItem], schemaVersion int64, end tableItem, matchFn func(a, b *tableItem) bool) (tableItem, bool) {
	var ok bool
	var target tableItem
	// Iterate through the btree, find the query item whose schema version is the largest one (latest).
	bt.Descend(end, func(item tableItem) bool {
		if !matchFn(&end, &item) {
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
	return target, ok
}

func (is *infoschemaV2) TableByID(id int64) (val table.Table, ok bool) {
	if isTableVirtual(id) {
		// Don't store the virtual table in the tableCache, because when cache missing
		// we can't refill it from tikv.
		// TODO: returns the correct result.
		return nil, false
	}

	// Get from the cache.
	key := tableCacheKey{id, is.schemaVersion}
	tbl, found := is.tableCache.Get(key)
	if found && tbl != nil {
		return tbl, true
	}

	eq := func(a, b *tableItem) bool { return a.tableID == b.tableID }
	itm, ok := search(is.byID, is.schemaVersion, tableItem{tableID: id, dbID: math.MaxInt64}, eq)
	if !ok {
		// TODO: in the future, this may happen and we need to check tikv to see whether table exists.
		return nil, false
	}

	// Maybe the table is evicted? need to reload.
	ret, err := loadTableInfo(is.r, is.Data, id, itm.dbID, is.ts, is.schemaVersion)
	if err == nil {
		is.tableCache.Set(key, ret)
		return ret, true
	}
	return nil, false
}

func isSpecialDB(dbName string) bool {
	return dbName == util.InformationSchemaName.L ||
		dbName == util.PerformanceSchemaName.L ||
		dbName == util.MetricSchemaName.L
}

func (is *infoschemaV2) TableByName(schema, tbl model.CIStr) (t table.Table, err error) {
	if isSpecialDB(schema.L) {
		if tbNames, ok := is.specials[schema.L]; ok {
			if t, ok = tbNames.tables[tbl.L]; ok {
				return
			}
		}
		return nil, ErrTableNotExists.GenWithStackByArgs(schema, tbl)
	}

	eq := func(a, b *tableItem) bool { return a.dbName == b.dbName && a.tableName == b.tableName }
	itm, ok := search(is.byName, is.schemaVersion, tableItem{dbName: schema.L, tableName: tbl.L, tableID: math.MaxInt64}, eq)
	if !ok {
		// TODO: in the future, this may happen and we need to check tikv to see whether table exists.
		return nil, ErrTableNotExists.GenWithStackByArgs(schema, tbl)
	}

	// Get from the cache.
	key := tableCacheKey{itm.tableID, is.schemaVersion}
	res, found := is.tableCache.Get(key)
	if found && res != nil {
		return res, nil
	}

	// Maybe the table is evicted? need to reload.
	ret, err := loadTableInfo(is.r, is.Data, itm.tableID, itm.dbID, is.ts, is.schemaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is.tableCache.Set(key, ret)
	return ret, nil
}

func (is *infoschemaV2) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	var dbInfo model.DBInfo
	dbInfo.Name = schema
	is.Data.schemaMap.Descend(schemaItem{dbInfo: &dbInfo, schemaVersion: math.MaxInt64}, func(item schemaItem) bool {
		if item.Name() != schema.L {
			ok = false
			return false
		}
		if item.schemaVersion <= is.schemaVersion {
			ok = true
			val = item.dbInfo
			return false
		}
		return true
	})
	return
}

func (is *infoschemaV2) AllSchemas() (schemas []*model.DBInfo) {
	is.Data.schemaMap.Scan(func(item schemaItem) bool {
		// TODO: version?
		schemas = append(schemas, item.dbInfo)
		return true
	})
	return
}

func (is *infoschemaV2) AllSchemaNames() []model.CIStr {
	rs := make([]model.CIStr, 0, is.schemaMap.Len())
	is.schemaMap.Scan(func(item schemaItem) bool {
		rs = append(rs, item.dbInfo.Name)
		return true
	})
	return rs
}

func (is *infoschemaV2) SchemaMetaVersion() int64 {
	return is.schemaVersion
}

func (is *infoschemaV2) SchemaExists(schema model.CIStr) bool {
	var ok bool
	// TODO: support different version
	is.Data.schemaMap.Scan(func(item schemaItem) bool {
		if item.dbInfo.Name.L == schema.L {
			ok = true
			return false
		}
		return true
	})
	return ok
}

func (is *infoschemaV2) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	panic("TODO")
}

func (is *infoschemaV2) TableExists(schema, table model.CIStr) bool {
	_, err := is.TableByName(schema, table)
	return err == nil
}

func (is *infoschemaV2) SchemaByID(id int64) (*model.DBInfo, bool) {
	var ok bool
	var dbInfo *model.DBInfo
	is.Data.schemaMap.Scan(func(item schemaItem) bool {
		if item.dbInfo.ID == id {
			ok = true
			dbInfo = item.dbInfo
			return false
		}
		return true
	})
	return dbInfo, ok
}

func (is *infoschemaV2) SchemaTables(schema model.CIStr) (tables []table.Table) {
	dbInfo, ok := is.SchemaByName(schema)
	if !ok {
		return
	}
	snapshot := is.r.Store().GetSnapshot(kv.NewVersion(is.ts))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewSnapshotMeta(snapshot)
	tblInfos, err := m.ListSimpleTables(dbInfo.ID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return nil
		}
		// TODO: error could happen, so do not panic!
		panic(err)
	}
	tables = make([]table.Table, 0, len(tblInfos))
	for _, tblInfo := range tblInfos {
		tbl, ok := is.TableByID(tblInfo.ID)
		if !ok {
			// what happen?
			continue
		}
		tables = append(tables, tbl)
	}
	return
}

func loadTableInfo(r autoid.Requirement, infoData *Data, tblID, dbID int64, ts uint64, schemaVersion int64) (table.Table, error) {
	// Try to avoid repeated concurrency loading.
	res, err, _ := loadTableSF.Do(fmt.Sprintf("%d-%d-%d", dbID, tblID, schemaVersion), func() (ret any, err error) {
		snapshot := r.Store().GetSnapshot(kv.NewVersion(ts))
		// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
		// the meta region leader is slow.
		snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
		m := meta.NewSnapshotMeta(snapshot)

		ret, err = m.GetTable(dbID, tblID)
		return
	})

	if err != nil {
		// TODO load table panic!!!
		panic(err)
	}
	tblInfo := res.(*model.TableInfo) // TODO: it could be missing!!!

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)
	allocs := autoid.NewAllocatorsFromTblInfo(r, dbID, tblInfo)
	// TODO: handle cached table!!!
	ret, err := tables.TableFromMeta(allocs, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

var loadTableSF = &singleflight.Group{}

func isTableVirtual(id int64) bool {
	// some kind of magic number...
	// we use special ids for tables in INFORMATION_SCHEMA/PERFORMANCE_SCHEMA/METRICS_SCHEMA
	// See meta/autoid/autoid.go for those definitions.
	return (id & autoid.SystemSchemaIDFlag) > 0
}

// IsV2 tells whether an InfoSchema is v2 or not.
func IsV2(is InfoSchema) bool {
	_, ok := is.(*infoschemaV2)
	return ok
}
