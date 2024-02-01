package infoschema

import (
	"fmt"
	"math"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/tidwall/btree"
	"golang.org/x/sync/singleflight"
)

type Item struct {
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

type VersionAndTimestamp struct {
	schemaVersion int64
	timestamp     uint64
}

type InfoSchemaData struct {
	// For the TableByName API, sorted by {dbName, tableName, tableID} => schemaVersion
	//
	// If the schema version +1 but a specific table does not change, the old record is
	// kept and no new {dbName, tableName, tableID} => schemaVersion+1 record been added.
	//
	// It means as long as we can find a item in it, the item is available, even through the
	// schema version maybe smaller than required.
	//
	// *IMPORTANT RESTRICTION*: Do we have the full data in memory? NO!
	name2id *btree.BTreeG[Item]

	// For the TableByID API, sorted by {tableID}
	// To reload model.TableInfo, we need both table ID and database ID for meta kv API.
	// It provides the tableID => databaseID mapping.
	//
	// *IMPORTANT RESTRICTION*: Do we have the full data in memory? NO!
	// But this mapping should be synced with name2id.
	byID  *btree.BTreeG[Item]
	cache *ristretto.Cache // {id, schemaVersion} => table.Table

	// For the SchemaByName API
	schemaMap *btree.BTreeG[schemaItem]

	// sorted by both SchemaVersion and timestamp in descending order, assume they have same order
	mu struct {
		sync.RWMutex
		versionTimestamps []VersionAndTimestamp
	}
}

func (isd *InfoSchemaData) getVersionByTS(ts uint64) (int64, bool) {
	isd.mu.RLock()
	defer isd.mu.RUnlock()

	return isd.getVersionByTSNoLock(ts)
}

func (isd *InfoSchemaData) getVersionByTSNoLock(ts uint64) (int64, bool) {
	// search one by one instead of binary search, because the timestamp of a schema could be 0
	// this is ok because the size of h.cache is small (currently set to 16)
	// moreover, the most likely hit element in the array is the first one in steady mode
	// thus it may have better performance than binary search
	for i, vt := range isd.mu.versionTimestamps {
		if vt.timestamp == 0 || ts < uint64(vt.timestamp) {
			// is.timestamp == 0 means the schema ts is unknown, so we can't use it, then just skip it.
			// ts < is.timestamp means the schema is newer than ts, so we can't use it too, just skip it to find the older one.
			continue
		}
		// ts >= is.timestamp must be true after the above condition.
		if i == 0 {
			// the first element is the latest schema, so we can return it directly.
			return vt.schemaVersion, true
		}
		if isd.mu.versionTimestamps[i-1].schemaVersion == vt.schemaVersion+1 && uint64(isd.mu.versionTimestamps[i-1].timestamp) > ts {
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

type cacheKey struct {
	tableID       int64
	schemaVersion int64
}

func keyToHash(key any) (uint64, uint64) {
	k := key.(cacheKey)
	return uint64(k.tableID), uint64(k.schemaVersion)
}

func NewInfoSchemaData() (*InfoSchemaData, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 50000, // this means at most 10,000 items, recommand num is 10x items
		// TODO: change the cost to size based and provide configuration.
		MaxCost:     5000, // if we measure one partition cost as 1, at most 10,000 partitions.
		BufferItems: 64,
		KeyToHash:   keyToHash,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &InfoSchemaData{
		cache:     cache,
		byID:      btree.NewBTreeG[Item](compareByID),
		name2id:   btree.NewBTreeG[Item](compareByName),
		schemaMap: btree.NewBTreeG[schemaItem](compareBySchema),
	}, nil
}

func (isd *InfoSchemaData) Close() {
	isd.cache.Close()
}

func (isd *InfoSchemaData) add(item Item, tbl table.Table) {
	isd.byID.Set(item)
	isd.name2id.Set(item)
	isd.cache.Set(cacheKey{item.tableID, item.schemaVersion}, tbl, cacheTableCost(tbl))
}

func (isd *InfoSchemaData) addDB(schemaVersion int64, dbInfo *model.DBInfo) {
	fmt.Println("=== addDB ===", dbInfo.Name.L, schemaVersion)
	isd.schemaMap.Set(schemaItem{schemaVersion: schemaVersion, dbInfo: dbInfo})
}

func compareByID(a, b Item) bool {
	if a.tableID < b.tableID {
		return true
	}
	if a.tableID > b.tableID {
		return false
	}

	return a.dbID < b.dbID
}

func compareByName(a, b Item) bool {
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

func compareBySchema(a, b schemaItem) bool {
	if a.Name() < b.Name() {
		return true
	}
	if a.Name() > b.Name() {
		return false
	}
	return a.schemaVersion < b.schemaVersion
}

type infoschemaV2 struct {
	r             autoid.Requirement
	ts            uint64
	schemaVersion int64
	*InfoSchemaData
}

func search(bt *btree.BTreeG[Item], schemaVersion int64, end Item, eq func(a, b *Item) bool) (Item, bool) {
	var ok bool
	var itm Item
	// Iterate through the btree, find the query item whose schema version is the largest one (latest).
	bt.Descend(end, func(item Item) bool {
		if !eq(&end, &item) {
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
			itm = item
		} else { // The latest one
			if item.schemaVersion > itm.schemaVersion {
				itm = item
			}
		}
		return true
	})
	return itm, ok
}

func (is *infoschemaV2) TableByID(id int64) (val table.Table, ok bool) {
	if isTableVirtual(id) {
		return nil, false
	}

	// Get from the cache.
	key := cacheKey{id, is.schemaVersion}
	tbl, found := is.cache.Get(key)
	if found && tbl != nil {
		return tbl.(table.Table), true
	}

	eq := func(a, b *Item) bool { return a.tableID == b.tableID }
	itm, ok := search(is.byID, is.schemaVersion, Item{tableID: id, dbID: math.MaxInt64}, eq)
	if !ok {
		return nil, ok
	}

	// Maybe the table is evicted? need to reload.
	ret, err := loadTableInfo(is.r, is.InfoSchemaData, id, itm.dbID, is.ts)
	if err == nil {
		is.cache.Set(key, ret, cacheTableCost(ret))
	}
	return ret, true
}

func (is *infoschemaV2) TableByName(schema, tbl model.CIStr) (t table.Table, err error) {
	if schema.L == "information_schema" {
		return nil, errors.New("not support yet")
	}

	eq := func(a, b *Item) bool { return a.dbName == b.dbName && a.tableName == b.tableName }
	itm, ok := search(is.name2id, is.schemaVersion, Item{dbName: schema.L, tableName: tbl.L, tableID: math.MaxInt64}, eq)
	if !ok {
		// TODO: in the future, this may happen and we need to check tikv to see whether table exists.
		return nil, ErrTableNotExists.GenWithStackByArgs(schema, tbl)
	}
	// Get from the cache.
	key := cacheKey{itm.tableID, is.schemaVersion}
	res, found := is.cache.Get(key)
	if found && res != nil {
		return res.(table.Table), nil
	}

	// Maybe the table is evicted? need to reload.
	ret, err := loadTableInfo(is.r, is.InfoSchemaData, itm.tableID, itm.dbID, is.ts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	is.cache.Set(key, ret, cacheTableCost(ret))
	return ret, nil
}

func (is *infoschemaV2) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	var dbInfo model.DBInfo
	dbInfo.Name = schema
	is.schemaMap.Descend(schemaItem{dbInfo: &dbInfo, schemaVersion: math.MaxInt64}, func(item schemaItem) bool {
		if item.Name() != schema.L {
			ok = false
			return false
		}
		// fmt.Println("schema by name .... search ==", item.dbInfo.Name.L, item.schemaVersion, is.schemaVersion)
		if item.schemaVersion <= is.schemaVersion {
			ok = true
			val = item.dbInfo
			return false
		}
		return true
	})
	return
}

func (is *infoschemaV2) SchemaByID(id int64) (*model.DBInfo, bool) {
	var ok bool
	var dbInfo *model.DBInfo
	is.schemaMap.Scan(func(item schemaItem) bool {
		if item.dbInfo.ID == id {
			ok = true
			dbInfo = item.dbInfo
			return false
		}
		return true
	})
	return dbInfo, ok
}

func (is *infoschemaV2) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	return is.SchemaByID(tableInfo.DBID)
}

func loadTableInfo(r autoid.Requirement, infoData *InfoSchemaData, tblID, dbID int64, ts uint64) (table.Table, error) {
	snapshot := r.Store().GetSnapshot(kv.NewVersion(ts))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewSnapshotMeta(snapshot)

	// Try to avoid repeated concurrency loading.
	res, err, _ := sf.Do(fmt.Sprintf("%d-%d", dbID, tblID), func() (any, error) {
		return m.GetTable(dbID, tblID)
	})
	if err != nil {
		// TODO???
		panic(err)
	}
	tblInfo := res.(*model.TableInfo) // TODO: it could be missing!!!

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)
	allocs := autoid.NewAllocatorsFromTblInfo(r, dbID, tblInfo)
	b := NewBuilder(r, nil, infoData) // TODO: handle cached table!!!
	ret, err := b.tableFromMeta(allocs, tblInfo)
	if err != nil {
		panic("todo, wtf")
	}
	return ret, nil
}

var sf = &singleflight.Group{}

func cacheTableCost(t table.Table) int64 {
	info := t.Meta()
	pi := info.GetPartitionInfo()
	if pi == nil {
		return 1
	}
	return int64(len(pi.Definitions))
}

func isTableVirtual(id int64) bool {
	return (id & (1 << 62)) > 0
}
