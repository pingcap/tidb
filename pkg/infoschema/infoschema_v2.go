package infoschema

import (
	// "runtime/debug"
	"time"
	"fmt"
	"math"
	"sync"

	"github.com/allegro/bigcache"
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

	cache tableCache

	// For the SchemaByName API
	schemaMap *btree.BTreeG[schemaItem]

	// sorted by both SchemaVersion and timestamp in descending order, assume they have same order
	mu struct {
		sync.RWMutex
		versionTimestamps []VersionAndTimestamp
	}
}

// table cache caches the {id, schemaVersion} => table.Table mapping.
// It consist of two components: the cache part and the data part.
// The cache itself only support the key => []byte mapping, it rely on the OnRemove
// finalizer to remove entity from the data part when the kv is evicted from the cache part.
// In this way it can support the general key => object mapping.
type tableCache struct {
	cache *bigcache.BigCache
	mu struct {
		sync.RWMutex
		data map[cacheKey]table.Table
	}
}

func (tc *tableCache) Init() {
	tc.mu.data = make(map[cacheKey]table.Table, 1024)
	config := bigcache.Config {
		// number of shards (must be a power of 2)
		Shards: 1024,
			// time after which entry can be evicted
			LifeWindow: 3 * time.Minute,
			// rps * lifeWindow, used only in initial memory allocation
			MaxEntriesInWindow: 1000 * 10 * 60,
			// max entry size in bytes, used only in initial memory allocation
			MaxEntrySize: 1000,
			// prints information about additional memory allocation
			Verbose: true,
			// cache will not allocate more memory than this limit, value in MB
			// if value is reached then the oldest entries can be overridden for the new ones
			// 0 value means no size limit
			HardMaxCacheSize: 64,
			// callback fired when the oldest entry is removed because of its expiration time or no space left
			// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
			// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
			OnRemove: nil,
			// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
			// for the new entry, or because delete was called. A constant representing the reason will be passed through.
			// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
			// Ignored if OnRemove is specified.
			OnRemoveWithReason: func(keyStr string, entry []byte, reason bigcache.RemoveReason) {
				tc.mu.Lock()
				defer tc.mu.Unlock()
				var key cacheKey 
				fmt.Sscanf(keyStr, "%d-%d", &key.tableID, &key.schemaVersion)
				fmt.Println("evict key ===", key.tableID, key.schemaVersion, "reason=", reason)
				delete(tc.mu.data, key)
			},
		}

	cache, initErr := bigcache.NewBigCache(config)
	if initErr != nil {
		panic(initErr)
	}
	// go func() {
	// 	for {
	// 		time.Sleep(3*time.Second)
	// 		stats := cache.Stats()
	// 		fmt.Printf("bigcache stats === %#v\n", stats)
	// 	}
	// }()
	tc.cache = cache
}

func (tc *tableCache) Close() {
	tc.cache.Close()
}

func (tc *tableCache) Set(key cacheKey, val []byte, tbl table.Table) {
	err := tc.cache.Set(fmt.Sprintf("%d-%d", key.tableID, key.schemaVersion), nil)
	if err == nil {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.mu.data[key] = tbl
	}
}

func (tc *tableCache) Get(key cacheKey) (table.Table, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	find, ok := tc.mu.data[key]
	if ok {
		return find, true
	}
	return nil, false
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

func NewInfoSchemaData() *InfoSchemaData {
	ret := &InfoSchemaData{
		byID:      btree.NewBTreeG[Item](compareByID),
		name2id:   btree.NewBTreeG[Item](compareByName),
		schemaMap: btree.NewBTreeG[schemaItem](compareBySchema),
	}
	ret.cache.Init()
	return ret
}

func (isd *InfoSchemaData) Close() {
	isd.cache.Close()
}

func (isd *InfoSchemaData) add(item Item, rawData []byte, tbl table.Table) {
	isd.byID.Set(item)
	isd.name2id.Set(item)
	isd.cache.Set(cacheKey{item.tableID, item.schemaVersion}, rawData, tbl)
}

func (isd *InfoSchemaData) addDB(schemaVersion int64, dbInfo *model.DBInfo) {
	fmt.Println("=== addDB ===", dbInfo.Name.L, schemaVersion)
	var dbInfo1 model.DBInfo
	dbInfo1 = *dbInfo
	dbInfo1.Tables = nil
	isd.schemaMap.Set(schemaItem{schemaVersion: schemaVersion, dbInfo: &dbInfo1})
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

	// fmt.Printf("TableByID(%d, %d)  %p\n", id, is.schemaVersion, is.cache)

	eq := func(a, b *Item) bool { return a.tableID == b.tableID }
	itm, ok := search(is.byID, is.schemaVersion, Item{tableID: id, dbID: math.MaxInt64}, eq)
	if !ok {
		// fmt.Printf("TableByID(%d) not found in byID\n", id)
		return nil, false
	}

	// Maybe the table is evicted? need to reload.
	ret, rawData, err := loadTableInfo(is.r, is.InfoSchemaData, id, itm.dbID, is.ts)
	if err == nil {
		is.cache.Set(key, rawData, ret)
		// fmt.Printf("update cache == %d %p\n", id, is.cache)
		// debug.PrintStack()
		return ret, true
	} else {
		fmt.Println("load table error ==", id, err)
	}
	return nil, false
}

func (is *infoschemaV2) TableByName(schema, tbl model.CIStr) (t table.Table, err error) {
	if schema.L == "information_schema" || schema.L == "metrics_schema" || schema.L == "performance_schema" {
		return nil, errors.New("not support yet")
	}

	eq := func(a, b *Item) bool { return a.dbName == b.dbName && a.tableName == b.tableName }
	itm, ok := search(is.name2id, is.schemaVersion, Item{dbName: schema.L, tableName: tbl.L, tableID: math.MaxInt64}, eq)
	if !ok {
		// TODO: in the future, this may happen and we need to check tikv to see whether table exists.
		return nil, ErrTableNotExists.GenWithStackByArgs(schema, tbl)
	}
	// Get from the cache.

	// fmt.Println("TableByName(", itm.tableID, is.schemaVersion, ")", schema.L, tbl.L)

	key := cacheKey{itm.tableID, is.schemaVersion}
	res, found := is.cache.Get(key)
	if found && res != nil {
		return res.(table.Table), nil
	}

	// Maybe the table is evicted? need to reload.
	ret, raw, err := loadTableInfo(is.r, is.InfoSchemaData, itm.tableID, itm.dbID, is.ts)
	if err != nil {
		fmt.Println("load table info error ===", itm.tableID, err)
		return nil, errors.Trace(err)
	}
	is.cache.Set(key, raw, ret)
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

// func (is *infoschemaV2) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
// 	return is.SchemaByID(tableInfo.DBID)
// }

func (is *infoschemaV2) SchemaTables(schema model.CIStr) (tables []table.Table) {
	dbInfo, ok := is.SchemaByName(schema)
	if !ok {
		fmt.Println("is this expected??")
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
		panic(err)
	}
	tables = make([]table.Table, 0, len(tblInfos))
	for _, tblInfo := range tblInfos {
		tbl, ok := is.TableByID(tblInfo.ID)
		if !ok {
			fmt.Println("what happen?")
			continue
		}
		tables = append(tables, tbl)
	}
	return
}

func loadTableInfo(r autoid.Requirement, infoData *InfoSchemaData, tblID, dbID int64, ts uint64) (table.Table, []byte, error) {
	var rawData []byte
	// Try to avoid repeated concurrency loading.
	res, err, _ := sf.Do(fmt.Sprintf("%d-%d", dbID, tblID), func() (ret any, err error) {
		fmt.Println("load table ...", tblID, dbID, ts)
		snapshot := r.Store().GetSnapshot(kv.NewVersion(ts))
		// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
		// the meta region leader is slow.
		snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
		m := meta.NewSnapshotMeta(snapshot)

		ret, rawData, err = m.GetTable(dbID, tblID)
		return
	})

	// debug.PrintStack()
	
	if err != nil {
		// TODO???
		fmt.Println("load table panic!!!", err)
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
	return ret, rawData, nil
}

var sf = &singleflight.Group{}

func isTableVirtual(id int64) bool {
	return (id & (1 << 62)) > 0
}
