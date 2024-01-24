package infoschema

import (
	"fmt"
	"math"

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
	dbName    string
	dbID      int64
	tableName string
	tableID   int64
	schemaTS  uint64
}

type schemaItem struct {
	schemaTS uint64
	dbInfo *model.DBInfo
}

func (si *schemaItem) Name() string {
	return si.dbInfo.Name.L
}

type InfoSchemaData struct {
	// For the TableByID API, sorted by {id, schemaTS}
	byID *btree.BTreeG[Item]
	// For the TableByName API, sorted by {name, schemaTS}
	byName *btree.BTreeG[Item]
	cache  *ristretto.Cache // {id, schemaTS} => table.Table

	// For the SchemaByName API, {dbName, schemaTS} => model.DBInfo
	schemaMap *btree.BTreeG[schemaItem]
}

type cacheKey struct {
	tableID  int64
	schemaTS uint64
}

func keyToHash(key interface{}) (uint64, uint64) {
	k := key.(cacheKey)
	return uint64(k.tableID), k.schemaTS
}

func NewInfoSchemaData() (*InfoSchemaData, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 50000,   // this means at most 10,000 items, recommand num is 10x items
		MaxCost:     5000, // if we measure one partition cost as 1, at most 10,000 partitions.
		// MaxCost:     1000000, // if we measure one partition cost as 1, at most 10,000 partitions.
		BufferItems: 64,
		KeyToHash:   keyToHash,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &InfoSchemaData{
		cache:  cache,
		byID:   btree.NewBTreeG[Item](compareByID),
		byName: btree.NewBTreeG[Item](compareByName),
		schemaMap: btree.NewBTreeG[schemaItem](compareBySchema),
	}, nil
}

func (isd *InfoSchemaData) Close() {
	isd.cache.Close()
}

func (isd *InfoSchemaData) add(item Item, tbl table.Table) {
	isd.byID.Set(item)
	isd.byName.Set(item)
	isd.cache.Set(cacheKey{item.tableID, item.schemaTS}, tbl, cacheTableCost(tbl))
}

func (isd *InfoSchemaData) addDB(schemaTS uint64, dbInfo *model.DBInfo) {
	isd.schemaMap.Set(schemaItem{schemaTS: schemaTS, dbInfo: dbInfo})
}

func compareByID(a, b Item) bool {
	if a.tableID < b.tableID {
		return true
	}
	if a.tableID > b.tableID {
		return false
	}
	return a.schemaTS < b.schemaTS
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

	return a.schemaTS < b.schemaTS
}

func compareBySchema(a, b schemaItem) bool {
	if a.Name() < b.Name() {
		return true
	}
	if a.Name() > b.Name() {
		return false
	}
	return a.schemaTS < b.schemaTS
}

type infoschemaV2 struct {
	ts uint64
	r  autoid.Requirement
	*InfoSchemaData
}

func (is *infoschemaV2) TableByID(id int64) (val table.Table, ok bool) {
	if isTableVirtual(id) {
		return nil, false
	}

	var itm Item
	// Find the first item whose schemaTS is smaller than ts.
	is.byID.Descend(Item{tableID: id, schemaTS: math.MaxUint64}, func(item Item) bool {
		if item.tableID != id {
			ok = false
			return false
		}
		if item.schemaTS <= is.ts {
			ok = true
			itm = item
			return false
		}
		return true
	})
	if !ok {
		return nil, ok
	}
	// Get from the cache.
	key := cacheKey{id, itm.schemaTS}
	tbl, found := is.cache.Get(key)
	if found && tbl != nil {
		return tbl.(table.Table), true
	}

	// Maybe the table is evicted? need to reload.
	ret := loadTableInfo(is.r, is.InfoSchemaData, id, itm.dbID, is.ts)
	is.cache.Set(key, ret, cacheTableCost(ret))
	return ret, true
}

func (is *infoschemaV2) TableByName(schema, tbl model.CIStr) (t table.Table, err error) {
	if schema.L == "information_schema" {
		return nil, errors.New("not support yet")
	}

	var ok bool
	var itm Item
	// Find the first item whose schemaTS is smaller than ts.
	is.byName.Descend(Item{dbName: schema.L, tableName: tbl.L, schemaTS: math.MaxUint64}, func(item Item) bool {
		if item.dbName != schema.L || item.tableName != tbl.L {
			ok = false
			return false
		}
		if item.schemaTS <= is.ts {
			ok = true
			itm = item
			return false
		}
		return true
	})
	if !ok {
		return nil, ErrTableNotExists.GenWithStackByArgs(schema, tbl)
	}
	// Get from the cache.
	key := cacheKey{itm.tableID, itm.schemaTS}
	res, found := is.cache.Get(key)
	if found && res != nil {
		return res.(table.Table), nil
	}

	// Maybe the table is evicted? need to reload.
	// fmt.Println("load table ===", schema, tbl)
	ret := loadTableInfo(is.r, is.InfoSchemaData, itm.tableID, itm.dbID, is.ts)
	is.cache.Set(key, ret, cacheTableCost(ret))
	return ret, nil
}

func (is *infoschemaV2) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	// Find the first item whose schemaTS is smaller than ts.
	var dbInfo model.DBInfo
	dbInfo.Name = schema
	is.schemaMap.Descend(schemaItem{dbInfo: &dbInfo, schemaTS: math.MaxUint64}, func(item schemaItem) bool {
		if item.Name() != schema.L {
			ok = false
			return false
		}
		if item.schemaTS <= is.ts {
			ok = true
			val = item.dbInfo
			return false
		}
		return true
	})
	return
}

func (is *infoschemaV2) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	var itm Item
	// Find the first item whose schemaTS is smaller than ts.
	is.byID.Descend(Item{tableID: tableInfo.ID, schemaTS: math.MaxUint64}, func(item Item) bool {
		if item.tableID != tableInfo.ID {
			ok = false
			return false
		}
		if item.schemaTS <= is.ts {
			ok = true
			itm = item
			return false
		}
		return true
	})
	if !ok {
		return nil, ok
	}

	var dbInfo model.DBInfo
	dbInfo.Name.L = itm.dbName
	is.schemaMap.Descend(schemaItem{dbInfo: &dbInfo, schemaTS: math.MaxUint64}, func(item schemaItem) bool {
		if item.Name() != itm.dbName {
			ok = false
			return false
		}
		if item.schemaTS <= is.ts {
			ok = true
			val = item.dbInfo
			return false
		}
		return true
	})
	return
}

func loadTableInfo(r autoid.Requirement, infoData *InfoSchemaData, tblID, dbID int64, ts uint64) table.Table {
	snapshot := r.Store().GetSnapshot(kv.NewVersion(ts))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewSnapshotMeta(snapshot)

	// Try to avoid repeated concurrency loading.
	res, err, _ := sf.Do(fmt.Sprintf("%d-%d", dbID, tblID), func() (interface{}, error) {
		return m.GetTable(dbID, tblID)
	})
	if err != nil {
		// TODO???
		panic(err)
	}
	tblInfo := res.(*model.TableInfo)  // TODO: it could be missing!!!

	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)
	allocs := autoid.NewAllocatorsFromTblInfo(r, dbID, tblInfo)
	b := NewBuilder(r, nil, infoData) // TODO: handle cached table!!!
	ret, err := b.tableFromMeta(allocs, tblInfo)
	if err != nil {
		panic("todo, wtf")
	}
	return ret
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
