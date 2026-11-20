// Copyright 2026 PingCAP, Inc.
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
	stdctx "context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
)

// OnDemandTables lists the tables that InfoCache.GetLatestWithOnDemandTables resolves besides
// the cached ones. A cross keyspace runtime only caches system tables, but analyzes user tables.
type OnDemandTables struct {
	store kv.Storage

	mu sync.RWMutex
	// listed maps the ID of a listed table to it.
	listed map[int64]*listedTable
}

// listedTable is a listed table, shared by all who listed it.
type listedTable struct {
	tbl table.Table
	db  *model.DBInfo
	// refs is the number of holders that have not released the table yet.
	refs int
}

func newOnDemandTables(store kv.Storage) *OnDemandTables {
	return &OnDemandTables{
		store:  store,
		listed: make(map[int64]*listedTable),
	}
}

// Allow lists the table of the database until release is called. It lists no table that does not
// exist, and fails while holders keep another version of the table than the latest meta has.
func (o *OnDemandTables) Allow(dbID, tableID int64) (release func(), err error) {
	loaded, err := o.load(dbID, tableID)
	if err != nil {
		return nil, err
	}
	if loaded == nil {
		return func() {}, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	l, ok := o.listed[tableID]
	if !ok {
		l = loaded
		o.listed[tableID] = l
	} else if l.tbl.Meta().Revision != loaded.tbl.Meta().Revision {
		// Every schema change of the table increases its revision, see
		// meta.Mutator.UpdateTable. The old version must stay for its holders.
		return nil, errors.Errorf("table %d has changed since it was listed, and its old version is still in use", tableID)
	}
	l.refs++
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		l.refs--
		if l.refs == 0 {
			delete(o.listed, tableID)
		}
	}, nil
}

func (o *OnDemandTables) empty() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.listed) == 0
}

// get returns the listed table, or nil if the table is not listed.
func (o *OnDemandTables) get(tableID int64) *listedTable {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.listed[tableID]
}

// all returns the listed tables.
func (o *OnDemandTables) all() []*listedTable {
	o.mu.RLock()
	defer o.mu.RUnlock()
	res := make([]*listedTable, 0, len(o.listed))
	for _, l := range o.listed {
		res = append(res, l)
	}
	return res
}

// load reads the table from the meta. It returns nil if the table or its
// database does not exist.
func (o *OnDemandTables) load(dbID, tableID int64) (*listedTable, error) {
	// The latest meta is read, not the version of the cache, which can be older: the DDL of a
	// table that the cache does not load does not wait for the cache.
	snapshot := o.store.GetSnapshot(kv.MaxVersion)
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewReader(snapshot)
	dbInfo, err := m.GetDatabase(dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if dbInfo == nil || dbInfo.State != model.StatePublic {
		return nil, nil
	}
	tblInfo, err := m.GetTable(dbID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil || tblInfo.State != model.StatePublic {
		return nil, nil
	}
	ConvertCharsetCollateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)
	// A cached table must be initialized with a session, which is not available here, before it
	// is read. Read it as a normal table: the cache only saves the reads of TiKV.
	tblInfo.TableCacheStatusType = model.TableCacheStatusDisable
	tbl, err := tables.TableFromMeta(autoid.NewAllocators(false), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &listedTable{tbl: tbl, db: dbInfo}, nil
}

// onDemandInfoSchema resolves a listed table or its database when the wrapped info schema does not.
// It overrides no TableInfo method: the SessionExtendedInfoSchema around it derives them.
type onDemandInfoSchema struct {
	InfoSchema
	tables *OnDemandTables
}

// TableByName implements InfoSchema.TableByName.
func (is *onDemandInfoSchema) TableByName(ctx stdctx.Context, schema, tbl ast.CIStr) (table.Table, error) {
	t, err := is.InfoSchema.TableByName(ctx, schema, tbl)
	if err == nil {
		return t, nil
	}
	for _, l := range is.tables.all() {
		if l.db.Name.L == schema.L && l.tbl.Meta().Name.L == tbl.L {
			return l.tbl, nil
		}
	}
	return nil, err
}

// TableByID implements InfoSchema.TableByID.
func (is *onDemandInfoSchema) TableByID(ctx stdctx.Context, id int64) (table.Table, bool) {
	if t, ok := is.InfoSchema.TableByID(ctx, id); ok {
		return t, true
	}
	if l := is.tables.get(id); l != nil {
		return l.tbl, true
	}
	return nil, false
}

// TableExists implements InfoSchema.TableExists.
func (is *onDemandInfoSchema) TableExists(schema, tbl ast.CIStr) bool {
	_, err := is.TableByName(stdctx.Background(), schema, tbl)
	return err == nil
}

// SchemaByID implements InfoSchema.SchemaByID.
func (is *onDemandInfoSchema) SchemaByID(id int64) (*model.DBInfo, bool) {
	if db, ok := is.InfoSchema.SchemaByID(id); ok {
		return db, true
	}
	for _, l := range is.tables.all() {
		if l.db.ID == id {
			return l.db, true
		}
	}
	return nil, false
}

// FindTableByPartitionID implements InfoSchema.FindTableByPartitionID.
func (is *onDemandInfoSchema) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	if t, db, def := is.InfoSchema.FindTableByPartitionID(partitionID); t != nil {
		return t, db, def
	}
	for _, l := range is.tables.all() {
		pi := l.tbl.Meta().GetPartitionInfo()
		if pi == nil {
			continue
		}
		for i := range pi.Definitions {
			if pi.Definitions[i].ID == partitionID {
				return l.tbl, l.db, &pi.Definitions[i]
			}
		}
	}
	return nil, nil, nil
}
