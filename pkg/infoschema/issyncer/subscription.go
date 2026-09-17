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

package issyncer

import (
	"context"
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// registeredTable holds an object's identity separately from its metadata location.
// schemaID may change after a cross-database rename; the registration still holds tableID.
type registeredTable struct {
	schemaID int64
	refs     int
}

// RegisterTables records table IDs without reading metadata. schemaID is their
// initial location. The returned function releases this registration.
func (s *Syncer) RegisterTables(ctx context.Context, schemaID int64, tableIDs ...int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.crossKS {
		return func() {}, nil
	}
	tableIDs = slices.Clone(tableIDs)
	s.m.Lock()
	s.changeTableSubscriptions(schemaID, tableIDs, 1)
	s.m.Unlock()
	return sync.OnceFunc(func() {
		s.m.Lock()
		defer s.m.Unlock()
		s.changeTableSubscriptions(schemaID, tableIDs, -1)
	}), nil
}

func (s *Syncer) changeTableSubscriptions(schemaID int64, tableIDs []int64, delta int) {
	l := s.loader
	if l.tableSubscriptions == nil {
		l.tableSubscriptions = make(map[int64]registeredTable)
	}
	for _, id := range tableIDs {
		entry := l.tableSubscriptions[id]
		if entry.refs == 0 {
			entry.schemaID = schemaID
		}
		if entry.refs == 0 || entry.refs+delta == 0 {
			l.subscriptionGeneration++
		}
		entry.refs += delta
		if entry.refs == 0 {
			delete(l.tableSubscriptions, id)
		} else {
			l.tableSubscriptions[id] = entry
		}
	}
	l.hasTableSubscriptions.Store(len(l.tableSubscriptions) > 0)
}

// LoadSnapshotInfoSchema resolves names and loads their definitions at ts. It
// neither registers live tables nor publishes the result to the shared cache.
func (s *Syncer) LoadSnapshotInfoSchema(ctx context.Context, names []ast.Ident, ts uint64) (infoschema.InfoSchema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.crossKS {
		is, _, _, _, err := s.LoadWithTS(ts, true)
		return is, err
	}
	loader := NewLoaderForCrossKS(s.store, infoschema.NewCache(s.store, 1))
	loader.initFields(s.loader.autoidClient, s.loader.sysExecutorFactory)
	loader.tableSubscriptions = make(map[int64]registeredTable)
	m := meta.NewReader(s.store.GetSnapshot(kv.NewVersion(ts)))
	dbs, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	byName := make(map[string]*model.DBInfo, len(dbs))
	for _, db := range dbs {
		byName[db.Name.L] = db
	}
	tablesByDB := make(map[int64]map[string]int64)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		db := byName[name.Schema.L]
		if db == nil {
			return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(name.Schema.O)
		}
		tables, ok := tablesByDB[db.ID]
		if !ok {
			tableNames, err := m.ListSimpleTables(db.ID)
			if err != nil {
				return nil, err
			}
			tables = make(map[string]int64, len(tableNames))
			for _, table := range tableNames {
				tables[table.Name.L] = table.ID
			}
			tablesByDB[db.ID] = tables
		}
		id, ok := tables[name.Name.L]
		if !ok {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(name.Schema.O, name.Name.O)
		}
		loader.tableSubscriptions[id] = registeredTable{schemaID: db.ID, refs: 1}
	}
	is, _, _, _, err := loader.LoadWithTS(ts, true)
	return is, err
}

// fetchRegisteredSchemas reads known IDs directly. If a table moved while the
// loader missed its rename diff, locate the same ID in the remaining databases.
func (l *Loader) fetchRegisteredSchemas(m meta.Reader) ([]*model.DBInfo, error) {
	dbs := make(map[int64]*model.DBInfo)
	missing := make(map[int64]struct{})
	for id, entry := range l.tableSubscriptions {
		if metadef.IsReservedID(id) {
			continue
		}
		db, known := dbs[entry.schemaID]
		if !known {
			var err error
			db, err = m.GetDatabase(entry.schemaID)
			if err != nil {
				return nil, err
			}
			dbs[entry.schemaID] = db
		}
		if db == nil {
			missing[id] = struct{}{}
			continue
		}
		tbl, err := m.GetTable(db.ID, id)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			missing[id] = struct{}{}
			continue
		}
		db.Deprecated.Tables = append(db.Deprecated.Tables, tbl)
	}
	if len(missing) > 0 {
		err := m.IterDatabases(func(db *model.DBInfo) error {
			if len(missing) == 0 || db.ID == metadef.SystemDatabaseID {
				return nil
			}
			names, err := m.ListSimpleTables(db.ID)
			if err != nil {
				return err
			}
			for _, name := range names {
				if _, ok := missing[name.ID]; !ok {
					continue
				}
				tbl, err := m.GetTable(db.ID, name.ID)
				if err != nil {
					return err
				}
				if dbs[db.ID] == nil {
					dbs[db.ID] = db
				}
				dbs[db.ID].Deprecated.Tables = append(dbs[db.ID].Deprecated.Tables, tbl)
				delete(missing, name.ID)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	result := make([]*model.DBInfo, 0, len(dbs))
	for _, db := range dbs {
		if db != nil {
			result = append(result, db)
		}
	}
	return result, nil
}
