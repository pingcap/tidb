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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOnDemandTables(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	store := r.Store()
	defer func() {
		require.NoError(t, store.Close())
	}()
	ctx := context.Background()

	dbInfo := internal.MockDBInfo(t, store, "userdb")
	internal.AddDB(t, store, dbInfo)
	t1 := internal.MockTableInfo(t, store, "t1")
	t2 := internal.MockTableInfo(t, store, "t2")
	pt := internal.MockTableInfo(t, store, "pt")
	p0ID, err := internal.GenGlobalID(store)
	require.NoError(t, err)
	pt.Partition = &model.PartitionInfo{
		Type:        ast.PartitionTypeRange,
		Enable:      true,
		Expr:        "`a`",
		Definitions: []model.PartitionDefinition{{ID: p0ID, Name: ast.NewCIStr("p0"), LessThan: []string{"MAXVALUE"}}},
	}
	for _, tblInfo := range []*model.TableInfo{t1, t2, pt} {
		tblInfo.DBID = dbInfo.ID
		internal.AddTable(t, store, dbInfo.ID, tblInfo)
	}
	db, tbl1, tbl2 := dbInfo.Name, t1.Name, t2.Name

	// The cache knows nothing about the database, like the cache of a cross
	// keyspace runtime that only loads the system tables.
	cache := NewCache(store, 16)
	onDemand := cache.EnableOnDemandTables()
	require.True(t, cache.Insert(MockInfoSchemaWithSchemaVer(nil, 1), 1))

	// Without a listed table, the info schema is the plain one.
	is := cache.GetLatestWithOnDemandTables()
	_, wrapped := is.(*onDemandInfoSchema)
	require.False(t, wrapped)
	require.False(t, is.TableExists(db, tbl1))

	// A listed table is resolved by its name and its ID, and so is its database.
	release1, err := onDemand.Allow(dbInfo.ID, t1.ID)
	require.NoError(t, err)
	is = cache.GetLatestWithOnDemandTables()
	tbl, err := is.TableByName(ctx, db, tbl1)
	require.NoError(t, err)
	require.Equal(t, t1.ID, tbl.Meta().ID)
	tbl, ok := is.TableByID(ctx, t1.ID)
	require.True(t, ok)
	require.Equal(t, "t1", tbl.Meta().Name.L)
	require.True(t, is.TableExists(db, tbl1))
	gotDB, ok := is.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	require.Equal(t, "userdb", gotDB.Name.L)

	// The other tables of the database stay invisible, and nothing is enumerated.
	require.False(t, is.TableExists(db, tbl2))
	_, ok = is.TableByID(ctx, t2.ID)
	require.False(t, ok)
	_, _, def := is.FindTableByPartitionID(p0ID)
	require.Nil(t, def)
	require.NotContains(t, is.AllSchemaNames(), db)

	// The name of a listed table is not resolved in another database.
	require.False(t, is.TableExists(ast.NewCIStr("test"), tbl1))

	// A partitioned table is also resolved by the IDs of its partitions.
	releasePT, err := onDemand.Allow(dbInfo.ID, pt.ID)
	require.NoError(t, err)
	is = cache.GetLatestWithOnDemandTables()
	tbl, gotDB, def = is.FindTableByPartitionID(p0ID)
	require.NotNil(t, def)
	require.Equal(t, pt.ID, tbl.Meta().ID)
	require.Equal(t, dbInfo.ID, gotDB.ID)
	require.Equal(t, "p0", def.Name.L)
	releasePT()

	// A listed table stays as it was loaded. The next listing sees a schema change.
	t1.Columns = append(t1.Columns, &model.ColumnInfo{ID: 100, Name: ast.NewCIStr("b"), Offset: 1, State: model.StatePublic})
	internal.UpdateTable(t, store, dbInfo, t1)
	tbl, ok = cache.GetLatestWithOnDemandTables().TableByID(ctx, t1.ID)
	require.True(t, ok)
	require.Len(t, tbl.Meta().Columns, 1)
	// A new holder gets no version from before the change while the old one is listed.
	_, err = onDemand.Allow(dbInfo.ID, t1.ID)
	require.ErrorContains(t, err, fmt.Sprintf("table %d has changed", t1.ID))
	release1()
	release1, err = onDemand.Allow(dbInfo.ID, t1.ID)
	require.NoError(t, err)
	tbl, ok = cache.GetLatestWithOnDemandTables().TableByID(ctx, t1.ID)
	require.True(t, ok)
	require.Len(t, tbl.Meta().Columns, 2)

	// A table that is gone when it is listed is not found.
	internal.DropTable(t, store, dbInfo, t2.ID, t2.Name.O)
	release2, err := onDemand.Allow(dbInfo.ID, t2.ID)
	require.NoError(t, err)
	require.False(t, cache.GetLatestWithOnDemandTables().TableExists(db, tbl2))
	release2()

	// Whatever maintains the cache keeps seeing the plain info schema.
	require.False(t, cache.GetLatest().TableExists(db, tbl1))

	// A table stays listed until all who listed it release it.
	release1Again, err := onDemand.Allow(dbInfo.ID, t1.ID)
	require.NoError(t, err)
	release1()
	require.True(t, cache.GetLatestWithOnDemandTables().TableExists(db, tbl1))
	release1Again()
	require.False(t, cache.GetLatestWithOnDemandTables().TableExists(db, tbl1))
}

func TestOnDemandTablesConcurrency(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	store := r.Store()
	defer func() {
		require.NoError(t, store.Close())
	}()
	dbInfo := internal.MockDBInfo(t, store, "userdb")
	internal.AddDB(t, store, dbInfo)
	tblInfo := internal.MockTableInfo(t, store, "t")
	tblInfo.DBID = dbInfo.ID
	internal.AddTable(t, store, dbInfo.ID, tblInfo)
	cache := NewCache(store, 16)
	onDemand := cache.EnableOnDemandTables()
	require.True(t, cache.Insert(MockInfoSchemaWithSchemaVer(nil, 1), 1))

	// All workers list and release the same table, so they share its reference count.
	// A worker must resolve the table while it holds it, whatever the others release.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				release, err := onDemand.Allow(dbInfo.ID, tblInfo.ID)
				if !assert.NoError(t, err) {
					return
				}
				_, ok := cache.GetLatestWithOnDemandTables().TableByID(context.Background(), tblInfo.ID)
				assert.True(t, ok)
				release()
			}
		}()
	}
	wg.Wait()
}
