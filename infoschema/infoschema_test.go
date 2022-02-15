// Copyright 2015 PingCAP, Inc.
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

package infoschema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testutil"
)

func TestBasic(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	// Make sure it calls perfschema.Init().
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	noexist := model.NewCIStr("noexist")

	colID, err := genGlobalID(store)
	require.NoError(t, err)
	colInfo := &model.ColumnInfo{
		ID:        colID,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}

	idxInfo := &model.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*model.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
	}

	tbID, err := genGlobalID(store)
	require.NoError(t, err)
	tblInfo := &model.TableInfo{
		ID:      tbID,
		Name:    tbName,
		Columns: []*model.ColumnInfo{colInfo},
		Indices: []*model.IndexInfo{idxInfo},
		State:   model.StatePublic,
	}

	dbID, err := genGlobalID(store)
	require.NoError(t, err)
	dbInfo := &model.DBInfo{
		ID:     dbID,
		Name:   dbName,
		Tables: []*model.TableInfo{tblInfo},
		State:  model.StatePublic,
	}

	dbInfos := []*model.DBInfo{dbInfo}
	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)

	builder, err := infoschema.NewBuilder(dom.Store(), nil).InitWithDBInfos(dbInfos, nil, nil, 1)
	require.NoError(t, err)

	txn, err := store.Begin()
	require.NoError(t, err)
	checkApplyCreateNonExistsSchemaDoesNotPanic(t, txn, builder)
	checkApplyCreateNonExistsTableDoesNotPanic(t, txn, builder, dbID)
	err = txn.Rollback()
	require.NoError(t, err)

	is := builder.Build()

	schemaNames := is.AllSchemaNames()
	require.Len(t, schemaNames, 4)
	require.True(t, testutil.CompareUnorderedStringSlice(schemaNames, []string{util.InformationSchemaName.O, util.MetricSchemaName.O, util.PerformanceSchemaName.O, "Test"}))

	schemas := is.AllSchemas()
	require.Len(t, schemas, 4)
	schemas = is.Clone()
	require.Len(t, schemas, 4)

	require.True(t, is.SchemaExists(dbName))
	require.False(t, is.SchemaExists(noexist))

	schema, ok := is.SchemaByID(dbID)
	require.True(t, ok)
	require.NotNil(t, schema)

	schema, ok = is.SchemaByID(tbID)
	require.False(t, ok)
	require.Nil(t, schema)

	schema, ok = is.SchemaByName(dbName)
	require.True(t, ok)
	require.NotNil(t, schema)

	schema, ok = is.SchemaByName(noexist)
	require.False(t, ok)
	require.Nil(t, schema)

	schema, ok = is.SchemaByTable(tblInfo)
	require.True(t, ok)
	require.NotNil(t, schema)

	noexistTblInfo := &model.TableInfo{ID: 12345, Name: tblInfo.Name}
	schema, ok = is.SchemaByTable(noexistTblInfo)
	require.False(t, ok)
	require.Nil(t, schema)

	require.True(t, is.TableExists(dbName, tbName))
	require.False(t, is.TableExists(dbName, noexist))
	require.False(t, is.TableIsView(dbName, tbName))
	require.False(t, is.TableIsSequence(dbName, tbName))

	tb, ok := is.TableByID(tbID)
	require.True(t, ok)
	require.NotNil(t, tb)

	tb, ok = is.TableByID(dbID)
	require.False(t, ok)
	require.Nil(t, tb)

	alloc, ok := is.AllocByID(tbID)
	require.True(t, ok)
	require.NotNil(t, alloc)

	tb, err = is.TableByName(dbName, tbName)
	require.NoError(t, err)
	require.NotNil(t, tb)

	_, err = is.TableByName(dbName, noexist)
	require.Error(t, err)

	tbs := is.SchemaTables(dbName)
	require.Len(t, tbs, 1)

	tbs = is.SchemaTables(noexist)
	require.Len(t, tbs, 0)

	// Make sure partitions table exists
	tb, err = is.TableByName(model.NewCIStr("information_schema"), model.NewCIStr("partitions"))
	require.NoError(t, err)
	require.NotNil(t, tb)

	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateTableOrView(dbID, tblInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: dbID, TableID: tbID, OldSchemaID: dbID})
	require.NoError(t, err)
	err = txn.Rollback()
	require.NoError(t, err)
	is = builder.Build()
	schema, ok = is.SchemaByID(dbID)
	require.True(t, ok)
	require.Equal(t, 1, len(schema.Tables))
}

func TestMockInfoSchema(t *testing.T) {
	tblID := int64(1234)
	tblName := model.NewCIStr("tbl_m")
	tableInfo := &model.TableInfo{
		ID:    tblID,
		Name:  tblName,
		State: model.StatePublic,
	}
	colInfo := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      model.NewCIStr("h"),
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        1,
	}
	tableInfo.Columns = []*model.ColumnInfo{colInfo}
	is := infoschema.MockInfoSchema([]*model.TableInfo{tableInfo})
	tbl, ok := is.TableByID(tblID)
	require.True(t, ok)
	require.Equal(t, tblName, tbl.Meta().Name)
	require.Equal(t, colInfo, tbl.Cols()[0].ColumnInfo)
}

func checkApplyCreateNonExistsSchemaDoesNotPanic(t *testing.T, txn kv.Transaction, builder *infoschema.Builder) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateSchema, SchemaID: 999})
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err))
}

func checkApplyCreateNonExistsTableDoesNotPanic(t *testing.T, txn kv.Transaction, builder *infoschema.Builder, dbID int64) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: dbID, TableID: 999})
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
}

// TestInfoTables makes sure that all tables of information_schema could be found in infoschema handle.
func TestInfoTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	builder, err := infoschema.NewBuilder(store, nil).InitWithDBInfos(nil, nil, nil, 0)
	require.NoError(t, err)
	is := builder.Build()

	infoTables := []string{
		"SCHEMATA",
		"TABLES",
		"COLUMNS",
		"STATISTICS",
		"CHARACTER_SETS",
		"COLLATIONS",
		"FILES",
		"PROFILING",
		"PARTITIONS",
		"KEY_COLUMN_USAGE",
		"REFERENTIAL_CONSTRAINTS",
		"SESSION_VARIABLES",
		"PLUGINS",
		"TABLE_CONSTRAINTS",
		"TRIGGERS",
		"USER_PRIVILEGES",
		"ENGINES",
		"VIEWS",
		"ROUTINES",
		"SCHEMA_PRIVILEGES",
		"COLUMN_PRIVILEGES",
		"TABLE_PRIVILEGES",
		"PARAMETERS",
		"EVENTS",
		"GLOBAL_STATUS",
		"GLOBAL_VARIABLES",
		"SESSION_STATUS",
		"OPTIMIZER_TRACE",
		"TABLESPACES",
		"COLLATION_CHARACTER_SET_APPLICABILITY",
		"PROCESSLIST",
		"TIDB_TRX",
		"DEADLOCKS",
		"PLACEMENT_RULES",
	}
	for _, tbl := range infoTables {
		tb, err1 := is.TableByName(util.InformationSchemaName, model.NewCIStr(tbl))
		require.Nil(t, err1)
		require.NotNil(t, tb)
	}
}

func genGlobalID(store kv.Storage) (int64, error) {
	var globalID int64
	err := kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		var err error
		globalID, err = meta.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})
	return globalID, errors.Trace(err)
}

func TestGetBundle(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	builder, err := infoschema.NewBuilder(store, nil).InitWithDBInfos(nil, nil, nil, 0)
	require.NoError(t, err)
	is := builder.Build()

	bundle := &placement.Bundle{
		ID: placement.PDBundleID,
		Rules: []*placement.Rule{
			{
				GroupID: placement.PDBundleID,
				ID:      "default",
				Role:    "voter",
				Count:   3,
			},
		},
	}
	is.SetBundle(bundle)

	b := infoschema.GetBundle(is, []int64{})
	require.Equal(t, bundle.Rules, b.Rules)

	// bundle itself is cloned
	b.ID = "test"
	require.Equal(t, placement.PDBundleID, bundle.ID)

	ptID := placement.GroupID(3)
	bundle = &placement.Bundle{
		ID: ptID,
		Rules: []*placement.Rule{
			{
				GroupID: ptID,
				ID:      "default",
				Role:    "voter",
				Count:   4,
			},
		},
	}
	is.SetBundle(bundle)

	b = infoschema.GetBundle(is, []int64{2, 3})
	require.Equal(t, bundle, b)

	// bundle itself is cloned
	b.ID = "test"
	require.Equal(t, ptID, bundle.ID)

	ptID = placement.GroupID(1)
	bundle = &placement.Bundle{
		ID: ptID,
		Rules: []*placement.Rule{
			{
				GroupID: ptID,
				ID:      "default",
				Role:    "voter",
				Count:   4,
			},
		},
	}
	is.SetBundle(bundle)

	b = infoschema.GetBundle(is, []int64{1, 2, 3})
	require.Equal(t, bundle, b)

	// bundle itself is cloned
	b.ID = "test"
	require.Equal(t, ptID, bundle.ID)
}

func TestLocalTemporaryTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	createNewSchemaInfo := func(schemaName string) *model.DBInfo {
		schemaID, err := genGlobalID(store)
		require.NoError(t, err)
		return &model.DBInfo{
			ID:    schemaID,
			Name:  model.NewCIStr(schemaName),
			State: model.StatePublic,
		}
	}

	createNewTable := func(schemaID int64, tbName string, tempType model.TempTableType) table.Table {
		colID, err := genGlobalID(store)
		require.NoError(t, err)

		colInfo := &model.ColumnInfo{
			ID:        colID,
			Name:      model.NewCIStr("col1"),
			Offset:    0,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
			State:     model.StatePublic,
		}

		tbID, err := genGlobalID(store)
		require.NoError(t, err)

		tblInfo := &model.TableInfo{
			ID:      tbID,
			Name:    model.NewCIStr(tbName),
			Columns: []*model.ColumnInfo{colInfo},
			Indices: []*model.IndexInfo{},
			State:   model.StatePublic,
		}

		allocs := autoid.NewAllocatorsFromTblInfo(store, schemaID, tblInfo)
		tbl, err := table.TableFromMeta(allocs, tblInfo)
		require.NoError(t, err)

		return tbl
	}

	assertTableByName := func(sc *infoschema.LocalTemporaryTables, schemaName, tableName string, schema *model.DBInfo, tb table.Table) {
		got, ok := sc.TableByName(model.NewCIStr(schemaName), model.NewCIStr(tableName))
		if tb == nil {
			require.Nil(t, schema)
			require.False(t, ok)
			require.Nil(t, got)
		} else {
			require.NotNil(t, schema)
			require.True(t, ok)
			require.Equal(t, tb, got)
		}
	}

	assertTableExists := func(sc *infoschema.LocalTemporaryTables, schemaName, tableName string, exists bool) {
		got := sc.TableExists(model.NewCIStr(schemaName), model.NewCIStr(tableName))
		require.Equal(t, exists, got)
	}

	assertTableByID := func(sc *infoschema.LocalTemporaryTables, tbID int64, schema *model.DBInfo, tb table.Table) {
		got, ok := sc.TableByID(tbID)
		if tb == nil {
			require.Nil(t, schema)
			require.False(t, ok)
			require.Nil(t, got)
		} else {
			require.NotNil(t, schema)
			require.True(t, ok)
			require.Equal(t, tb, got)
		}
	}

	assertSchemaByTable := func(sc *infoschema.LocalTemporaryTables, db *model.DBInfo, tb *model.TableInfo) {
		got, ok := sc.SchemaByTable(tb)
		if db == nil {
			require.Nil(t, got)
			require.False(t, ok)
		} else {
			require.NotNil(t, got)
			require.Equal(t, db.Name.L, got.Name.L)
			require.True(t, ok)
		}
	}

	sc := infoschema.NewLocalTemporaryTables()
	db1 := createNewSchemaInfo("db1")
	tb11 := createNewTable(db1.ID, "tb1", model.TempTableLocal)
	tb12 := createNewTable(db1.ID, "Tb2", model.TempTableLocal)
	tb13 := createNewTable(db1.ID, "tb3", model.TempTableLocal)

	// db1b has the same name with db1
	db1b := createNewSchemaInfo("db1")
	tb15 := createNewTable(db1b.ID, "tb5", model.TempTableLocal)
	tb16 := createNewTable(db1b.ID, "tb6", model.TempTableLocal)
	tb17 := createNewTable(db1b.ID, "tb7", model.TempTableLocal)

	db2 := createNewSchemaInfo("db2")
	tb21 := createNewTable(db2.ID, "tb1", model.TempTableLocal)
	tb22 := createNewTable(db2.ID, "TB2", model.TempTableLocal)
	tb24 := createNewTable(db2.ID, "tb4", model.TempTableLocal)

	prepareTables := []struct {
		db *model.DBInfo
		tb table.Table
	}{
		{db1, tb11}, {db1, tb12}, {db1, tb13},
		{db1b, tb15}, {db1b, tb16}, {db1b, tb17},
		{db2, tb21}, {db2, tb22}, {db2, tb24},
	}

	for _, p := range prepareTables {
		err = sc.AddTable(p.db, p.tb)
		require.NoError(t, err)
	}

	// test exist tables
	for _, p := range prepareTables {
		dbName := p.db.Name
		tbName := p.tb.Meta().Name

		assertTableByName(sc, dbName.O, tbName.O, p.db, p.tb)
		assertTableByName(sc, dbName.L, tbName.L, p.db, p.tb)
		assertTableByName(
			sc,
			strings.ToUpper(dbName.L[:1])+dbName.L[1:],
			strings.ToUpper(tbName.L[:1])+tbName.L[1:],
			p.db, p.tb,
		)

		assertTableExists(sc, dbName.O, tbName.O, true)
		assertTableExists(sc, dbName.L, tbName.L, true)
		assertTableExists(
			sc,
			strings.ToUpper(dbName.L[:1])+dbName.L[1:],
			strings.ToUpper(tbName.L[:1])+tbName.L[1:],
			true,
		)

		assertTableByID(sc, p.tb.Meta().ID, p.db, p.tb)
		assertSchemaByTable(sc, p.db, p.tb.Meta())
	}

	// test add dup table
	err = sc.AddTable(db1, tb11)
	require.True(t, infoschema.ErrTableExists.Equal(err))
	err = sc.AddTable(db1b, tb15)
	require.True(t, infoschema.ErrTableExists.Equal(err))
	err = sc.AddTable(db1b, tb11)
	require.True(t, infoschema.ErrTableExists.Equal(err))
	db1c := createNewSchemaInfo("db1")
	err = sc.AddTable(db1c, createNewTable(db1c.ID, "tb1", model.TempTableLocal))
	require.True(t, infoschema.ErrTableExists.Equal(err))
	err = sc.AddTable(db1b, tb11)
	require.True(t, infoschema.ErrTableExists.Equal(err))

	// failed add has no effect
	assertTableByName(sc, db1.Name.L, tb11.Meta().Name.L, db1, tb11)

	// delete some tables
	require.True(t, sc.RemoveTable(model.NewCIStr("db1"), model.NewCIStr("tb1")))
	require.True(t, sc.RemoveTable(model.NewCIStr("Db2"), model.NewCIStr("tB2")))
	require.False(t, sc.RemoveTable(model.NewCIStr("db1"), model.NewCIStr("tbx")))
	require.False(t, sc.RemoveTable(model.NewCIStr("dbx"), model.NewCIStr("tbx")))

	// test non exist tables by name
	for _, c := range []struct{ dbName, tbName string }{
		{"db1", "tb1"}, {"db1", "tb4"}, {"db1", "tbx"},
		{"db2", "tb2"}, {"db2", "tb3"}, {"db2", "tbx"},
		{"dbx", "tb1"},
	} {
		assertTableByName(sc, c.dbName, c.tbName, nil, nil)
		assertTableExists(sc, c.dbName, c.tbName, false)
	}

	// test non exist tables by id
	nonExistID, err := genGlobalID(store)
	require.NoError(t, err)

	for _, id := range []int64{nonExistID, tb11.Meta().ID, tb22.Meta().ID} {
		assertTableByID(sc, id, nil, nil)
	}

	// test non exist table schemaByTable
	assertSchemaByTable(sc, nil, tb11.Meta())
	assertSchemaByTable(sc, nil, tb22.Meta())
	assertSchemaByTable(sc, nil, nil)

	// test TemporaryTableAttachedInfoSchema
	dbTest := createNewSchemaInfo("test")
	tmpTbTestA := createNewTable(dbTest.ID, "tba", model.TempTableLocal)
	normalTbTestA := createNewTable(dbTest.ID, "tba", model.TempTableNone)
	normalTbTestB := createNewTable(dbTest.ID, "tbb", model.TempTableNone)
	normalTbTestC := createNewTable(db1.ID, "tbc", model.TempTableNone)

	is := &infoschema.TemporaryTableAttachedInfoSchema{
		InfoSchema:           infoschema.MockInfoSchema([]*model.TableInfo{normalTbTestA.Meta(), normalTbTestB.Meta()}),
		LocalTemporaryTables: sc,
	}

	err = sc.AddTable(dbTest, tmpTbTestA)
	require.NoError(t, err)

	// test TableByName
	tbl, err := is.TableByName(dbTest.Name, normalTbTestA.Meta().Name)
	require.NoError(t, err)
	require.Equal(t, tmpTbTestA, tbl)
	tbl, err = is.TableByName(dbTest.Name, normalTbTestB.Meta().Name)
	require.NoError(t, err)
	require.Equal(t, normalTbTestB.Meta(), tbl.Meta())
	tbl, err = is.TableByName(db1.Name, tb11.Meta().Name)
	require.True(t, infoschema.ErrTableNotExists.Equal(err))
	require.Nil(t, tbl)
	tbl, err = is.TableByName(db1.Name, tb12.Meta().Name)
	require.NoError(t, err)
	require.Equal(t, tb12, tbl)

	// test TableByID
	tbl, ok := is.TableByID(normalTbTestA.Meta().ID)
	require.True(t, ok)
	require.Equal(t, normalTbTestA.Meta(), tbl.Meta())
	tbl, ok = is.TableByID(normalTbTestB.Meta().ID)
	require.True(t, ok)
	require.Equal(t, normalTbTestB.Meta(), tbl.Meta())
	tbl, ok = is.TableByID(tmpTbTestA.Meta().ID)
	require.True(t, ok)
	require.Equal(t, tmpTbTestA, tbl)
	tbl, ok = is.TableByID(tb12.Meta().ID)
	require.True(t, ok)
	require.Equal(t, tb12, tbl)

	// test SchemaByTable
	info, ok := is.SchemaByTable(normalTbTestA.Meta())
	require.True(t, ok)
	require.Equal(t, dbTest.Name.L, info.Name.L)
	info, ok = is.SchemaByTable(normalTbTestB.Meta())
	require.True(t, ok)
	require.Equal(t, dbTest.Name.L, info.Name.L)
	info, ok = is.SchemaByTable(tmpTbTestA.Meta())
	require.True(t, ok)
	require.Equal(t, dbTest.Name.L, info.Name.L)
	// SchemaByTable also returns DBInfo when the schema is not in the infoSchema but the table is an existing tmp table.
	info, ok = is.SchemaByTable(tb12.Meta())
	require.True(t, ok)
	require.Equal(t, db1.Name.L, info.Name.L)
	// SchemaByTable returns nil when the schema is not in the infoSchema and the table is an non-existing normal table.
	info, ok = is.SchemaByTable(normalTbTestC.Meta())
	require.False(t, ok)
	require.Nil(t, info)
	// SchemaByTable returns nil when the schema is not in the infoSchema and the table is an non-existing tmp table.
	info, ok = is.SchemaByTable(tb22.Meta())
	require.False(t, ok)
	require.Nil(t, info)
}
