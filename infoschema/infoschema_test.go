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
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	// Make sure it calls perfschema.Init().
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()

	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	noexist := model.NewCIStr("noexist")

	colID, err := genGlobalID(store)
	c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)
	tblInfo := &model.TableInfo{
		ID:      tbID,
		Name:    tbName,
		Columns: []*model.ColumnInfo{colInfo},
		Indices: []*model.IndexInfo{idxInfo},
		State:   model.StatePublic,
	}

	dbID, err := genGlobalID(store)
	c.Assert(err, IsNil)
	dbInfo := &model.DBInfo{
		ID:     dbID,
		Name:   dbName,
		Tables: []*model.TableInfo{tblInfo},
		State:  model.StatePublic,
	}

	dbInfos := []*model.DBInfo{dbInfo}
	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		c.Assert(err, IsNil)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)

	builder, err := infoschema.NewBuilder(dom.Store()).InitWithDBInfos(dbInfos, nil, 1)
	c.Assert(err, IsNil)

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	checkApplyCreateNonExistsSchemaDoesNotPanic(c, txn, builder)
	checkApplyCreateNonExistsTableDoesNotPanic(c, txn, builder, dbID)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	is := builder.Build()

	schemaNames := is.AllSchemaNames()
	c.Assert(schemaNames, HasLen, 4)
	c.Assert(testutil.CompareUnorderedStringSlice(schemaNames, []string{util.InformationSchemaName.O, util.MetricSchemaName.O, util.PerformanceSchemaName.O, "Test"}), IsTrue)

	schemas := is.AllSchemas()
	c.Assert(schemas, HasLen, 4)
	schemas = is.Clone()
	c.Assert(schemas, HasLen, 4)

	c.Assert(is.SchemaExists(dbName), IsTrue)
	c.Assert(is.SchemaExists(noexist), IsFalse)

	schema, ok := is.SchemaByID(dbID)
	c.Assert(ok, IsTrue)
	c.Assert(schema, NotNil)

	schema, ok = is.SchemaByID(tbID)
	c.Assert(ok, IsFalse)
	c.Assert(schema, IsNil)

	schema, ok = is.SchemaByName(dbName)
	c.Assert(ok, IsTrue)
	c.Assert(schema, NotNil)

	schema, ok = is.SchemaByName(noexist)
	c.Assert(ok, IsFalse)
	c.Assert(schema, IsNil)

	schema, ok = is.SchemaByTable(tblInfo)
	c.Assert(ok, IsTrue)
	c.Assert(schema, NotNil)

	noexistTblInfo := &model.TableInfo{ID: 12345, Name: tblInfo.Name}
	schema, ok = is.SchemaByTable(noexistTblInfo)
	c.Assert(ok, IsFalse)
	c.Assert(schema, IsNil)

	c.Assert(is.TableExists(dbName, tbName), IsTrue)
	c.Assert(is.TableExists(dbName, noexist), IsFalse)
	c.Assert(is.TableIsView(dbName, tbName), IsFalse)
	c.Assert(is.TableIsSequence(dbName, tbName), IsFalse)

	tb, ok := is.TableByID(tbID)
	c.Assert(ok, IsTrue)
	c.Assert(tb, NotNil)

	tb, ok = is.TableByID(dbID)
	c.Assert(ok, IsFalse)
	c.Assert(tb, IsNil)

	alloc, ok := is.AllocByID(tbID)
	c.Assert(ok, IsTrue)
	c.Assert(alloc, NotNil)

	tb, err = is.TableByName(dbName, tbName)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)

	_, err = is.TableByName(dbName, noexist)
	c.Assert(err, NotNil)

	tbs := is.SchemaTables(dbName)
	c.Assert(tbs, HasLen, 1)

	tbs = is.SchemaTables(noexist)
	c.Assert(tbs, HasLen, 0)

	// Make sure partitions table exists
	tb, err = is.TableByName(model.NewCIStr("information_schema"), model.NewCIStr("partitions"))
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)

	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateTableOrView(dbID, tblInfo)
		c.Assert(err, IsNil)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)
	txn, err = store.Begin()
	c.Assert(err, IsNil)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: dbID, TableID: tbID, OldSchemaID: dbID})
	c.Assert(err, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)
	is = builder.Build()
	schema, ok = is.SchemaByID(dbID)
	c.Assert(ok, IsTrue)
	c.Assert(len(schema.Tables), Equals, 1)
}

func (testSuite) TestMockInfoSchema(c *C) {
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
	c.Assert(ok, IsTrue)
	c.Assert(tbl.Meta().Name, Equals, tblName)
	c.Assert(tbl.Cols()[0].ColumnInfo, Equals, colInfo)
}

func checkApplyCreateNonExistsSchemaDoesNotPanic(c *C, txn kv.Transaction, builder *infoschema.Builder) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateSchema, SchemaID: 999})
	c.Assert(infoschema.ErrDatabaseNotExists.Equal(err), IsTrue)
}

func checkApplyCreateNonExistsTableDoesNotPanic(c *C, txn kv.Transaction, builder *infoschema.Builder, dbID int64) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: dbID, TableID: 999})
	c.Assert(infoschema.ErrTableNotExists.Equal(err), IsTrue)
}

// TestInfoTables makes sure that all tables of information_schema could be found in infoschema handle.
func (*testSuite) TestInfoTables(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	builder, err := infoschema.NewBuilder(store).InitWithDBInfos(nil, nil, 0)
	c.Assert(err, IsNil)
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
	}
	for _, t := range infoTables {
		tb, err1 := is.TableByName(util.InformationSchemaName, model.NewCIStr(t))
		c.Assert(err1, IsNil)
		c.Assert(tb, NotNil)
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

func (*testSuite) TestGetBundle(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	builder, err := infoschema.NewBuilder(store).InitWithDBInfos(nil, nil, 0)
	c.Assert(err, IsNil)
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
	c.Assert(b.Rules, DeepEquals, bundle.Rules)

	// bundle itself is cloned
	b.ID = "test"
	c.Assert(bundle.ID, Equals, placement.PDBundleID)

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
	c.Assert(b, DeepEquals, bundle)

	// bundle itself is cloned
	b.ID = "test"
	c.Assert(bundle.ID, Equals, ptID)

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
	c.Assert(b, DeepEquals, bundle)

	// bundle itself is cloned
	b.ID = "test"
	c.Assert(bundle.ID, Equals, ptID)
}

func (*testSuite) TestLocalTemporaryTables(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)

	createNewSchemaInfo := func(schemaName string) *model.DBInfo {
		schemaID, err := genGlobalID(store)
		c.Assert(err, IsNil)
		return &model.DBInfo{
			ID:    schemaID,
			Name:  model.NewCIStr(schemaName),
			State: model.StatePublic,
		}
	}

	createNewTable := func(schemaID int64, tbName string, tempType model.TempTableType) table.Table {
		colID, err := genGlobalID(store)
		c.Assert(err, IsNil)

		colInfo := &model.ColumnInfo{
			ID:        colID,
			Name:      model.NewCIStr("col1"),
			Offset:    0,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
			State:     model.StatePublic,
		}

		tbID, err := genGlobalID(store)
		c.Assert(err, IsNil)

		tblInfo := &model.TableInfo{
			ID:      tbID,
			Name:    model.NewCIStr(tbName),
			Columns: []*model.ColumnInfo{colInfo},
			Indices: []*model.IndexInfo{},
			State:   model.StatePublic,
		}

		allocs := autoid.NewAllocatorsFromTblInfo(store, schemaID, tblInfo)
		tbl, err := table.TableFromMeta(allocs, tblInfo)
		c.Assert(err, IsNil)

		return tbl
	}

	assertTableByName := func(sc *infoschema.LocalTemporaryTables, schemaName, tableName string, schema *model.DBInfo, tb table.Table) {
		got, ok := sc.TableByName(model.NewCIStr(schemaName), model.NewCIStr(tableName))
		if tb == nil {
			c.Assert(schema, IsNil)
			c.Assert(ok, IsFalse)
			c.Assert(got, IsNil)
		} else {
			c.Assert(schema, NotNil)
			c.Assert(ok, IsTrue)
			c.Assert(got, Equals, tb)
		}
	}

	assertTableExists := func(sc *infoschema.LocalTemporaryTables, schemaName, tableName string, exists bool) {
		got := sc.TableExists(model.NewCIStr(schemaName), model.NewCIStr(tableName))
		c.Assert(got, Equals, exists)
	}

	assertTableByID := func(sc *infoschema.LocalTemporaryTables, tbID int64, schema *model.DBInfo, tb table.Table) {
		got, ok := sc.TableByID(tbID)
		if tb == nil {
			c.Assert(schema, IsNil)
			c.Assert(ok, IsFalse)
			c.Assert(got, IsNil)
		} else {
			c.Assert(schema, NotNil)
			c.Assert(ok, IsTrue)
			c.Assert(got, Equals, tb)
		}
	}

	assertSchemaByTable := func(sc *infoschema.LocalTemporaryTables, schema model.CIStr, tb *model.TableInfo) {
		got, ok := sc.SchemaByTable(tb)
		if tb == nil {
			c.Assert(schema.L == "", IsTrue)
			c.Assert(got, Equals, "")
			c.Assert(ok, IsFalse)
		} else {
			c.Assert(ok, Equals, schema.L != "")
			c.Assert(schema.L, Equals, got)
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
		err = sc.AddTable(p.db.Name, p.tb)
		c.Assert(err, IsNil)
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
		assertSchemaByTable(sc, p.db.Name, p.tb.Meta())
	}

	// test add dup table
	err = sc.AddTable(db1.Name, tb11)
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)
	err = sc.AddTable(db1b.Name, tb15)
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)
	err = sc.AddTable(db1b.Name, tb11)
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)
	db1c := createNewSchemaInfo("db1")
	err = sc.AddTable(db1c.Name, createNewTable(db1c.ID, "tb1", model.TempTableLocal))
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)
	err = sc.AddTable(db1b.Name, tb11)
	c.Assert(infoschema.ErrTableExists.Equal(err), IsTrue)

	// failed add has no effect
	assertTableByName(sc, db1.Name.L, tb11.Meta().Name.L, db1, tb11)

	// delete some tables
	c.Assert(sc.RemoveTable(model.NewCIStr("db1"), model.NewCIStr("tb1")), IsTrue)
	c.Assert(sc.RemoveTable(model.NewCIStr("Db2"), model.NewCIStr("tB2")), IsTrue)
	c.Assert(sc.RemoveTable(model.NewCIStr("db1"), model.NewCIStr("tbx")), IsFalse)
	c.Assert(sc.RemoveTable(model.NewCIStr("dbx"), model.NewCIStr("tbx")), IsFalse)

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
	c.Assert(err, IsNil)

	for _, id := range []int64{nonExistID, tb11.Meta().ID, tb22.Meta().ID} {
		assertTableByID(sc, id, nil, nil)
	}

	// test non exist table schemaByTable
	assertSchemaByTable(sc, model.NewCIStr(""), tb11.Meta())
	assertSchemaByTable(sc, model.NewCIStr(""), tb22.Meta())
	assertSchemaByTable(sc, model.NewCIStr(""), nil)

	// test TemporaryTableAttachedInfoSchema
	dbTest := createNewSchemaInfo("test")
	tmpTbTestA := createNewTable(dbTest.ID, "tba", model.TempTableLocal)
	normalTbTestA := createNewTable(dbTest.ID, "tba", model.TempTableNone)
	normalTbTestB := createNewTable(dbTest.ID, "tbb", model.TempTableNone)

	is := &infoschema.TemporaryTableAttachedInfoSchema{
		InfoSchema:           infoschema.MockInfoSchema([]*model.TableInfo{normalTbTestA.Meta(), normalTbTestB.Meta()}),
		LocalTemporaryTables: sc,
	}

	err = sc.AddTable(dbTest.Name, tmpTbTestA)
	c.Assert(err, IsNil)

	// test TableByName
	tbl, err := is.TableByName(dbTest.Name, normalTbTestA.Meta().Name)
	c.Assert(err, IsNil)
	c.Assert(tbl, Equals, tmpTbTestA)
	tbl, err = is.TableByName(dbTest.Name, normalTbTestB.Meta().Name)
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta(), Equals, normalTbTestB.Meta())
	tbl, err = is.TableByName(db1.Name, tb11.Meta().Name)
	c.Assert(infoschema.ErrTableNotExists.Equal(err), IsTrue)
	c.Assert(tbl, IsNil)
	tbl, err = is.TableByName(db1.Name, tb12.Meta().Name)
	c.Assert(err, IsNil)
	c.Assert(tbl, Equals, tb12)

	// test TableByID
	tbl, ok := is.TableByID(normalTbTestA.Meta().ID)
	c.Assert(ok, IsTrue)
	c.Assert(tbl.Meta(), Equals, normalTbTestA.Meta())
	tbl, ok = is.TableByID(normalTbTestB.Meta().ID)
	c.Assert(ok, IsTrue)
	c.Assert(tbl.Meta(), Equals, normalTbTestB.Meta())
	tbl, ok = is.TableByID(tmpTbTestA.Meta().ID)
	c.Assert(ok, IsTrue)
	c.Assert(tbl, Equals, tmpTbTestA)
	tbl, ok = is.TableByID(tb12.Meta().ID)
	c.Assert(ok, IsTrue)
	c.Assert(tbl, Equals, tb12)

	// test SchemaByTable
	info, ok := is.SchemaByTable(normalTbTestA.Meta())
	c.Assert(ok, IsTrue)
	c.Assert(info.Name.L, Equals, dbTest.Name.L)
	info, ok = is.SchemaByTable(normalTbTestB.Meta())
	c.Assert(ok, IsTrue)
	c.Assert(info.Name.L, Equals, dbTest.Name.L)
	info, ok = is.SchemaByTable(tmpTbTestA.Meta())
	c.Assert(ok, IsTrue)
	c.Assert(info.Name.L, Equals, dbTest.Name.L)
	info, ok = is.SchemaByTable(tb12.Meta())
	c.Assert(ok, IsFalse)
	c.Assert(info, IsNil)
}
