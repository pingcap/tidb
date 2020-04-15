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
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/meta"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/store/mockstore"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util"
	"github.com/pingcap/tidb/v4/util/testleak"
	"github.com/pingcap/tidb/v4/util/testutil"
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
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	// Make sure it calls perfschema.Init().
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()

	handle := infoschema.NewHandle(store)
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
	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		meta.NewMeta(txn).CreateDatabase(dbInfo)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)

	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(dbInfos, 1)
	c.Assert(err, IsNil)

	txn, err := store.Begin()
	c.Assert(err, IsNil)
	checkApplyCreateNonExistsSchemaDoesNotPanic(c, txn, builder)
	checkApplyCreateNonExistsTableDoesNotPanic(c, txn, builder, dbID)
	txn.Rollback()

	builder.Build()
	is := handle.Get()

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

	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		meta.NewMeta(txn).CreateTableOrView(dbID, tblInfo)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)
	txn, err = store.Begin()
	c.Assert(err, IsNil)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: dbID, TableID: tbID, OldSchemaID: dbID})
	c.Assert(err, IsNil)
	txn.Rollback()
	builder.Build()
	is = handle.Get()
	schema, ok = is.SchemaByID(dbID)
	c.Assert(ok, IsTrue)
	c.Assert(len(schema.Tables), Equals, 1)

	emptyHandle := handle.EmptyClone()
	c.Assert(emptyHandle.Get(), IsNil)
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

// TestConcurrent makes sure it is safe to concurrently create handle on multiple stores.
func (testSuite) TestConcurrent(c *C) {
	defer testleak.AfterTest(c)()
	storeCount := 5
	stores := make([]kv.Storage, storeCount)
	for i := 0; i < storeCount; i++ {
		store, err := mockstore.NewMockTikvStore()
		c.Assert(err, IsNil)
		stores[i] = store
	}
	defer func() {
		for _, store := range stores {
			store.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(storeCount)
	for _, store := range stores {
		go func(s kv.Storage) {
			defer wg.Done()
			_ = infoschema.NewHandle(s)
		}(store)
	}
	wg.Wait()
}

// TestInfoTables makes sure that all tables of information_schema could be found in infoschema handle.
func (*testSuite) TestInfoTables(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	handle := infoschema.NewHandle(store)
	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(nil, 0)
	c.Assert(err, IsNil)
	builder.Build()
	is := handle.Get()
	c.Assert(is, NotNil)

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
	}
	for _, t := range infoTables {
		tb, err1 := is.TableByName(util.InformationSchemaName, model.NewCIStr(t))
		c.Assert(err1, IsNil)
		c.Assert(tb, NotNil)
	}
}

func genGlobalID(store kv.Storage) (int64, error) {
	var globalID int64
	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		var err error
		globalID, err = meta.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})
	return globalID, errors.Trace(err)
}
