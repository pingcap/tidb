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
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
)

func TestInfoSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(InfoSchemaTestSuite))
}

type InfoSchemaTestSuite struct {
	suite.Suite
}

func (s *InfoSchemaTestSuite) TestBasic() {
	store, err := mockstore.NewMockStore()
	s.Nil(err)

	defer func() {
		err := store.Close()
		s.Nil(err)
	}()

	// Make sure it calls perfschema.Init().
	dom, err := session.BootstrapSession(store)
	s.Nil(err)
	defer dom.Close()

	handle := infoschema.NewHandle(store)
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	noexist := model.NewCIStr("noexist")

	colID, err := genGlobalID(store)
	s.Nil(err)
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
	s.Nil(err)
	tblInfo := &model.TableInfo{
		ID:      tbID,
		Name:    tbName,
		Columns: []*model.ColumnInfo{colInfo},
		Indices: []*model.IndexInfo{idxInfo},
		State:   model.StatePublic,
	}

	dbID, err := genGlobalID(store)
	s.Nil(err)
	dbInfo := &model.DBInfo{
		ID:     dbID,
		Name:   dbName,
		Tables: []*model.TableInfo{tblInfo},
		State:  model.StatePublic,
	}

	dbInfos := []*model.DBInfo{dbInfo}
	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		s.Nil(err)
		return errors.Trace(err)
	})
	s.Nil(err)

	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(dbInfos, nil, 1)
	s.Nil(err)

	txn, err := store.Begin()
	s.Nil(err)
	checkApplyCreateNonExistsSchemaDoesNotPanic(s, txn, builder)
	checkApplyCreateNonExistsTableDoesNotPanic(s, txn, builder, dbID)
	err = txn.Rollback()
	s.Nil(err)

	builder.Build()
	is := handle.Get()

	schemaNames := is.AllSchemaNames()
	expectedSchemaNames := []string{
		util.InformationSchemaName.O,
		util.MetricSchemaName.O,
		util.PerformanceSchemaName.O,
		"Test",
	}
	s.Len(schemaNames, 4)
	s.ElementsMatch(schemaNames, expectedSchemaNames)

	schemas := is.AllSchemas()
	s.Len(schemas, 4)
	schemas = is.Clone()
	s.Len(schemas, 4)

	s.True(is.SchemaExists(dbName))
	s.False(is.SchemaExists(noexist))

	schema, ok := is.SchemaByID(dbID)
	s.True(ok)
	s.NotNil(schema)

	schema, ok = is.SchemaByID(tbID)
	s.False(ok)
	s.Nil(schema)

	schema, ok = is.SchemaByName(dbName)
	s.True(ok)
	s.NotNil(schema)

	schema, ok = is.SchemaByName(noexist)
	s.False(ok)
	s.Nil(schema)

	schema, ok = is.SchemaByTable(tblInfo)
	s.True(ok)
	s.NotNil(schema)

	noexistTblInfo := &model.TableInfo{ID: 12345, Name: tblInfo.Name}
	schema, ok = is.SchemaByTable(noexistTblInfo)
	s.False(ok)
	s.Nil(schema)

	s.True(is.TableExists(dbName, tbName))
	s.False(is.TableExists(dbName, noexist))
	s.False(is.TableIsView(dbName, tbName))
	s.False(is.TableIsSequence(dbName, tbName))

	tb, ok := is.TableByID(tbID)
	s.True(ok)
	s.NotNil(tb)

	tb, ok = is.TableByID(dbID)
	s.False(ok)
	s.Nil(tb)

	alloc, ok := is.AllocByID(tbID)
	s.True(ok)
	s.NotNil(alloc)

	tb, err = is.TableByName(dbName, tbName)
	s.Nil(err)
	s.NotNil(tb)

	_, err = is.TableByName(dbName, noexist)
	s.NotNil(err)

	tbs := is.SchemaTables(dbName)
	s.Len(tbs, 1)

	tbs = is.SchemaTables(noexist)
	s.Len(tbs, 0)

	// Make sure partitions table exists
	tb, err = is.TableByName(model.NewCIStr("information_schema"), model.NewCIStr("partitions"))
	s.Nil(err)
	s.NotNil(tb)

	err = kv.RunInNewTxn(context.Background(), store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateTableOrView(dbID, tblInfo)
		s.Nil(err)
		return errors.Trace(err)
	})
	s.Nil(err)
	txn, err = store.Begin()
	s.Nil(err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: dbID, TableID: tbID, OldSchemaID: dbID})
	s.Nil(err)
	err = txn.Rollback()
	s.Nil(err)
	builder.Build()
	is = handle.Get()
	schema, ok = is.SchemaByID(dbID)
	s.True(ok)
	s.Len(schema.Tables, 1)

	emptyHandle := handle.EmptyClone()
	s.Nil(emptyHandle.Get())
}

func (s *InfoSchemaTestSuite) TestMockInfoSchema() {
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
	s.True(ok)
	s.Equal(tbl.Meta().Name, tblName)
	s.Equal(tbl.Cols()[0].ColumnInfo, colInfo)
}

func checkApplyCreateNonExistsSchemaDoesNotPanic(s *InfoSchemaTestSuite, txn kv.Transaction, builder *infoschema.Builder) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateSchema, SchemaID: 999})
	s.True(infoschema.ErrDatabaseNotExists.Equal(err))
}

func checkApplyCreateNonExistsTableDoesNotPanic(s *InfoSchemaTestSuite, txn kv.Transaction, builder *infoschema.Builder, dbID int64) {
	m := meta.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: dbID, TableID: 999})
	s.True(infoschema.ErrTableNotExists.Equal(err))
}

// TestConcurrent makes sure it is safe to concurrently create handle on multiple stores.
func (s *InfoSchemaTestSuite) TestConcurrent() {
	storeCount := 5
	stores := make([]kv.Storage, storeCount)
	for i := 0; i < storeCount; i++ {
		store, err := mockstore.NewMockStore()
		s.Nil(err)
		stores[i] = store
	}
	defer func() {
		for _, store := range stores {
			err := store.Close()
			s.Nil(err)
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
func (s *InfoSchemaTestSuite) TestInfoTables() {
	store, err := mockstore.NewMockStore()
	s.Nil(err)
	defer func() {
		err := store.Close()
		s.Nil(err)
	}()
	handle := infoschema.NewHandle(store)
	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(nil, nil, 0)
	s.Nil(err)
	builder.Build()
	is := handle.Get()
	s.NotNil(is)

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
	}
	for _, t := range infoTables {
		tb, err1 := is.TableByName(util.InformationSchemaName, model.NewCIStr(t))
		s.Nil(err1)
		s.NotNil(tb)
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

func (s *InfoSchemaTestSuite) TestGetBundle() {
	store, err := mockstore.NewMockStore()
	s.Nil(err)
	defer func() {
		err := store.Close()
		s.Nil(err)
	}()

	handle := infoschema.NewHandle(store)
	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(nil, nil, 0)
	s.Nil(err)
	builder.Build()

	is := handle.Get()

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
	s.Equal(b.Rules, bundle.Rules)

	// bundle itself is cloned
	b.ID = "test"
	s.Equal(bundle.ID, placement.PDBundleID)

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
	s.Equal(b, bundle)

	// bundle itself is cloned
	b.ID = "test"
	s.Equal(bundle.ID, ptID)

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
	s.Equal(b, bundle)

	// bundle itself is cloned
	b.ID = "test"
	s.Equal(bundle.ID, ptID)
}
