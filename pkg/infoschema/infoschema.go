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

package infoschema

import (
	"cmp"
	stdctx "context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mock"
)

var _ context.Misc = &infoSchemaMisc{}

type sortedTables []table.Table

func (s sortedTables) searchTable(id int64) int {
	idx := sort.Search(len(s), func(i int) bool {
		return s[i].Meta().ID >= id
	})
	if idx == len(s) || s[idx].Meta().ID != id {
		return -1
	}
	return idx
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]table.Table
}

const bucketCount = 512

type infoSchema struct {
	infoSchemaMisc
	schemaMap map[string]*schemaTables
	// schemaID2Name is a map from schema ID to schema name.
	// it should be enough to query by name only theoretically, but there are some
	// places we only have schema ID, and we check both name and id in some sanity checks.
	schemaID2Name map[int64]string

	// sortedTablesBuckets is a slice of sortedTables, a table's bucket index is (tableID % bucketCount).
	sortedTablesBuckets []sortedTables

	// referredForeignKeyMap records all table's ReferredFKInfo.
	// referredSchemaAndTableName => child SchemaAndTableAndForeignKeyName => *model.ReferredFKInfo
	referredForeignKeyMap map[SchemaAndTableName][]*model.ReferredFKInfo

	r autoid.Requirement
}

type infoSchemaMisc struct {
	// schemaMetaVersion is the version of schema, and we should check version when change schema.
	schemaMetaVersion int64

	// ruleBundleMap stores all placement rules
	ruleBundleMap map[int64]*placement.Bundle

	// policyMap stores all placement policies.
	policyMutex sync.RWMutex
	policyMap   map[string]*model.PolicyInfo

	// resourceGroupMap stores all resource groups.
	resourceGroupMutex sync.RWMutex
	resourceGroupMap   map[string]*model.ResourceGroupInfo

	// temporaryTables stores the temporary table ids
	temporaryTableIDs map[int64]struct{}
}

// SchemaAndTableName contains the lower-case schema name and table name.
type SchemaAndTableName struct {
	schema string
	table  string
}

// MockInfoSchema only serves for test.
func MockInfoSchema(tbList []*model.TableInfo) InfoSchema {
	result := newInfoSchema(nil)
	dbInfo := &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}
	dbInfo.Deprecated.Tables = tbList
	tableNames := &schemaTables{
		dbInfo: dbInfo,
		tables: make(map[string]table.Table),
	}
	result.addSchema(tableNames)
	var tableIDs map[int64]struct{}
	for _, tb := range tbList {
		intest.AssertFunc(func() bool {
			if tableIDs == nil {
				tableIDs = make(map[int64]struct{})
			}
			_, ok := tableIDs[tb.ID]
			intest.Assert(!ok)
			tableIDs[tb.ID] = struct{}{}
			return true
		})
		tb.DBID = dbInfo.ID
		tbl := table.MockTableFromMeta(tb)
		tableNames.tables[tb.Name.L] = tbl
		bucketIdx := tableBucketIdx(tb.ID)
		result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
	}
	// Add a system table.
	tables := []*model.TableInfo{
		{
			// Use a very big ID to avoid conflict with normal tables.
			ID:   9999,
			Name: ast.NewCIStr("stats_meta"),
			Columns: []*model.ColumnInfo{
				{
					State:  model.StatePublic,
					Offset: 0,
					Name:   ast.NewCIStr("a"),
					ID:     1,
				},
			},
			State: model.StatePublic,
		},
	}
	mysqlDBInfo := &model.DBInfo{ID: 2, Name: ast.NewCIStr("mysql")}
	mysqlDBInfo.Deprecated.Tables = tables
	tableNames = &schemaTables{
		dbInfo: mysqlDBInfo,
		tables: make(map[string]table.Table),
	}
	result.addSchema(tableNames)
	for _, tb := range tables {
		tb.DBID = mysqlDBInfo.ID
		tbl := table.MockTableFromMeta(tb)
		tableNames.tables[tb.Name.L] = tbl
		bucketIdx := tableBucketIdx(tb.ID)
		result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
	}
	for i := range result.sortedTablesBuckets {
		slices.SortFunc(result.sortedTablesBuckets[i], func(i, j table.Table) int {
			return cmp.Compare(i.Meta().ID, j.Meta().ID)
		})
	}
	return result
}

// MockInfoSchemaWithSchemaVer only serves for test.
func MockInfoSchemaWithSchemaVer(tbList []*model.TableInfo, schemaVer int64) InfoSchema {
	result := newInfoSchema(nil)
	dbInfo := &model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}
	dbInfo.Deprecated.Tables = tbList
	tableNames := &schemaTables{
		dbInfo: dbInfo,
		tables: make(map[string]table.Table),
	}
	result.addSchema(tableNames)
	for _, tb := range tbList {
		tb.DBID = dbInfo.ID
		tbl := table.MockTableFromMeta(tb)
		tableNames.tables[tb.Name.L] = tbl
		bucketIdx := tableBucketIdx(tb.ID)
		result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
	}
	for i := range result.sortedTablesBuckets {
		slices.SortFunc(result.sortedTablesBuckets[i], func(i, j table.Table) int {
			return cmp.Compare(i.Meta().ID, j.Meta().ID)
		})
	}
	result.schemaMetaVersion = schemaVer
	return result
}

var _ InfoSchema = (*infoSchema)(nil)

func (is *infoSchema) base() *infoSchema {
	return is
}

func newInfoSchema(r autoid.Requirement) *infoSchema {
	return &infoSchema{
		infoSchemaMisc: infoSchemaMisc{
			policyMap:        map[string]*model.PolicyInfo{},
			resourceGroupMap: map[string]*model.ResourceGroupInfo{},
			ruleBundleMap:    map[int64]*placement.Bundle{},
		},
		schemaMap:             map[string]*schemaTables{},
		schemaID2Name:         map[int64]string{},
		sortedTablesBuckets:   make([]sortedTables, bucketCount),
		referredForeignKeyMap: make(map[SchemaAndTableName][]*model.ReferredFKInfo),
		r:                     r,
	}
}

func (is *infoSchema) SchemaByName(schema ast.CIStr) (val *model.DBInfo, ok bool) {
	return is.schemaByName(schema.L)
}

func (is *infoSchema) schemaByName(name string) (val *model.DBInfo, ok bool) {
	tableNames, ok := is.schemaMap[name]
	if !ok {
		return
	}
	return tableNames.dbInfo, true
}

func (is *infoSchema) SchemaExists(schema ast.CIStr) bool {
	_, ok := is.schemaMap[schema.L]
	return ok
}

func (is *infoSchema) TableByName(ctx stdctx.Context, schema, table ast.CIStr) (t table.Table, err error) {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if t, ok = tbNames.tables[table.L]; ok {
			return
		}
	}
	return nil, ErrTableNotExists.FastGenByArgs(schema, table)
}

// TableInfoByName implements InfoSchema.TableInfoByName
func (is *infoSchema) TableInfoByName(schema, table ast.CIStr) (*model.TableInfo, error) {
	tbl, err := is.TableByName(stdctx.Background(), schema, table)
	return getTableInfo(tbl), err
}

// TableIsView indicates whether the schema.table is a view.
func TableIsView(is InfoSchema, schema, table ast.CIStr) bool {
	tbl, err := is.TableByName(stdctx.Background(), schema, table)
	if err == nil {
		return tbl.Meta().IsView()
	}
	return false
}

// TableIsSequence indicates whether the schema.table is a sequence.
func TableIsSequence(is InfoSchema, schema, table ast.CIStr) bool {
	tbl, err := is.TableByName(stdctx.Background(), schema, table)
	if err == nil {
		return tbl.Meta().IsSequence()
	}
	return false
}

func (is *infoSchema) TableExists(schema, table ast.CIStr) bool {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if _, ok = tbNames.tables[table.L]; ok {
			return true
		}
	}
	return false
}

func (is *infoSchema) PolicyByID(id int64) (val *model.PolicyInfo, ok bool) {
	// TODO: use another hash map to avoid traveling on the policy map
	for _, v := range is.policyMap {
		if v.ID == id {
			return v, true
		}
	}
	return nil, false
}

func (is *infoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	name, ok := is.schemaID2Name[id]
	if !ok {
		return nil, false
	}
	return is.schemaByName(name)
}

// SchemaByTable get a table's schema name
func SchemaByTable(is InfoSchema, tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	if tableInfo == nil {
		return nil, false
	}
	if tableInfo.DBID > 0 {
		return is.SchemaByID(tableInfo.DBID)
	}
	tbl, ok := is.TableByID(stdctx.Background(), tableInfo.ID)
	if !ok {
		return nil, false
	}
	return is.SchemaByID(tbl.Meta().DBID)
}

func (is *infoSchema) TableByID(_ stdctx.Context, id int64) (val table.Table, ok bool) {
	if !tableIDIsValid(id) {
		return nil, false
	}

	slice := is.sortedTablesBuckets[tableBucketIdx(id)]
	idx := slice.searchTable(id)
	if idx == -1 {
		return nil, false
	}
	return slice[idx], true
}

// TableItemByID implements InfoSchema.TableItemByID.
func (is *infoSchema) TableItemByID(id int64) (TableItem, bool) {
	tbl, ok := is.TableByID(stdctx.Background(), id)
	if !ok {
		return TableItem{}, false
	}
	db, ok := is.SchemaByID(tbl.Meta().DBID)
	if !ok {
		return TableItem{}, false
	}
	return TableItem{DBName: db.Name, TableName: tbl.Meta().Name}, true
}

// TableInfoByID implements InfoSchema.TableInfoByID
func (is *infoSchema) TableInfoByID(id int64) (*model.TableInfo, bool) {
	tbl, ok := is.TableByID(stdctx.Background(), id)
	return getTableInfo(tbl), ok
}

// FindTableInfoByPartitionID implements InfoSchema.FindTableInfoByPartitionID
func (is *infoSchema) FindTableInfoByPartitionID(
	partitionID int64,
) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition) {
	tbl, db, partDef := is.FindTableByPartitionID(partitionID)
	return getTableInfo(tbl), db, partDef
}

// SchemaTableInfos implements MetaOnlyInfoSchema.
func (is *infoSchema) SchemaTableInfos(ctx stdctx.Context, schema ast.CIStr) ([]*model.TableInfo, error) {
	schemaTables, ok := is.schemaMap[schema.L]
	if !ok {
		return nil, nil
	}
	tables := make([]*model.TableInfo, 0, len(schemaTables.tables))
	for _, tbl := range schemaTables.tables {
		tables = append(tables, tbl.Meta())
	}
	return tables, nil
}

// SchemaSimpleTableInfos implements MetaOnlyInfoSchema.
func (is *infoSchema) SchemaSimpleTableInfos(ctx stdctx.Context, schema ast.CIStr) ([]*model.TableNameInfo, error) {
	schemaTables, ok := is.schemaMap[schema.L]
	if !ok {
		return nil, nil
	}
	ret := make([]*model.TableNameInfo, 0, len(schemaTables.tables))
	for _, t := range schemaTables.tables {
		ret = append(ret, &model.TableNameInfo{
			ID:   t.Meta().ID,
			Name: t.Meta().Name,
		})
	}
	return ret, nil
}

func (is *infoSchema) ListTablesWithSpecialAttribute(filter context.SpecialAttributeFilter) []context.TableInfoResult {
	ret := make([]context.TableInfoResult, 0, 10)
	for _, dbName := range is.AllSchemaNames() {
		res := context.TableInfoResult{DBName: dbName}
		tblInfos, err := is.SchemaTableInfos(stdctx.Background(), dbName)
		terror.Log(err)
		for _, tblInfo := range tblInfos {
			if !filter(tblInfo) {
				continue
			}
			res.TableInfos = append(res.TableInfos, tblInfo)
		}
		ret = append(ret, res)
	}
	return ret
}

// AllSchemaNames returns all the schemas' names.
func AllSchemaNames(is InfoSchema) (names []string) {
	schemas := is.AllSchemaNames()
	for _, v := range schemas {
		names = append(names, v.O)
	}
	return
}

func (is *infoSchema) AllSchemas() (schemas []*model.DBInfo) {
	for _, v := range is.schemaMap {
		schemas = append(schemas, v.dbInfo)
	}
	return
}

func (is *infoSchema) AllSchemaNames() (schemas []ast.CIStr) {
	rs := make([]ast.CIStr, 0, len(is.schemaMap))
	for _, v := range is.schemaMap {
		rs = append(rs, v.dbInfo.Name)
	}
	return rs
}

func (is *infoSchema) TableItemByPartitionID(partitionID int64) (TableItem, bool) {
	tbl, db, _ := is.FindTableByPartitionID(partitionID)
	if tbl == nil {
		return TableItem{}, false
	}
	return TableItem{DBName: db.Name, TableName: tbl.Meta().Name}, true
}

// TableIDByPartitionID implements InfoSchema.TableIDByPartitionID.
func (is *infoSchema) TableIDByPartitionID(partitionID int64) (tableID int64, ok bool) {
	tbl, _, _ := is.FindTableByPartitionID(partitionID)
	if tbl == nil {
		return
	}
	return tbl.Meta().ID, true
}

// FindTableByPartitionID finds the partition-table info by the partitionID.
// FindTableByPartitionID will traverse all the tables to find the partitionID partition in which partition-table.
func (is *infoSchema) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	for _, v := range is.schemaMap {
		for _, tbl := range v.tables {
			pi := tbl.Meta().GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, p := range pi.Definitions {
				if p.ID == partitionID {
					return tbl, v.dbInfo, &p
				}
			}
		}
	}
	return nil, nil, nil
}

// addSchema is used to add a schema to the infoSchema, it will overwrite the old
// one if it already exists.
func (is *infoSchema) addSchema(st *schemaTables) {
	is.schemaMap[st.dbInfo.Name.L] = st
	is.schemaID2Name[st.dbInfo.ID] = st.dbInfo.Name.L
}

func (is *infoSchema) delSchema(di *model.DBInfo) {
	delete(is.schemaMap, di.Name.L)
	delete(is.schemaID2Name, di.ID)
}

// HasTemporaryTable returns whether information schema has temporary table
func (is *infoSchemaMisc) HasTemporaryTable() bool {
	return len(is.temporaryTableIDs) != 0
}

func (is *infoSchemaMisc) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

// GetSequenceByName gets the sequence by name.
func GetSequenceByName(is InfoSchema, schema, sequence ast.CIStr) (util.SequenceTable, error) {
	tbl, err := is.TableByName(stdctx.Background(), schema, sequence)
	if err != nil {
		return nil, err
	}
	if !tbl.Meta().IsSequence() {
		return nil, ErrWrongObject.GenWithStackByArgs(schema, sequence, "SEQUENCE")
	}
	return tbl.(util.SequenceTable), nil
}

func init() {
	// Initialize the information shema database and register the driver to `drivers`
	dbID := autoid.InformationSchemaDBID
	infoSchemaTables := make([]*model.TableInfo, 0, len(tableNameToColumns))
	for name, cols := range tableNameToColumns {
		tableInfo := buildTableMeta(name, cols)
		tableInfo.DBID = dbID
		infoSchemaTables = append(infoSchemaTables, tableInfo)
		var ok bool
		tableInfo.ID, ok = tableIDMap[tableInfo.Name.O]
		if !ok {
			panic(fmt.Sprintf("get information_schema table id failed, unknown system table `%v`", tableInfo.Name.O))
		}
		for i, c := range tableInfo.Columns {
			c.ID = int64(i) + 1
		}
		tableInfo.MaxColumnID = int64(len(tableInfo.Columns))
		tableInfo.MaxIndexID = int64(len(tableInfo.Indices))
	}
	infoSchemaDB := &model.DBInfo{
		ID:      dbID,
		Name:    metadef.InformationSchemaName,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
	infoSchemaDB.Deprecated.Tables = infoSchemaTables
	RegisterVirtualTable(infoSchemaDB, createInfoSchemaTable)
	util.GetSequenceByName = func(is context.MetaOnlyInfoSchema, schema, sequence ast.CIStr) (util.SequenceTable, error) {
		return GetSequenceByName(is.(InfoSchema), schema, sequence)
	}
	mock.MockInfoschema = func(tbList []*model.TableInfo) context.MetaOnlyInfoSchema {
		return MockInfoSchema(tbList)
	}
}
