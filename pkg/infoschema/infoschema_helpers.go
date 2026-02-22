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
	stdctx "context"
	"maps"
	"sort"

	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/intest"
)


// HasAutoIncrementColumn checks whether the table has auto_increment columns, if so, return true and the column name.
func HasAutoIncrementColumn(tbInfo *model.TableInfo) (bool, string) {
	for _, col := range tbInfo.Columns {
		if mysql.HasAutoIncrementFlag(col.GetFlag()) {
			return true, col.Name.L
		}
	}
	return false, ""
}

// PolicyByName is used to find the policy.
func (is *infoSchemaMisc) PolicyByName(name ast.CIStr) (*model.PolicyInfo, bool) {
	is.policyMutex.RLock()
	defer is.policyMutex.RUnlock()
	t, r := is.policyMap[name.L]
	return t, r
}

// ResourceGroupByName is used to find the resource group.
func (is *infoSchemaMisc) ResourceGroupByName(name ast.CIStr) (*model.ResourceGroupInfo, bool) {
	is.resourceGroupMutex.RLock()
	defer is.resourceGroupMutex.RUnlock()
	t, r := is.resourceGroupMap[name.L]
	return t, r
}

// ResourceGroupByID is used to find the resource group.
func (is *infoSchemaMisc) ResourceGroupByID(id int64) (*model.ResourceGroupInfo, bool) {
	is.resourceGroupMutex.RLock()
	defer is.resourceGroupMutex.RUnlock()
	for _, v := range is.resourceGroupMap {
		if v.ID == id {
			return v, true
		}
	}
	return nil, false
}

// AllResourceGroups returns all resource groups.
func (is *infoSchemaMisc) AllResourceGroups() []*model.ResourceGroupInfo {
	is.resourceGroupMutex.RLock()
	defer is.resourceGroupMutex.RUnlock()
	groups := make([]*model.ResourceGroupInfo, 0, len(is.resourceGroupMap))
	for _, group := range is.resourceGroupMap {
		groups = append(groups, group)
	}
	return groups
}

func (is *infoSchemaMisc) CloneResourceGroups() map[string]*model.ResourceGroupInfo {
	is.resourceGroupMutex.RLock()
	defer is.resourceGroupMutex.RUnlock()
	return maps.Clone(is.resourceGroupMap)
}

// AllPlacementPolicies returns all placement policies
func (is *infoSchemaMisc) AllPlacementPolicies() []*model.PolicyInfo {
	is.policyMutex.RLock()
	defer is.policyMutex.RUnlock()
	policies := make([]*model.PolicyInfo, 0, len(is.policyMap))
	for _, policy := range is.policyMap {
		policies = append(policies, policy)
	}
	return policies
}

func (is *infoSchemaMisc) ClonePlacementPolicies() map[string]*model.PolicyInfo {
	is.policyMutex.RLock()
	defer is.policyMutex.RUnlock()
	return maps.Clone(is.policyMap)
}

func (is *infoSchemaMisc) PlacementBundleByPhysicalTableID(id int64) (*placement.Bundle, bool) {
	t, r := is.ruleBundleMap[id]
	return t, r
}

func (is *infoSchemaMisc) AllPlacementBundles() []*placement.Bundle {
	bundles := make([]*placement.Bundle, 0, len(is.ruleBundleMap))
	for _, bundle := range is.ruleBundleMap {
		bundles = append(bundles, bundle)
	}
	return bundles
}

func (is *infoSchemaMisc) setResourceGroup(resourceGroup *model.ResourceGroupInfo) {
	is.resourceGroupMutex.Lock()
	defer is.resourceGroupMutex.Unlock()
	is.resourceGroupMap[resourceGroup.Name.L] = resourceGroup
}

func (is *infoSchemaMisc) deleteResourceGroup(name string) {
	is.resourceGroupMutex.Lock()
	defer is.resourceGroupMutex.Unlock()
	delete(is.resourceGroupMap, name)
}

func (is *infoSchemaMisc) setPolicy(policy *model.PolicyInfo) {
	is.policyMutex.Lock()
	defer is.policyMutex.Unlock()
	is.policyMap[policy.Name.L] = policy
}

func (is *infoSchemaMisc) deletePolicy(name string) {
	is.policyMutex.Lock()
	defer is.policyMutex.Unlock()
	delete(is.policyMap, name)
}

func (is *infoSchema) addReferredForeignKeys(schema ast.CIStr, tbInfo *model.TableInfo) {
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		refer := SchemaAndTableName{schema: fk.RefSchema.L, table: fk.RefTable.L}
		referredFKList := is.referredForeignKeyMap[refer]
		found := false
		for _, referredFK := range referredFKList {
			if referredFK.ChildSchema.L == schema.L && referredFK.ChildTable.L == tbInfo.Name.L && referredFK.ChildFKName.L == fk.Name.L {
				referredFK.Cols = fk.RefCols
				found = true
				break
			}
		}
		if found {
			continue
		}

		newReferredFKList := make([]*model.ReferredFKInfo, 0, len(referredFKList)+1)
		newReferredFKList = append(newReferredFKList, referredFKList...)
		newReferredFKList = append(newReferredFKList, &model.ReferredFKInfo{
			Cols:        fk.RefCols,
			ChildSchema: schema,
			ChildTable:  tbInfo.Name,
			ChildFKName: fk.Name,
		})
		sort.Slice(newReferredFKList, func(i, j int) bool {
			if newReferredFKList[i].ChildSchema.L != newReferredFKList[j].ChildSchema.L {
				return newReferredFKList[i].ChildSchema.L < newReferredFKList[j].ChildSchema.L
			}
			if newReferredFKList[i].ChildTable.L != newReferredFKList[j].ChildTable.L {
				return newReferredFKList[i].ChildTable.L < newReferredFKList[j].ChildTable.L
			}
			return newReferredFKList[i].ChildFKName.L < newReferredFKList[j].ChildFKName.L
		})
		is.referredForeignKeyMap[refer] = newReferredFKList
	}
}

func (is *infoSchema) deleteReferredForeignKeys(schema ast.CIStr, tbInfo *model.TableInfo) {
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		refer := SchemaAndTableName{schema: fk.RefSchema.L, table: fk.RefTable.L}
		referredFKList := is.referredForeignKeyMap[refer]
		if len(referredFKList) == 0 {
			continue
		}
		newReferredFKList := make([]*model.ReferredFKInfo, 0, len(referredFKList)-1)
		for _, referredFK := range referredFKList {
			if referredFK.ChildSchema.L == schema.L && referredFK.ChildTable.L == tbInfo.Name.L && referredFK.ChildFKName.L == fk.Name.L {
				continue
			}
			newReferredFKList = append(newReferredFKList, referredFK)
		}
		is.referredForeignKeyMap[refer] = newReferredFKList
	}
}

// GetTableReferredForeignKeys gets the table's ReferredFKInfo by lowercase schema and table name.
func (is *infoSchema) GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo {
	name := SchemaAndTableName{schema: schema, table: table}
	return is.referredForeignKeyMap[name]
}

func (is *infoSchema) GetAutoIDRequirement() autoid.Requirement {
	return is.r
}

// SessionTables store local temporary tables
type SessionTables struct {
	// Session tables can be accessed after the db is dropped, so there needs a way to retain the DBInfo.
	// schemaTables.dbInfo will only be used when the db is dropped and it may be stale after the db is created again.
	// But it's fine because we only need its name.
	schemaMap map[string]*schemaTables
	idx2table map[int64]table.Table
}

// NewSessionTables creates a new NewSessionTables object
func NewSessionTables() *SessionTables {
	return &SessionTables{
		schemaMap: make(map[string]*schemaTables),
		idx2table: make(map[int64]table.Table),
	}
}

// TableByName get table by name
func (is *SessionTables) TableByName(ctx stdctx.Context, schema, table ast.CIStr) (table.Table, bool) {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if t, ok := tbNames.tables[table.L]; ok {
			return t, true
		}
	}
	return nil, false
}

// TableExists check if table with the name exists
func (is *SessionTables) TableExists(schema, table ast.CIStr) (ok bool) {
	_, ok = is.TableByName(stdctx.Background(), schema, table)
	return
}

// TableByID get table by table id
func (is *SessionTables) TableByID(id int64) (tbl table.Table, ok bool) {
	tbl, ok = is.idx2table[id]
	return
}

// AddTable add a table
func (is *SessionTables) AddTable(db *model.DBInfo, tbl table.Table) error {
	schemaTables := is.ensureSchema(db)
	tblMeta := tbl.Meta()
	if _, ok := schemaTables.tables[tblMeta.Name.L]; ok {
		return ErrTableExists.GenWithStackByArgs(tblMeta.Name)
	}

	if _, ok := is.idx2table[tblMeta.ID]; ok {
		return ErrTableExists.GenWithStackByArgs(tblMeta.Name)
	}
	intest.Assert(db.ID == tbl.Meta().DBID)

	schemaTables.tables[tblMeta.Name.L] = tbl
	is.idx2table[tblMeta.ID] = tbl

	return nil
}

// RemoveTable remove a table
func (is *SessionTables) RemoveTable(schema, table ast.CIStr) (exist bool) {
	tbls := is.schemaTables(schema)
	if tbls == nil {
		return false
	}

	oldTable, exist := tbls.tables[table.L]
	if !exist {
		return false
	}

	delete(tbls.tables, table.L)
	delete(is.idx2table, oldTable.Meta().ID)
	if len(tbls.tables) == 0 {
		delete(is.schemaMap, schema.L)
	}
	return true
}

// Count gets the count of the temporary tables.
func (is *SessionTables) Count() int {
	return len(is.idx2table)
}

// SchemaByID get a table's schema from the schema ID.
func (is *SessionTables) SchemaByID(id int64) (*model.DBInfo, bool) {
	for _, v := range is.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}

	return nil, false
}

func (is *SessionTables) ensureSchema(db *model.DBInfo) *schemaTables {
	if tbls, ok := is.schemaMap[db.Name.L]; ok {
		return tbls
	}

	tbls := &schemaTables{dbInfo: db, tables: make(map[string]table.Table)}
	is.schemaMap[db.Name.L] = tbls
	return tbls
}

func (is *SessionTables) schemaTables(schema ast.CIStr) *schemaTables {
	if is.schemaMap == nil {
		return nil
	}

	if tbls, ok := is.schemaMap[schema.L]; ok {
		return tbls
	}

	return nil
}

