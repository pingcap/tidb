// Copyright 2024 PingCAP, Inc.
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
	"encoding/json"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
)

type infoschemaV3 struct {
	infoV2 *infoschemaV2
	infoV1 *infoSchema
}

func tblEqual(tbl1, tbl2 table.Table) bool {
	if tbl1 == nil && tbl2 == nil {
		return true
	}
	m1, err := json.Marshal(tbl1.Meta())
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(tbl2.Meta())
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func tblInfoEqual(tbl1, tbl2 *model.TableInfo) bool {
	if tbl1 == nil && tbl2 == nil {
		return true
	}
	m1, err := json.Marshal(tbl1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(tbl2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func dbInfoEqual(db1, db2 *model.DBInfo) bool {
	if db1 == nil && db2 == nil {
		return true
	}
	m1, err := json.Marshal(db1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(db2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func partitionEqual(p1, p2 *model.PartitionDefinition) bool {
	if p1 == nil && p2 == nil {
		return true
	}
	m1, err := json.Marshal(p1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(p2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func policyEqual(p1, p2 *model.PolicyInfo) bool {
	if p1 == nil && p2 == nil {
		return true
	}
	m1, err := json.Marshal(p1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(p2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func resourceGroupEqual(r1, r2 *model.ResourceGroupInfo) bool {
	if r1 == nil && r2 == nil {
		return true
	}
	m1, err := json.Marshal(r1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(r2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func bundleEqual(b1, b2 *placement.Bundle) bool {
	if b1 == nil && b2 == nil {
		return true
	}
	m1, err := json.Marshal(b1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(b2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func referredFKInfoEqual(r1, r2 *model.ReferredFKInfo) bool {
	if r1 == nil && r2 == nil {
		return true
	}
	m1, err := json.Marshal(r1)
	if err != nil {
		panic(err)
	}
	m2, err := json.Marshal(r2)
	if err != nil {
		panic(err)
	}
	if string(m1) != string(m2) {
		return false
	}
	return true
}

func (is *infoschemaV3) TableByID(id int64) (val table.Table, ok bool) {
	tbl1, ok1 := is.infoV1.TableByID(id)
	tbl2, ok2 := is.infoV2.TableByID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return tbl2, ok2
	}
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	return tbl2, ok1
}

func (is *infoschemaV3) TableByName(schema, tbl model.CIStr) (t table.Table, err error) {
	tbl1, err1 := is.infoV1.TableByName(schema, tbl)
	tbl2, err2 := is.infoV2.TableByName(schema, tbl)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema")
	}
	if err1 != nil {
		return tbl2, err1
	}
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	return tbl2, err1
}

func (is *infoschemaV3) TableInfoByName(schema, table model.CIStr) (*model.TableInfo, error) {
	tbl1, err1 := is.infoV1.TableInfoByName(schema, table)
	tbl2, err2 := is.infoV2.TableInfoByName(schema, table)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema")
	}
	if err1 != nil {
		return tbl2, err1
	}
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	return tbl2, err1
}

func (is *infoschemaV3) TableInfoByID(id int64) (*model.TableInfo, bool) {
	tbl1, ok1 := is.infoV1.TableInfoByID(id)
	tbl2, ok2 := is.infoV2.TableInfoByID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return tbl2, ok2
	}
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	return tbl2, ok1
}

func (is *infoschemaV3) SchemaTables(schema model.CIStr) (tables []table.Table) {
	tbl1 := is.infoV1.SchemaTables(schema)
	tbl2 := is.infoV2.SchemaTables(schema)
	if len(tbl1) != len(tbl2) {
		panic("inconsistent infoschema")
	}
	slices.SortFunc(tbl1, func(i, j table.Table) int {
		return strings.Compare(i.Meta().Name.O, j.Meta().Name.O)
	})
	slices.SortFunc(tbl2, func(i, j table.Table) int {
		return strings.Compare(i.Meta().Name.O, j.Meta().Name.O)
	})
	for i := range tbl1 {
		if !tblEqual(tbl1[i], tbl2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return tbl2
}

func (is *infoschemaV3) SchemaTableInfos(schema model.CIStr) []*model.TableInfo {
	tbl1 := is.infoV1.SchemaTableInfos(schema)
	tbl2 := is.infoV2.SchemaTableInfos(schema)
	if len(tbl1) != len(tbl2) {
		panic("inconsistent infoschema")
	}
	slices.SortFunc(tbl1, func(i, j *model.TableInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	slices.SortFunc(tbl2, func(i, j *model.TableInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	for i := range tbl1 {
		if !tblInfoEqual(tbl1[i], tbl2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return tbl2
}

func (is *infoschemaV3) FindTableInfoByPartitionID(
	partitionID int64,
) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition) {
	tbl1, db1, part1 := is.infoV1.FindTableInfoByPartitionID(partitionID)
	tbl2, db2, part2 := is.infoV2.FindTableInfoByPartitionID(partitionID)
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema")
	}
	if !partitionEqual(part1, part2) {
		panic("inconsistent infoschema")
	}
	return tbl2, db2, part2
}

func (is *infoschemaV3) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	db1, ok1 := is.infoV1.SchemaByName(schema)
	db2, ok2 := is.infoV2.SchemaByName(schema)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return db2, ok2
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema")
	}
	return db2, ok1
}

func (is *infoschemaV3) AllSchemas() (schemas []*model.DBInfo) {
	db1 := is.infoV1.AllSchemas()
	db2 := is.infoV2.AllSchemas()
	if len(db1) != len(db2) {
		panic("inconsistent infoschema")
	}
	slices.SortFunc(db1, func(i, j *model.DBInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	slices.SortFunc(db2, func(i, j *model.DBInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	for i := range db1 {
		if !dbInfoEqual(db1[i], db2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return db2
}

func (is *infoschemaV3) AllSchemaNames() []model.CIStr {
	names1 := is.infoV1.AllSchemaNames()
	names2 := is.infoV2.AllSchemaNames()
	if len(names1) != len(names2) {
		panic("inconsistent infoschema")
	}
	slices.SortFunc(names1, func(i, j model.CIStr) int {
		return strings.Compare(i.O, j.O)
	})
	slices.SortFunc(names2, func(i, j model.CIStr) int {
		return strings.Compare(i.O, j.O)
	})
	for i := range names1 {
		if names1[i] != names2[i] {
			panic("inconsistent infoschema")
		}
	}
	return names2
}

func (is *infoschemaV3) SchemaExists(schema model.CIStr) bool {
	ok1 := is.infoV1.SchemaExists(schema)
	ok2 := is.infoV2.SchemaExists(schema)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	return ok2
}

func (is *infoschemaV3) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	tbl1, db1, part1 := is.infoV1.FindTableByPartitionID(partitionID)
	tbl2, db2, part2 := is.infoV2.FindTableByPartitionID(partitionID)
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema")
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema")
	}
	if !partitionEqual(part1, part2) {
		panic("inconsistent infoschema")
	}
	return tbl2, db2, part2
}

func (is *infoschemaV3) TableExists(schema, table model.CIStr) bool {
	ok1 := is.infoV1.TableExists(schema, table)
	ok2 := is.infoV2.TableExists(schema, table)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	return ok2
}

func (is *infoschemaV3) SchemaByID(id int64) (*model.DBInfo, bool) {
	db1, ok1 := is.infoV1.SchemaByID(id)
	db2, ok2 := is.infoV2.SchemaByID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return db2, ok2
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema")
	}
	return db2, ok1
}

func (is *infoschemaV3) base() *infoSchema {
	return is.infoV2.base()
}

func (is *infoschemaV3) PolicyByName(name model.CIStr) (*model.PolicyInfo, bool) {
	p1, ok1 := is.infoV1.PolicyByName(name)
	p2, ok2 := is.infoV2.PolicyByName(name)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return p2, ok2
	}
	if !policyEqual(p1, p2) {
		panic("inconsistent infoschema")
	}
	return p2, ok1
}

func (is *infoschemaV3) ResourceGroupByName(name model.CIStr) (*model.ResourceGroupInfo, bool) {
	r1, ok1 := is.infoV1.ResourceGroupByName(name)
	r2, ok2 := is.infoV2.ResourceGroupByName(name)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return r2, ok2
	}
	if !resourceGroupEqual(r1, r2) {
		panic("inconsistent infoschema")
	}
	return r2, ok1
}

func (is *infoschemaV3) PlacementBundleByPhysicalTableID(id int64) (*placement.Bundle, bool) {
	b1, ok1 := is.infoV1.PlacementBundleByPhysicalTableID(id)
	b2, ok2 := is.infoV2.PlacementBundleByPhysicalTableID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema")
	}
	if !ok1 {
		return b2, ok2
	}
	if !bundleEqual(b1, b2) {
		panic("inconsistent infoschema")
	}
	return b2, ok1
}

func (is *infoschemaV3) AllPlacementBundles() []*placement.Bundle {
	b1 := is.infoV1.AllPlacementBundles()
	b2 := is.infoV2.AllPlacementBundles()
	if len(b1) != len(b2) {
		panic("inconsistent infoschema")
	}
	for i := range b1 {
		if !bundleEqual(b1[i], b2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return b2
}

func (is *infoschemaV3) AllPlacementPolicies() []*model.PolicyInfo {
	p1 := is.infoV1.AllPlacementPolicies()
	p2 := is.infoV2.AllPlacementPolicies()
	if len(p1) != len(p2) {
		panic("inconsistent infoschema")
	}
	for i := range p1 {
		if !policyEqual(p1[i], p2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return p2
}

func (is *infoschemaV3) AllResourceGroups() []*model.ResourceGroupInfo {
	r1 := is.infoV1.AllResourceGroups()
	r2 := is.infoV2.AllResourceGroups()
	if len(r1) != len(r2) {
		panic("inconsistent infoschema")
	}
	for i := range r1 {
		if !resourceGroupEqual(r1[i], r2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return r2
}

func (is *infoschemaV3) HasTemporaryTable() bool {
	ok := is.infoV1.HasTemporaryTable()
	if ok != is.infoV2.HasTemporaryTable() {
		panic("inconsistent infoschema")
	}
	return ok
}

func (is *infoschemaV3) GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo {
	r1 := is.infoV1.GetTableReferredForeignKeys(schema, table)
	r2 := is.infoV2.GetTableReferredForeignKeys(schema, table)
	if len(r1) != len(r2) {
		panic("inconsistent infoschema")
	}
	for i := range r1 {
		if !referredFKInfoEqual(r1[i], r2[i]) {
			panic("inconsistent infoschema")
		}
	}
	return r2
}

func (is *infoschemaV3) SchemaMetaVersion() int64 {
	if is.infoV1.SchemaMetaVersion() != is.infoV2.SchemaMetaVersion() {
		panic("inconsistent infoschema")
	}
	return is.infoV2.SchemaMetaVersion()
}
