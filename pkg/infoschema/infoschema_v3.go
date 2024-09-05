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
	"cmp"
	"context"
	"encoding/json"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
)

type InfoschemaV3 struct {
	infoV2 *infoschemaV2
	infoV1 *infoSchema
	IsV2   bool
}

func tblEqual(tbl1, tbl2 table.Table) bool {
	if tbl1 == nil && tbl2 == nil {
		return true
	}

	tbl1m := tbl1.Meta()
	save1 := tbl1m.TiFlashReplica
	tbl1m.TiFlashReplica = nil
	m1, err := json.Marshal(tbl1.Meta())
	if err != nil {
		panic(err)
	}
	tbl1m.TiFlashReplica = save1

	tbl2m := tbl2.Meta()
	save := tbl2m.TiFlashReplica
	tbl2m.TiFlashReplica = nil
	m2, err := json.Marshal(tbl2.Meta())
	if err != nil {
		panic(err)
	}
	tbl2m.TiFlashReplica = save
	if string(m1) != string(m2) {
		logutil.BgLogger().Warn("table not equal", zap.String("tbl1", string(m1)), zap.String("tbl2", string(m2)))
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
		logutil.BgLogger().Warn("table not equal", zap.String("tbl1", string(m1)), zap.String("tbl2", string(m2)))
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

func tableInfoResultEqual(t1, t2 []tableInfoResult) bool {
	t1Info := make([]*model.TableInfo, 0, len(t1))
	t2Info := make([]*model.TableInfo, 0, len(t2))
	for i := range t1 {
		t1Info = append(t1Info, t1[i].TableInfos...)
	}
	for i := range t2 {
		t2Info = append(t2Info, t2[i].TableInfos...)
	}
	slices.SortFunc(t1Info, func(i, j *model.TableInfo) int {
		return int(i.ID - j.ID)
	})
	slices.SortFunc(t2Info, func(i, j *model.TableInfo) int { return int(i.ID - j.ID) })
	for i := range t1Info {
		if !tblInfoEqual(t1Info[i], t2Info[i]) {
			return false
		}
	}
	return true
}

func (is *InfoschemaV3) TableByID(ctx context.Context, id int64) (val table.Table, ok bool) {
	tbl1, ok1 := is.infoV1.TableByID(ctx, id)
	tbl2, ok2 := is.infoV2.TableByID(ctx, id)
	if ok1 != ok2 {
		panic("inconsistent infoschema 1")
	}
	if !ok1 {
		return tbl2, ok2
	}
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 2")
	}
	return tbl2, ok1
}

func (is *InfoschemaV3) TableByName(ctx context.Context, schema, tbl model.CIStr) (t table.Table, err error) {
	tbl1, err1 := is.infoV1.TableByName(ctx, schema, tbl)
	tbl2, err2 := is.infoV2.TableByName(ctx, schema, tbl)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema 3")
	}
	if err1 != nil {
		return tbl2, err1
	}
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 4")
	}
	return tbl2, err1
}

func (is *InfoschemaV3) SchemaSimpleTableInfos(ctx context.Context, schema model.CIStr) ([]*model.TableNameInfo, error) {
	tbl1, err1 := is.infoV1.SchemaSimpleTableInfos(ctx, schema)
	tbl2, err2 := is.infoV2.SchemaSimpleTableInfos(ctx, schema)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema 5")
	}
	if len(tbl1) != len(tbl2) {
		panic("inconsistent infoschema 6")
	}
	return tbl2, err2
}

func (is *InfoschemaV3) CloneResourceGroups() map[string]*model.ResourceGroupInfo {
	is.infoV1.CloneResourceGroups()
	return is.infoV2.CloneResourceGroups()
}

func (is *InfoschemaV3) ClonePlacementPolicies() map[string]*model.PolicyInfo {
	is.infoV1.ClonePlacementPolicies()
	return is.infoV2.ClonePlacementPolicies()
}

func (is *InfoschemaV3) TableInfoByName(schema, table model.CIStr) (*model.TableInfo, error) {
	tbl1, err1 := is.infoV1.TableInfoByName(schema, table)
	tbl2, err2 := is.infoV2.TableInfoByName(schema, table)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema 7")
	}
	if err1 != nil {
		return tbl2, err1
	}
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 8")
	}
	return tbl2, err1
}

func (is *InfoschemaV3) TableInfoByID(id int64) (*model.TableInfo, bool) {
	tbl1, ok1 := is.infoV1.TableInfoByID(id)
	tbl2, ok2 := is.infoV2.TableInfoByID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema 9")
	}
	if !ok1 {
		return tbl2, ok2
	}
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 10")
	}
	return tbl2, ok1
}

func (is *InfoschemaV3) SchemaTableInfos(ctx context.Context, schema model.CIStr) ([]*model.TableInfo, error) {
	tbl1, err1 := is.infoV1.SchemaTableInfos(ctx, schema)
	tbl2, err2 := is.infoV2.SchemaTableInfos(ctx, schema)
	if !errors.ErrorEqual(err2, err1) {
		panic("inconsistent infoschema 11")
	}
	if len(tbl1) != len(tbl2) {
		panic("inconsistent infoschema 12")
	}
	slices.SortFunc(tbl1, func(i, j *model.TableInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	slices.SortFunc(tbl2, func(i, j *model.TableInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	for i := range tbl1 {
		if !tblInfoEqual(tbl1[i], tbl2[i]) {
			panic("inconsistent infoschema 13")
		}
	}
	return tbl2, err2
}

func (is *InfoschemaV3) FindTableInfoByPartitionID(
	partitionID int64,
) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition) {
	tbl1, db1, part1 := is.infoV1.FindTableInfoByPartitionID(partitionID)
	tbl2, db2, part2 := is.infoV2.FindTableInfoByPartitionID(partitionID)
	if !tblInfoEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 14")
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema 15")
	}
	if !partitionEqual(part1, part2) {
		panic("inconsistent infoschema 16")
	}
	return tbl2, db2, part2
}

func (is *InfoschemaV3) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	db1, ok1 := is.infoV1.SchemaByName(schema)
	db2, ok2 := is.infoV2.SchemaByName(schema)
	if ok1 != ok2 {
		panic("inconsistent infoschema 17")
	}
	if !ok1 {
		return db2, ok2
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema 18")
	}
	return db2, ok1
}

func (is *InfoschemaV3) AllSchemas() (schemas []*model.DBInfo) {
	db1 := is.infoV1.AllSchemas()
	db2 := is.infoV2.AllSchemas()
	if len(db1) != len(db2) {
		panic("inconsistent infoschema 19")
	}
	slices.SortFunc(db1, func(i, j *model.DBInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	slices.SortFunc(db2, func(i, j *model.DBInfo) int {
		return strings.Compare(i.Name.O, j.Name.O)
	})
	for i := range db1 {
		if !dbInfoEqual(db1[i], db2[i]) {
			panic("inconsistent infoschema 20")
		}
	}
	return db2
}

func (is *InfoschemaV3) AllSchemaNames() []model.CIStr {
	names1 := is.infoV1.AllSchemaNames()
	names2 := is.infoV2.AllSchemaNames()
	if len(names1) != len(names2) {
		panic("inconsistent infoschema 21")
	}
	slices.SortFunc(names1, func(i, j model.CIStr) int {
		return strings.Compare(i.O, j.O)
	})
	slices.SortFunc(names2, func(i, j model.CIStr) int {
		return strings.Compare(i.O, j.O)
	})
	for i := range names1 {
		if names1[i] != names2[i] {
			panic("inconsistent infoschema 22")
		}
	}
	return names2
}

func (is *InfoschemaV3) SchemaExists(schema model.CIStr) bool {
	ok1 := is.infoV1.SchemaExists(schema)
	ok2 := is.infoV2.SchemaExists(schema)
	if ok1 != ok2 {
		panic("inconsistent infoschema 23")
	}
	return ok2
}

func (is *InfoschemaV3) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo, *model.PartitionDefinition) {
	tbl1, db1, part1 := is.infoV1.FindTableByPartitionID(partitionID)
	tbl2, db2, part2 := is.infoV2.FindTableByPartitionID(partitionID)
	if !tblEqual(tbl1, tbl2) {
		panic("inconsistent infoschema 24")
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema 25")
	}
	if !partitionEqual(part1, part2) {
		panic("inconsistent infoschema 26")
	}
	return tbl2, db2, part2
}

func (is *InfoschemaV3) TableExists(schema, table model.CIStr) bool {
	ok1 := is.infoV1.TableExists(schema, table)
	ok2 := is.infoV2.TableExists(schema, table)
	if ok1 != ok2 {
		panic("inconsistent infoschema 27")
	}
	return ok2
}

func (is *InfoschemaV3) SchemaByID(id int64) (*model.DBInfo, bool) {
	db1, ok1 := is.infoV1.SchemaByID(id)
	db2, ok2 := is.infoV2.SchemaByID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema 28")
	}
	if !ok1 {
		return db2, ok2
	}
	if !dbInfoEqual(db1, db2) {
		panic("inconsistent infoschema 29")
	}
	return db2, ok1
}

func (is *InfoschemaV3) base() *infoSchema {
	return is.infoV2.base()
}

func (is *InfoschemaV3) PolicyByName(name model.CIStr) (*model.PolicyInfo, bool) {
	p1, ok1 := is.infoV1.PolicyByName(name)
	p2, ok2 := is.infoV2.PolicyByName(name)
	if ok1 != ok2 {
		panic("inconsistent infoschema 30")
	}
	if !ok1 {
		return p2, ok2
	}
	if !policyEqual(p1, p2) {
		panic("inconsistent infoschema 31")
	}
	return p2, ok1
}

func (is *InfoschemaV3) ResourceGroupByName(name model.CIStr) (*model.ResourceGroupInfo, bool) {
	r1, ok1 := is.infoV1.ResourceGroupByName(name)
	r2, ok2 := is.infoV2.ResourceGroupByName(name)
	if ok1 != ok2 {
		panic("inconsistent infoschema 32")
	}
	if !ok1 {
		return r2, ok2
	}
	if !resourceGroupEqual(r1, r2) {
		panic("inconsistent infoschema 33")
	}
	return r2, ok1
}

func (is *InfoschemaV3) PlacementBundleByPhysicalTableID(id int64) (*placement.Bundle, bool) {
	b1, ok1 := is.infoV1.PlacementBundleByPhysicalTableID(id)
	b2, ok2 := is.infoV2.PlacementBundleByPhysicalTableID(id)
	if ok1 != ok2 {
		panic("inconsistent infoschema 34")
	}
	if !ok1 {
		return b2, ok2
	}
	if !bundleEqual(b1, b2) {
		panic("inconsistent infoschema 35")
	}
	return b2, ok1
}

func (is *InfoschemaV3) AllPlacementBundles() []*placement.Bundle {
	b1 := is.infoV1.AllPlacementBundles()
	b2 := is.infoV2.AllPlacementBundles()
	if len(b1) != len(b2) {
		panic("inconsistent infoschema 36")
	}
	slices.SortFunc(b1, func(a, b *placement.Bundle) int {
		return cmp.Compare(a.ID, b.ID)
	})
	slices.SortFunc(b2, func(a, b *placement.Bundle) int {
		return cmp.Compare(a.ID, b.ID)
	})
	for i := range b1 {
		if !bundleEqual(b1[i], b2[i]) {
			panic("inconsistent infoschema 37")
		}
	}
	return b2
}

func (is *InfoschemaV3) AllPlacementPolicies() []*model.PolicyInfo {
	p1 := is.infoV1.AllPlacementPolicies()
	p2 := is.infoV2.AllPlacementPolicies()
	if len(p1) != len(p2) {
		panic("inconsistent infoschema 38")
	}
	slices.SortFunc(p1, func(a, b *model.PolicyInfo) int {
		return cmp.Compare(a.ID, b.ID)
	})
	slices.SortFunc(p2, func(a, b *model.PolicyInfo) int {
		return cmp.Compare(a.ID, b.ID)
	})
	for i := range p1 {
		if !policyEqual(p1[i], p2[i]) {
			panic("inconsistent infoschema 39")
		}
	}
	return p2
}

func (is *InfoschemaV3) AllResourceGroups() []*model.ResourceGroupInfo {
	r1 := is.infoV1.AllResourceGroups()
	r2 := is.infoV2.AllResourceGroups()
	if len(r1) != len(r2) {
		panic("inconsistent infoschema 40")
	}
	slices.SortFunc(r1, func(a, b *model.ResourceGroupInfo) int {
		return cmp.Compare(a.ID, b.ID)
	})
	slices.SortFunc(r2, func(a, b *model.ResourceGroupInfo) int {
		return cmp.Compare(a.ID, b.ID)
	})
	for i := range r1 {
		if !resourceGroupEqual(r1[i], r2[i]) {
			panic("inconsistent infoschema 41")
		}
	}
	return r2
}

func (is *InfoschemaV3) HasTemporaryTable() bool {
	ok := is.infoV1.HasTemporaryTable()
	if ok != is.infoV2.HasTemporaryTable() {
		panic("inconsistent infoschema 42")
	}
	return ok
}

func (is *InfoschemaV3) GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo {
	r1 := is.infoV1.GetTableReferredForeignKeys(schema, table)
	r2 := is.infoV2.GetTableReferredForeignKeys(schema, table)
	if len(r1) != len(r2) {
		panic("inconsistent infoschema 43")
	}
	for i := range r1 {
		if !referredFKInfoEqual(r1[i], r2[i]) {
			panic("inconsistent infoschema 44")
		}
	}
	return r2
}

func (is *InfoschemaV3) ListTablesWithSpecialAttribute(filter specialAttributeFilter) []tableInfoResult {
	rs1 := is.infoV1.ListTablesWithSpecialAttribute(filter)
	rs2 := is.infoV2.ListTablesWithSpecialAttribute(filter)
	if !tableInfoResultEqual(rs1, rs2) {
		panic("inconsistent infoschema 45")
	}
	return rs2
}

func (is *InfoschemaV3) SchemaMetaVersion() int64 {
	if is.infoV1.SchemaMetaVersion() != is.infoV2.SchemaMetaVersion() {
		panic("inconsistent infoschema 46")
	}
	return is.infoV2.SchemaMetaVersion()
}

// IsV3 tells whether an InfoSchema is v3 or not.
func IsV3(is InfoSchema) (bool, *InfoschemaV3) {
	ret, ok := is.(*InfoschemaV3)
	return ok, ret
}

func (is *InfoschemaV3) CloneAndUpdateTS(startTS uint64) *InfoschemaV3 {
	tmp := *is
	tmp.infoV2.ts = startTS
	return &tmp
}
