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

package context

import (
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// MetaOnlyInfoSchema is a workaround.
// Due to circular dependency cannot return the complete interface.
// But MetaOnlyInfoSchema is widely used for scenes that require meta only, so we give a convenience for that.
type MetaOnlyInfoSchema interface {
	SchemaMetaVersion() int64
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	SchemaExists(schema model.CIStr) bool
	TableInfoByName(schema, table model.CIStr) (*model.TableInfo, error)
	TableInfoByID(id int64) (*model.TableInfo, bool)
	FindTableInfoByPartitionID(partitionID int64) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition)
	TableExists(schema, table model.CIStr) bool
	SchemaByID(id int64) (*model.DBInfo, bool)
	AllSchemas() []*model.DBInfo
	AllSchemaNames() []model.CIStr
	SchemaTableInfos(schema model.CIStr) []*model.TableInfo
	Misc
}

// Misc contains the methods that are not closely related to InfoSchema.
type Misc interface {
	PolicyByName(name model.CIStr) (*model.PolicyInfo, bool)
	ResourceGroupByName(name model.CIStr) (*model.ResourceGroupInfo, bool)
	// PlacementBundleByPhysicalTableID is used to get a rule bundle.
	PlacementBundleByPhysicalTableID(id int64) (*placement.Bundle, bool)
	// AllPlacementBundles is used to get all placement bundles
	AllPlacementBundles() []*placement.Bundle
	// AllPlacementPolicies returns all placement policies
	AllPlacementPolicies() []*model.PolicyInfo
	// AllResourceGroups returns all resource groups
	AllResourceGroups() []*model.ResourceGroupInfo
	// HasTemporaryTable returns whether information schema has temporary table
	HasTemporaryTable() bool
	// GetTableReferredForeignKeys gets the table's ReferredFKInfo by lowercase schema and table name.
	GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo
}
